use super::coroutine::CoroutineFuture;
use crate::bindings::sys;
use dashmap::DashMap;
use libc::c_void;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::time::Duration;
use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Semaphore};
use tokio::time::Instant;
use tokio::{
    sync::{mpsc::*, Notify},
    time::timeout_at,
};

struct TimerTask {
    deadline: Instant,
    id: u64,
    func: Option<unsafe extern "C" fn(*mut c_void)>,
    arg: *mut c_void,
    map: Arc<DashMap<u64, Arc<Notify>>>,
    tq_ptr: *mut c_void,
    handle: Option<Handle>,
}

static TASKQ_KEY: OnceCell<u32> = OnceCell::new();

impl TimerTask {
    async fn execute(self) {
        let mut coroutine = CoroutineFuture::new(self.func.unwrap(), self.arg as usize);
        let tq_key = *TASKQ_KEY.get_or_init(|| CoroutineFuture::create_key(None));
        coroutine.set_specific(tq_key, self.tq_ptr);
        coroutine.await;
        let notify = self.map.remove(&self.id).unwrap().1;
        notify.notify_one();
    }
}

unsafe impl Send for TimerTask {}
unsafe impl Sync for TimerTask {}

impl PartialEq for TimerTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TimerTask {}

impl PartialOrd for TimerTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerTask {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.deadline.cmp(&other.deadline);
        if res == Ordering::Equal {
            self.id.cmp(&other.id)
        } else {
            res
        }
    }
}

enum TimerEvent {
    Register(TimerTask),
    Unregister((u64, oneshot::Sender<()>)),
}

struct TimerManager {
    receiver: UnboundedReceiver<TimerEvent>,
    set: BTreeSet<TimerTask>,
}

struct TimerManagerHandle {
    sender: UnboundedSender<TimerEvent>,
}

const MAX_TIMER_BATCH: usize = 1024;

impl TimerManager {
    async fn serve(mut self) {
        loop {
            let mut events = Vec::with_capacity(MAX_TIMER_BATCH);
            let front = self.set.first();
            if let Some(task) = front {
                let _ = timeout_at(
                    task.deadline,
                    self.receiver.recv_many(&mut events, MAX_TIMER_BATCH),
                )
                .await;
            } else {
                self.receiver.recv_many(&mut events, MAX_TIMER_BATCH).await;
            };

            let mut unregister_ids = HashMap::new();

            for event in events {
                match event {
                    TimerEvent::Register(task) => assert!(self.set.insert(task)),
                    TimerEvent::Unregister((id, sender)) => {
                        unregister_ids.insert(id, sender);
                    }
                }
            }

            self.set.retain(|t| {
                if let Some(sender) = unregister_ids.remove(&t.id) {
                    t.map.remove(&t.id).unwrap();
                    sender.send(()).unwrap();
                    false
                } else {
                    true
                }
            });

            let now = Instant::now();
            while let Some(mut task) = self.set.pop_first() {
                if task.deadline <= now {
                    let handle = task.handle.take().unwrap();
                    handle.spawn(async move { task.execute().await });
                } else {
                    self.set.insert(task);
                    break;
                }
            }
        }
    }

    fn start() -> TimerManagerHandle {
        let (sender, receiver) = unbounded_channel();
        let manager = TimerManager {
            receiver,
            set: BTreeSet::new(),
        };
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(manager.serve());
        });

        TimerManagerHandle { sender }
    }
}

static TIMER_MANAGER_HANDLE: OnceCell<TimerManagerHandle> = OnceCell::new();

struct TaskQueue {
    cur_id: AtomicU64,
    task_map: Arc<DashMap<u64, Arc<Notify>>>,
    name: String,
    concurrency_permits: Arc<Semaphore>,
}

impl TaskQueue {
    pub(super) fn new(name: String, max_concurrency: i32) -> Box<Self> {
        Box::new(Self {
            cur_id: AtomicU64::new(1),
            task_map: Arc::new(DashMap::new()),
            name,
            concurrency_permits: Arc::new(Semaphore::new(max_concurrency as usize)),
        })
    }

    pub(super) fn dispatch(
        &self,
        func: Option<unsafe extern "C" fn(*mut c_void)>,
        arg: *mut c_void,
    ) -> u64 {
        let id = self.cur_id.fetch_add(1, Relaxed);
        let notify = Arc::new(Notify::new());
        self.task_map.insert(id, notify);
        let task_map = self.task_map.clone();
        let arg_usize = arg as usize;
        let tq_ptr_usize = self as *const _ as usize;
        let permits = self.concurrency_permits.clone();

        tokio::spawn(async move {
            let _permit = permits.acquire().await.unwrap();
            let tq_key = *TASKQ_KEY.get_or_init(|| CoroutineFuture::create_key(None));
            let mut coroutine = CoroutineFuture::new(func.unwrap(), arg_usize);
            coroutine.set_specific(tq_key, tq_ptr_usize as *mut c_void);
            coroutine.await;
            let notify = task_map.remove(&id).unwrap().1;
            notify.notify_one();
        });
        id
    }

    pub(super) async fn wait_id(&self, id: u64) {
        if let Some(notify) = self.task_map.get(&id).map(|notify| notify.clone()) {
            notify.notified().await;
        }
    }

    pub(super) fn delay_dispatch(
        &self,
        func: Option<unsafe extern "C" fn(*mut c_void)>,
        arg: *mut c_void,
        timeout: Duration,
    ) -> u64 {
        let id = self.cur_id.fetch_add(1, Relaxed);
        let id = u64::MAX - id;
        self.task_map.insert(id, Arc::new(Notify::new()));
        let deadline = Instant::now().checked_add(timeout).unwrap();
        let task = TimerTask {
            deadline,
            id,
            func,
            arg,
            map: self.task_map.clone(),
            tq_ptr: self as *const _ as *mut c_void,
            handle: Some(Handle::current()),
        };
        let manager_hdl = TIMER_MANAGER_HANDLE.get_or_init(TimerManager::start);
        manager_hdl.sender.send(TimerEvent::Register(task)).unwrap();

        id
    }

    pub(super) async fn cancel_delayed_task(&self, id: u64) -> i32 {
        assert!(id >> 63 > 0, "id: {id}, taskq_name: {:?}", self.name);
        if let Some(notify) = self.task_map.get(&id).map(|notify| notify.clone()) {
            let manager_hdl = TIMER_MANAGER_HANDLE.get().unwrap();
            let (sender, receiver) = oneshot::channel();
            manager_hdl
                .sender
                .send(TimerEvent::Unregister((id, sender)))
                .unwrap();
            if receiver.await.is_err() {
                notify.notified().await;
                libc::EBUSY
            } else {
                0
            }
        } else {
            libc::ENOENT
        }
    }

    pub(super) async fn wait(&self) {
        while !self.task_map.is_empty() {
            let notifies: Vec<_> = self.task_map.iter().map(|v| (v.clone())).collect();
            for notify in notifies {
                notify.notified().await;
            }
        }
    }
}

pub(crate) unsafe extern "C" fn taskq_create(
    tq_name: *const libc::c_char,
    nthreads: i32,
) -> *mut c_void {
    let tq_name = CStr::from_ptr(tq_name);
    let tq = TaskQueue::new(tq_name.to_string_lossy().to_string(), nthreads);
    Box::into_raw(tq) as *mut _
}

pub(crate) unsafe extern "C" fn taskq_dispatch(
    tq: *const c_void,
    func: Option<unsafe extern "C" fn(arg1: *mut c_void)>,
    arg: *mut c_void,
    _flag: u32,
) -> u64 {
    let tq = &*(tq as *const TaskQueue);
    tq.dispatch(func, arg)
}

pub(crate) unsafe extern "C" fn taskq_delay_dispatch(
    tq: *const c_void,
    func: Option<unsafe extern "C" fn(arg1: *mut c_void)>,
    arg: *mut c_void,
    timeout: *const sys::timespec,
) -> u64 {
    let timeout = &*timeout;
    let timeout = Duration::new(timeout.tv_sec as u64, timeout.tv_nsec as u32);
    let tq = &*(tq as *const TaskQueue);
    tq.delay_dispatch(func, arg, timeout)
}

pub(crate) unsafe extern "C" fn taskq_is_member(_: *const c_void, _: u64) -> i32 {
    unreachable!("taskq_is_member not supported");
}

pub(crate) unsafe extern "C" fn taskq_of_curthread() -> *mut c_void {
    let tq_key = *TASKQ_KEY.get_or_init(|| CoroutineFuture::create_key(None));
    CoroutineFuture::tls_coroutine().get_specific(tq_key)
}

pub(crate) unsafe extern "C" fn taskq_wait(tq: *const c_void) {
    let tq = &*(tq as *const TaskQueue);
    CoroutineFuture::poll_until_ready(tq.wait());
}

pub(crate) unsafe extern "C" fn taskq_destroy(tq: *const c_void) {
    let tq = Box::from_raw(tq as *mut TaskQueue);
    CoroutineFuture::poll_until_ready(tq.wait());
}

pub(crate) unsafe extern "C" fn taskq_wait_id(tq: *const c_void, id: u64) {
    let tq = &*(tq as *const TaskQueue);
    CoroutineFuture::poll_until_ready(tq.wait_id(id));
}

pub(crate) unsafe extern "C" fn taskq_cancel_id(tq: *const c_void, id: u64) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    CoroutineFuture::poll_until_ready(tq.cancel_delayed_task(id))
}

pub(crate) unsafe extern "C" fn taskq_is_empty(tq: *const c_void) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.task_map.is_empty() as i32
}

pub(crate) unsafe extern "C" fn taskq_nalloc(tq: *const c_void) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.task_map.len() as i32
}
