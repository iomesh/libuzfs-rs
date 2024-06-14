use super::coroutine::AsyncCoroutine;
use crate::bindings::sys;
use dashmap::DashMap;
use libc::c_void;
use once_cell::sync::OnceCell;
use std::collections::HashSet;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering::*};
use std::time::Duration;
use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};
use tokio::runtime::Handle;
use tokio::time::Instant;
use tokio::{
    sync::{mpsc::*, Notify},
    time::timeout_at,
};

#[derive(Clone)]
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
        let mut coroutine = AsyncCoroutine::new(self.func.unwrap(), self.arg as usize, true);
        let tq_key = *TASKQ_KEY.get_or_init(|| AsyncCoroutine::create_key(None));
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
        let res = self.deadline.cmp(&other.deadline);
        if res == Ordering::Equal {
            Some(self.id.cmp(&other.id))
        } else {
            Some(res)
        }
    }
}

impl Ord for TimerTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

enum TimerEvent {
    Register(TimerTask),
    Unregister(u64),
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

            let mut unregister_ids = HashSet::new();

            for event in events {
                match event {
                    TimerEvent::Register(task) => assert!(self.set.insert(task)),
                    TimerEvent::Unregister(id) => {
                        unregister_ids.insert(id);
                    }
                }
            }

            let mut fake_tasks = Vec::with_capacity(MAX_TIMER_BATCH);
            for task in &self.set {
                if unregister_ids.is_empty() {
                    break;
                }

                if unregister_ids.remove(&task.id) {
                    fake_tasks.push(task.clone());
                }
            }

            for fake_task in fake_tasks {
                self.set.remove(&fake_task);
                let notify = fake_task.map.remove(&fake_task.id).unwrap().1;
                notify.notify_one();
            }

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
    cur_delay_id: AtomicU64,
    task_map: Arc<DashMap<u64, Arc<Notify>>>,
    stop: AtomicBool,
}

impl TaskQueue {
    pub(super) fn new() -> Box<Self> {
        Box::new(Self {
            cur_delay_id: AtomicU64::new(u64::MAX),
            task_map: Arc::new(DashMap::new()),
            stop: AtomicBool::new(false),
        })
    }

    pub(super) fn dispatch(
        &self,
        func: Option<unsafe extern "C" fn(*mut c_void)>,
        arg: *mut c_void,
    ) -> u64 {
        if self.stop.load(Acquire) {
            return 0;
        }

        let tq_key = *TASKQ_KEY.get_or_init(|| AsyncCoroutine::create_key(None));
        let mut coroutine = AsyncCoroutine::new(func.unwrap(), arg as usize, false);
        coroutine.set_specific(tq_key, self as *const _ as *mut c_void);
        let id = coroutine.id;
        let notify = Arc::new(Notify::new());
        self.task_map.insert(id, notify);
        let task_map = self.task_map.clone();
        tokio::spawn(async move {
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
        if self.stop.load(Acquire) {
            return 0;
        }

        let id = self.cur_delay_id.fetch_sub(1, Relaxed);
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

    pub(super) async fn cancel_delayed_task(&self, id: u64) {
        if id == 0 {
            return;
        }

        assert!(id >> 63 > 0, "{:?}", std::backtrace::Backtrace::capture());
        if let Some(notify) = self.task_map.get(&id).map(|notify| notify.clone()) {
            let manager_hdl = TIMER_MANAGER_HANDLE.get().unwrap();
            manager_hdl.sender.send(TimerEvent::Unregister(id)).unwrap();
            notify.notified().await;
        }
    }

    pub(super) async fn wait(&self) {
        let notifies: Vec<_> = self.task_map.iter().map(|v| (v.clone())).collect();
        for notify in notifies {
            notify.notified().await;
        }
    }

    pub(super) async fn stop(&self) {
        self.stop.store(true, Relaxed);
        fence(SeqCst);
        self.wait().await;
    }

    pub(super) fn is_member(&self, id: u64) -> bool {
        self.task_map.contains_key(&id)
    }
}

pub(crate) unsafe extern "C" fn taskq_create() -> *mut c_void {
    let tq = TaskQueue::new();
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

pub(crate) unsafe extern "C" fn taskq_is_member(tq: *const c_void, id: u64) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.is_member(id) as i32
}

pub(crate) unsafe extern "C" fn taskq_of_curthread() -> *mut c_void {
    let tq_key = *TASKQ_KEY.get_or_init(|| AsyncCoroutine::create_key(None));
    AsyncCoroutine::tls_coroutine().get_specific(tq_key)
}

pub(crate) unsafe extern "C" fn taskq_wait(tq: *const c_void) {
    let tq = &*(tq as *const TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(tq.wait());
}

pub(crate) unsafe extern "C" fn taskq_destroy(tq: *const c_void) {
    let tq = Box::from_raw(tq as *mut TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(tq.stop());
}

pub(crate) unsafe extern "C" fn taskq_wait_id(tq: *const c_void, id: u64) {
    let tq = &*(tq as *const TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(tq.wait_id(id));
}

pub(crate) unsafe extern "C" fn taskq_cancel_id(tq: *const c_void, id: u64) {
    let tq = &*(tq as *const TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(tq.cancel_delayed_task(id));
}

pub(crate) unsafe extern "C" fn taskq_is_empty(tq: *const c_void) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.task_map.is_empty() as i32
}

pub(crate) unsafe extern "C" fn taskq_nalloc(tq: *const c_void) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.task_map.len() as i32
}
