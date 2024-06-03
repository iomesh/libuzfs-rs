use crate::{
    bindings::sys::{pri_t, task_func_t, uint_t},
    context::{
        coroutine::AsyncCoroutine,
        coroutine_c::{co_get_key, COROUTINE_KEY},
    },
};
use dashmap::DashMap;
use futures::FutureExt;
use once_cell::sync::OnceCell;
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering::*},
        Arc,
    },
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    sync::{Notify, Semaphore},
};

static TASKQ_KEY: OnceCell<u32> = OnceCell::new();

const INIT: u32 = 0;
const RUNNING: u32 = 1;
const ABORTED: u32 = 2;

struct TaskEntry {
    delay: Option<Duration>,
    state: AtomicU32,
    stop_notify: Notify,
    tasks: Arc<DashMap<u64, Arc<TaskEntry>>>,
    inflight_limit: Arc<Semaphore>,
}

unsafe impl Send for TaskEntry {}
unsafe impl Sync for TaskEntry {}

struct TaskQueue {
    tasks: Arc<DashMap<u64, Arc<TaskEntry>>>,
    inflight_limit: Arc<Semaphore>,
    max_inflight: usize,
    rt: Option<Runtime>,
}

impl TaskQueue {
    fn new(max_inflight: usize, new_rt: bool) -> Self {
        Self {
            tasks: Arc::new(DashMap::new()),
            inflight_limit: Arc::new(Semaphore::new(max_inflight)),
            max_inflight,
            rt: new_rt.then(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            }),
        }
    }

    async fn dispatch(
        &self,
        taskq_ent: Option<usize>,
        func: task_func_t,
        arg: *mut libc::c_void,
        delay: Option<Duration>,
    ) -> u64 {
        self.inflight_limit.acquire().await.unwrap().forget();

        let mut coroutine = AsyncCoroutine::new(func.unwrap(), arg as usize, false);
        coroutine.set_specific(
            *TASKQ_KEY.get_or_init(|| COROUTINE_KEY.fetch_add(1, Relaxed)),
            self as *const _ as *mut libc::c_void,
        );

        let id = coroutine.id;
        if let Some(taskq_ent) = taskq_ent {
            unsafe { *(taskq_ent as *mut u64) = id };
        }
        let ent = Arc::new(TaskEntry {
            delay,
            state: AtomicU32::new(INIT),
            stop_notify: Notify::new(),
            tasks: self.tasks.clone(),
            inflight_limit: self.inflight_limit.clone(),
        });
        self.tasks.insert(id, ent.clone());

        let fut = async move {
            if let Some(delay) = ent.delay {
                tokio::time::sleep(delay).await;
            }

            if ent
                .state
                .compare_exchange(INIT, RUNNING, Acquire, Relaxed)
                .is_ok()
            {
                coroutine.fuse().await;
            }

            if let Some(taskq_ent) = taskq_ent {
                let taskq_ent = unsafe { AtomicU64::from_ptr(taskq_ent as *mut u64) };
                taskq_ent.compare_exchange(id, 0, Relaxed, Relaxed).unwrap();
            }

            let ent = ent.tasks.remove(&id).unwrap().1;
            ent.stop_notify.notify_one();
            ent.inflight_limit.add_permits(1);
        };

        if let Some(ref rt) = self.rt {
            rt.spawn(fut);
        } else {
            tokio::spawn(fut);
        };

        id
    }

    async fn wait_one(&self, id: u64) {
        if let Some(entry) = self.tasks.get(&id) {
            let entry = entry.clone();
            entry.stop_notify.notified().await;
        }
    }

    fn cancel_one(&self, id: u64) -> bool {
        if let Some(entry) = self.tasks.get(&id) {
            entry
                .state
                .compare_exchange(INIT, ABORTED, Acquire, Relaxed)
                .is_ok()
        } else {
            false
        }
    }

    async fn wait_all_outstanding(&self) {
        for task in self.tasks.iter() {
            let task = task.clone();
            task.stop_notify.notified().await;
        }
    }
}

impl Drop for TaskQueue {
    fn drop(&mut self) {
        unsafe { AsyncCoroutine::tls_coroutine().poll_until_ready(self.wait_all_outstanding()) };
    }
}

const TASKQ_NEW_RUNTIME: uint_t = 0x0020;

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_create(
    _: *const i8,
    _: i32,
    _: pri_t,
    _: i32,
    max_inflight: i32,
    flags: uint_t,
) -> *mut libc::c_void {
    let queue = Box::new(TaskQueue::new(
        max_inflight as usize,
        (flags & TASKQ_NEW_RUNTIME) != 0,
    ));

    Box::into_raw(queue) as *mut libc::c_void
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_dispatch(
    taskq: *mut libc::c_void,
    func: task_func_t,
    arg: *mut libc::c_void,
    _: uint_t,
) -> u64 {
    let taskq = &*(taskq as *mut TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(taskq.dispatch(None, func, arg, None))
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_dispatch_delay(
    taskq: *mut libc::c_void,
    func: task_func_t,
    arg: *mut libc::c_void,
    _: uint_t,
    nanos: u64,
) -> u64 {
    let taskq = &*(taskq as *mut TaskQueue);
    let duration = Duration::from_nanos(nanos);
    AsyncCoroutine::tls_coroutine().poll_until_ready(taskq.dispatch(
        None,
        func,
        arg,
        Some(duration),
    ))
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_dispatch_ent(
    taskq: *mut libc::c_void,
    func: task_func_t,
    arg: *mut libc::c_void,
    _: uint_t,
    taskq_ent: *mut u64,
) {
    let taskq = &*(taskq as *mut TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(taskq.dispatch(
        Some(taskq_ent as usize),
        func,
        arg,
        None,
    ));
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_empty_ent(ent: *mut u64) -> i32 {
    let ent = AtomicU64::from_ptr(ent);
    (ent.load(Relaxed) == 0) as i32
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_init_ent(ent: *mut u64) {
    *ent = 0;
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_destroy(taskq: *mut libc::c_void) {
    let taskq = Box::from_raw(taskq as *mut TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(taskq.wait_all_outstanding());
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_wait(taskq: *mut libc::c_void) {
    let taskq = &*(taskq as *mut TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(taskq.wait_all_outstanding());
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_wait_outstanding(taskq: *mut libc::c_void, _: u64) {
    taskq_wait(taskq);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_wait_id(taskq: *mut libc::c_void, id: u64) {
    let taskq = &*(taskq as *mut TaskQueue);
    AsyncCoroutine::tls_coroutine().poll_until_ready(taskq.wait_one(id));
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_member(taskq: *mut libc::c_void, id: *mut libc::c_void) -> i32 {
    let taskq = &*(taskq as *mut TaskQueue);
    taskq.tasks.contains_key(&(id as u64)) as i32
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_of_curthread() -> *mut libc::c_void {
    co_get_key(*TASKQ_KEY.get().unwrap())
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_cancel_id(taskq: *mut libc::c_void, id: u64) -> i32 {
    let taskq = &*(taskq as *mut TaskQueue);
    if taskq.cancel_one(id) {
        0
    } else {
        libc::ENOENT
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_nalloc(taskq: *mut libc::c_void) -> i32 {
    let taskq = &*(taskq as *mut TaskQueue);
    (taskq.max_inflight - taskq.inflight_limit.available_permits()) as i32
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn taskq_empty(taskq: *mut libc::c_void) -> i32 {
    let taskq = &*(taskq as *mut TaskQueue);
    taskq.tasks.is_empty() as i32
}
