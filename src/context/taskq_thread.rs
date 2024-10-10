use dashmap::DashMap;
use libc::c_void;
use once_cell::sync::OnceCell;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};
use crate::bindings::sys;
use crossbeam_channel::*;

struct Notify {
    notified: Mutex<bool>,
    signal: Condvar,
}

impl Notify {
    fn new() -> Self {
        Self {
            notified: Mutex::new(false),
            signal: Condvar::new(),
        }
    }

    fn wait_notification(&self) {
        let mut notified = self.notified.lock().unwrap();
        while !*notified {
            notified = self.signal.wait(notified).unwrap();
        }
    }

    fn notify_all(&self) {
        let mut notified = self.notified.lock().unwrap();
        *notified = true;
        self.signal.notify_all();
    }
}

struct Task {
    func: unsafe extern "C" fn(*mut c_void),
    arg: *mut c_void,
    id: u64,
    task_map: Arc<DashMap<u64, Arc<Notify>>>,
}

unsafe impl Send for Task {}
unsafe impl Sync for Task {}

pub(super) struct TaskQueue {
    sender: Sender<Task>,
    cur_id: AtomicU64,
    task_map: Arc<DashMap<u64, Arc<Notify>>>,
}

static TASKQ_KEY: OnceCell<u32> = OnceCell::new();

impl TaskQueue {
    pub(super) fn new(nthreads: i32) -> Box<Self> {
        let (sender, recver) = unbounded::<Task>();
        let recver = Arc::new(recver);
        let tq = Box::new(Self {
            sender,
            cur_id: AtomicU64::new(2),
            task_map: Arc::new(DashMap::new()),
        });

        let tq_key = *TASKQ_KEY.get_or_init(|| {
            let mut key = 0;
            unsafe { libc::pthread_key_create(&mut key, None) };
            key
        });
        let ptr = tq.as_ref() as *const _ as usize;

        for _ in 0..nthreads {
            let recver = recver.clone();
            thread::spawn(move || {
                unsafe { libc::pthread_setspecific(tq_key, ptr as *mut _) };
                while let Ok(task) = recver.recv() {
                    unsafe { (task.func)(task.arg) };
                    let notify = task.task_map.remove(&task.id).unwrap().1;
                    notify.notify_all();
                }
            });
        }

        tq
    }

    fn wait(&self) {
        while !self.task_map.is_empty() {
            let notifies: Vec<_> = self.task_map.iter().map(|kv| kv.value().clone()).collect();
            for notify in notifies {
                notify.wait_notification();
            }
        }
    }

    pub(super) fn dispatch(&self, func: unsafe extern "C" fn(arg1: *mut c_void), arg: *mut c_void) {
        let task = Task {
            func,
            arg,
            id: self.cur_id.fetch_add(1, Ordering::Relaxed),
            task_map: self.task_map.clone(),
        };
        self.task_map.insert(task.id, Arc::new(Notify::new()));
        self.sender.send(task).unwrap();
    }
}

pub(crate) unsafe extern "C" fn taskq_create(
    _tq_name: *const libc::c_char,
    nthreads: i32,
) -> *mut c_void {
    let tq = TaskQueue::new(nthreads);
    Box::into_raw(tq) as *mut _
}

pub(crate) unsafe extern "C" fn taskq_dispatch(
    tq: *const c_void,
    func: Option<unsafe extern "C" fn(arg1: *mut c_void)>,
    arg: *mut c_void,
    _flag: u32,
) -> u64 {
    let tq = &*(tq as *const TaskQueue);
    tq.dispatch(func.unwrap(), arg);
    1
}

pub(crate) unsafe extern "C" fn taskq_delay_dispatch(
    _tq: *const c_void,
    _func: Option<unsafe extern "C" fn(arg1: *mut c_void)>,
    _arg: *mut c_void,
    _timeout: *const sys::timespec,
) -> u64 {
    0
}

pub(crate) unsafe extern "C" fn taskq_is_member(_: *const c_void, _: u64) -> i32 {
    unreachable!("taskq_is_member not supported");
}

pub(crate) unsafe extern "C" fn taskq_of_curthread() -> *mut c_void {
    let tq_key = *TASKQ_KEY.get_or_init(|| {
        let mut key = 0;
        unsafe { libc::pthread_key_create(&mut key, None) };
        key
    });
    libc::pthread_getspecific(tq_key)
}

pub(crate) unsafe extern "C" fn taskq_wait(tq: *const c_void) {
    let tq = &*(tq as *const TaskQueue);
    tq.wait();
}

pub(crate) unsafe extern "C" fn taskq_destroy(tq: *const c_void) {
    let tq = Box::from_raw(tq as *mut TaskQueue);
    tq.wait()
}

pub(crate) unsafe extern "C" fn taskq_wait_id(tq: *const c_void, _id: u64) {
    let tq = &*(tq as *const TaskQueue);
    tq.wait();
}

pub(crate) unsafe extern "C" fn taskq_cancel_id(_tq: *const c_void, _id: u64) -> i32 {
    libc::ENOENT
}

pub(crate) unsafe extern "C" fn taskq_is_empty(tq: *const c_void) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.task_map.is_empty() as i32
}

pub(crate) unsafe extern "C" fn taskq_nalloc(tq: *const c_void) -> i32 {
    let tq = &*(tq as *const TaskQueue);
    tq.task_map.len() as i32
}
