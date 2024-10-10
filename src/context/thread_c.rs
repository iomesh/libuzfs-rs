use super::taskq_thread::TaskQueue;
use crate::bindings::sys;
use libc::{c_void, pthread_getspecific, pthread_key_create, pthread_setspecific};
use once_cell::sync::OnceCell;
use std::ptr::null_mut;

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sched_yield() {
    libc::sched_yield();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_create_key(
    key: *mut u32,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    pthread_key_create(key, destructor);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_get_key(key: u32) -> *mut c_void {
    pthread_getspecific(key)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_set_key(key: u32, data: *mut c_void) -> i32 {
    pthread_setspecific(key, data)
}

// #[allow(clippy::missing_safety_doc)]
// pub unsafe extern "C" fn co_delete_key(key: u32) {
//     CoroutineFuture::tls_coroutine().delete_key(key)
// }

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_self() -> u64 {
    libc::pthread_self() as u64
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sleep(duration: *const sys::timespec) {
    let duration = libc::timespec {
        tv_sec: (*duration).tv_sec,
        tv_nsec: (*duration).tv_nsec,
    };
    libc::nanosleep(&duration, null_mut());
}

// const TS_RUN: i32 = 0x00000002;
// TS_JOINABLE means that we will call thread_join on the coroutine id
// const TS_JOINABLE: i32 = 0x00000004;
// TS_BLOCKING means the callback function may block the thread, so we need to use tokio::task::spawn_blocking to implement it
const TS_BLOCKING: i32 = 0x00000008;

static SHARED_TQ: OnceCell<Box<TaskQueue>> = OnceCell::new();

struct Task {
    func: unsafe extern "C" fn(*mut c_void),
    arg: *mut c_void,
}

extern "C" fn thread_func(arg: *mut c_void) -> *mut c_void {
    unsafe {
        let t = Box::from_raw(arg as *mut Task);
        (t.func)(t.arg);
    }
    null_mut()
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_create(
    func: Option<unsafe extern "C" fn(*mut c_void)>,
    arg: *mut c_void,
    state: i32,
) -> u64 {
    if (state & TS_BLOCKING) != 0 {
        let tq = SHARED_TQ.get_or_init(|| TaskQueue::new(32));
        tq.dispatch(func.unwrap(), arg);
        1
    } else {
        let arg = Box::into_raw(Box::new(Task {
            func: func.unwrap(),
            arg,
        })) as *mut _;
        let mut tid = 0;
        libc::pthread_create(&mut tid, null_mut(), thread_func, arg);
        tid
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_exit() {
    libc::pthread_exit(null_mut());
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_join(_id: u64) {
    unreachable!("join not supported");
}
