use super::coroutine::AsyncCoroutine;
use crate::bindings::sys;
use dashmap::DashMap;
use futures::FutureExt;
use libc::c_void;
use once_cell::sync::OnceCell;
use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

pub(crate) static COROUTINE_KEY: AtomicU32 = AtomicU32::new(0);

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sched_yield() {
    AsyncCoroutine::tls_coroutine().sched_yield();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_create_key(key: *mut u32) {
    *key = COROUTINE_KEY.fetch_add(1, Ordering::Relaxed);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_get_key(key: u32) -> *mut c_void {
    AsyncCoroutine::tls_coroutine().get_specific(key)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_set_key(key: u32, data: *mut c_void) -> i32 {
    AsyncCoroutine::tls_coroutine().set_specific(key, data);
    0
}

// #[allow(clippy::missing_safety_doc)]
// pub unsafe extern "C" fn co_delete_key(key: u32) {
//     AsyncCoroutine::tls_coroutine().delete_key(key)
// }

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_self() -> u64 {
    AsyncCoroutine::tls_coroutine().id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sleep(duration: *const sys::timespec) {
    let duration = &*duration;
    let duration = Duration::new(duration.tv_sec as u64, duration.tv_nsec as u32);
    AsyncCoroutine::tls_coroutine().poll_until_ready(tokio::time::sleep(duration));
}

// const TS_RUN: i32 = 0x00000002;
const TS_JOINABLE: i32 = 0x00000004;
const TS_NEW_RUNTIME: i32 = 0x00000008;

static ID_TASK_HANDLE_MAP: OnceCell<DashMap<u64, tokio::task::JoinHandle<()>>> = OnceCell::new();
static ID_RUNTIME_MAP: OnceCell<DashMap<u64, std::thread::JoinHandle<()>>> = OnceCell::new();

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_create(
    thread_func: Option<unsafe extern "C" fn(*mut c_void)>,
    arg: *mut c_void,
    state: i32,
) -> u64 {
    let coroutine = AsyncCoroutine::new(thread_func.unwrap(), arg as usize, false);
    let id = coroutine.id;
    let fut = coroutine.fuse();
    let blocking = (state & TS_NEW_RUNTIME) != 0;
    let joinable = (state & TS_JOINABLE) != 0;
    if blocking {
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(fut);
        });
        if joinable {
            ID_RUNTIME_MAP.get_or_init(DashMap::new).insert(id, handle);
        }
    } else {
        let handle = tokio::spawn(fut);
        if joinable {
            ID_TASK_HANDLE_MAP
                .get_or_init(DashMap::new)
                .insert(id, handle);
        }
    }

    id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_exit() {
    AsyncCoroutine::tls_coroutine().exit();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_join(id: u64) {
    if let Some((_, handle)) = ID_TASK_HANDLE_MAP.get().unwrap().remove(&id) {
        AsyncCoroutine::tls_coroutine()
            .poll_until_ready(handle)
            .unwrap();
    } else if let Some((_, handle)) = ID_RUNTIME_MAP.get().unwrap().remove(&id) {
        handle.join().unwrap();
    }
}
