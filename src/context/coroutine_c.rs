use super::coroutine::CoroutineFuture;
use crate::{bindings::sys, time::sleep};
use dashmap::DashMap;
use libc::c_void;
use once_cell::sync::OnceCell;
use std::time::Duration;
use tokio::runtime::{EnterGuard, Handle, Runtime};

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sched_yield() {
    CoroutineFuture::tls_coroutine().sched_yield();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_create_key(
    key: *mut u32,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    *key = CoroutineFuture::create_key(destructor);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_get_key(key: u32) -> *mut c_void {
    CoroutineFuture::tls_coroutine().get_specific(key)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_set_key(key: u32, data: *mut c_void) -> i32 {
    CoroutineFuture::tls_coroutine().set_specific(key, data);
    0
}

// #[allow(clippy::missing_safety_doc)]
// pub unsafe extern "C" fn co_delete_key(key: u32) {
//     CoroutineFuture::tls_coroutine().delete_key(key)
// }

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_self() -> u64 {
    CoroutineFuture::tls_coroutine().id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_sleep(duration: *const sys::timespec) {
    let duration = &*duration;
    let duration = Duration::new(duration.tv_sec as u64, duration.tv_nsec as u32);
    CoroutineFuture::poll_until_ready(sleep(duration));
}

// const TS_RUN: i32 = 0x00000002;
// TS_JOINABLE means that we will call thread_join on the coroutine id
const TS_JOINABLE: i32 = 0x00000004;
// TS_BLOCKING means the callback function may block the thread, so we need to use tokio::task::spawn_blocking to implement it
const TS_BLOCKING: i32 = 0x00000008;

static ID_TASK_HANDLE_MAP: OnceCell<DashMap<u64, tokio::task::JoinHandle<()>>> = OnceCell::new();
static BACKGROUND_RT: OnceCell<Runtime> = OnceCell::new();

pub(crate) fn enter_background_rt<'a>() -> EnterGuard<'a> {
    let rt = BACKGROUND_RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });
    rt.enter()
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_create(
    thread_func: Option<unsafe extern "C" fn(*mut c_void)>,
    arg: *mut c_void,
    state: i32,
) -> u64 {
    let mut coroutine = CoroutineFuture::new(thread_func.unwrap(), arg as usize);
    let id = coroutine.id;
    let _guard = enter_background_rt();
    let handle = if (state & TS_BLOCKING) != 0 {
        tokio::task::spawn_blocking(move || {
            coroutine.global = true;
            Handle::current().block_on(coroutine);
        })
    } else {
        tokio::spawn(coroutine)
    };

    if (state & TS_JOINABLE) != 0 {
        ID_TASK_HANDLE_MAP
            .get_or_init(DashMap::new)
            .insert(id, handle);
    }

    id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_exit() {
    CoroutineFuture::tls_coroutine().exit();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_join(id: u64) {
    if let Some((_, handle)) = ID_TASK_HANDLE_MAP.get().unwrap().remove(&id) {
        CoroutineFuture::poll_until_ready(handle).unwrap();
    }
}
