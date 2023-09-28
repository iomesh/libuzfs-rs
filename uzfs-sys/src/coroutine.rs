use crate::bindings::*;
use dashmap::DashMap;
use futures::{Future, FutureExt};
use once_cell::sync::OnceCell;
use std::{
    os::raw::{c_int, c_void},
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll, Waker},
};
use tokio::runtime::Runtime;

static id_task_handle_map: OnceCell<DashMap<u64, tokio::task::JoinHandle<()>>> = OnceCell::new();
static id_runtime_map: OnceCell<DashMap<u64, std::thread::JoinHandle<()>>> = OnceCell::new();
static uzfs_runtime: OnceCell<Runtime> = OnceCell::new();
// start from 1 to pass null pointer check
static current_task_id: AtomicU64 = AtomicU64::new(1);

const STACK_SIZE_DEFAULT: i32 = 1 << 20;

pub struct UzfsCoroutineFuture {
    uc: *mut uzfs_coroutine_t,
    pub task_id: u64,
}

type CoroutineFunc = unsafe extern "C" fn(arg: *mut c_void);

impl UzfsCoroutineFuture {
    // FIXME(sundengyu): pin arg to the address of arg to prevent future swap
    pub fn new(func: CoroutineFunc, arg: u64, mut stack_size: i32) -> Self {
        if stack_size == 0 {
            stack_size = STACK_SIZE_DEFAULT;
        }
        let task_id = current_task_id.fetch_add(1, Ordering::Relaxed);
        let uc =
            unsafe { libuzfs_new_coroutine(stack_size, Some(func), arg as *mut c_void, task_id) };
        UzfsCoroutineFuture { uc, task_id }
    }

    unsafe extern "C" fn wake(arg: *mut c_void) {
        Box::from_raw(arg as *mut Waker).wake();
    }
}

impl Future for UzfsCoroutineFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = Box::leak(Box::new(cx.waker().clone()));
        let res = unsafe {
            libuzfs_run_coroutine(
                self.uc,
                Some(UzfsCoroutineFuture::wake),
                waker as *mut Waker as *mut c_void,
            )
        };

        if res != 0 {
            Poll::Pending
        } else {
            // when pending, its the callee's duty to free the waker
            // so we should free the waker when poll returns ready
            let _ = unsafe { Box::from_raw(waker) };
            Poll::Ready(())
        }
    }
}

impl Drop for UzfsCoroutineFuture {
    fn drop(&mut self) {
        unsafe { libuzfs_destroy_coroutine(self.uc) };
    }
}

unsafe impl Send for UzfsCoroutineFuture {}
unsafe impl Sync for UzfsCoroutineFuture {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_create(
    thread_func: Option<unsafe extern "C" fn(arg1: *mut ::std::os::raw::c_void)>,
    arg: *mut c_void,
    stksize: c_int,
    joinable: boolean_t,
    new_runtime: boolean_t,
) -> u64 {
    let coroutine = UzfsCoroutineFuture::new(thread_func.unwrap(), arg as u64, stksize);
    let task_id = coroutine.task_id;

    if new_runtime != 0 {
        let handle = std::thread::Builder::new()
            .name("sep_runtime".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(coroutine);
            })
            .unwrap();

        if joinable != 0 {
            id_runtime_map
                .get_or_init(DashMap::new)
                .insert(task_id, handle);
        }
    } else {
        let handle = uzfs_runtime
            .get_or_init(|| tokio::runtime::Builder::new_multi_thread().build().unwrap())
            .spawn(coroutine);

        if joinable != 0 {
            id_task_handle_map
                .get_or_init(DashMap::new)
                .insert(task_id, handle);
        }
    }

    task_id
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_exit() {
    libuzfs_coroutine_exit();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_join(task_id: u64) {
    if let Some(map) = id_task_handle_map.get() {
        if let Some(pair) = map.remove(&task_id) {
            let mut handle = pair.1;
            loop {
                let arg = libuzfs_current_coroutine_arg();
                let mut waker = Box::from_raw(arg as *mut Waker);
                let context = &mut Context::from_waker(waker.as_mut());
                match handle.poll_unpin(context) {
                    Poll::Pending => libuzfs_coroutine_yield(),
                    Poll::Ready(_) => {
                        // leak this waker to reuse it
                        Box::leak(waker);
                        break;
                    }
                }
            }
            return;
        }
    }

    let handle = id_runtime_map.get().unwrap().remove(&task_id).unwrap().1;
    handle.join().unwrap();
}
