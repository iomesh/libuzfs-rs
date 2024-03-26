use crate::bindings::*;
#[cfg(debug_assertions)]
use backtrace::Symbol;
use dashmap::DashMap;
use futures::{Future, FutureExt};
use once_cell::sync::OnceCell;
use std::{
    os::raw::{c_int, c_void},
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll, Waker},
};
use tokio::runtime::{Handle, Runtime};

static id_task_handle_map: OnceCell<DashMap<u64, tokio::task::JoinHandle<()>>> = OnceCell::new();
static uzfs_runtime: OnceCell<Runtime> = OnceCell::new();
// start from 1 to pass null pointer check
static current_task_id: AtomicU64 = AtomicU64::new(1);

pub static add_backtrace: OnceCell<fn(u64, String)> = OnceCell::new();
pub static remove_backtrace: OnceCell<fn(u64)> = OnceCell::new();
pub static add_creation_pos: OnceCell<fn(u64, String)> = OnceCell::new();
pub static remove_creation_pos: OnceCell<fn(u64)> = OnceCell::new();

#[cfg(debug_assertions)]
const MAX_BACKTRACE_DEPTH: u32 = 20;

pub struct UzfsCoroutineFuture {
    uc: *mut uzfs_coroutine_t,
    pub task_id: u64,
    #[cfg(debug_assertions)]
    backtrace: bool,
}

type CoroutineFunc = unsafe extern "C" fn(arg: *mut c_void);

impl UzfsCoroutineFuture {
    // FIXME(sundengyu): pin arg to the address of arg to prevent future swap
    #[allow(unused_variables)]
    pub fn new(func: CoroutineFunc, arg: usize, foreground: bool, backtrace: bool) -> Self {
        let task_id = current_task_id.fetch_add(1, Ordering::Relaxed);

        #[cfg(not(debug_assertions))]
        let record_backtrace = None;

        #[cfg(debug_assertions)]
        let record_backtrace = if backtrace {
            if let Some(add_creation_pos_func) = add_creation_pos.get() {
                add_creation_pos_func(task_id, UzfsCoroutineFuture::get_creation_pos(foreground));
            }
            Some(UzfsCoroutineFuture::record_backtrace as unsafe extern "C" fn(u64))
        } else {
            None
        };

        let uc = unsafe {
            libuzfs_new_coroutine(
                Some(func),
                arg as *mut c_void,
                task_id,
                foreground as u32,
                record_backtrace,
            )
        };
        UzfsCoroutineFuture {
            uc,
            task_id,
            #[cfg(debug_assertions)]
            backtrace,
        }
    }

    #[cfg(debug_assertions)]
    fn parse_frame_symbol(symbol: &Symbol) -> (&str, u32) {
        let filename = match symbol.filename() {
            Some(path) => path.to_str().unwrap(),
            None => "",
        };

        let lineno = symbol.lineno().unwrap_or(0);

        (filename, lineno)
    }

    #[inline]
    #[cfg(debug_assertions)]
    fn get_creation_pos(foreground: bool) -> String {
        let mut continue_trace = true;
        let mut pos = String::new();
        backtrace::trace(|frame| {
            let mut ret = continue_trace;
            backtrace::resolve_frame(frame, |symbol| {
                let name = if let Some(name) = symbol.name() {
                    name.as_str().unwrap()
                } else {
                    return;
                };

                if !ret {
                    if !foreground && name.eq("taskq_create") {
                        ret = true;
                        return;
                    }

                    let (filename, lineno) = UzfsCoroutineFuture::parse_frame_symbol(symbol);
                    pos = format!("{name} {filename}:{lineno}");
                }

                if (foreground && name.contains("UzfsCoroutineFuture::new"))
                    || (!foreground && name.eq("zk_thread_create"))
                {
                    continue_trace = false;
                }
            });
            ret
        });
        pos
    }

    unsafe extern "C" fn wake(arg: *mut c_void) {
        Box::from_raw(arg as *mut Waker).wake();
    }

    #[cfg(debug_assertions)]
    unsafe extern "C" fn record_backtrace(task_id: u64) {
        use std::fmt::Write;
        if let Some(add_backtrace_func) = add_backtrace.get() {
            let mut bt_string = String::from("");
            let mut depth = 0;
            let mut begin_record = false;
            backtrace::trace(|frame| {
                backtrace::resolve_frame(frame, |symbol| {
                    let name = if let Some(name) = symbol.name() {
                        name.as_str().unwrap()
                    } else {
                        return;
                    };

                    if begin_record {
                        let (filename, lineno) = UzfsCoroutineFuture::parse_frame_symbol(symbol);
                        writeln!(bt_string, "{depth} {name} {filename}:{lineno}").unwrap();
                        depth += 1;
                    } else if name.contains("record_backtrace") {
                        begin_record = true;
                    }
                });
                depth < MAX_BACKTRACE_DEPTH
            });
            add_backtrace_func(task_id, bt_string);
        }
    }
}

impl Future for UzfsCoroutineFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = Box::into_raw(Box::new(cx.waker().clone()));
        let run_state = unsafe {
            libuzfs_run_coroutine(
                self.uc,
                Some(UzfsCoroutineFuture::wake),
                waker as *mut c_void,
            )
        };

        match run_state {
            run_state_RUN_STATE_PENDING => Poll::Pending,
            run_state_RUN_STATE_YIELDED => {
                // this waker is not useful anymore
                let _ = unsafe { Box::from_raw(waker) };
                // use better way like context::defer to yield
                let _ = Box::pin(tokio::task::yield_now()).poll_unpin(cx);
                Poll::Pending
            }
            run_state_RUN_STATE_DONE => {
                // when pending, its the callee's duty to free the waker
                // so we should free the waker when poll returns ready
                let _ = unsafe { Box::from_raw(waker) };
                Poll::Ready(())
            }
            _ => panic!("{run_state} not expected"),
        }
    }
}

impl Drop for UzfsCoroutineFuture {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if self.backtrace {
            if let Some(remove_backtrace_func) = remove_backtrace.get() {
                remove_backtrace_func(self.task_id);
            }
            if let Some(remove_creation_pos_func) = remove_creation_pos.get() {
                remove_creation_pos_func(self.task_id);
            }
        }

        unsafe { libuzfs_destroy_coroutine(self.uc) };
    }
}

unsafe impl Send for UzfsCoroutineFuture {}
unsafe impl Sync for UzfsCoroutineFuture {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn thread_create(
    thread_func: Option<unsafe extern "C" fn(arg1: *mut ::std::os::raw::c_void)>,
    arg: *mut c_void,
    _stksize: c_int,
    joinable: boolean_t,
    blocking: boolean_t,
    foreground: boolean_t,
) -> u64 {
    let coroutine =
        UzfsCoroutineFuture::new(thread_func.unwrap(), arg as usize, foreground != 0, false);
    let task_id = coroutine.task_id;

    let handle = if blocking != 0 {
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(coroutine);
        })
    } else if foreground == 0 {
        uzfs_runtime
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_name("uzfs-background")
                    .build()
                    .unwrap()
            })
            .spawn(coroutine)
    } else {
        tokio::spawn(coroutine)
    };

    if joinable != 0 {
        id_task_handle_map
            .get_or_init(|| DashMap::with_shard_amount(1024))
            .insert(task_id, handle);
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
        if let Some((_, mut handle)) = map.remove(&task_id) {
            loop {
                let arg = libuzfs_current_coroutine_arg();
                let mut waker = Box::from_raw(arg as *mut Waker);
                let context = &mut Context::from_waker(waker.as_mut());
                match handle.poll_unpin(context) {
                    Poll::Pending => libuzfs_coroutine_yield(),
                    Poll::Ready(_) => {
                        // leak this waker to reuse it
                        assert_eq!(Box::into_raw(waker), arg as *mut Waker);
                        break;
                    }
                }
            }
            return;
        }
    }
}
