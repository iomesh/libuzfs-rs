use crate::bindings::*;
#[cfg(debug_assertions)]
use backtrace::Symbol;
use dashmap::DashMap;
use futures::Future;
use once_cell::sync::OnceCell;
use std::{
    os::raw::{c_int, c_void},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    task::{Context, Poll, Waker},
};
use tokio::runtime::Runtime;

static ID_TASK_HANDLE_MAP: OnceCell<DashMap<u64, tokio::task::JoinHandle<()>>> = OnceCell::new();
static ID_RUNTIME_MAP: OnceCell<DashMap<u64, std::thread::JoinHandle<()>>> = OnceCell::new();
static UZFS_RUNTIME: OnceCell<Runtime> = OnceCell::new();
// start from 1 to pass null pointer check
static CURRENT_TASK_ID: AtomicU64 = AtomicU64::new(1);

pub static ADD_BACKTRACE: OnceCell<fn(u64, String)> = OnceCell::new();
pub static REMOVE_BACKTRACE: OnceCell<fn(u64)> = OnceCell::new();
pub static ADD_CREATION_POS: OnceCell<fn(u64, String)> = OnceCell::new();
pub static REMOVE_CREATION_POS: OnceCell<fn(u64)> = OnceCell::new();

#[cfg(debug_assertions)]
const MAX_BACKTRACE_DEPTH: u32 = 20;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum CoroutineState {
    Runnable,
    Pending,
    Done,
}

struct WakeArg {
    state: CoroutineState,
    waker: Option<Waker>,
}

pub struct UzfsCoroutineFuture {
    uc: *mut uzfs_coroutine_t,
    pub task_id: u64,
    #[cfg(debug_assertions)]
    backtrace: bool,
    waker_arg: Box<Mutex<WakeArg>>,
}

type CoroutineFunc = unsafe extern "C" fn(arg: *mut c_void);

impl UzfsCoroutineFuture {
    // FIXME(sundengyu): pin arg to the address of arg to prevent future swap
    #[allow(unused_variables)]
    pub fn new(func: CoroutineFunc, arg: usize, foreground: bool, backtrace: bool) -> Self {
        let task_id = CURRENT_TASK_ID.fetch_add(1, Ordering::Relaxed);

        #[cfg(not(debug_assertions))]
        let record_backtrace = None;

        #[cfg(debug_assertions)]
        let record_backtrace = if backtrace {
            if let Some(add_creation_pos_func) = ADD_CREATION_POS.get() {
                add_creation_pos_func(task_id, UzfsCoroutineFuture::get_creation_pos(foreground));
            }
            Some(UzfsCoroutineFuture::record_backtrace as unsafe extern "C" fn(u64))
        } else {
            None
        };

        let mut waker_arg = Box::new(Mutex::new(WakeArg {
            state: CoroutineState::Runnable,
            waker: None,
        }));

        let uc = unsafe {
            libuzfs_new_coroutine(
                Some(func),
                arg as *mut c_void,
                task_id,
                foreground as u32,
                record_backtrace,
                Some(UzfsCoroutineFuture::wake),
                waker_arg.as_mut() as *mut _ as *mut c_void,
            )
        };

        UzfsCoroutineFuture {
            uc,
            task_id,
            #[cfg(debug_assertions)]
            backtrace,
            waker_arg,
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

    unsafe extern "C" fn wake(waker_arg: *mut c_void) {
        let waker_arg = &*(waker_arg as *const Mutex<WakeArg>);
        let mut waker_arg = waker_arg.lock().unwrap();
        assert_eq!(waker_arg.state, CoroutineState::Pending);
        waker_arg.state = CoroutineState::Runnable;
        waker_arg.waker.as_ref().unwrap().wake_by_ref();
    }

    #[cfg(debug_assertions)]
    unsafe extern "C" fn record_backtrace(task_id: u64) {
        use std::fmt::Write;
        if let Some(add_backtrace_func) = ADD_BACKTRACE.get() {
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
        let self_ref = self.as_ref();
        // TODO(sundengyu): make sure this won't result in dead lock
        let mut waker_arg = self_ref.waker_arg.lock().unwrap();

        if waker_arg
            .waker
            .as_ref()
            .map(|waker| !waker.will_wake(cx.waker()))
            .unwrap_or(true)
        {
            waker_arg.waker.replace(cx.waker().clone());
        }

        // check state after waker replacing to make waker is latest
        match waker_arg.state {
            CoroutineState::Runnable => (),
            CoroutineState::Pending => return Poll::Pending,
            CoroutineState::Done => return Poll::Ready(()),
        }

        match unsafe { libuzfs_run_coroutine(self.uc) } {
            #[allow(non_upper_case_globals)]
            run_state_RUN_STATE_PENDING => {
                waker_arg.state = CoroutineState::Pending;
                Poll::Pending
            }
            #[allow(non_upper_case_globals)]
            run_state_RUN_STATE_YIELDED => {
                // use better way like context::defer to yield
                let fut = tokio::task::yield_now();
                tokio::pin!(fut);
                let _ = fut.poll(cx);
                Poll::Pending
            }
            #[allow(non_upper_case_globals)]
            run_state_RUN_STATE_DONE => {
                waker_arg.state = CoroutineState::Done;
                Poll::Ready(())
            }
            other => unreachable!("{other} not expected"),
        }
    }
}

impl Drop for UzfsCoroutineFuture {
    fn drop(&mut self) {
        let state = self.waker_arg.lock().unwrap().state;
        if state != CoroutineState::Done {
            panic!(
                "coroutine {} dropped before it is ready, current state: {state:?}",
                self.task_id
            );
        }
        unsafe { assert_eq!((*self.uc).pending, 0) };
        unsafe { assert_eq!((*self.uc).yielded, 0) };

        #[cfg(debug_assertions)]
        if self.backtrace {
            if let Some(remove_backtrace_func) = REMOVE_BACKTRACE.get() {
                remove_backtrace_func(self.task_id);
            }
            if let Some(remove_creation_pos_func) = REMOVE_CREATION_POS.get() {
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
    new_runtime: boolean_t,
) -> u64 {
    let coroutine = UzfsCoroutineFuture::new(thread_func.unwrap(), arg as usize, false, false);
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
            ID_RUNTIME_MAP
                .get_or_init(DashMap::new)
                .insert(task_id, handle);
        }
    } else {
        let handle = UZFS_RUNTIME
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_name("uzfs-background")
                    .build()
                    .unwrap()
            })
            .spawn(coroutine);

        if joinable != 0 {
            ID_TASK_HANDLE_MAP
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
    if let Some(map) = ID_TASK_HANDLE_MAP.get() {
        if map.remove(&task_id).is_some() {
            panic!("async join not supported !!!");
        }
    }

    let handle = ID_RUNTIME_MAP.get().unwrap().remove(&task_id).unwrap().1;
    handle.join().unwrap();
}
