use std::cell::Cell;
use std::collections::HashMap;
use std::mem::transmute;
use std::pin::Pin;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::Context;
use std::task::Poll;

use dashmap::DashMap;
use futures::Future;
use libc::{c_void, intptr_t};
use once_cell::sync::OnceCell;

use super::libcontext::*;
use super::stack::*;

unsafe extern "C" fn task_runner_c(_: intptr_t) {
    let tls_coroutine = CoroutineFuture::tls_coroutine();

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        use std::arch::asm;
        let mut fp: u64;

        #[cfg(target_arch = "x86_64")]
        asm!(
            "mov {fp}, rbp",	// Move the value of rbp (frame pointer) into fp
            fp = out(reg) fp
        );

        #[cfg(target_arch = "aarch64")]
        asm!(
            "mov {0}, x29",	// Move the value of x29 (frame pointer) into fp
            out(reg) fp,
        );

        tls_coroutine.bottom_fpp = fp;
        *(fp as *mut u64) = tls_coroutine.saved_fp;
    }

    (tls_coroutine.func)(tls_coroutine.arg);
    tls_coroutine.exit();
}

pub(crate) static COROUTINE_KEY: AtomicU32 = AtomicU32::new(0);
static KEY_DESTRUCTORS: OnceCell<DashMap<u32, unsafe extern "C" fn(*mut c_void)>> = OnceCell::new();

thread_local! {
    static TLS_COROUTINE: Cell<*mut CoroutineFuture> = const { Cell::new(null_mut()) };
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum RunState {
    Runnable,
    Pending,
    Yielded,
    Done,
}

pub struct CoroutineFuture {
    poller_context: fcontext_t,
    pollee_context: fcontext_t,
    stack: Stack,
    context: *mut Context<'static>,
    state: RunState,
    arg: *mut c_void,
    func: unsafe extern "C" fn(*mut c_void),
    // this is used to replace pthread_specific
    co_specific: HashMap<u32, *mut c_void>,
    saved_errno: i32,

    // this id is equal to the stack id, when we return id to stack pool,
    // we will add the stack id
    pub(crate) id: u64,

    // in some cases, coroutine is created in thread t1 and dropped
    // in thread t2, which will fill tls stack pool of t2 but this
    // stack pool will never be popped, so we need the global flag
    // to return stack to global pool when coroutine is dropped
    pub(super) global: bool,

    record_pending_time: bool,

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    bottom_fpp: u64,
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    saved_fp: u64,
}

// using 1m stack size can prevent gdb from reporting corrupted stack
const STACK_SIZE: usize = 1 << 20;

impl CoroutineFuture {
    #[inline(never)]
    pub fn tls_coroutine<'a>() -> &'a mut Self {
        unsafe { &mut *TLS_COROUTINE.get() }
    }

    #[inline]
    // this is only used in uzdb, other usage should call new
    pub(crate) fn new_with_stack_size(
        func: unsafe extern "C" fn(arg1: *mut c_void),
        arg: usize,
        stack_size: usize,
    ) -> Self {
        let stack = fetch_or_alloc_stack(stack_size);
        let pollee_context =
            unsafe { make_fcontext(stack.stack_bottom, stack_size, Some(task_runner_c)) };

        // this position stores the finish addr which will be executed when task_runner_c returns,
        // in order to backtrace to the poll function in backtrace, this addr is replaced with Self::poll
        #[cfg(target_arch = "x86_64")]
        #[allow(clippy::fn_to_numeric_cast)]
        unsafe {
            *(stack.stack_bottom.byte_sub(0x08) as *mut u64) = Self::poll as u64 + 8
        };
        #[cfg(target_arch = "aarch64")]
        #[allow(clippy::fn_to_numeric_cast)]
        unsafe {
            // The values `0x18` and `16` are both empirical values,
            // and they need to be adjusted based on the specific instruction set.
            *(stack.stack_bottom.byte_sub(0x18) as *mut u64) = Self::poll as u64 + 0x10
        };

        Self {
            poller_context: null_mut(),
            pollee_context,
            id: stack.stack_id,
            stack,
            context: null_mut(),
            state: RunState::Runnable,
            arg: arg as *mut c_void,
            func,
            co_specific: HashMap::new(),
            saved_errno: 0,
            global: false,
            record_pending_time: false,

            #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
            bottom_fpp: 0,
            #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
            saved_fp: 0,
        }
    }

    #[inline]
    pub fn new(func: unsafe extern "C" fn(arg1: *mut c_void), arg: usize) -> Self {
        Self::new_with_stack_size(func, arg, STACK_SIZE)
    }

    #[inline]
    pub fn record_pending_time(mut self) -> Self {
        self.record_pending_time = true;
        self
    }

    #[inline]
    pub(crate) unsafe fn sched_yield(&mut self) {
        self.state = RunState::Yielded;
        jump_fcontext(&mut self.pollee_context, self.poller_context, 0, 1);
    }

    #[inline]
    pub(crate) unsafe fn exit(&mut self) {
        self.state = RunState::Done;
        jump_fcontext(&mut self.pollee_context, self.poller_context, 0, 1);
    }

    #[inline]
    pub(crate) unsafe fn pend_and_switch(&mut self) {
        self.state = RunState::Pending;
        jump_fcontext(&mut self.pollee_context, self.poller_context, 0, 1);
    }

    #[inline(always)]
    unsafe fn run(&mut self, cx: &mut Context<'_>) -> RunState {
        self.state = RunState::Runnable;
        let prev = TLS_COROUTINE.get();
        TLS_COROUTINE.set(self as *mut _);
        self.context = transmute(cx);
        *libc::__errno_location() = self.saved_errno;

        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            use std::arch::asm;
            let mut fp: u64;
            #[cfg(target_arch = "x86_64")]
            asm!(
                "mov {fp}, rbp",	// Move the value of rbp (frame pointer) into fp
                fp = out(reg) fp
            );

            #[cfg(target_arch = "aarch64")]
            asm!(
                "mov {0}, x29",		// Move the value of x29 (frame pointer) into fp
                out(reg) fp,
            );

            if self.bottom_fpp != 0 {
                *(self.bottom_fpp as *mut u64) = fp;
            } else {
                // bottom_fpp == 0 means this is the first poll of self, so we shouldn't
                // just overwrite the position because there stores the entry pointer
                self.saved_fp = fp;
            }
        }

        jump_fcontext(&mut self.poller_context, self.pollee_context, 0, 1);
        self.saved_errno = *libc::__errno_location();
        TLS_COROUTINE.set(prev);
        self.state
    }

    #[inline]
    pub(crate) fn create_key(destructor: Option<unsafe extern "C" fn(*mut c_void)>) -> u32 {
        let key = COROUTINE_KEY.fetch_add(1, Ordering::Relaxed);
        if let Some(destructor) = destructor {
            KEY_DESTRUCTORS
                .get_or_init(DashMap::new)
                .insert(key, destructor);
        }
        key
    }

    #[inline]
    pub(crate) fn set_specific(&mut self, k: u32, v: *mut c_void) {
        self.co_specific.insert(k, v);
    }

    #[inline]
    pub(crate) fn get_specific(&self, k: u32) -> *mut c_void {
        self.co_specific
            .get(&k)
            .map_or(std::ptr::null_mut(), |v| *v)
    }

    // #[inline]
    // pub(crate) unsafe fn delete_key(&mut self, k: u32) {
    //     if let Some(v) = self.co_specific.remove(&k) {
    //         if let Some(destructor) = KEY_DESTRUCTORS.get().unwrap().get(&k) {
    //             let destructor = destructor.value().to_owned();
    //             destructor(v);
    //         }
    //     }
    // }

    // Why use inline(never) here?
    //
    // The rust compiler may optimize the second thread local access, consider
    // the following operation sequence:
    // first get tls -> jump_fcontext -> thread switch -> second get tls
    // because the two tls_get are called in the same function, the compiler may
    // think the second get is useless and optimize it, such that the second tls_get
    // gets the address of tls in last thread, resulting in polluted memory
    #[inline]
    pub unsafe fn poll_until_ready<F: Future<Output = T>, T>(mut f: F) -> T {
        let tls_coroutine = Self::tls_coroutine();
        loop {
            let pinned_ref = Pin::new_unchecked(&mut f);
            match pinned_ref.poll(&mut *tls_coroutine.context) {
                Poll::Ready(res) => return res,
                Poll::Pending => tls_coroutine.pend_and_switch(),
            }
        }
    }
}

unsafe impl Send for CoroutineFuture {}
unsafe impl Sync for CoroutineFuture {}

impl Drop for CoroutineFuture {
    fn drop(&mut self) {
        assert_eq!(self.state, RunState::Done);
        let stack = self.stack.clone();
        if self.global {
            return_stack_to_global(stack);
        } else {
            return_stack(stack);
        }
        for (k, v) in &self.co_specific {
            if let Some(destructor) = KEY_DESTRUCTORS.get().unwrap().get(k) {
                let destructor = destructor.value().to_owned();
                unsafe { destructor(*v) };
            }
        }
    }
}

impl Future for CoroutineFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.stack.remove_stack_record() };

        match unsafe { self.run(cx) } {
            RunState::Runnable => unreachable!("runnable unexpected"),
            RunState::Pending => {
                unsafe {
                    self.stack
                        .record_stack(self.pollee_context, self.record_pending_time)
                };
                Poll::Pending
            }
            RunState::Yielded => {
                unsafe {
                    self.stack
                        .record_stack(self.pollee_context, self.record_pending_time)
                };
                let fut = tokio::task::yield_now();
                tokio::pin!(fut);
                fut.poll(cx)
            }
            RunState::Done => Poll::Ready(()),
        }
    }
}
