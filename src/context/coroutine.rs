use super::libcontext::*;
use super::stack::*;
use dashmap::DashMap;
use futures::Future;
use libc::{c_void, intptr_t};
use once_cell::sync::OnceCell;
use std::arch::asm;
use std::cell::Cell;
use std::collections::HashMap;
use std::mem::transmute;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::Context;
use std::task::Poll;

unsafe extern "C" fn task_runner_c(_: intptr_t) {
    let tls_coroutine = AsyncCoroutine::tls_coroutine();

    #[cfg(target_arch = "x86_64")]
    {
        let mut fp: u64;
        asm!(
            "mov {fp}, rbp",
            fp = out(reg) fp
        );
        tls_coroutine.bottom_fpp = fp as usize;
        *(fp as *mut usize) = tls_coroutine.saved_fp;
    }

    (tls_coroutine.func)(tls_coroutine.arg);
    tls_coroutine.exit();
}

pub(crate) static COROUTINE_KEY: AtomicU32 = AtomicU32::new(0);
static KEY_DESTRUCTORS: OnceCell<DashMap<u32, unsafe extern "C" fn(*mut c_void)>> = OnceCell::new();

thread_local! {
    static TLS_COROUTINE: Cell<usize> = const { Cell::new(0) };
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum RunState {
    Runnable,
    Pending,
    Yielded,
    Done,
}

pub struct AsyncCoroutine {
    poller_context: fcontext_t,
    pollee_context: fcontext_t,
    stack: Stack,
    context: Option<NonNull<Context<'static>>>,
    state: RunState,
    arg: *mut c_void,
    func: unsafe extern "C" fn(*mut c_void),
    specific: HashMap<u32, *mut c_void>,
    saved_errno: i32,
    pub(crate) id: u64,
    foreground: bool,

    // in some cases, coroutine is created in thread t1 and dropped
    // in thread t2, which will fill tls stack pool of t2 but this
    // stack pool will never be popped, so we need the global flag
    // to return stack to global pool when coroutine is dropped
    pub(super) global: bool,

    #[cfg(target_arch = "x86_64")]
    bottom_fpp: usize,
    #[cfg(target_arch = "x86_64")]
    saved_fp: usize,
}

// using 1m stack size can prevent gdb from reporting corrupted stack
const STACK_SIZE: usize = 1 << 20;

impl AsyncCoroutine {
    #[inline]
    pub fn tls_coroutine<'a>() -> &'a mut Self {
        unsafe { &mut *(TLS_COROUTINE.get() as *mut Self) }
    }

    #[inline]
    pub fn new(
        func: unsafe extern "C" fn(arg1: *mut c_void),
        arg: usize,
        foreground: bool,
    ) -> Self {
        let stack = fetch_or_alloc_stack(STACK_SIZE);
        let pollee_context =
            unsafe { make_fcontext(stack.stack_bottom, STACK_SIZE, Some(task_runner_c)) };

        // this position stores the finish addr which will be executed when task_runner_c returns,
        // in order to backtrace to the poll function in backtrace, this addr is replaced with Self::poll
        #[cfg(target_arch = "x86_64")]
        unsafe {
            *(stack.stack_bottom.byte_sub(8) as *mut usize) = Self::poll as usize + 8
        };

        Self {
            poller_context: ptr::null_mut(),
            pollee_context,
            id: stack.stack_id,
            stack,
            context: None,
            state: RunState::Runnable,
            arg: arg as *mut c_void,
            func,
            specific: HashMap::new(),
            saved_errno: 0,
            foreground,
            global: false,

            #[cfg(target_arch = "x86_64")]
            bottom_fpp: 0,
            #[cfg(target_arch = "x86_64")]
            saved_fp: 0,
        }
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
        TLS_COROUTINE.set(self as *mut _ as usize);
        let cx: &mut Context<'static> = transmute(cx);
        self.context.replace(NonNull::from(cx));
        *libc::__errno_location() = self.saved_errno;

        #[cfg(target_arch = "x86_64")]
        {
            let mut fp: u64;
            asm!(
                "mov {fp}, rbp",
                fp = out(reg) fp
            );

            if self.bottom_fpp != 0 {
                *(self.bottom_fpp as *mut u64) = fp;
            } else {
                // bottom_fpp == 0 means this is the first poll of self, so we shouldn't
                // just overwrite the position because there stores the entry pointer
                self.saved_fp = fp as usize;
            }
        }

        jump_fcontext(&mut self.poller_context, self.pollee_context, 0, 1);
        self.saved_errno = *libc::__errno_location();
        TLS_COROUTINE.set(0);
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
        self.specific.insert(k, v);
    }

    #[inline]
    pub(crate) fn get_specific(&self, k: u32) -> *mut c_void {
        self.specific.get(&k).map_or(std::ptr::null_mut(), |v| *v)
    }

    // #[inline]
    // pub(crate) unsafe fn delete_key(&mut self, k: u32) {
    //     if let Some(v) = self.specific.remove(&k) {
    //         if let Some(destructor) = KEY_DESTRUCTORS.get().unwrap().get(&k) {
    //             let destructor = destructor.value().to_owned();
    //             destructor(v);
    //         }
    //     }
    // }

    #[inline]
    pub unsafe fn poll_until_ready<F: Future<Output = T>, T>(&mut self, mut f: F) -> T {
        loop {
            let pinned_ref = Pin::new_unchecked(&mut f);
            match pinned_ref.poll(self.context.unwrap().as_mut()) {
                Poll::Ready(res) => return res,
                Poll::Pending => self.pend_and_switch(),
            }
        }
    }
}

unsafe impl Send for AsyncCoroutine {}
unsafe impl Sync for AsyncCoroutine {}

impl Drop for AsyncCoroutine {
    fn drop(&mut self) {
        assert_eq!(self.state, RunState::Done);
        let stack = self.stack.clone();
        match self.global {
            true => return_stack_to_global(stack, !self.foreground),
            false => return_stack(stack, !self.foreground),
        };
        for (k, v) in &self.specific {
            if let Some(destructor) = KEY_DESTRUCTORS.get().unwrap().get(k) {
                let destructor = destructor.value().to_owned();
                unsafe { destructor(*v) };
            }
        }
    }
}

impl Future for AsyncCoroutine {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.stack.remove_stack_record() };

        match unsafe { self.run(cx) } {
            RunState::Runnable => unreachable!("runnable unexpected"),
            RunState::Pending => {
                unsafe { self.stack.record_stack(self.pollee_context) };
                Poll::Pending
            }
            RunState::Yielded => {
                unsafe { self.stack.record_stack(self.pollee_context) };
                let fut = tokio::task::yield_now();
                tokio::pin!(fut);
                fut.poll(cx)
            }
            RunState::Done => Poll::Ready(()),
        }
    }
}
