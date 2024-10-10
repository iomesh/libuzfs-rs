use std::future::Future;

use libc::c_void;

pub struct CoroutineFuture {
    arg: *mut c_void,
    func: unsafe extern "C" fn(*mut c_void),
}

pub struct ThreadID {
    pub id: u64,
}

impl CoroutineFuture {
    #[inline]
    // this is only used in uzdb, other usage should call new
    pub(crate) fn new_with_stack_size(
        func: unsafe extern "C" fn(arg1: *mut c_void),
        arg: usize,
        _stack_size: usize,
    ) -> Self {
        Self {
            func,
            arg: arg as *mut _,
        }
    }

    #[inline]
    pub fn new(func: unsafe extern "C" fn(arg1: *mut c_void), arg: usize) -> Self {
        Self::new_with_stack_size(func, arg, 123)
    }

    #[inline]
    pub fn tls_coroutine() -> ThreadID {
        ThreadID {
            id: unsafe { libc::pthread_self() },
        }
    }
}

unsafe impl Send for CoroutineFuture {}
unsafe impl Sync for CoroutineFuture {}

impl Future for CoroutineFuture {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unsafe { (self.func)(self.arg) };
        std::task::Poll::Ready(())
    }
}
