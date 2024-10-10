use crate::bindings::sys::*;
#[cfg(not(feature = "thread"))]
use crate::context::coroutine::CoroutineFuture;
#[cfg(not(feature = "thread"))]
use futures::Future;
use std::hint;
#[cfg(not(feature = "thread"))]
use std::pin::Pin;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(feature = "thread"))]
use std::sync::Mutex;
#[cfg(not(feature = "thread"))]
use std::task::{Context, Poll, Waker};
use std::time::Duration;
#[cfg(not(feature = "thread"))]
use tokio::time::timeout;

#[repr(C)]
#[cfg(not(feature = "thread"))]
struct WaiterNode {
    prev: *mut WaiterNode,
    next: *mut WaiterNode,
    waker: Mutex<Option<Waker>>,
}

#[cfg(not(feature = "thread"))]
impl WaiterNode {
    fn new() -> Self {
        Self {
            prev: null_mut(),
            next: null_mut(),
            waker: Mutex::new(None),
        }
    }
}

impl WaiterList {
    #[inline]
    fn new() -> Self {
        let mut lock = 0;
        assert_eq!(unsafe { libc::pthread_spin_init(&mut lock, 0) }, 0);
        Self {
            head: null_mut(),
            tail: null_mut(),
            lock,
        }
    }
}

#[cfg(not(feature = "thread"))]
impl WaiterList {
    #[inline]
    unsafe fn push_back(&mut self, node: &mut WaiterNode) {
        node.next = null_mut();
        node.prev = self.tail as *mut _;
        if let Some(tail) = (self.tail as *mut WaiterNode).as_mut() {
            tail.next = node;
        }
        self.tail = node as *mut _ as *mut libc::c_void;
        if self.head.is_null() {
            self.head = self.tail;
        }
    }

    #[inline]
    unsafe fn pop_and_wake_front(&mut self) -> bool {
        if self.head.is_null() {
            false
        } else {
            if self.head == self.tail {
                self.tail = null_mut();
            }
            let waiter = &*(self.head as *mut WaiterNode);
            self.head = waiter.next as *mut _;
            if let Some(next) = waiter.next.as_mut() {
                next.prev = null_mut();
            }
            waiter.waker.lock().unwrap().take().unwrap().wake_by_ref();

            true
        }
    }

    #[inline]
    unsafe fn remove(&mut self, node: &mut WaiterNode) {
        if self.head == node as *mut _ as *mut libc::c_void {
            self.head = node.next as *mut libc::c_void;
        }
        if self.tail == node as *mut _ as *mut libc::c_void {
            self.tail = node.prev as *mut libc::c_void;
        }

        if let Some(prev) = node.prev.as_mut() {
            prev.next = node.next;
        }
        if let Some(next) = node.next.as_mut() {
            next.prev = node.prev;
        }
    }

    #[inline]
    unsafe fn wake_one(&mut self) -> bool {
        self.lock();
        let res = self.pop_and_wake_front();
        self.unlock();
        res
    }

    #[inline]
    unsafe fn wake_all(&mut self) {
        self.lock();
        while self.pop_and_wake_front() {}
        self.unlock();
    }

    #[inline]
    unsafe fn lock(&mut self) {
        unsafe { assert_eq!(libc::pthread_spin_lock(&mut self.lock), 0) };
    }

    #[inline]
    unsafe fn unlock(&mut self) {
        unsafe { assert_eq!(libc::pthread_spin_unlock(&mut self.lock), 0) };
    }
}

#[cfg(not(feature = "thread"))]
struct FutexWaiter<'a> {
    node: WaiterNode,
    expected_value: u32,
    futex: &'a mut Futex,
    queued: bool,
}

#[cfg(not(feature = "thread"))]
impl Future for FutexWaiter<'_> {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut = unsafe { self.get_unchecked_mut() };
        if !self_mut.queued {
            let cur_val = self_mut.futex.value().load(Ordering::Relaxed);
            if cur_val != self_mut.expected_value {
                return Poll::Ready(libc::EWOULDBLOCK);
            }

            unsafe { self_mut.futex.waiters.lock() };
            let cur_val = self_mut.futex.value().load(Ordering::Relaxed);
            if cur_val != self_mut.expected_value {
                unsafe { self_mut.futex.waiters.unlock() };
                return Poll::Ready(libc::EWOULDBLOCK);
            }

            self_mut.queued = true;
            assert!(self_mut
                .node
                .waker
                .lock()
                .unwrap()
                .replace(cx.waker().clone())
                .is_none());
            unsafe { self_mut.futex.waiters.push_back(&mut self_mut.node) };
            unsafe { self_mut.futex.waiters.unlock() };
            Poll::Pending
        } else {
            let mut waker = self_mut.node.waker.lock().unwrap();
            match waker.as_mut() {
                None => {
                    self_mut.queued = false;
                    Poll::Ready(0)
                }
                Some(w) => {
                    if !w.will_wake(cx.waker()) {
                        waker.replace(cx.waker().clone());
                    }
                    Poll::Pending
                }
            }
        }
    }
}

#[cfg(not(feature = "thread"))]
impl Drop for FutexWaiter<'_> {
    fn drop(&mut self) {
        if self.queued {
            unsafe { self.futex.waiters.lock() };
            let waker = self.node.waker.lock().unwrap();
            if waker.is_some() {
                drop(waker);
                unsafe { self.futex.waiters.remove(&mut self.node) };
            }
            unsafe { self.futex.waiters.unlock() };
        }
    }
}

impl Futex {
    #[inline]
    pub(crate) fn new(value: u32) -> Self {
        Self {
            waiters: WaiterList::new(),
            value,
            ref_cnt: 0,
        }
    }

    #[inline]
    pub(crate) fn value(&mut self) -> &AtomicU32 {
        unsafe { AtomicU32::from_ptr(&mut self.value) }
    }

    #[inline]
    pub(crate) fn inc_ref(&mut self) {
        let ref_cnt = unsafe { AtomicU32::from_ptr(&mut self.ref_cnt) };
        ref_cnt.fetch_add(1, Ordering::Acquire);
    }

    #[inline]
    pub(crate) fn dec_ref(&mut self) {
        let ref_cnt = unsafe { AtomicU32::from_ptr(&mut self.ref_cnt) };
        ref_cnt.fetch_sub(1, Ordering::Release);
    }

    #[inline]
    #[cfg(not(feature = "thread"))]
    fn wait(&mut self, expected_value: u32) -> FutexWaiter {
        FutexWaiter {
            node: WaiterNode::new(),
            expected_value,
            futex: self,
            queued: false,
        }
    }

    #[inline]
    #[cfg(not(feature = "thread"))]
    pub(crate) unsafe fn wait_until(
        &mut self,
        expected_value: u32,
        duration: Option<Duration>,
    ) -> bool {
        let waiter = self.wait(expected_value);
        if let Some(duration) = duration {
            CoroutineFuture::poll_until_ready(timeout(duration, waiter)).is_ok()
        } else {
            CoroutineFuture::poll_until_ready(waiter);
            true
        }
    }

    #[cfg(feature = "thread")]
    pub(crate) unsafe fn wait_until(
        &mut self,
        expected_value: u32,
        duration: Option<Duration>,
    ) -> bool {
        use std::{io::Error, mem::MaybeUninit, ptr::null};
        const NANOSEC: i64 = 1000000000;

        let deadline = duration.map(|d| {
            let mut t = MaybeUninit::uninit();
            if libc::clock_gettime(libc::CLOCK_MONOTONIC, t.as_mut_ptr()) == -1 {
                panic!("get time failed, err: {}", Error::last_os_error());
            }
            let mut t = t.assume_init();
            t.tv_nsec += d.as_nanos() as i64;
            t.tv_sec += t.tv_nsec / NANOSEC;
            t.tv_nsec %= NANOSEC;
            t
        });

        loop {
            let res = libc::syscall(
                libc::SYS_futex,
                &self.value as *const _,
                libc::FUTEX_WAIT_BITSET | libc::FUTEX_PRIVATE_FLAG,
                expected_value,
                deadline
                    .as_ref()
                    .map_or(null(), |t| t as *const libc::timespec),
                null::<u32>(), // This argument is unused for FUTEX_WAIT_BITSET.
                !0u32,         // A full bitmask, to make it behave like a regular FUTEX_WAIT.
            );

            match (res < 0).then(|| *libc::__errno_location() as i32) {
                Some(libc::ETIMEDOUT) => return false,
                Some(libc::EINTR) => continue,
                _ => return true,
            }
        }
    }

    #[inline]
    #[cfg(not(feature = "thread"))]
    pub(crate) unsafe fn wake_one(&mut self) -> bool {
        unsafe { self.waiters.wake_one() }
    }

    #[cfg(feature = "thread")]
    pub(crate) unsafe fn wake_one(&mut self) -> bool {
        let ptr = &self.value as *const _;
        let op = libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG;
        libc::syscall(libc::SYS_futex, ptr, op, 1) > 0
    }

    // TODO(sundengyu): implement futex requeue
    #[inline]
    #[cfg(not(feature = "thread"))]
    pub(crate) unsafe fn wake_all(&mut self) {
        self.waiters.wake_all();
    }

    #[cfg(feature = "thread")]
    pub(crate) unsafe fn wake_all(&mut self) {
        let ptr = &self.value as *const _;
        let op = libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG;
        libc::syscall(libc::SYS_futex, ptr, op, i32::MAX);
    }

    // Why do we need a destroy function like this?
    //
    // In traditional c usage of mutex or conditional varialbles, the waiter may free the memory
    // of futex after it is waken, but this waken up may be triggered by a atomic fetch add of
    // the futex value, so the waker may want to access the waiter after the fetch_add. To avoid
    // potential memory use after free, we need to call inc_ref before wake, and the waiter should
    // wait until the ref count reduces to 0
    #[inline]
    #[cfg(not(feature = "thread"))]
    pub(crate) unsafe fn destroy(&mut self) {
        let mut nspin = 0;
        let ref_cnt = unsafe { AtomicU32::from_ptr(&mut self.ref_cnt) };
        while ref_cnt.load(Ordering::Acquire) > 0 {
            hint::spin_loop();
            nspin += 1;
            if nspin % 100 == 0 {
                CoroutineFuture::tls_coroutine().sched_yield();
            }
        }
        assert_eq!(libc::pthread_spin_destroy(&mut self.waiters.lock), 0);
    }

    #[cfg(feature = "thread")]
    pub(crate) unsafe fn destroy(&mut self) {
        let mut nspin = 0;
        let ref_cnt = unsafe { AtomicU32::from_ptr(&mut self.ref_cnt) };
        while ref_cnt.load(Ordering::Acquire) > 0 {
            hint::spin_loop();
            nspin += 1;
            if nspin % 100 == 0 {
                libc::sched_yield();
            }
        }
        assert_eq!(libc::pthread_spin_destroy(&mut self.waiters.lock), 0);
    }
}

#[cfg(test)]
#[cfg(not(feature = "thread"))]
mod tests {
    use crate::bindings::sys::Futex;
    use futures::{task::noop_waker, Future};
    use std::task::Context;

    #[test]
    fn futex_test() {
        let mut futex = Futex::new(1);
        let futex_ptr = &mut futex as *mut Futex;

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let waiter = unsafe { (*futex_ptr).wait(1) };
        tokio::pin!(waiter);
        assert!(waiter.poll(&mut cx).is_pending());
        unsafe { assert!((*futex_ptr).wake_one()) };

        {
            let waiter = unsafe { (*futex_ptr).wait(1) };
            tokio::pin!(waiter);
            assert!(waiter.poll(&mut cx).is_pending());
        }
        unsafe { assert!(!(*futex_ptr).wake_one()) };

        {
            let waiter = unsafe { (*futex_ptr).wait(1) };
            tokio::pin!(waiter);
            assert!(waiter.poll(&mut cx).is_pending());
            unsafe { assert!((*futex_ptr).wake_one()) };
        }
        unsafe { assert!(!(*futex_ptr).wake_one()) };
    }
}
