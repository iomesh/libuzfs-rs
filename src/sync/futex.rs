use crate::bindings::sys::*;
use crate::context::coroutine::AsyncCoroutine;
use futures::{Future, FutureExt};
use std::hint;
use std::mem::offset_of;
use std::pin::Pin;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::time::timeout;

struct ListNode {
    prev: *mut ListNode,
    next: *mut ListNode,
}

impl ListNode {
    fn new() -> Self {
        Self {
            prev: null_mut(),
            next: null_mut(),
        }
    }
}

#[repr(C)]
struct WaiterNode {
    node: ListNode,
    waker: Mutex<Option<Waker>>,
}

impl WaiterNode {
    fn new() -> Self {
        Self {
            node: ListNode::new(),
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

    #[inline]
    unsafe fn push_back(&mut self, node: &mut WaiterNode) {
        assert_eq!(libc::pthread_spin_lock(&mut self.lock), 0);
        node.node.next = null_mut();
        node.node.prev = self.tail as *mut ListNode;
        self.tail = &mut node.node as *mut ListNode as *mut libc::c_void;
        if self.head.is_null() {
            self.head = self.tail;
        }
        unsafe { assert_eq!(libc::pthread_spin_unlock(&mut self.lock), 0) };
    }

    #[inline]
    unsafe fn pop_and_wake_front(&mut self) -> bool {
        if self.head.is_null() {
            false
        } else {
            if self.head == self.tail {
                self.tail = null_mut();
            }
            let waiter = &*(self.head.byte_sub(offset_of!(WaiterNode, node)) as *mut WaiterNode);
            waiter.waker.lock().unwrap().take().unwrap().wake_by_ref();

            self.head = waiter.node.next as *mut libc::c_void;

            true
        }
    }

    #[inline]
    unsafe fn remove(&mut self, node: &mut ListNode) {
        if let Some(prev) = node.prev.as_mut() {
            prev.next = node.next;
        }
        if let Some(next) = node.next.as_mut() {
            next.prev = node.prev;
        }
        if self.head == self.tail {
            self.head = null_mut();
            self.tail = null_mut();
        }
    }

    #[inline]
    unsafe fn wake_one(&mut self) -> bool {
        assert_eq!(libc::pthread_spin_lock(&mut self.lock), 0);
        let res = self.pop_and_wake_front();
        unsafe { assert_eq!(libc::pthread_spin_unlock(&mut self.lock), 0) };
        res
    }

    #[inline]
    unsafe fn wake_all(&mut self) {
        assert_eq!(libc::pthread_spin_lock(&mut self.lock), 0);
        while self.pop_and_wake_front() {}
        unsafe { assert_eq!(libc::pthread_spin_unlock(&mut self.lock), 0) };
    }
}

struct FutexWaiter<'a> {
    node: WaiterNode,
    expected_value: u32,
    futex: &'a mut Futex,
    queued: bool,
}

impl Future for FutexWaiter<'_> {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut = unsafe { self.get_unchecked_mut() };
        if !self_mut.queued {
            let cur_val = self_mut.futex.value().load(Ordering::Relaxed);
            if cur_val != self_mut.expected_value {
                return Poll::Ready(libc::EWOULDBLOCK);
            }

            unsafe { assert_eq!(libc::pthread_spin_lock(&mut self_mut.futex.waiters.lock), 0) };
            let cur_val = self_mut.futex.value().load(Ordering::Relaxed);
            if cur_val != self_mut.expected_value {
                unsafe {
                    assert_eq!(
                        libc::pthread_spin_unlock(&mut self_mut.futex.waiters.lock),
                        0
                    )
                };
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
            unsafe {
                assert_eq!(
                    libc::pthread_spin_unlock(&mut self_mut.futex.waiters.lock),
                    0
                )
            };
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

impl Drop for FutexWaiter<'_> {
    fn drop(&mut self) {
        if self.queued {
            unsafe { assert_eq!(libc::pthread_spin_lock(&mut self.futex.waiters.lock), 0) };
            let waker = self.node.waker.lock().unwrap();
            if waker.is_some() {
                drop(waker);
                unsafe { self.futex.waiters.remove(&mut self.node.node) };
            } else {
                unsafe { self.futex.waiters.wake_one() };
            }
            unsafe { assert_eq!(libc::pthread_spin_unlock(&mut self.futex.waiters.lock), 0) };
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
    fn wait(&mut self, expected_value: u32) -> FutexWaiter {
        FutexWaiter {
            node: WaiterNode::new(),
            expected_value,
            futex: self,
            queued: false,
        }
    }

    #[inline]
    pub(crate) unsafe fn wait_until(
        &mut self,
        expected_value: u32,
        duration: Option<Duration>,
    ) -> bool {
        let waiter = self.wait(expected_value).fuse();
        if let Some(duration) = duration {
            AsyncCoroutine::tls_coroutine()
                .poll_until_ready(timeout(duration, waiter))
                .is_ok()
        } else {
            AsyncCoroutine::tls_coroutine().poll_until_ready(waiter);
            true
        }
    }

    #[inline]
    pub(crate) unsafe fn wake_one(&mut self) -> bool {
        unsafe { self.waiters.wake_one() }
    }

    // TODO(sundengyu): implement futex requeue
    #[inline]
    pub(crate) unsafe fn wake_all(&mut self) {
        self.waiters.wake_all();
    }

    #[inline]
    pub(crate) unsafe fn destroy(&mut self) {
        let mut nspin = 0;
        let ref_cnt = unsafe { AtomicU32::from_ptr(&mut self.ref_cnt) };
        while ref_cnt.load(Ordering::Acquire) > 0 {
            hint::spin_loop();
            nspin += 1;
            if nspin % 100 == 0 {
                AsyncCoroutine::tls_coroutine().sched_yield();
            }
        }
        assert_eq!(libc::pthread_spin_destroy(&mut self.waiters.lock), 0);
    }
}
