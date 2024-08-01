use crate::bindings::sys::*;
use std::sync::atomic::Ordering::*;
use std::{ptr::NonNull, time::Duration};

impl CondVar {
    // All the memory orderings here are `Relaxed`,
    // because synchronization is done by unlocking and locking the mutex.

    pub fn notify_one(&mut self) {
        self.futex.inc_ref();
        self.futex.value().fetch_add(1, Relaxed);
        unsafe { self.futex.wake_one() };
        self.futex.dec_ref();
    }

    pub fn notify_all(&mut self) {
        self.futex.inc_ref();
        self.futex.value().fetch_add(1, Relaxed);
        unsafe { self.futex.wake_all() };
        self.futex.dec_ref();
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn wait(&mut self, mutex: NonNull<Mutex>) {
        self.wait_optional_timeout(mutex, None);
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn wait_timeout(&mut self, mutex: NonNull<Mutex>, duration: Duration) -> bool {
        self.wait_optional_timeout(mutex, Some(duration))
    }

    unsafe fn wait_optional_timeout(
        &mut self,
        mut mutex: NonNull<Mutex>,
        duration: Option<Duration>,
    ) -> bool {
        // Examine the notification counter _before_ we unlock the mutex.
        let futex_value = self.futex.value().load(Relaxed);

        self.futex.inc_ref();
        // Unlock the mutex before going to sleep.
        mutex.as_mut().unlock();

        // Wait, but only if there hasn't been any
        // notification since we unlocked the mutex.
        let res = self.futex.wait_until(futex_value, duration);
        self.futex.dec_ref();

        // Lock the mutex again.
        mutex.as_mut().lock();

        res
    }

    pub fn destroy(&mut self) {
        unsafe { self.futex.destroy() };
    }
}
