// Copyright (c) 2010-2024 The Rust Project Developers. 
// Copyright (c) 2024 IOMesh Inc
// 
// Licensed under either of
// 
//     * Apache License, Version 2.0, (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
//     * MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)
// 
// at your option. This file may not be copied, modified, or distributed except according to
// those terms.

use crate::bindings::sys::*;
use crate::context::coroutine::CoroutineFuture;
use std::sync::atomic::Ordering::*;

impl Mutex {
    #[inline]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            owner: 0,
            futex: Futex::new(0),
        }
    }

    #[inline]
    pub fn try_lock(&mut self) -> bool {
        let res = self
            .futex
            .value()
            .compare_exchange(0, 1, Acquire, Relaxed)
            .is_ok();
        if res {
            self.owner = CoroutineFuture::tls_coroutine().id;
        }
        res
    }

    #[inline]
    pub fn lock(&mut self) {
        if self
            .futex
            .value()
            .compare_exchange(0, 1, Acquire, Relaxed)
            .is_err()
        {
            self.lock_contended();
        }
        self.owner = CoroutineFuture::tls_coroutine().id;
    }

    #[cold]
    fn lock_contended(&mut self) {
        // Spin first to speed things up if the lock is released quickly.
        let mut state = self.spin();

        // If it's unlocked now, attempt to take the lock
        // without marking it as contended.
        if state == 0 {
            match self.futex.value().compare_exchange(0, 1, Acquire, Relaxed) {
                Ok(_) => return, // Locked!
                Err(s) => state = s,
            }
        }

        loop {
            // Put the lock in contended state.
            // We avoid an unnecessary write if it as already set to 2,
            // to be friendlier for the caches.
            if state != 2 && self.futex.value().swap(2, Acquire) == 0 {
                // We changed it from 0 to 2, so we just successfully locked it.
                return;
            }

            unsafe { self.futex.wait_until(2, None) };

            // Spin again after waking up.
            state = self.spin();
        }
    }

    fn spin(&mut self) -> u32 {
        let mut spin = 100;
        loop {
            // We only use `load` (and not `swap` or `compare_exchange`)
            // while spinning, to be easier on the caches.
            let state = self.futex.value().load(Relaxed);

            // We stop spinning when the mutex is unlocked (0),
            // but also when it's contended (2).
            if state != 1 || spin == 0 {
                return state;
            }

            std::hint::spin_loop();
            spin -= 1;
        }
    }

    #[inline]
    pub fn unlock(&mut self) {
        self.owner = 0;
        self.futex.inc_ref();
        if self.futex.value().swap(0, Release) == 2 {
            // We only wake up one coroutine. When that coroutine locks the mutex, it
            // will mark the mutex as contended (2) (see lock_contended above),
            // which makes sure that any other waiting coroutines will also be
            // woken up eventually.
            self.wake();
        }
        self.futex.dec_ref();
    }

    #[inline]
    fn wake(&mut self) {
        unsafe { self.futex.wake_one() };
    }

    #[inline]
    pub fn destroy(&mut self) {
        unsafe { self.futex.destroy() };
    }

    #[inline]
    pub fn held_by_me(&mut self) -> bool {
        self.owner == CoroutineFuture::tls_coroutine().id
    }
}
