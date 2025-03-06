// Copyright (c) 2010-2024 The Rust Project Developers.
// Copyright (c) 2024 IOMesh Inc.
//
// Portions of this file are derived from the Rust standard library, originally licensed
// under both the MIT License and the Apache License, Version 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.
//
// Modifications made by IOMesh Inc in 2024.

use crate::bindings::sys::*;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::*;

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
    pub unsafe fn wait_timeout(&mut self, mutex: NonNull<Mutex>, duration: libc::timespec) -> bool {
        self.wait_optional_timeout(mutex, Some(duration))
    }

    unsafe fn wait_optional_timeout(
        &mut self,
        mut mutex: NonNull<Mutex>,
        duration: Option<libc::timespec>,
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
