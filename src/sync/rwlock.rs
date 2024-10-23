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
use crate::context::coroutine::CoroutineFuture;
use std::sync::atomic::Ordering::*;

const READ_LOCKED: u32 = 1;
const MASK: u32 = (1 << 30) - 1;
const WRITE_LOCKED: u32 = MASK;
const MAX_READERS: u32 = MASK - 1;
const READERS_WAITING: u32 = 1 << 30;
const WRITERS_WAITING: u32 = 1 << 31;

#[inline]
fn is_unlocked(state: u32) -> bool {
    state & MASK == 0
}

#[inline]
fn is_write_locked(state: u32) -> bool {
    state & MASK == WRITE_LOCKED
}

#[inline]
fn has_readers_waiting(state: u32) -> bool {
    state & READERS_WAITING != 0
}

#[inline]
fn has_writers_waiting(state: u32) -> bool {
    state & WRITERS_WAITING != 0
}

#[inline]
fn is_read_lockable(state: u32) -> bool {
    // This also returns false if the counter could overflow if we tried to read lock it.
    //
    // We don't allow read-locking if there's readers waiting, even if the lock is unlocked
    // and there's no writers waiting. The only situation when this happens is after unlocking,
    // at which point the unlocking thread might be waking up writers, which have priority over readers.
    // The unlocking thread will clear the readers waiting bit and wake up readers, if necessary.
    state & MASK < MAX_READERS && !has_readers_waiting(state) && !has_writers_waiting(state)
}

#[inline]
fn has_reached_max_readers(state: u32) -> bool {
    state & MASK == MAX_READERS
}

impl RwLock {
    #[inline]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            owner: 0,
            state: Futex::new(0),
            writer_notify: Futex::new(0),
        }
    }

    #[inline]
    pub fn try_read(&mut self) -> bool {
        self.state
            .value()
            .fetch_update(Acquire, Relaxed, |s| {
                is_read_lockable(s).then_some(s + READ_LOCKED)
            })
            .is_ok()
    }

    #[inline]
    pub fn read(&mut self) {
        let state = self.state.value().load(Relaxed);
        if !is_read_lockable(state)
            || self
                .state
                .value()
                .compare_exchange_weak(state, state + READ_LOCKED, Acquire, Relaxed)
                .is_err()
        {
            self.read_contended();
        }
    }

    #[inline]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn read_unlock(&mut self) {
        let state = self.state.value().fetch_sub(READ_LOCKED, Release) - READ_LOCKED;

        // It's impossible for a reader to be waiting on a read-locked RwLock,
        // except if there is also a writer waiting.
        debug_assert!(!has_readers_waiting(state) || has_writers_waiting(state));

        // Wake up a writer if we were the last reader and there's a writer waiting.
        if is_unlocked(state) && has_writers_waiting(state) {
            self.wake_writer_or_readers(state);
        }
    }

    #[inline]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn unlock(&mut self) {
        let state = self.state.value().load(Relaxed);
        self.state.inc_ref();
        if is_write_locked(state) {
            self.write_unlock();
        } else {
            self.read_unlock();
        }
        self.state.dec_ref();
    }

    #[inline]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn destroy(&mut self) {
        self.state.destroy();
    }

    #[cold]
    fn read_contended(&mut self) {
        let mut state = self.spin_read();

        loop {
            // If we can lock it, lock it.
            if is_read_lockable(state) {
                match self.state.value().compare_exchange_weak(
                    state,
                    state + READ_LOCKED,
                    Acquire,
                    Relaxed,
                ) {
                    Ok(_) => return, // Locked!
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }

            // Check for overflow.
            if has_reached_max_readers(state) {
                panic!("too many active read locks on RwLock");
            }

            // Make sure the readers waiting bit is set before we go to sleep.
            if !has_readers_waiting(state) {
                if let Err(s) = self.state.value().compare_exchange(
                    state,
                    state | READERS_WAITING,
                    Relaxed,
                    Relaxed,
                ) {
                    state = s;
                    continue;
                }
            }

            // Wait for the state to change.
            unsafe { self.state.wait_until(state | READERS_WAITING, None) };

            // Spin again after waking up.
            state = self.spin_read();
        }
    }

    #[inline]
    pub fn try_write(&mut self) -> bool {
        let res = self
            .state
            .value()
            .fetch_update(Acquire, Relaxed, |s| {
                is_unlocked(s).then_some(s + WRITE_LOCKED)
            })
            .is_ok();
        if res {
            self.owner = CoroutineFuture::tls_coroutine().id;
        }
        res
    }

    #[inline]
    pub fn write(&mut self) {
        if self
            .state
            .value()
            .compare_exchange_weak(0, WRITE_LOCKED, Acquire, Relaxed)
            .is_err()
        {
            self.write_contended();
        }
        self.owner = CoroutineFuture::tls_coroutine().id;
    }

    #[inline]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn write_unlock(&mut self) {
        self.owner = 0;
        let state = self.state.value().fetch_sub(WRITE_LOCKED, Release) - WRITE_LOCKED;

        debug_assert!(is_unlocked(state));

        if has_writers_waiting(state) || has_readers_waiting(state) {
            self.wake_writer_or_readers(state);
        }
    }

    #[cold]
    fn write_contended(&mut self) {
        let mut state = self.spin_write();

        let mut other_writers_waiting = 0;

        loop {
            // If it's unlocked, we try to lock it.
            if is_unlocked(state) {
                match self.state.value().compare_exchange_weak(
                    state,
                    state | WRITE_LOCKED | other_writers_waiting,
                    Acquire,
                    Relaxed,
                ) {
                    Ok(_) => return, // Locked!
                    Err(s) => {
                        state = s;
                        continue;
                    }
                }
            }

            // Set the waiting bit indicating that we're waiting on it.
            if !has_writers_waiting(state) {
                if let Err(s) = self.state.value().compare_exchange(
                    state,
                    state | WRITERS_WAITING,
                    Relaxed,
                    Relaxed,
                ) {
                    state = s;
                    continue;
                }
            }

            // Other writers might be waiting now too, so we should make sure
            // we keep that bit on once we manage lock it.
            other_writers_waiting = WRITERS_WAITING;

            // Examine the notification counter before we check if `state` has changed,
            // to make sure we don't miss any notifications.
            let seq = self.writer_notify.value().load(Acquire);

            // Don't go to sleep if the lock has become available,
            // or if the writers waiting bit is no longer set.
            state = self.state.value().load(Relaxed);
            if is_unlocked(state) || !has_writers_waiting(state) {
                continue;
            }

            // Wait for the state to change.
            unsafe { self.writer_notify.wait_until(seq, None) };

            // Spin again after waking up.
            state = self.spin_write();
        }
    }

    /// Wake up waiting threads after unlocking.
    ///
    /// If both are waiting, this will wake up only one writer, but will fall
    /// back to waking up readers if there was no writer to wake up.
    #[cold]
    fn wake_writer_or_readers(&mut self, mut state: u32) {
        assert!(is_unlocked(state));

        // The readers waiting bit might be turned on at any point now,
        // since readers will block when there's anything waiting.
        // Writers will just lock the lock though, regardless of the waiting bits,
        // so we don't have to worry about the writer waiting bit.
        //
        // If the lock gets locked in the meantime, we don't have to do
        // anything, because then the thread that locked the lock will take
        // care of waking up waiters when it unlocks.

        // If only writers are waiting, wake one of them up.
        if state == WRITERS_WAITING {
            match self
                .state
                .value()
                .compare_exchange(state, 0, Relaxed, Relaxed)
            {
                Ok(_) => {
                    self.wake_writer();
                    return;
                }
                Err(s) => {
                    // Maybe some readers are now waiting too. So, continue to the next `if`.
                    state = s;
                }
            }
        }

        // If both writers and readers are waiting, leave the readers waiting
        // and only wake up one writer.
        if state == READERS_WAITING + WRITERS_WAITING {
            if self
                .state
                .value()
                .compare_exchange(state, READERS_WAITING, Relaxed, Relaxed)
                .is_err()
            {
                // The lock got locked. Not our problem anymore.
                return;
            }
            if self.wake_writer() {
                return;
            }
            // No writers were actually blocked on futex_wait, so we continue
            // to wake up readers instead, since we can't be sure if we notified a writer.
            state = READERS_WAITING;
        }

        // If readers are waiting, wake them all up.
        if state == READERS_WAITING
            && self
                .state
                .value()
                .compare_exchange(state, 0, Relaxed, Relaxed)
                .is_ok()
        {
            unsafe { self.state.wake_all() };
        }
    }

    /// This wakes one writer and returns true if we woke up a writer that was
    /// blocked on futex_wait.
    ///
    /// If this returns false, it might still be the case that we notified a
    /// writer that was about to go to sleep.
    fn wake_writer(&mut self) -> bool {
        self.writer_notify.value().fetch_add(1, Release);
        unsafe { self.writer_notify.wake_one() }
        // Note that FreeBSD and DragonFlyBSD don't tell us whether they woke
        // up any threads or not, and always return `false` here. That still
        // results in correct behaviour: it just means readers get woken up as
        // well in case both readers and writers were waiting.
    }

    /// Spin for a while, but stop directly at the given condition.
    #[inline]
    fn spin_until(&mut self, f: impl Fn(u32) -> bool) -> u32 {
        let mut spin = 100; // Chosen by fair dice roll.
        loop {
            let state = self.state.value().load(Relaxed);
            if f(state) || spin == 0 {
                return state;
            }
            std::hint::spin_loop();
            spin -= 1;
        }
    }

    #[inline]
    fn spin_write(&mut self) -> u32 {
        // Stop spinning when it's unlocked or when there's waiting writers, to keep things somewhat fair.
        self.spin_until(|state| is_unlocked(state) || has_writers_waiting(state))
    }

    #[inline]
    fn spin_read(&mut self) -> u32 {
        // Stop spinning when it's unlocked or read locked, or when there's waiting threads.
        self.spin_until(|state| {
            !is_write_locked(state) || has_readers_waiting(state) || has_writers_waiting(state)
        })
    }

    #[inline]
    pub fn write_held(&mut self) -> bool {
        self.owner == CoroutineFuture::tls_coroutine().id
    }

    #[inline]
    pub fn read_held(&mut self) -> bool {
        let state = self.state.value().load(Relaxed);
        (state & MASK) > 0
    }
}
