use crate::bindings::sys::*;
use std::{ptr::NonNull, time::Duration};

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_held(mutex: *mut Mutex) -> i32 {
    (*mutex).held_by_me() as i32
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_init(mutex: *mut Mutex) {
    *mutex = Mutex::new();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_destroy(mutex: *mut Mutex) {
    (*mutex).destroy();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_lock(mutex: *mut Mutex) {
    (*mutex).lock();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_trylock(mutex: *mut Mutex) -> i32 {
    if (*mutex).try_lock() {
        1
    } else {
        0
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_mutex_unlock(mutex: *mut Mutex) {
    (*mutex).unlock();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_init(cond: *mut CondVar) {
    (*cond) = CondVar {
        futex: Futex::new(0),
    };
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_destroy(cond: *mut CondVar) {
    (*cond).destroy();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_wait(cond: *mut CondVar, mutex: *mut Mutex) -> i32 {
    (*cond).wait(NonNull::new_unchecked(mutex));
    0
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_timedwait(
    cond: *mut CondVar,
    mutex: *mut Mutex,
    duration: *const timespec,
) -> i32 {
    let duration = &*duration;
    let duration = Duration::new(duration.tv_sec as u64, duration.tv_nsec as u32);
    if (*cond).wait_timeout(NonNull::new_unchecked(mutex), duration) {
        0
    } else {
        libc::ETIMEDOUT
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_signal(cond: *mut CondVar) -> i32 {
    (*cond).notify_one();
    0
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_cond_broadcast(cond: *mut CondVar) -> i32 {
    (*cond).notify_all();
    0
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_read_held(rwlock: *mut RwLock) -> i32 {
    (*rwlock).read_held() as i32
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_write_held(rwlock: *mut RwLock) -> i32 {
    (*rwlock).write_held() as i32
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_init(rwlock: *mut RwLock) {
    *rwlock = RwLock::new();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_destroy(rwlock: *mut RwLock) {
    (*rwlock).destroy();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rw_lock_write(rwlock: *mut RwLock) {
    (*rwlock).write();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rw_lock_read(rwlock: *mut RwLock) {
    (*rwlock).read();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_try_write(rwlock: *mut RwLock) -> i32 {
    if (*rwlock).try_write() {
        0
    } else {
        libc::EBUSY
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rwlock_try_read(rwlock: *mut RwLock) -> i32 {
    if (*rwlock).try_read() {
        0
    } else {
        libc::EBUSY
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn co_rw_unlock(rwlock: *mut RwLock) {
    (*rwlock).unlock();
}
