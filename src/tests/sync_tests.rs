use crate::bindings::sys::*;
use crate::context::coroutine::AsyncCoroutine;
use crate::context::coroutine_c::co_sleep;
use crate::sync::sync_c::{
    co_cond_broadcast, co_cond_destroy, co_cond_wait, co_mutex_destroy, co_mutex_lock,
    co_mutex_unlock, co_rw_lock_read, co_rw_lock_write, co_rw_unlock,
};
use libc::c_void;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};

struct LockedValue {
    lock: Mutex,
    value: i32,
}

unsafe extern "C" fn mutex_worker(arg: *mut c_void) {
    let value = &mut *(arg as *mut LockedValue);
    co_mutex_lock(&mut value.lock);
    value.value += 1;
    co_mutex_unlock(&mut value.lock);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutex_test() {
    let concurrency = 256;
    for _ in 0..10000 {
        let mut value = LockedValue {
            lock: Mutex::new(),
            value: 0,
        };
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let coroutine = AsyncCoroutine::new(mutex_worker, &mut value as *mut _ as usize, false);
            handles.push(tokio::spawn(coroutine));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(value.value, concurrency as i32);
    }
}

struct RwlockedValue {
    lock: RwLock,
    value: i32,
}

unsafe extern "C" fn rwlock_reader(arg: *mut c_void) {
    let value = &mut *(arg as *mut RwlockedValue);
    co_rw_lock_read(&mut value.lock);
    let orig_val = value.value;
    let duration = timespec {
        tv_sec: 0,
        tv_nsec: 1000000,
    };
    co_sleep(&duration);
    assert_eq!(orig_val, value.value);
    co_rw_unlock(&mut value.lock);
}

unsafe extern "C" fn rwlock_writer(arg: *mut c_void) {
    let value = &mut *(arg as *mut RwlockedValue);
    co_rw_lock_write(&mut value.lock);
    value.value += 1;
    co_rw_unlock(&mut value.lock);
}

#[tokio::test(flavor = "multi_thread")]
async fn rwlock_test() {
    let concurrency = 1024;
    for _ in 0..100 {
        let mut value = RwlockedValue {
            lock: RwLock::new(),
            value: 0,
        };
        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let coroutine =
                AsyncCoroutine::new(rwlock_writer, &mut value as *mut _ as usize, false);
            handles.push(tokio::spawn(coroutine));

            let coroutine =
                AsyncCoroutine::new(rwlock_reader, &mut value as *mut _ as usize, false);
            handles.push(tokio::spawn(coroutine));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(value.value, concurrency as i32);
    }
}

struct BlockQueue {
    mutex: Mutex,
    cv_pop: CondVar,
    cv_push: CondVar,
    queue: Vec<u64>,
    capacity: usize,
}

impl BlockQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            mutex: Mutex::new(),
            cv_pop: CondVar {
                futex: Futex::new(0),
            },
            cv_push: CondVar {
                futex: Futex::new(0),
            },
            queue: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub unsafe fn pop(&mut self) -> u64 {
        co_mutex_lock(&mut self.mutex);
        while self.queue.is_empty() {
            co_cond_wait(&mut self.cv_pop, &mut self.mutex);
        }

        let res = self.queue.pop().unwrap();
        if self.queue.len() + 1 == self.capacity {
            co_cond_broadcast(&mut self.cv_push);
        }
        co_mutex_unlock(&mut self.mutex);

        res
    }

    pub unsafe fn push(&mut self, value: u64) {
        co_mutex_lock(&mut self.mutex);
        while self.queue.len() >= self.capacity {
            assert_eq!(self.queue.len(), self.capacity);
            co_cond_wait(&mut self.cv_push, &mut self.mutex);
        }

        self.queue.push(value);
        if self.queue.len() == 1 {
            co_cond_broadcast(&mut self.cv_pop);
        }
        co_mutex_unlock(&mut self.mutex);
    }
}

impl Drop for BlockQueue {
    fn drop(&mut self) {
        unsafe {
            co_mutex_destroy(&mut self.mutex);
            co_cond_destroy(&mut self.cv_pop);
            co_cond_destroy(&mut self.cv_push);
        }
    }
}

struct BlockQueueTest {
    bq: BlockQueue,
    sum_producer: AtomicU64,
    sum_consumer: AtomicU64,
    // same for producers and consumers
    nops_per_worker: usize,
}

unsafe extern "C" fn block_queue_consumer(queue: *mut c_void) {
    let bq_test = (queue as *mut BlockQueueTest).as_mut().unwrap();
    for _ in 0..bq_test.nops_per_worker {
        let value = bq_test.bq.pop();
        bq_test.sum_producer.fetch_add(value, Ordering::Relaxed);
    }
}

unsafe extern "C" fn block_queue_producer(queue: *mut c_void) {
    let bq_test = (queue as *mut BlockQueueTest).as_mut().unwrap();
    for _ in 0..bq_test.nops_per_worker {
        let value = rand::thread_rng().gen_range(1..(1 << 20));
        bq_test.bq.push(value);
        bq_test.sum_consumer.fetch_add(value, Ordering::Relaxed);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn block_queue_test() {
    let workers = 32;
    let queue_capacity = 20;
    let nops_per_worker = 1 << 10;

    let mut bq_test = BlockQueueTest {
        bq: BlockQueue::new(queue_capacity),
        sum_consumer: AtomicU64::new(0),
        sum_producer: AtomicU64::new(0),
        nops_per_worker,
    };

    let mut handles = Vec::new();
    for _ in 0..workers {
        let coroutine =
            AsyncCoroutine::new(block_queue_producer, &mut bq_test as *mut _ as usize, false);
        handles.push(tokio::spawn(coroutine));

        let coroutine =
            AsyncCoroutine::new(block_queue_consumer, &mut bq_test as *mut _ as usize, false);
        handles.push(tokio::spawn(coroutine));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(
        bq_test.sum_consumer.load(Ordering::Relaxed),
        bq_test.sum_producer.load(Ordering::Relaxed)
    );
}
