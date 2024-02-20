#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(deref_nullptr)]

#[rustfmt::skip]
pub mod bindings;
pub mod async_sys;
pub mod coroutine;

#[cfg(test)]
mod tests {
    use crate::{bindings::*, coroutine::UzfsCoroutineFuture};
    use rand::Rng;
    use std::{
        os::raw::c_void,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[derive(Default)]
    struct BlockQueue {
        mutex: co_mutex_t,
        cv_pop: co_cond_t,
        cv_push: co_cond_t,
        queue: Vec<u64>,
        capacity: usize,
    }

    impl BlockQueue {
        pub fn init(&mut self, capacity: usize) {
            unsafe {
                co_mutex_init(&mut self.mutex);
                co_cond_init(&mut self.cv_pop);
                co_cond_init(&mut self.cv_push);
            }
            self.queue.reserve(capacity);
            self.capacity = capacity;
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

    #[derive(Default)]
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
        let nops_per_worker = 1 << 20;

        let mut bq_test = BlockQueueTest {
            bq: BlockQueue::default(),
            sum_consumer: AtomicU64::new(0),
            sum_producer: AtomicU64::new(0),
            nops_per_worker,
        };
        bq_test.bq.init(queue_capacity);

        let arg_usize = &mut bq_test as *mut BlockQueueTest as usize;
        let mut handles = Vec::new();
        for _ in 0..workers {
            let coroutine = UzfsCoroutineFuture::new(block_queue_producer, arg_usize, false, false);
            handles.push(tokio::spawn(coroutine));

            let coroutine = UzfsCoroutineFuture::new(block_queue_consumer, arg_usize, false, false);
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

    #[derive(Default)]
    struct RwlockTest {
        rwlock: co_rw_lock_t,
        current: u64,

        max: u64,
    }

    unsafe extern "C" fn reader(arg: *mut c_void) {
        let rwlock_test = (arg as *mut RwlockTest).as_mut().unwrap();
        loop {
            co_rw_lock_read(&mut rwlock_test.rwlock);
            let current = rwlock_test.current;
            for _ in 0..10 {
                assert_eq!(current, rwlock_test.current);
            }
            co_rw_lock_exit(&mut rwlock_test.rwlock);

            if current >= rwlock_test.max {
                break;
            }

            coroutine_sched_yield();
        }
    }

    unsafe extern "C" fn writer(arg: *mut c_void) {
        let rwlock_test = (arg as *mut RwlockTest).as_mut().unwrap();
        loop {
            co_rw_lock_write(&mut rwlock_test.rwlock);
            if rwlock_test.current < rwlock_test.max {
                rwlock_test.current += 1;
            }
            let exit = rwlock_test.max <= rwlock_test.current;
            co_rw_lock_exit(&mut rwlock_test.rwlock);

            if exit {
                break;
            }

            coroutine_sched_yield();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_write_lock_test() {
        let mut rwlock_test = RwlockTest::default();
        unsafe { co_rw_lock_init(&mut rwlock_test.rwlock) };

        let readers = 64;
        let writers = 16;
        rwlock_test.max = 16 << 20;

        let mut handles = Vec::new();
        let arg_usize = &mut rwlock_test as *mut RwlockTest as usize;
        for _ in 0..readers {
            let coroutine = UzfsCoroutineFuture::new(reader, arg_usize, false, false);
            handles.push(tokio::spawn(coroutine));
        }
        for _ in 0..writers {
            let coroutine = UzfsCoroutineFuture::new(writer, arg_usize, false, false);
            handles.push(tokio::spawn(coroutine));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(rwlock_test.max, rwlock_test.current);
    }
}
