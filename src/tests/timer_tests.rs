use std::time::{Duration, Instant};

use rand::{thread_rng, Rng};

use crate::time::*;

fn duration_to_timespec(value: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: value.as_secs() as i64,
        tv_nsec: value.subsec_nanos() as i64,
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn sleep_test() {
    init_timer();
    for i in 0..10 {
        let handles: Vec<_> = (0..100)
            .map(|_| {
                tokio::spawn(async move {
                    let duration_us = thread_rng().gen_range(1..10) * 10;
                    let duration = Duration::from_micros(duration_us);
                    let now = Instant::now();
                    sleep(duration_to_timespec(duration)).await;
                    let elapsed = now.elapsed();
                    let bias = elapsed.abs_diff(duration);
                    //     assert!(bias <= 1000, "elapsed: {elapsed:?}, duration: {duration:?}, bias: {bias}us, iter: {i}");
                    println!(
                        "elapsed: {elapsed:?}, duration: {duration:?}, bias: {bias:?}, iter: {i}"
                    )
                })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn timeout_test() {
    init_timer();
    for _ in 0..10 {
        let handles: Vec<_> = (0..100)
            .map(|_| {
                tokio::spawn(async move {
                    let to = Duration::from_millis(thread_rng().gen_range(1..10) * 10);
                    let task_pending = Duration::from_millis(thread_rng().gen_range(1..10) * 10);
                    let res =
                        timeout(duration_to_timespec(to), tokio::time::sleep(task_pending)).await;
                    if task_pending != to {
                        assert_eq!(
                            res.is_none(),
                            to < task_pending,
                            "timeout: {to:?}, task_pending: {task_pending:?}"
                        );
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }
    }
}
