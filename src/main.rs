use std::{sync::Arc, time::SystemTime};
use uzfs::{uzfs_env_init, Dataset};

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(bench_main());
}

async fn bench_main() {
    let concurrency = std::env::args().nth(1).unwrap().parse().unwrap();
    let ninodes: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    uzfs_env_init().await;
    let ds = Arc::new(
        Dataset::init("testzp/ds1", "/dev/sdb", uzfs::DatasetType::Meta, 0)
            .await
            .unwrap(),
    );

    let mut handles = Vec::with_capacity(concurrency);
    let now = SystemTime::now();
    for _ in 0..concurrency {
        let ds = ds.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..ninodes {
                ds.create_inode(uzfs::InodeType::DIR).await.unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let micros = now.elapsed().unwrap().as_micros() as usize;
    let iops = concurrency * ninodes * 1000000 / micros;
    println!("iops: {iops}");
}
