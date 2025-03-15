use std::{sync::Arc, time::Instant};
use uzfs::*;

#[derive(Clone, Copy, Debug)]
enum BenchOp {
    Write,
    Read,
}

async fn worker(obj: u64, ds: Arc<Dataset>, blksize: u64, file_size: u64, sync: bool, op: BenchOp) {
    let mut offset = 0;
    let mut ino_hdl = ds.get_inode_handle(obj, u64::MAX, true).await.unwrap();
    let mut data = vec![1; blksize as usize];
    while offset < file_size {
        let ds = ds.clone();
        (ino_hdl, data) = tokio::spawn(async move {
            match op {
                BenchOp::Write => {
                    ds.write_object(&ino_hdl, offset, sync, vec![&data])
                        .await
                        .unwrap();
                }
                BenchOp::Read => {
                    ds.read_object(&ino_hdl, offset, blksize).await.unwrap();
                }
            }
            (ino_hdl, data)
        })
        .await
        .unwrap();
        offset += blksize;
    }
    ds.release_inode_handle(&mut ino_hdl).await;
}

async fn bench(
    objs: &[u64],
    ds: Arc<Dataset>,
    blksize: u64,
    file_size: u64,
    sync: bool,
    op: BenchOp,
) {
    let now = Instant::now();
    let handles: Vec<_> = objs
        .iter()
        .map(|obj| tokio::spawn(worker(*obj, ds.clone(), blksize, file_size, sync, op)))
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }
    let micros = now.elapsed().as_micros() as u64;

    let throughput = (file_size * objs.len() as u64 * 1000000 / micros) >> 20;
    println!("{op:?} throughput: {throughput}MB/s");
}

#[tokio::main]
async fn main() {
    uzfs_env_init().await;
    let dev_path = std::env::args().nth(1).unwrap();
    let sync: bool = std::env::args().nth(2).unwrap().parse().unwrap();
    config_uzfs(4 << 30, 10, true);
    let concurrency = 64;
    let blksize = 1 << 20;
    let file_size = 256 << 20;

    let ds = Arc::new(
        Dataset::init("testzp/ds", &dev_path, DatasetType::Data, 0, false)
            .await
            .unwrap(),
    );

    let objs = ds.create_objects(concurrency).await.unwrap().0;

    bench(&objs, ds.clone(), blksize, file_size, sync, BenchOp::Write).await;
    ds.wait_synced().await;
    bench(&objs, ds.clone(), blksize, file_size, sync, BenchOp::Read).await;

    for obj in objs {
        let mut obj_hdl = ds.get_inode_handle(obj, u64::MAX, true).await.unwrap();
        ds.delete_object(&mut obj_hdl).await.unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}
