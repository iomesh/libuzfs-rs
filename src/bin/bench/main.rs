use std::io::Result;
use std::ops::Deref;
use std::{sync::Arc, time::Instant};
use uzfs::*;

#[derive(Clone, Copy, Debug)]
enum BenchOp {
    Write,
    Read,
}

async fn worker(
    obj: u64,
    ds: Arc<DatasetWrapper>,
    blksize: u64,
    file_size: u64,
    sync: bool,
    op: BenchOp,
) {
    let mut offset = 0;
    let mut ino_hdl = ds.get_inode_handle(obj, u64::MAX, true).await.unwrap();
    while offset < file_size {
        let ds = ds.clone();
        ino_hdl = tokio::spawn(async move {
            match op {
                BenchOp::Write => {
                    let data = vec![1; blksize as usize];
                    ds.write_object(&ino_hdl, offset, sync, vec![&data])
                        .await
                        .unwrap();
                }
                BenchOp::Read => {
                    ds.read_object(&ino_hdl, offset, blksize).await.unwrap();
                }
            }
            ino_hdl
        })
        .await
        .unwrap();
        offset += blksize;
    }
    ds.release_inode_handle(&mut ino_hdl).await;
}

async fn bench(
    objs: &[u64],
    ds: Arc<DatasetWrapper>,
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

struct DatasetWrapper(Dataset);

impl FileSystem for DatasetWrapper {
    async fn init(ds: Dataset, _fsid: u32, _poolname: &str) -> Result<Self> {
        Ok(Self(ds))
    }

    async fn close(&mut self) -> Result<()> {
        self.0.close().await;
        Ok(())
    }
}

impl Deref for DatasetWrapper {
    type Target = Dataset;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tokio::main]
async fn main() {
    uzfs_env_init().await;
    let dev_path = std::env::args().nth(1).unwrap();
    let sync: bool = std::env::args().nth(2).unwrap().parse().unwrap();
    let concurrency = 64;
    let blksize = 1 << 20;
    let file_size = 1 << 30;

    let mut zp = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>("testzp", &[&dev_path])
        .await
        .unwrap();

    zp.create_filesystem(0).await.unwrap();
    let ds = zp.get_filesystem(0).unwrap();

    let objs = ds.create_objects(concurrency).await.unwrap().0;

    bench(&objs, ds.clone(), blksize, file_size, sync, BenchOp::Write).await;
    bench(&objs, ds.clone(), blksize, file_size, sync, BenchOp::Read).await;

    for obj in objs {
        let mut obj_hdl = ds.get_inode_handle(obj, u64::MAX, true).await.unwrap();
        ds.delete_object(&mut obj_hdl).await.unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    zp.close().await;
    uzfs_env_fini().await;
}
