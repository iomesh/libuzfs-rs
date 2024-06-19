use std::sync::Arc;
use uzfs::{uzfs_env_fini, uzfs_env_init, Dataset, DatasetType};

#[tokio::main]
async fn main() {
    let dev_path = std::env::args().nth(1).unwrap();
    let concurrency = 64;
    let blksize = 1 << 20;
    let file_size = 2 << 30;
    uzfs_env_init().await;
    let ds = Arc::new(
        Dataset::init("testzp/ds", &dev_path, DatasetType::Data, 0, false)
            .await
            .unwrap(),
    );
    println!("{:?}", ds.space().await);

    let objs = ds.create_objects(concurrency).await.unwrap().0;
    let handles: Vec<_> = objs
        .iter()
        .map(|obj| {
            let ds = ds.clone();
            let obj = *obj;
            tokio::spawn(async move {
                let data = vec![1; blksize as usize];
                for i in 0..262144 {
                    let mut off = 0;
                    while off < file_size {
                        ds.write_object(obj, off, false, vec![&data]).await.unwrap();
                        off += blksize;
                    }
                    println!("obj: {obj}, iter: {i} finished");
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}
