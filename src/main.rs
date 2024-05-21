use nix::{
    sys::wait::{waitpid, WaitStatus},
    unistd::{fork, ForkResult},
};
use std::{
    process::{abort, exit},
    sync::Arc,
    time::SystemTime,
};
use uzfs::{uzfs_env_init, Dataset};

async fn test_main(dev_path: &str, last_obj: u64) -> u64 {
    uzfs_env_init().await;
    let concurrency = 64;
    let blksize = 1 << 18;
    let blks = 1 << 15;
    let ds = Arc::new(
        Dataset::init("testzp/ds", dev_path, uzfs::DatasetType::Data, 0, false)
            .await
            .unwrap(),
    );

    if last_obj != 0 {
        ds.get_object_attr(last_obj).await.unwrap_err();
    }

    let obj = ds.create_objects(1).await.unwrap().0[0];
    let now = SystemTime::now();
    let mut handles = Vec::with_capacity(concurrency as usize);
    for mut offset in 0..concurrency {
        let ds = ds.clone();
        handles.push(tokio::spawn(async move {
            let data = vec![1; blksize as usize];
            while offset < blks {
                let write_off = offset * blksize;
                ds.write_object(obj, write_off, true, vec![&data])
                    .await
                    .unwrap();
                offset += concurrency;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    println!(
        "write {obj} cost {}s, attr: {:?}",
        now.elapsed().unwrap().as_secs(),
        ds.get_object_attr(obj).await.unwrap()
    );

    let now = SystemTime::now();
    ds.delete_object(obj).await.unwrap();
    ds.wait_log_commit().await;
    println!("delete {obj} cost {}s", now.elapsed().unwrap().as_secs());

    obj
}

fn main() {
    let dev_path = std::env::args().nth(1).unwrap();
    let mut last_obj = 0;

    for _ in 0..10 {
        match unsafe { fork() }.unwrap() {
            ForkResult::Parent { child, .. } => {
                let status = waitpid(child, None).unwrap();
                if let WaitStatus::Exited(_, res) = status {
                    last_obj = res as u64;
                } else {
                    println!("{status:?} not expected");
                    abort();
                }
            }
            ForkResult::Child => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                exit(rt.block_on(test_main(&dev_path, last_obj)) as i32);
            }
        }
    }
}
