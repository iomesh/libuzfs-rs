use super::UzfsTestEnv;
use crate::uzfs_env_fini;
use crate::uzfs_env_init;
use crate::Dataset;
use crate::FileSystem;
use crate::InodeType;
use crate::ZpoolOpenOptions;
use crate::ZpoolType;
use std::future::ready;
use std::io::Result;
use std::ops::Deref;
use std::sync::Arc;

struct DatasetWrapper(Dataset);

impl FileSystem for DatasetWrapper {
    async fn init(ds: Dataset, _fsid: u32, _snapid: Option<u32>, _poolname: &str) -> Result<Self> {
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

// 基础快照测试 - 创建和删除
#[tokio::test]
async fn snapshot_basic_test() {
    let poolname = "snapshot_basic_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create objects and write data
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let mut obj_hdl = ds.get_inode_handle(objs[0], gen, true).await.unwrap();
    let data = vec![1u8; 64 << 10];
    ds.write_object(&obj_hdl, 0, false, vec![&data])
        .await
        .unwrap();
    ds.release_inode_handle(&mut obj_hdl).await;

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Snapshot created successfully");

    // Delete snapshot
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    println!("Snapshot destroyed successfully");

    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    zpool.get_or_open_snapshot(fsid, 1).await.unwrap();
    zpool.destroy_filesystem(fsid).await.unwrap();

    zpool.close().await;
    uzfs_env_fini().await;
}

// Meta 类型 zpool 的快照测试 - 测试元数据操作
#[tokio::test]
async fn snapshot_meta_operations_test() {
    let poolname = "snapshot_meta_ops_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Meta)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create multiple FILE inodes
    let mut inodes = vec![];
    for _ in 0..5 {
        let mut ino_hdl = ds.create_inode(InodeType::FILE).await.unwrap();
        let ino = ino_hdl.ino;
        let gen = ino_hdl.gen;
        ds.release_inode_handle(&mut ino_hdl).await;
        inodes.push((ino, gen));
    }
    println!("Created {} FILE inodes", inodes.len());

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Snapshot created");

    // Delete some inodes in active dataset
    for i in 0..3 {
        let (ino, gen) = inodes[i];
        let mut ino_hdl = ds.get_inode_handle(ino, gen, false).await.unwrap();
        ds.delete_inode(&mut ino_hdl, InodeType::FILE)
            .await
            .unwrap();
        ds.release_inode_handle(&mut ino_hdl).await;
    }

    // Verify all inodes still exist in snapshot
    let snap_ds = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();
    for (ino, gen) in &inodes {
        let mut snap_hdl = snap_ds.get_inode_handle(*ino, *gen, false).await.unwrap();
        let snap_attr = snap_ds.get_attr(&snap_hdl).await.unwrap();
        assert_eq!(snap_attr.gen, *gen);
        snap_ds.release_inode_handle(&mut snap_hdl).await;
    }
    println!("Verified all {} inodes exist in snapshot", inodes.len());

    drop(snap_ds);
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// Data 类型 zpool 的快照测试 - 测试数据操作
#[tokio::test]
async fn snapshot_data_operations_test() {
    use rand::Rng;
    let poolname = "snapshot_data_ops_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();
    let mut rng = rand::thread_rng();

    // Create multiple data objects
    let (objs, gen) = ds.create_objects(5).await.unwrap();
    let mut objects = vec![];

    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();

        // Write data with random sizes
        let size = rng.gen_range(8192..131072);
        let data = vec![i as u8; size];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;

        objects.push((obj, gen, size, i as u8));
    }
    println!("Created {} data objects", objects.len());

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Snapshot created");

    // Modify all data in active dataset
    for (obj, gen, size, _) in &objects {
        let mut obj_hdl = ds.get_inode_handle(*obj, *gen, true).await.unwrap();
        let new_data = vec![200u8; *size];
        ds.write_object(&obj_hdl, 0, false, vec![&new_data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Delete some objects in active dataset
    for i in 0..3 {
        let (obj, gen, _, _) = objects[i];
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        ds.delete_object(&mut obj_hdl).await.unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Verify snapshot has original data
    let snap_ds = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();
    for (obj, gen, size, original_value) in &objects {
        let mut snap_obj_hdl = snap_ds.get_inode_handle(*obj, *gen, true).await.unwrap();
        let snap_data = snap_ds
            .read_object(&snap_obj_hdl, 0, *size as u64)
            .await
            .unwrap();
        assert_eq!(snap_data, vec![*original_value; *size]);
        snap_ds.release_inode_handle(&mut snap_obj_hdl).await;
    }
    println!(
        "Verified all {} objects exist in snapshot with original data",
        objects.len()
    );

    drop(snap_ds);
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试 get_or_open_snapshot 接口
#[tokio::test]
async fn snapshot_open_test() {
    let poolname = "snapshot_open_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create one object
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let obj = objs[0];

    // Create 3 snapshots with different data
    for i in 0..3 {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 16 << 10];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;

        let snapid = i + 1;
        zpool.create_snapshot(fsid, snapid).await.unwrap();
        println!("Created snapshot {}", snapid);
    }

    // Open all snapshots simultaneously using get_or_open_snapshot
    let snap1 = zpool.get_or_open_snapshot(fsid, 1).await.unwrap();
    let snap2 = zpool.get_or_open_snapshot(fsid, 2).await.unwrap();
    let snap3 = zpool.get_or_open_snapshot(fsid, 3).await.unwrap();
    println!("Opened all 3 snapshots simultaneously");

    // Verify each snapshot has correct data
    let mut hdl1 = snap1.get_inode_handle(obj, gen, true).await.unwrap();
    let buf1 = snap1.read_object(&hdl1, 0, 16 << 10).await.unwrap();
    assert_eq!(buf1, vec![0u8; 16 << 10]);
    snap1.release_inode_handle(&mut hdl1).await;

    let mut hdl2 = snap2.get_inode_handle(obj, gen, true).await.unwrap();
    let buf2 = snap2.read_object(&hdl2, 0, 16 << 10).await.unwrap();
    assert_eq!(buf2, vec![1u8; 16 << 10]);
    snap2.release_inode_handle(&mut hdl2).await;

    let mut hdl3 = snap3.get_inode_handle(obj, gen, true).await.unwrap();
    let buf3 = snap3.read_object(&hdl3, 0, 16 << 10).await.unwrap();
    assert_eq!(buf3, vec![2u8; 16 << 10]);
    snap3.release_inode_handle(&mut hdl3).await;

    println!("Verified all snapshots have correct isolated data");

    // Test reopening same snapshot multiple times
    drop(snap1);
    let snap1_reopen = zpool.get_or_open_snapshot(fsid, 1).await.unwrap();
    let mut hdl1_reopen = snap1_reopen.get_inode_handle(obj, gen, true).await.unwrap();
    let buf1_reopen = snap1_reopen
        .read_object(&hdl1_reopen, 0, 16 << 10)
        .await
        .unwrap();
    assert_eq!(buf1_reopen, vec![0u8; 16 << 10]);
    snap1_reopen.release_inode_handle(&mut hdl1_reopen).await;
    println!("Verified snapshot can be reopened");

    drop(snap1_reopen);
    drop(snap2);
    drop(snap3);
    drop(ds);

    for snapid in 1..=3 {
        zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    }

    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试快照回滚
#[tokio::test]
async fn snapshot_rollback_test() {
    let poolname = "snapshot_rollback_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create multiple objects with different data
    let (objs, gen) = ds.create_objects(5).await.unwrap();

    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 16 << 10];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Snapshot created with {} objects", objs.len());

    // Modify all objects
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![(i + 10) as u8; 16 << 10];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Verify modified data
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let buf = ds.read_object(&obj_hdl, 0, 16 << 10).await.unwrap();
        assert_eq!(buf, vec![(i + 10) as u8; 16 << 10]);
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Rollback to snapshot
    drop(ds);
    zpool.rollback_to_snapshot(fsid, snapid).await.unwrap();
    println!("Rolled back to snapshot");

    // Verify data is restored
    let ds = zpool.get_or_open_filesystem(fsid, false).await.unwrap();
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let buf = ds.read_object(&obj_hdl, 0, 16 << 10).await.unwrap();
        assert_eq!(buf, vec![i as u8; 16 << 10]);
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试 zpool close 后重新 open - Data 类型
#[tokio::test]
async fn snapshot_reopen_data_test() {
    let poolname = "snapshot_reopen_data_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;

    let mut saved_objects = vec![];

    // First session: create objects and snapshot
    {
        let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
            .create(true)
            .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
            .await
            .unwrap();

        let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

        // Create objects with data
        let (objs, gen) = ds.create_objects(3).await.unwrap();
        for (i, &obj) in objs.iter().enumerate() {
            let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
            let data = vec![i as u8; 32 << 10];
            ds.write_object(&obj_hdl, 0, false, vec![&data])
                .await
                .unwrap();
            ds.release_inode_handle(&mut obj_hdl).await;
            saved_objects.push((obj, gen, i as u8));
        }

        // Create snapshot
        let snapid = 1;
        zpool.create_snapshot(fsid, snapid).await.unwrap();
        println!(
            "Created snapshot with {} objects before close",
            saved_objects.len()
        );

        drop(ds);
        zpool.close().await;
    }

    // Second session: reopen and verify snapshot
    {
        let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
            .create(false)
            .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
            .await
            .unwrap();

        // Open snapshot after reopen
        let snap_ds = zpool.get_or_open_snapshot(fsid, 1).await.unwrap();

        // Verify snapshot data is correct
        for (obj, gen, original_value) in &saved_objects {
            let mut snap_obj_hdl = snap_ds.get_inode_handle(*obj, *gen, true).await.unwrap();
            let snap_data = snap_ds
                .read_object(&snap_obj_hdl, 0, 32 << 10)
                .await
                .unwrap();
            assert_eq!(snap_data, vec![*original_value; 32 << 10]);
            snap_ds.release_inode_handle(&mut snap_obj_hdl).await;
        }
        println!(
            "Verified all {} objects have correct data in snapshot after reopen",
            saved_objects.len()
        );

        drop(snap_ds);
        zpool.destroy_snapshot(fsid, 1).await.unwrap();
        zpool.close().await;
    }

    uzfs_env_fini().await;
}

// 测试 zpool close 后重新 open - Meta 类型
#[tokio::test]
async fn snapshot_reopen_meta_test() {
    let poolname = "snapshot_reopen_meta_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;

    let mut saved_inodes = vec![];

    // First session: create inodes and snapshot
    {
        let mut zpool = ZpoolOpenOptions::new(ZpoolType::Meta)
            .create(true)
            .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
            .await
            .unwrap();

        let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

        // Create FILE inodes
        for _ in 0..3 {
            let mut ino_hdl = ds.create_inode(InodeType::FILE).await.unwrap();
            saved_inodes.push((ino_hdl.ino, ino_hdl.gen));
            ds.release_inode_handle(&mut ino_hdl).await;
        }

        // Create snapshot
        let snapid = 1;
        zpool.create_snapshot(fsid, snapid).await.unwrap();
        println!(
            "Created snapshot with {} inodes before close",
            saved_inodes.len()
        );

        drop(ds);
        zpool.close().await;
    }

    // Second session: reopen and verify snapshot
    {
        let mut zpool = ZpoolOpenOptions::new(ZpoolType::Meta)
            .create(false)
            .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
            .await
            .unwrap();

        // Open snapshot after reopen
        let snap_ds = zpool.get_or_open_snapshot(fsid, 1).await.unwrap();

        // Verify inodes exist in snapshot
        for (ino, gen) in &saved_inodes {
            let mut snap_hdl = snap_ds.get_inode_handle(*ino, *gen, false).await.unwrap();
            let snap_attr = snap_ds.get_attr(&snap_hdl).await.unwrap();
            assert_eq!(snap_attr.gen, *gen);
            snap_ds.release_inode_handle(&mut snap_hdl).await;
        }
        println!(
            "Verified all {} inodes exist in snapshot after reopen",
            saved_inodes.len()
        );

        drop(snap_ds);
        zpool.destroy_snapshot(fsid, 1).await.unwrap();
        zpool.close().await;
    }

    uzfs_env_fini().await;
}

// 测试多个 filesystem 的快照 - Data 类型
#[tokio::test]
async fn snapshot_multi_filesystem_data_test() {
    let poolname = "snapshot_multi_fs_data_test";
    let uzfs_test_env = UzfsTestEnv::new(2048 * 1024 * 1024);
    uzfs_env_init().await;

    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    // Create data in multiple filesystems
    let mut fs_objects = vec![];
    for fsid in 0..3 {
        let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

        let (objs, gen) = ds.create_objects(2).await.unwrap();
        for (i, &obj) in objs.iter().enumerate() {
            let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
            let data = vec![(fsid * 10 + i as u32) as u8; 16 << 10];
            ds.write_object(&obj_hdl, 0, false, vec![&data])
                .await
                .unwrap();
            ds.release_inode_handle(&mut obj_hdl).await;
        }

        fs_objects.push((fsid, objs, gen));
        drop(ds);
    }

    // Create snapshots for each filesystem
    for fsid in 0..3 {
        let snapid = fsid + 1;
        zpool.create_snapshot(fsid, snapid).await.unwrap();
        println!("Created snapshot {} for filesystem {}", snapid, fsid);
    }

    // Verify each snapshot
    for (fsid, objs, gen) in &fs_objects {
        let snapid = fsid + 1;
        let snap_ds = zpool.get_or_open_snapshot(*fsid, snapid).await.unwrap();

        for (i, &obj) in objs.iter().enumerate() {
            let mut snap_obj_hdl = snap_ds.get_inode_handle(obj, *gen, true).await.unwrap();
            let snap_data = snap_ds
                .read_object(&snap_obj_hdl, 0, 16 << 10)
                .await
                .unwrap();
            assert_eq!(snap_data, vec![(fsid * 10 + i as u32) as u8; 16 << 10]);
            snap_ds.release_inode_handle(&mut snap_obj_hdl).await;
        }

        drop(snap_ds);
        println!("Verified snapshot {} for filesystem {}", snapid, fsid);
    }

    // Destroy all snapshots
    for fsid in 0..3 {
        let snapid = fsid + 1;
        zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    }

    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试快照链 - Data 类型
#[tokio::test]
async fn snapshot_chain_data_test() {
    let poolname = "snapshot_chain_data_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;

    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create one object
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let obj = objs[0];

    // Create chain of snapshots with incremental changes
    for i in 0..5 {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 32 << 10];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;

        let snapid = i + 1;
        zpool.create_snapshot(fsid, snapid).await.unwrap();
        println!("Created snapshot {} with value {}", snapid, i);
    }

    // Verify each snapshot has correct data
    for i in 0..5 {
        let snapid = i + 1;
        let snap_ds = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();
        let mut snap_obj_hdl = snap_ds.get_inode_handle(obj, gen, true).await.unwrap();
        let snap_data = snap_ds
            .read_object(&snap_obj_hdl, 0, 32 << 10)
            .await
            .unwrap();
        assert_eq!(snap_data, vec![i as u8; 32 << 10]);
        snap_ds.release_inode_handle(&mut snap_obj_hdl).await;
        drop(snap_ds);
        println!("Verified snapshot {} has correct data", snapid);
    }

    // Destroy snapshots in reverse order
    drop(ds);
    for i in (0..5).rev() {
        let snapid = i + 1;
        zpool.destroy_snapshot(fsid, snapid).await.unwrap();
        println!("Destroyed snapshot {}", snapid);
    }

    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试混合操作 - 创建、修改、删除、回滚
#[tokio::test]
async fn snapshot_complex_operations_test() {
    use rand::Rng;
    let poolname = "snapshot_complex_ops_test";
    let uzfs_test_env = UzfsTestEnv::new(2048 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;

    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();
    let mut rng = rand::thread_rng();

    // Create initial objects
    let (objs, gen) = ds.create_objects(5).await.unwrap();
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let size = rng.gen_range(4096..32768);
        let data = vec![i as u8; size];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Create first snapshot
    zpool.create_snapshot(fsid, 1).await.unwrap();
    println!("Created snapshot 1");

    // Modify some objects
    for i in 0..3 {
        let mut obj_hdl = ds.get_inode_handle(objs[i], gen, true).await.unwrap();
        let data = vec![(i + 10) as u8; 8192];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }

    // Create second snapshot
    zpool.create_snapshot(fsid, 2).await.unwrap();
    println!("Created snapshot 2");

    // Delete some objects
    for i in 0..2 {
        let mut obj_hdl = ds.get_inode_handle(objs[i], gen, true).await.unwrap();
        ds.delete_object(&mut obj_hdl).await.unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }
    ds.wait_synced().await;

    // To rollback to snapshot 1, must destroy snapshot 2 first (ZFS only allows rollback to latest snapshot)
    drop(ds);
    zpool.destroy_snapshot(fsid, 2).await.unwrap();
    println!("Destroyed snapshot 2 to make snapshot 1 the latest");

    // Now rollback to snapshot 1 (which is now the latest)
    zpool.rollback_to_snapshot(fsid, 1).await.unwrap();
    println!("Rolled back to snapshot 1");

    // Verify data is from snapshot 1
    let ds = zpool.get_or_open_filesystem(fsid, false).await.unwrap();
    for i in 0..5 {
        let mut obj_hdl = ds.get_inode_handle(objs[i], gen, true).await.unwrap();
        // Just verify we can read the object
        let _ = ds.get_object_attr(&obj_hdl).await.unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }
    println!("Verified all objects exist after rollback");

    drop(ds);
    zpool.destroy_snapshot(fsid, 1).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试快照克隆的读写能力和独立性 - Data 类型
#[tokio::test]
async fn snapshot_clone_write_isolation_data_test() {
    let poolname = "snapshot_clone_write_isolation_data_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create objects with initial data
    let (objs, gen) = ds.create_objects(3).await.unwrap();
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 16 << 10];
        ds.write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds.release_inode_handle(&mut obj_hdl).await;
    }
    println!("Created {} objects with initial data", objs.len());

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Created snapshot");

    // Get snapshot clone (should be writable)
    let snap_clone = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();

    // Write to snapshot clone - this should succeed as clones are writable
    for (i, &obj) in objs.iter().enumerate() {
        let mut snap_obj_hdl = snap_clone.get_inode_handle(obj, gen, true).await.unwrap();
        let new_data = vec![(i + 100) as u8; 16 << 10];
        snap_clone
            .write_object(&snap_obj_hdl, 0, false, vec![&new_data])
            .await
            .unwrap();
        snap_clone.release_inode_handle(&mut snap_obj_hdl).await;
    }
    println!("Modified data in snapshot clone");

    // Verify snapshot clone has modified data
    for (i, &obj) in objs.iter().enumerate() {
        let mut snap_obj_hdl = snap_clone.get_inode_handle(obj, gen, true).await.unwrap();
        let snap_data = snap_clone
            .read_object(&snap_obj_hdl, 0, 16 << 10)
            .await
            .unwrap();
        assert_eq!(snap_data, vec![(i + 100) as u8; 16 << 10]);
        snap_clone.release_inode_handle(&mut snap_obj_hdl).await;
    }
    println!("Verified snapshot clone has modified data");

    // Verify original filesystem still has original data (isolation)
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
        let orig_data = ds.read_object(&obj_hdl, 0, 16 << 10).await.unwrap();
        assert_eq!(orig_data, vec![i as u8; 16 << 10]);
        ds.release_inode_handle(&mut obj_hdl).await;
    }
    println!("Verified original filesystem still has original data (isolation confirmed)");

    drop(snap_clone);
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试快照克隆的读写能力和独立性 - Meta 类型
#[tokio::test]
async fn snapshot_clone_write_isolation_meta_test() {
    let poolname = "snapshot_clone_write_isolation_meta_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Meta)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create FILE inodes
    let mut inodes = vec![];
    for _ in 0..3 {
        let mut ino_hdl = ds.create_inode(InodeType::FILE).await.unwrap();
        inodes.push((ino_hdl.ino, ino_hdl.gen));
        ds.release_inode_handle(&mut ino_hdl).await;
    }
    println!("Created {} FILE inodes", inodes.len());

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Created snapshot");

    // Get snapshot clone
    let snap_clone = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();

    // Create new inodes in snapshot clone
    let mut new_inodes = vec![];
    for _ in 0..2 {
        let mut ino_hdl = snap_clone.create_inode(InodeType::FILE).await.unwrap();
        new_inodes.push((ino_hdl.ino, ino_hdl.gen));
        snap_clone.release_inode_handle(&mut ino_hdl).await;
    }
    println!("Created {} new inodes in snapshot clone", new_inodes.len());

    // Verify new inodes exist in snapshot clone
    for (ino, gen) in &new_inodes {
        let mut snap_hdl = snap_clone
            .get_inode_handle(*ino, *gen, false)
            .await
            .unwrap();
        snap_clone.release_inode_handle(&mut snap_hdl).await;
    }
    println!("Verified new inodes exist in snapshot clone");

    // Verify new inodes do NOT exist in original filesystem (isolation)
    for (ino, gen) in &new_inodes {
        let result = ds.get_inode_handle(*ino, *gen, false).await;
        assert!(
            result.is_err(),
            "New inode should not exist in original filesystem"
        );
    }
    println!("Verified new inodes do NOT exist in original filesystem (isolation confirmed)");

    drop(snap_clone);
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试快照缓存机制 - 多次 get_or_open_snapshot 应返回同一个实例
#[tokio::test]
async fn snapshot_cache_mechanism_test() {
    let poolname = "snapshot_cache_mechanism_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create object
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let obj = objs[0];
    let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
    let data = vec![42u8; 16 << 10];
    ds.write_object(&obj_hdl, 0, false, vec![&data])
        .await
        .unwrap();
    ds.release_inode_handle(&mut obj_hdl).await;

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Created snapshot");

    // Open snapshot multiple times
    let snap1 = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();
    let snap2 = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();
    let snap3 = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();

    // Verify they point to the same Arc instance (same pointer)
    let ptr1 = Arc::as_ptr(&snap1);
    let ptr2 = Arc::as_ptr(&snap2);
    let ptr3 = Arc::as_ptr(&snap3);

    assert_eq!(
        ptr1, ptr2,
        "First and second get_or_open_snapshot should return same Arc"
    );
    assert_eq!(
        ptr2, ptr3,
        "Second and third get_or_open_snapshot should return same Arc"
    );
    println!("Verified cache mechanism: all get_or_open_snapshot calls return same Arc instance");

    // Verify Arc strong count
    let strong_count = Arc::strong_count(&snap1);
    assert!(
        strong_count >= 3,
        "Arc strong count should be at least 3 (snap1, snap2, snap3)"
    );
    println!("Arc strong count: {}", strong_count);

    drop(snap1);
    drop(snap2);
    drop(snap3);
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试错误场景 - 打开不存在的快照
#[tokio::test]
async fn snapshot_error_open_nonexistent_test() {
    let poolname = "snapshot_error_open_nonexistent_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Try to open non-existent snapshot
    let result = zpool.get_or_open_snapshot(fsid, 999).await;
    assert!(result.is_err(), "Opening non-existent snapshot should fail");
    println!("Verified opening non-existent snapshot returns error");

    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试错误场景 - 销毁快照后再访问
#[tokio::test]
async fn snapshot_error_access_after_destroy_test() {
    let poolname = "snapshot_error_access_after_destroy_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create object and snapshot
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let obj = objs[0];
    let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
    let data = vec![1u8; 16 << 10];
    ds.write_object(&obj_hdl, 0, false, vec![&data])
        .await
        .unwrap();
    ds.release_inode_handle(&mut obj_hdl).await;

    let snapid = 1;
    zpool.create_snapshot(fsid, snapid).await.unwrap();
    println!("Created snapshot");

    // Open snapshot
    let snap = zpool.get_or_open_snapshot(fsid, snapid).await.unwrap();
    println!("Opened snapshot");

    // Destroy snapshot (this should close the clone dataset)
    drop(snap);
    drop(ds);
    zpool.destroy_snapshot(fsid, snapid).await.unwrap();
    println!("Destroyed snapshot");

    // Try to open destroyed snapshot
    let result = zpool.get_or_open_snapshot(fsid, snapid).await;
    assert!(result.is_err(), "Opening destroyed snapshot should fail");
    println!("Verified opening destroyed snapshot returns error");

    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试错误场景 - 回滚后访问被回滚掉的快照
#[tokio::test]
async fn snapshot_error_access_after_rollback_test() {
    let poolname = "snapshot_error_access_after_rollback_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Create object
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let obj = objs[0];

    // Create snapshot 1
    let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
    let data1 = vec![1u8; 16 << 10];
    ds.write_object(&obj_hdl, 0, false, vec![&data1])
        .await
        .unwrap();
    ds.release_inode_handle(&mut obj_hdl).await;
    zpool.create_snapshot(fsid, 1).await.unwrap();
    println!("Created snapshot 1");

    // Create snapshot 2
    let mut obj_hdl = ds.get_inode_handle(obj, gen, true).await.unwrap();
    let data2 = vec![2u8; 16 << 10];
    ds.write_object(&obj_hdl, 0, false, vec![&data2])
        .await
        .unwrap();
    ds.release_inode_handle(&mut obj_hdl).await;
    zpool.create_snapshot(fsid, 2).await.unwrap();
    println!("Created snapshot 2");

    // Rollback to snapshot 1 (must destroy snapshot 2 first)
    drop(ds);
    zpool.destroy_snapshot(fsid, 2).await.unwrap();
    zpool.rollback_to_snapshot(fsid, 1).await.unwrap();
    println!("Rolled back to snapshot 1");

    // Try to access snapshot 2 (should fail as it was destroyed)
    let result = zpool.get_or_open_snapshot(fsid, 2).await;
    assert!(
        result.is_err(),
        "Accessing destroyed snapshot 2 should fail"
    );
    println!("Verified accessing destroyed snapshot 2 returns error");

    // Snapshot 1 should still be accessible
    let snap1 = zpool.get_or_open_snapshot(fsid, 1).await.unwrap();
    let mut snap_obj_hdl = snap1.get_inode_handle(obj, gen, true).await.unwrap();
    let snap_data = snap1.read_object(&snap_obj_hdl, 0, 16 << 10).await.unwrap();
    assert_eq!(snap_data, vec![1u8; 16 << 10]);
    snap1.release_inode_handle(&mut snap_obj_hdl).await;
    println!("Verified snapshot 1 is still accessible with correct data");

    drop(snap1);
    zpool.destroy_snapshot(fsid, 1).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试基础 send/recv - Data 类型，同 zpool 内传输
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_basic_data_test() {
    use std::sync::Mutex;
    let poolname = "snapshot_send_recv_basic_data_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid_src = 0;
    let fsid_dst = 1;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    // Create source filesystem with data
    let ds_src = zpool.get_or_open_filesystem(fsid_src, true).await.unwrap();
    let (objs, gen) = ds_src.create_objects(3).await.unwrap();
    let mut objects_data = vec![];

    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds_src.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 32 << 10];
        ds_src
            .write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds_src.release_inode_handle(&mut obj_hdl).await;
        objects_data.push((obj, gen, i as u8));
    }
    println!("Created source filesystem with {} objects", objs.len());

    // Create snapshot on source
    let snapid = 1;
    zpool.create_snapshot(fsid_src, snapid).await.unwrap();
    println!("Created snapshot on source filesystem");

    // Send snapshot to buffer
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = buffer.clone();

    zpool
        .send_snapshot(
            fsid_src,
            snapid,
            None,
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec(); // Copy data before moving into async block
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    let sent_size = buffer.lock().unwrap().len();
    println!("Sent snapshot, size: {} bytes", sent_size);

    // Receive snapshot to destination filesystem
    let buffer_clone = buffer.clone();
    let offset = Arc::new(Mutex::new(0usize));

    zpool
        .receive_snapshot(fsid_dst, snapid, |buf| {
            let buffer = buffer_clone.clone();
            let offset = offset.clone();
            let data = buffer.lock().unwrap();
            let mut off = offset.lock().unwrap();
            let remaining = data.len() - *off;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
            *off += to_copy;
            ready(Ok(to_copy))
        })
        .await
        .unwrap();

    println!("Received snapshot to destination filesystem");

    let offset = Arc::new(Mutex::new(0usize));
    zpool
        .receive_snapshot(fsid_dst, snapid, |buf| {
            let buffer = buffer_clone.clone();
            let offset = offset.clone();
            let data = buffer.lock().unwrap();
            let mut off = offset.lock().unwrap();
            let remaining = data.len() - *off;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
            *off += to_copy;
            ready(Ok(to_copy))
        })
        .await
        .unwrap();

    // Verify data in destination filesystem
    let ds_dst = zpool.get_or_open_filesystem(fsid_dst, false).await.unwrap();
    for (obj, gen, original_value) in &objects_data {
        let mut obj_hdl = ds_dst.get_inode_handle(*obj, *gen, true).await.unwrap();
        let data = ds_dst.read_object(&obj_hdl, 0, 32 << 10).await.unwrap();
        assert_eq!(data, vec![*original_value; 32 << 10]);
        ds_dst.release_inode_handle(&mut obj_hdl).await;
    }
    println!(
        "Verified all {} objects in destination filesystem",
        objects_data.len()
    );

    drop(ds_src);
    drop(ds_dst);
    zpool.destroy_snapshot(fsid_src, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试基础 send/recv - Meta 类型
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_basic_meta_test() {
    use std::sync::Mutex;
    let poolname = "snapshot_send_recv_basic_meta_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid_src = 0;
    let fsid_dst = 1;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Meta)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    // Create source filesystem with inodes
    let ds_src = zpool.get_or_open_filesystem(fsid_src, true).await.unwrap();
    let mut inodes = vec![];

    for _ in 0..5 {
        let mut ino_hdl = ds_src.create_inode(InodeType::FILE).await.unwrap();
        inodes.push((ino_hdl.ino, ino_hdl.gen));
        ds_src.release_inode_handle(&mut ino_hdl).await;
    }
    println!("Created source filesystem with {} inodes", inodes.len());

    // Create snapshot on source
    let snapid = 1;
    zpool.create_snapshot(fsid_src, snapid).await.unwrap();
    println!("Created snapshot on source filesystem");

    // Send snapshot to buffer
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = buffer.clone();

    zpool
        .send_snapshot(
            fsid_src,
            snapid,
            None,
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec(); // Copy data before moving into async block
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    let sent_size = buffer.lock().unwrap().len();
    println!("Sent snapshot, size: {} bytes", sent_size);

    // Receive snapshot to destination filesystem
    let buffer_clone = buffer.clone();
    let offset = Arc::new(Mutex::new(0usize));

    zpool
        .receive_snapshot(fsid_dst, snapid, |buf| {
            let buffer = buffer_clone.clone();
            let offset = offset.clone();
            let data = buffer.lock().unwrap();
            let mut off = offset.lock().unwrap();
            let remaining = data.len() - *off;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
            *off += to_copy;
            ready(Ok(to_copy))
        })
        .await
        .unwrap();

    println!("Received snapshot to destination filesystem");

    // Verify inodes in destination filesystem
    let ds_dst = zpool.get_or_open_filesystem(fsid_dst, false).await.unwrap();
    for (ino, gen) in &inodes {
        let mut ino_hdl = ds_dst.get_inode_handle(*ino, *gen, false).await.unwrap();
        let attr = ds_dst.get_attr(&ino_hdl).await.unwrap();
        assert_eq!(attr.gen, *gen);
        ds_dst.release_inode_handle(&mut ino_hdl).await;
    }
    println!(
        "Verified all {} inodes in destination filesystem",
        inodes.len()
    );

    drop(ds_src);
    drop(ds_dst);
    zpool.destroy_snapshot(fsid_src, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试跨 zpool 的 send/recv - Data 类型
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_cross_zpool_test() {
    use std::sync::Mutex;
    let poolname_src = "snapshot_send_recv_cross_src";
    let poolname_dst = "snapshot_send_recv_cross_dst";
    let uzfs_test_env_src = UzfsTestEnv::new(1024 * 1024 * 1024);
    let uzfs_test_env_dst = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;

    // Create source zpool and filesystem
    let mut zpool_src = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname_src, &[uzfs_test_env_src.get_dev_path()])
        .await
        .unwrap();

    let ds_src = zpool_src.get_or_open_filesystem(fsid, true).await.unwrap();
    let (objs, gen) = ds_src.create_objects(3).await.unwrap();
    let mut objects_data = vec![];

    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds_src.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![(i + 10) as u8; 64 << 10];
        ds_src
            .write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds_src.release_inode_handle(&mut obj_hdl).await;
        objects_data.push((obj, gen, (i + 10) as u8));
    }
    println!("Created source zpool with {} objects", objs.len());

    // Create snapshot on source
    let snapid = 1;
    zpool_src.create_snapshot(fsid, snapid).await.unwrap();
    println!("Created snapshot on source zpool");

    // Send snapshot to buffer
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = buffer.clone();

    zpool_src
        .send_snapshot(
            fsid,
            snapid,
            None,
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    let sent_size = buffer.lock().unwrap().len();
    println!("Sent snapshot from source zpool, size: {} bytes", sent_size);

    // Create destination zpool
    let mut zpool_dst = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname_dst, &[uzfs_test_env_dst.get_dev_path()])
        .await
        .unwrap();

    // Receive snapshot to destination zpool
    let buffer_clone = buffer.clone();
    let offset = Arc::new(Mutex::new(0usize));

    zpool_dst
        .receive_snapshot(fsid, snapid, |buf| {
            let buffer = buffer_clone.clone();
            let offset = offset.clone();
            let data = buffer.lock().unwrap();
            let mut off = offset.lock().unwrap();
            let remaining = data.len() - *off;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
            *off += to_copy;
            ready(Ok(to_copy))
        })
        .await
        .unwrap();

    println!("Received snapshot to destination zpool");

    // Verify data in destination zpool
    let ds_dst = zpool_dst.get_or_open_filesystem(fsid, false).await.unwrap();
    for (obj, gen, original_value) in &objects_data {
        let mut obj_hdl = ds_dst.get_inode_handle(*obj, *gen, true).await.unwrap();
        let data = ds_dst.read_object(&obj_hdl, 0, 64 << 10).await.unwrap();
        assert_eq!(data, vec![*original_value; 64 << 10]);
        ds_dst.release_inode_handle(&mut obj_hdl).await;
    }
    println!(
        "Verified all {} objects in destination zpool",
        objects_data.len()
    );

    drop(ds_src);
    drop(ds_dst);
    zpool_src.destroy_snapshot(fsid, snapid).await.unwrap();
    zpool_src.close().await;
    zpool_dst.close().await;
    uzfs_env_fini().await;
}

// 测试增量 send - Data 类型
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_incremental_test() {
    use std::sync::Mutex;
    let poolname = "snapshot_send_recv_incremental_test";
    let uzfs_test_env = UzfsTestEnv::new(2048 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid_src = 0;
    let fsid_dst = 1;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    // Create source filesystem with initial data
    let ds_src = zpool.get_or_open_filesystem(fsid_src, true).await.unwrap();
    let (objs, gen) = ds_src.create_objects(5).await.unwrap();

    // Write initial data and create snapshot 1
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds_src.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 32 << 10];
        ds_src
            .write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds_src.release_inode_handle(&mut obj_hdl).await;
    }

    zpool.create_snapshot(fsid_src, 1).await.unwrap();
    println!("Created snapshot 1 with initial data");

    // Send full snapshot 1 to destination
    let buffer1 = Arc::new(Mutex::new(Vec::new()));
    let buffer1_clone = buffer1.clone();

    zpool
        .send_snapshot(
            fsid_src,
            1,
            None,
            |data| {
                let buf = buffer1_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    println!(
        "Sent full snapshot 1, size: {} bytes",
        buffer1.lock().unwrap().len()
    );

    // Receive snapshot 1
    let buffer1_clone = buffer1.clone();
    let offset1 = Arc::new(Mutex::new(0usize));

    zpool
        .receive_snapshot(fsid_dst, 1, |buf| {
            let buffer = buffer1_clone.clone();
            let offset = offset1.clone();
            let data = buffer.lock().unwrap();
            let mut off = offset.lock().unwrap();
            let remaining = data.len() - *off;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
            *off += to_copy;
            ready(Ok(to_copy))
        })
        .await
        .unwrap();

    println!("Received snapshot 1 to destination");

    // Modify data and create snapshot 2
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds_src.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![(i + 100) as u8; 32 << 10];
        ds_src
            .write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds_src.release_inode_handle(&mut obj_hdl).await;
    }

    zpool.create_snapshot(fsid_src, 2).await.unwrap();
    println!("Created snapshot 2 with modified data");

    // Send incremental snapshot (from 1 to 2)
    let buffer2 = Arc::new(Mutex::new(Vec::new()));
    let buffer2_clone = buffer2.clone();

    zpool
        .send_snapshot(
            fsid_src,
            2,
            Some(1),
            |data| {
                let buf = buffer2_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    let incremental_size = buffer2.lock().unwrap().len();
    let full_size = buffer1.lock().unwrap().len();
    println!(
        "Sent incremental snapshot (1->2), size: {} bytes (full was {} bytes)",
        incremental_size, full_size
    );

    // Receive incremental snapshot
    let buffer2_clone = buffer2.clone();
    let offset2 = Arc::new(Mutex::new(0usize));

    zpool
        .receive_snapshot(fsid_dst, 2, |buf| {
            let buffer = buffer2_clone.clone();
            let offset = offset2.clone();
            let data = buffer.lock().unwrap();
            let mut off = offset.lock().unwrap();
            let remaining = data.len() - *off;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
            *off += to_copy;
            ready(Ok(to_copy))
        })
        .await
        .unwrap();

    println!("Received incremental snapshot to destination");

    // Verify data in destination snapshot 2
    let snap_dst = zpool.get_or_open_snapshot(fsid_dst, 2).await.unwrap();
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = snap_dst.get_inode_handle(obj, gen, true).await.unwrap();
        let data = snap_dst.read_object(&obj_hdl, 0, 32 << 10).await.unwrap();
        assert_eq!(data, vec![(i + 100) as u8; 32 << 10]);
        snap_dst.release_inode_handle(&mut obj_hdl).await;
    }
    println!("Verified incremental snapshot has correct modified data");

    drop(ds_src);
    drop(snap_dst);
    zpool.destroy_snapshot(fsid_src, 1).await.unwrap();
    zpool.destroy_snapshot(fsid_src, 2).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试 send/recv 错误场景 - 发送不存在的快照
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_error_nonexistent_test() {
    use std::sync::Mutex;
    let poolname = "snapshot_send_recv_error_nonexistent_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    zpool.get_or_open_filesystem(fsid, true).await.unwrap();

    // Try to send non-existent snapshot
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = buffer.clone();

    let result = zpool
        .send_snapshot(
            fsid,
            999,
            None,
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await;

    assert!(result.is_err(), "Sending non-existent snapshot should fail");
    println!("Verified sending non-existent snapshot returns error");

    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试 send/recv 错误场景 - 增量发送时 base 快照不存在
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_error_invalid_base_test() {
    use std::sync::Mutex;
    let poolname = "snapshot_send_recv_error_invalid_base_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid = 0;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds = zpool.get_or_open_filesystem(fsid, true).await.unwrap();
    let (objs, gen) = ds.create_objects(1).await.unwrap();
    let mut obj_hdl = ds.get_inode_handle(objs[0], gen, true).await.unwrap();
    let data = vec![1u8; 16 << 10];
    ds.write_object(&obj_hdl, 0, false, vec![&data])
        .await
        .unwrap();
    ds.release_inode_handle(&mut obj_hdl).await;

    // Create snapshot 2 (but no snapshot 1)
    zpool.create_snapshot(fsid, 2).await.unwrap();

    // Try to send incremental from non-existent snapshot 1 to snapshot 2
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = buffer.clone();

    let result = zpool
        .send_snapshot(
            fsid,
            2,
            Some(1),
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await;

    assert!(
        result.is_err(),
        "Incremental send with non-existent base should fail"
    );
    println!("Verified incremental send with invalid base returns error");

    drop(ds);
    zpool.destroy_snapshot(fsid, 2).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试多个快照链的连续增量 send/recv
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_chain_test() {
    use std::sync::Mutex;
    let poolname = "snapshot_send_recv_chain_test";
    let uzfs_test_env = UzfsTestEnv::new(2048 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid_src = 0;
    let fsid_dst = 1;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    let ds_src = zpool.get_or_open_filesystem(fsid_src, true).await.unwrap();
    let (objs, gen) = ds_src.create_objects(1).await.unwrap();
    let obj = objs[0];

    // Create and send snapshot chain: 1 -> 2 -> 3
    for i in 1..=3 {
        // Write data
        let mut obj_hdl = ds_src.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![i as u8; 32 << 10];
        ds_src
            .write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds_src.release_inode_handle(&mut obj_hdl).await;

        // Create snapshot
        zpool.create_snapshot(fsid_src, i).await.unwrap();
        println!("Created snapshot {} with value {}", i, i);

        // Send snapshot (incremental if not first)
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let from_snap = if i == 1 { None } else { Some(i - 1) };
        zpool
            .send_snapshot(
                fsid_src,
                i,
                from_snap,
                |data| {
                    let buf = buffer_clone.clone();
                    let data = data.to_vec();
                    async move {
                        buf.lock().unwrap().extend_from_slice(&data);
                        Ok(())
                    }
                },
                None,
            )
            .await
            .unwrap();

        let size = buffer.lock().unwrap().len();
        if i == 1 {
            println!("Sent full snapshot {}, size: {} bytes", i, size);
        } else {
            println!(
                "Sent incremental snapshot ({}->{}), size: {} bytes",
                i - 1,
                i,
                size
            );
        }

        // Receive snapshot
        let buffer_clone = buffer.clone();
        let offset = Arc::new(Mutex::new(0usize));

        zpool
            .receive_snapshot(fsid_dst, i, |buf| {
                let buffer = buffer_clone.clone();
                let offset = offset.clone();
                let data = buffer.lock().unwrap();
                let mut off = offset.lock().unwrap();
                let remaining = data.len() - *off;
                let to_copy = remaining.min(buf.len());
                buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
                *off += to_copy;
                ready(Ok(to_copy))
            })
            .await
            .unwrap();

        println!("Received snapshot {} to destination", i);
    }

    // Verify all snapshots in destination
    for i in 1..=3 {
        let snap_dst = zpool.get_or_open_snapshot(fsid_dst, i).await.unwrap();
        let mut obj_hdl = snap_dst.get_inode_handle(obj, gen, true).await.unwrap();
        let data = snap_dst.read_object(&obj_hdl, 0, 32 << 10).await.unwrap();
        assert_eq!(data, vec![i as u8; 32 << 10]);
        snap_dst.release_inode_handle(&mut obj_hdl).await;
        drop(snap_dst);
        println!("Verified snapshot {} has correct data", i);
    }

    drop(ds_src);
    for i in 1..=3 {
        zpool.destroy_snapshot(fsid_src, i).await.unwrap();
    }
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试断点续传 - 使用大数据集模拟多次随机中断和恢复
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_resumable_test() {
    use rand::Rng;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    let poolname = "snapshot_send_recv_resumable_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid_src = 0;
    let fsid_dst = 1;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Data)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    // Create source filesystem with large dataset
    let ds_src = zpool.get_or_open_filesystem(fsid_src, true).await.unwrap();

    // Create many large objects to ensure significant data size
    let num_objects = 50;
    let object_size = 512 << 10; // 512KB per object, total ~15MB
    let (objs, gen) = ds_src.create_objects(num_objects).await.unwrap();

    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds_src.get_inode_handle(obj, gen, true).await.unwrap();
        let data = vec![(i % 256) as u8; object_size];
        ds_src
            .write_object(&obj_hdl, 0, false, vec![&data])
            .await
            .unwrap();
        ds_src.release_inode_handle(&mut obj_hdl).await;
    }
    println!(
        "Created source filesystem with {} objects, total size: {} MB",
        num_objects,
        num_objects * object_size / (1024 * 1024)
    );

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid_src, snapid).await.unwrap();
    println!("Created snapshot on source");

    // Send full snapshot to buffer once
    let full_buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = full_buffer.clone();

    zpool
        .send_snapshot(
            fsid_src,
            snapid,
            None,
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    let total_size = full_buffer.lock().unwrap().len();
    println!(
        "Full snapshot size: {} bytes ({} MB)",
        total_size,
        total_size / (1024 * 1024)
    );

    // Simulate multiple random interruptions
    let mut rng = rand::thread_rng();
    let max_attempts = 5; // Maximum number of interruptions
    let mut attempt = 0;
    let mut resume_info_opt: Option<crate::ResumeInfo> = None;

    loop {
        attempt += 1;
        println!("\n=== Attempt {} ===", attempt);

        // Decide if we should interrupt this attempt (except the last one)
        let should_interrupt_this_attempt = attempt < max_attempts && rng.gen_bool(0.9); // 30% chance to interrupt

        // Random interruption point (between 10% and 90% of remaining data)
        let interrupt_ratio = if should_interrupt_this_attempt {
            rng.gen_range(0.1..0.9)
        } else {
            1.0 // Don't interrupt on last attempt
        };

        let bytes_received = Arc::new(AtomicUsize::new(0));
        let interrupt_after = (total_size as f64 * interrupt_ratio) as usize;

        // Send with resume info if available
        let send_buffer = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = send_buffer.clone();

        zpool
            .send_snapshot(
                fsid_src,
                snapid,
                None,
                |data| {
                    let buf = buffer_clone.clone();
                    let data = data.to_vec();
                    async move {
                        buf.lock().unwrap().extend_from_slice(&data);
                        Ok(())
                    }
                },
                resume_info_opt.clone(),
            )
            .await
            .unwrap();

        let send_size = send_buffer.lock().unwrap().len();
        println!("Send size for this attempt: {} bytes", send_size);

        // Receive with potential interruption
        let buffer_clone = send_buffer.clone();
        let bytes_received_clone = bytes_received.clone();
        let offset = Arc::new(Mutex::new(0usize));

        let result = zpool
            .receive_snapshot(fsid_dst, snapid, |buf| {
                let buffer = buffer_clone.clone();
                let offset = offset.clone();
                let bytes_received = bytes_received_clone.clone();

                let data = buffer.lock().unwrap();
                let mut off = offset.lock().unwrap();

                // Check if we should interrupt
                let current_bytes = bytes_received.load(Ordering::SeqCst);
                if should_interrupt_this_attempt && current_bytes >= interrupt_after {
                    println!(
                        "Simulating interruption at {} bytes ({}% of send size)",
                        current_bytes,
                        current_bytes * 100 / send_size
                    );
                    return ready(Err(std::io::Error::from_raw_os_error(libc::EINTR)));
                }

                let remaining = data.len() - *off;
                let to_copy = remaining.min(buf.len());
                buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
                *off += to_copy;
                bytes_received.fetch_add(to_copy, Ordering::SeqCst);

                ready(Ok(to_copy))
            })
            .await;

        let received_bytes = bytes_received.load(Ordering::SeqCst);

        if result.is_ok() {
            println!(
                "Receive completed successfully after {} bytes",
                received_bytes
            );
            println!("Total attempts: {}", attempt);
            break;
        } else {
            println!(
                "Receive interrupted after {} bytes, {result:?}",
                received_bytes
            );

            // Get resume info for next attempt
            let resume_info = zpool.get_receive_resume_info(fsid_dst).await.unwrap();
            assert!(
                resume_info.is_some(),
                "Resume info should be available after interruption"
            );

            resume_info_opt = resume_info;
            let ri = resume_info_opt.as_ref().unwrap();
            println!(
                "Resume info: object={}, offset={}",
                ri.resume_object, ri.resume_offset
            );

            if attempt >= max_attempts {
                panic!(
                    "Exceeded maximum attempts ({}), but receive still not complete",
                    max_attempts
                );
            }
        }
    }

    println!("\n=== Verifying data ===");

    // Verify data in destination
    let ds_dst = zpool.get_or_open_filesystem(fsid_dst, false).await.unwrap();
    for (i, &obj) in objs.iter().enumerate() {
        let mut obj_hdl = ds_dst.get_inode_handle(obj, gen, true).await.unwrap();
        let data = ds_dst
            .read_object(&obj_hdl, 0, object_size as u64)
            .await
            .unwrap();
        assert_eq!(data.len(), object_size, "Object {} size mismatch", i);
        assert_eq!(
            data,
            vec![(i % 256) as u8; object_size],
            "Object {} data mismatch after resume",
            i
        );
        ds_dst.release_inode_handle(&mut obj_hdl).await;
    }
    println!(
        "Verified all {} objects have correct data after resumable receive with {} interruptions",
        num_objects,
        attempt - 1
    );

    drop(ds_src);
    drop(ds_dst);
    zpool.destroy_snapshot(fsid_src, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}

// 测试断点续传 - Meta类型，使用大量inode模拟多次随机中断和恢复
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_send_recv_resumable_meta_test() {
    use rand::Rng;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    let poolname = "snapshot_send_recv_resumable_meta_test";
    let uzfs_test_env = UzfsTestEnv::new(1024 * 1024 * 1024);
    uzfs_env_init().await;
    let fsid_src = 0;
    let fsid_dst = 1;
    let mut zpool = ZpoolOpenOptions::new(ZpoolType::Meta)
        .create(true)
        .open::<DatasetWrapper>(poolname, &[uzfs_test_env.get_dev_path()])
        .await
        .unwrap();

    // Create source filesystem with many inodes
    let ds_src = zpool.get_or_open_filesystem(fsid_src, true).await.unwrap();

    // Create many FILE inodes to ensure significant metadata size
    let num_inodes = 10000; // Large number of inodes for substantial metadata
    let mut inodes = vec![];

    for _ in 0..num_inodes {
        let mut ino_hdl = ds_src.create_inode(InodeType::FILE).await.unwrap();
        inodes.push((ino_hdl.ino, ino_hdl.gen));
        ds_src.release_inode_handle(&mut ino_hdl).await;
    }
    println!("Created source filesystem with {} FILE inodes", num_inodes);

    // Create directories with many dentries
    let num_dirs = 1000;
    let dentries_per_dir = 500;
    let mut dirs = vec![];

    for i in 0..num_dirs {
        // Create directory inode
        let mut dir_hdl = ds_src.create_inode(InodeType::DIR).await.unwrap();
        let dir_ino = dir_hdl.ino;
        let dir_gen = dir_hdl.gen;

        // Create dentries in this directory
        for j in 0..dentries_per_dir {
            let dentry_name = format!("file_{}", j);
            let dentry_value = (i * dentries_per_dir + j) as u64;
            ds_src
                .create_dentry(&mut dir_hdl, dentry_name.as_bytes(), dentry_value)
                .await
                .unwrap();
        }

        ds_src.release_inode_handle(&mut dir_hdl).await;
        dirs.push((dir_ino, dir_gen));

        if (i + 1) % 100 == 0 {
            println!(
                "Created {} directories with {} dentries each",
                i + 1,
                dentries_per_dir
            );
        }
    }
    println!(
        "Created {} directories with total {} dentries",
        num_dirs,
        num_dirs * dentries_per_dir
    );

    // Create snapshot
    let snapid = 1;
    zpool.create_snapshot(fsid_src, snapid).await.unwrap();
    println!("Created snapshot on source");

    // Send full snapshot to buffer once
    let full_buffer = Arc::new(Mutex::new(Vec::new()));
    let buffer_clone = full_buffer.clone();

    zpool
        .send_snapshot(
            fsid_src,
            snapid,
            None,
            |data| {
                let buf = buffer_clone.clone();
                let data = data.to_vec();
                async move {
                    buf.lock().unwrap().extend_from_slice(&data);
                    Ok(())
                }
            },
            None,
        )
        .await
        .unwrap();

    let total_size = full_buffer.lock().unwrap().len();
    println!(
        "Full snapshot size: {} bytes ({} KB)",
        total_size,
        total_size / 1024
    );

    // Simulate multiple random interruptions
    let mut rng = rand::thread_rng();
    let max_attempts = 5; // Maximum number of interruptions
    let mut attempt = 0;
    let mut resume_info_opt: Option<crate::ResumeInfo> = None;

    loop {
        attempt += 1;
        println!("\n=== Attempt {} ===", attempt);

        // Decide if we should interrupt this attempt (except the last one)
        let should_interrupt_this_attempt = attempt < max_attempts && rng.gen_bool(0.9); // 90% chance to interrupt

        // Random interruption point (between 10% and 90% of remaining data)
        let interrupt_ratio = if should_interrupt_this_attempt {
            rng.gen_range(0.3..0.9)
        } else {
            1.0 // Don't interrupt on last attempt
        };

        let bytes_received = Arc::new(AtomicUsize::new(0));
        let interrupt_after = (total_size as f64 * interrupt_ratio) as usize;

        // Send with resume info if available
        let send_buffer = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = send_buffer.clone();

        zpool
            .send_snapshot(
                fsid_src,
                snapid,
                None,
                |data| {
                    let buf = buffer_clone.clone();
                    let data = data.to_vec();
                    async move {
                        buf.lock().unwrap().extend_from_slice(&data);
                        Ok(())
                    }
                },
                resume_info_opt.clone(),
            )
            .await
            .unwrap();

        let send_size = send_buffer.lock().unwrap().len();
        println!("Send size for this attempt: {} bytes", send_size);

        // Receive with potential interruption
        let buffer_clone = send_buffer.clone();
        let bytes_received_clone = bytes_received.clone();
        let offset = Arc::new(Mutex::new(0usize));

        let result = zpool
            .receive_snapshot(fsid_dst, snapid, |buf| {
                let buffer = buffer_clone.clone();
                let offset = offset.clone();
                let bytes_received = bytes_received_clone.clone();

                let data = buffer.lock().unwrap();
                let mut off = offset.lock().unwrap();

                // Check if we should interrupt
                let current_bytes = bytes_received.load(Ordering::SeqCst);
                if should_interrupt_this_attempt && current_bytes >= interrupt_after {
                    println!(
                        "Simulating interruption at {} bytes ({}% of send size)",
                        current_bytes,
                        current_bytes * 100 / send_size
                    );
                    return ready(Err(std::io::Error::from_raw_os_error(libc::EINTR)));
                }

                let remaining = data.len() - *off;
                let to_copy = remaining.min(buf.len());
                buf[..to_copy].copy_from_slice(&data[*off..*off + to_copy]);
                *off += to_copy;
                bytes_received.fetch_add(to_copy, Ordering::SeqCst);

                ready(Ok(to_copy))
            })
            .await;

        let received_bytes = bytes_received.load(Ordering::SeqCst);

        if result.is_ok() {
            println!(
                "Receive completed successfully after {} bytes",
                received_bytes
            );
            println!("Total attempts: {}", attempt);
            break;
        } else {
            println!("Receive interrupted after {} bytes", received_bytes);

            // Get resume info for next attempt
            let resume_info = zpool.get_receive_resume_info(fsid_dst).await.unwrap();
            assert!(
                resume_info.is_some(),
                "Resume info should be available after interruption"
            );

            resume_info_opt = resume_info;
            let ri = resume_info_opt.as_ref().unwrap();
            println!(
                "Resume info: object={}, offset={}",
                ri.resume_object, ri.resume_offset
            );

            if attempt >= max_attempts {
                panic!(
                    "Exceeded maximum attempts ({}), but receive still not complete",
                    max_attempts
                );
            }
        }
    }

    println!("\n=== Verifying metadata ===");

    // Verify all inodes exist in destination
    let ds_dst = zpool.get_or_open_filesystem(fsid_dst, false).await.unwrap();
    for (ino, gen) in &inodes {
        let mut ino_hdl = ds_dst.get_inode_handle(*ino, *gen, false).await.unwrap();
        let attr = ds_dst.get_attr(&ino_hdl).await.unwrap();
        assert_eq!(attr.gen, *gen, "Inode {} generation mismatch", ino);
        ds_dst.release_inode_handle(&mut ino_hdl).await;
    }
    println!(
        "Verified all {} FILE inodes exist with correct metadata",
        num_inodes
    );

    // Verify directories and dentries
    for (i, (dir_ino, dir_gen)) in dirs.iter().enumerate() {
        let mut dir_hdl = ds_dst
            .get_inode_handle(*dir_ino, *dir_gen, false)
            .await
            .unwrap();
        let attr = ds_dst.get_attr(&dir_hdl).await.unwrap();
        assert_eq!(
            attr.gen, *dir_gen,
            "Directory {} generation mismatch",
            dir_ino
        );

        // Verify some dentries in this directory
        for j in 0..10 {
            // Sample first 10 dentries
            let dentry_name = format!("file_{}", j);
            let expected_value = (i * dentries_per_dir + j) as u64;
            let actual_value = ds_dst
                .lookup_dentry(&dir_hdl, dentry_name.as_bytes())
                .await
                .unwrap();
            assert_eq!(
                actual_value, expected_value,
                "Dentry {} in directory {} value mismatch",
                dentry_name, dir_ino
            );
        }

        ds_dst.release_inode_handle(&mut dir_hdl).await;

        if (i + 1) % 100 == 0 {
            println!("Verified {} directories with dentries", i + 1);
        }
    }
    println!("Verified all {} directories with {} total dentries after resumable receive with {} interruptions",
             num_dirs, num_dirs * dentries_per_dir, attempt - 1);

    drop(ds_src);
    drop(ds_dst);
    zpool.destroy_snapshot(fsid_src, snapid).await.unwrap();
    zpool.close().await;
    uzfs_env_fini().await;
}
