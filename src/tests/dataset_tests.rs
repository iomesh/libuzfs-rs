use crate::bindings::sys::timespec;
use crate::uzfs_env_fini;
use crate::uzfs_env_init;
use crate::Dataset;
use crate::DatasetType;
use crate::InodeType;
use crate::KvSetOption;
use crate::MAX_RESERVED_SIZE;
use dashmap::DashMap;
use nix::sys::wait::waitpid;
use nix::sys::wait::WaitStatus;
use nix::unistd::fork;
use nix::unistd::ForkResult;
use petgraph::algo::is_cyclic_directed;
use petgraph::prelude::DiGraph;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serial_test::serial;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::process::abort;
use std::process::exit;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::task::JoinHandle;

pub struct UzfsTestEnv {
    dev_file: NamedTempFile,
}

impl UzfsTestEnv {
    /// Create an env for uzfs test.
    ///
    /// If dev_size is not zero, this function will create a temp file as block device,
    /// the size of device is `dev_size` bytes.
    ///
    /// If dev_size is zero, no block device and pool/dataset is created
    ///
    /// All of the resources (temp file, dev_file) will be deleted automatically when the env
    /// goes out of scope
    pub fn new(dev_size: u64) -> Self {
        let mut dev_file = NamedTempFile::new().unwrap();

        if dev_size > 0 {
            dev_file.as_file_mut().set_len(dev_size).unwrap();
        }

        UzfsTestEnv { dev_file }
    }

    pub fn get_dev_path(&self) -> std::io::Result<String> {
        let dev_path = self.dev_file.path().to_str().unwrap();
        Ok(dev_path.to_owned())
    }

    pub fn set_dev_size(&mut self, new_size: u64) {
        self.dev_file.as_file_mut().set_len(new_size).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_test() {
    let rwobj;
    let mut gen;
    let sb_ino;
    let tmp_ino;
    let tmp_name = "tmp_dir";
    let s = String::from("Hello uzfs!");
    let t = vec!['H' as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let file_ino;
    let dir_ino;
    let num;
    let mut txg;
    let key = "acl";
    let value = "root,admin";
    let file_name = "fileA";
    let reserved = vec![1; 128];

    let dsname = "uzfs-test/ds";
    uzfs_env_init().await;
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);

    {
        Dataset::init(
            dsname,
            &uzfs_test_env.get_dev_path().unwrap(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

        for _ in 0..10 {
            Dataset::init(
                dsname,
                &uzfs_test_env.get_dev_path().unwrap(),
                DatasetType::Meta,
                4096,
                false,
            )
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        }

        let ds = Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            0,
            false,
        )
        .await
        .unwrap();

        sb_ino = ds.get_superblock_ino();
        let last_txg = ds.get_last_synced_txg();

        txg = ds
            .set_kvattr(
                sb_ino,
                key,
                value.as_bytes(),
                KvSetOption::HighPriority as u32,
            )
            .await
            .unwrap();
        assert!(txg > last_txg);

        let value_read = ds.get_kvattr(sb_ino, key).await.unwrap();
        assert_eq!(value_read.as_slice(), value.as_bytes());
        ds.wait_synced().await.unwrap();

        (tmp_ino, gen) = ds.create_inode(InodeType::DIR).await.unwrap();
        ds.check_valid(tmp_ino, gen).await.unwrap();
        let err = ds.check_valid(tmp_ino, gen + 1).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);

        txg = ds.create_dentry(sb_ino, tmp_name, tmp_ino).await.unwrap();
        ds.wait_synced().await.unwrap();
        assert!(ds.get_last_synced_txg() >= txg);

        let tmp_dentry_data_read = ds.lookup_dentry(sb_ino, tmp_name).await.unwrap();
        assert_eq!(tmp_ino, tmp_dentry_data_read);

        num = ds.list_object().await.unwrap();
        let objs;
        (objs, gen) = ds.create_objects(1).await.unwrap();

        rwobj = objs[0];
        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().size, 0);
        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().gen, gen);
        assert_eq!(ds.list_object().await.unwrap(), num + 1);

        let doi = ds.stat_object(rwobj).await.unwrap();
        Dataset::dump_object_doi(rwobj, doi);

        let data = s.as_bytes();
        let size = s.len() as u64;
        ds.write_object(rwobj, 0, true, vec![data]).await.unwrap();
        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().size, size);
        assert_eq!(ds.read_object(rwobj, 0, size).await.unwrap(), data);
        assert_eq!(ds.read_object(rwobj, 0, size + 10).await.unwrap(), data);
        assert!(ds.read_object(rwobj, size, size).await.unwrap().is_empty());

        // offset must be 0 for truncate
        assert!(ds.truncate_object(rwobj, 1, size - 1).await.is_err());
        ds.truncate_object(rwobj, 0, 1).await.unwrap();
        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().size, 1);
        assert_eq!(ds.read_object(rwobj, 0, size).await.unwrap().len(), 1);

        // extend size via truncate
        ds.truncate_object(rwobj, 0, size).await.unwrap();
        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().size, size);
        assert_eq!(ds.read_object(rwobj, 0, size).await.unwrap(), t);

        (file_ino, _) = ds.create_inode(InodeType::FILE).await.unwrap();
        (dir_ino, _) = ds.create_inode(InodeType::DIR).await.unwrap();

        txg = ds
            .create_dentry(dir_ino, file_name, file_ino)
            .await
            .unwrap();
        let dentry_data_read = ds.lookup_dentry(dir_ino, file_name).await.unwrap();
        assert_eq!(file_ino, dentry_data_read);
        ds.wait_synced().await.unwrap();
        assert!(ds.get_last_synced_txg() >= txg);

        let (_, dentry_num) = ds.iterate_dentry(dir_ino, 0, 4096).await.unwrap();

        // TODO(hping): verify dentry content
        assert_eq!(dentry_num, 1);

        assert_eq!(ds.list_object().await.unwrap(), num + 3);

        _ = ds.set_attr(file_ino, &reserved).await.unwrap();

        let attr = ds.get_attr(file_ino).await.unwrap();
        assert_eq!(attr.reserved, reserved);

        _ = ds
            .set_kvattr(
                file_ino,
                key,
                value.as_bytes(),
                KvSetOption::HighPriority as u32,
            )
            .await
            .unwrap();

        let value_read = ds.get_kvattr(file_ino, key).await.unwrap();
        assert_eq!(value_read.as_slice(), value.as_bytes());

        assert_eq!(ds.list_object().await.unwrap(), num + 3);

        let obj = ds.create_objects(1).await.unwrap().0[0];
        let size = 1 << 18;
        let mut data = Vec::<u8>::with_capacity(size);
        data.resize(size, 1);
        ds.write_object(obj, 0, false, vec![&data]).await.unwrap();
        ds.write_object(obj, (size * 2) as u64, false, vec![&data])
            .await
            .unwrap();
        ds.wait_synced().await.unwrap();
        assert!(!ds
            .object_has_hole_in_range(obj, 0, size as u64)
            .await
            .unwrap());
        assert!(ds
            .object_has_hole_in_range(obj, size as u64, size as u64 * 2)
            .await
            .unwrap());
        ds.delete_object(obj).await.unwrap();
        ds.close().await.unwrap();
    }

    {
        let ds = Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap();

        assert!(ds.get_last_synced_txg() >= txg);
        assert_eq!(ds.list_object().await.unwrap(), num + 3);

        assert_eq!(ds.get_superblock_ino(), sb_ino);

        let value_read = ds.get_kvattr(sb_ino, key).await.unwrap();
        assert_eq!(value_read.as_slice(), value.as_bytes());

        let tmp_dentry_data_read = ds.lookup_dentry(sb_ino, tmp_name).await.unwrap();
        assert_eq!(tmp_ino, tmp_dentry_data_read);

        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().gen, gen);

        let size = s.len() as u64;
        assert_eq!(ds.get_object_attr(rwobj).await.unwrap().size, size);
        assert_eq!(ds.read_object(rwobj, 0, size).await.unwrap(), t);
        assert_eq!(ds.read_object(rwobj, 0, size + 10).await.unwrap(), t);
        assert!(ds.read_object(rwobj, size, size).await.unwrap().is_empty());

        ds.delete_object(rwobj).await.unwrap();

        assert_eq!(ds.list_object().await.unwrap(), num + 2);

        let dentry_data_read = ds.lookup_dentry(dir_ino, file_name).await.unwrap();
        assert_eq!(file_ino, dentry_data_read);

        let (_, dentry_num) = ds.iterate_dentry(dir_ino, 0, 4096).await.unwrap();
        assert_eq!(dentry_num, 1);

        _ = ds.delete_dentry(dir_ino, file_name).await.unwrap();

        let (_, dentry_num) = ds.iterate_dentry(dir_ino, 0, 4096).await.unwrap();
        assert_eq!(dentry_num, 0);

        let attr = ds.get_attr(file_ino).await.unwrap();
        assert_eq!(attr.reserved, reserved);

        let value_read = ds.get_kvattr(file_ino, key).await.unwrap();
        assert_eq!(value_read.as_slice(), value.as_bytes());

        txg = ds.remove_kvattr(file_ino, key).await.unwrap();
        ds.wait_synced().await.unwrap();
        assert!(ds.get_last_synced_txg() >= txg);

        _ = ds.delete_inode(dir_ino, InodeType::DIR).await.unwrap();
        txg = ds.delete_inode(file_ino, InodeType::FILE).await.unwrap();
        ds.wait_synced().await.unwrap();
        assert!(ds.get_last_synced_txg() >= txg);

        assert_eq!(ds.list_object().await.unwrap(), num);
        ds.close().await.unwrap();
    }

    {
        let ds = Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap();

        let ino = ds.create_inode(InodeType::FILE).await.unwrap().0;
        let keys = ds.list_kvattrs(ino).await.unwrap();
        assert!(keys.is_empty());

        let total_kvs: usize = 4096;
        for i in 0..total_kvs {
            let key = i.to_string();
            let mut value: Vec<u8> = vec![];
            let value_size: usize = 256;
            value.resize_with(value_size, Default::default);

            ds.set_kvattr(ino, key.as_str(), &value, KvSetOption::HighPriority as u32)
                .await
                .unwrap();
            assert_eq!(ds.get_kvattr(ino, &key).await.unwrap(), value);
        }

        let keys = ds.list_kvattrs(ino).await.unwrap();
        assert_eq!(keys.len(), total_kvs);

        let mut numbers: Vec<usize> = Vec::<usize>::with_capacity(total_kvs);
        for key in keys {
            numbers.push(key.parse::<usize>().unwrap());
        }
        numbers.sort();

        let expect_vec: Vec<usize> = (0..total_kvs).collect();
        assert_eq!(numbers, expect_vec);

        ds.delete_inode(ino, InodeType::FILE).await.unwrap();
        ds.close().await.unwrap();
    }
    uzfs_env_fini().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_claim_test() {
    let ino;
    let dsname = "uzfs_claim_test/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    uzfs_env_init().await;

    {
        let ds = Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap();

        let claim_ino = ds.create_inode(InodeType::DIR).await.unwrap().0;
        ds.delete_inode(claim_ino, InodeType::DIR).await.unwrap();
        ds.claim_inode(claim_ino, 123456, InodeType::DIR)
            .await
            .unwrap();
        ds.create_dentry(claim_ino, "name", 0).await.unwrap();
        ds.delete_inode(claim_ino, InodeType::DIR).await.unwrap();
        ds.claim_inode(claim_ino, 123456, InodeType::DIR)
            .await
            .unwrap();
        ds.check_valid(claim_ino, 123456).await.unwrap();
        ds.close().await.unwrap();
    }

    {
        let ds = Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap();

        (ino, _) = ds.create_inode(InodeType::DIR).await.unwrap();

        ds.wait_synced().await.unwrap();
        ds.close().await.unwrap();
    }

    {
        let ds = Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap();

        // test claim when inode exists
        ds.claim_inode(ino, 0, InodeType::DIR).await.unwrap();

        ds.delete_inode(ino, InodeType::DIR).await.unwrap();
        ds.wait_synced().await.unwrap();

        // test claim when inode doesn't exist
        ds.claim_inode(ino, 0, InodeType::DIR).await.unwrap();
        ds.close().await.unwrap();
    }

    uzfs_env_fini().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_zap_iterator_test() {
    let dsname = "uzfs_zap_iterator_test/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    uzfs_env_init().await;

    let ds = Arc::new(
        Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap(),
    );

    let (zap_obj, _) = ds.zap_create().await.unwrap();
    let num_adders = 10;
    let num_ops_per_adder = 20000;

    let ds_remover = ds.clone();
    let remover_handle = tokio::task::spawn(async move {
        let mut total_ops = num_adders * num_ops_per_adder;
        while total_ops > 0 {
            for (key, value) in ds_remover.zap_list(zap_obj).await.unwrap() {
                ds_remover.zap_remove(zap_obj, &key).await.unwrap();
                assert_eq!(key.as_bytes(), value.as_slice());
                total_ops -= 1;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let mut adder_handles = vec![];
    for i in 0..num_adders {
        let adder_idx = i;
        let ds_adder = ds.clone();
        adder_handles.push(tokio::task::spawn(async move {
            for j in 0..num_ops_per_adder {
                let name = format!("{}_{}fghkjsghj", adder_idx, j);
                ds_adder
                    .zap_add(zap_obj, &name, name.as_bytes())
                    .await
                    .unwrap();
            }
        }));
    }

    for adder_handle in adder_handles {
        adder_handle.await.unwrap();
    }

    remover_handle.await.unwrap();
    ds.close().await.unwrap();
    uzfs_env_fini().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_expand_test() {
    let dsname = "uzfs_expand_test/ds";
    let mut uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    uzfs_env_init().await;

    let dev_path = uzfs_test_env.get_dev_path().unwrap();
    let ds = Arc::new(
        Dataset::init(dsname, &dev_path, DatasetType::Data, 4096, false)
            .await
            .unwrap(),
    );

    let io_workers = 10;
    let size = 20 << 20;
    let block_size = 1 << 18;
    let mut workers = Vec::new();
    for _ in 0..io_workers {
        let ds_clone = ds.clone();
        workers.push(tokio::task::spawn(async move {
            let buf = vec![123 as u8; block_size];
            let mut offset = 0;
            let obj = ds_clone.create_objects(1).await.unwrap().0[0];
            while offset < size {
                while ds_clone
                    .write_object(obj, offset, false, vec![&buf])
                    .await
                    .is_err()
                {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                offset += block_size as u64;
            }
        }));
    }

    let ds_expander = ds.clone();
    let expander = tokio::task::spawn(async move {
        let mut cur_size = 100 << 20;
        let target_size = 400 << 20;
        let incr_size = 20 << 20;
        while cur_size < target_size {
            tokio::time::sleep(Duration::from_secs(2)).await;
            cur_size += incr_size;
            uzfs_test_env.set_dev_size(cur_size);
            ds_expander.expand().await.unwrap();
        }
        uzfs_test_env
    });

    let _ = expander.await.unwrap();

    for worker in workers {
        worker.await.unwrap();
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_rangelock_test() {
    let dsname = "uzfs_rangelock_test/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    uzfs_env_init().await;

    let ds = Arc::new(
        Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Data,
            4096,
            false,
        )
        .await
        .unwrap(),
    );

    let obj = ds.create_objects(1).await.unwrap().0[0];

    let num_writers = 10;
    let max_file_size = 1 << 24;
    let write_size = 1 << 12;
    let num_writes_per_writer = 1 << 12;
    let version = Arc::new(AtomicU16::new(1));
    let num_readers = 5;

    let write_offsets = Arc::new(DashMap::new());

    let mut handles = Vec::<JoinHandle<()>>::with_capacity(num_readers + num_writers);

    for _ in 0..num_writers {
        let ds_clone = ds.clone();
        let version_clone = version.clone();
        let write_offsets_clone = write_offsets.clone();
        handles.push(tokio::task::spawn(async move {
            for _ in 0..num_writes_per_writer {
                let offset = rand::thread_rng().gen_range(0..(max_file_size - write_size));
                let my_version = version_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                write_offsets_clone.insert(my_version, offset);

                let mut buf_u16 = Vec::<u16>::with_capacity(write_size);
                buf_u16.resize(write_size, my_version);
                let buf_u8 = unsafe { buf_u16.align_to::<u8>().1 };
                ds_clone
                    .write_object(obj, offset as u64 * 2, false, vec![buf_u8])
                    .await
                    .unwrap();
            }
        }));
    }

    let read_size = 1 << 12;
    let num_reads_per_reader = 1 << 14;
    for _ in 0..num_readers {
        let ds_clone = ds.clone();
        let write_offsets_clone = write_offsets.clone();
        handles.push(tokio::task::spawn(async move {
            for _ in 0..num_reads_per_reader {
                let offset = rand::thread_rng().gen_range(0..(max_file_size - read_size));
                let data_u8 = ds_clone
                    .read_object(obj, offset as u64 * 2, read_size as u64)
                    .await
                    .unwrap();
                let data_u16 = unsafe { data_u8.align_to::<u16>().1 };

                // thread 1 a writes [l1, r1] with 1, thread 2 writes [l2, r2] with 2,
                // if the two intervals have common elements and some element is 1, thread 1 must writes after thread2,
                // so we can draw an edge from 1 to 2 in the dependency graph, no circle in this graph means writes are atomic
                let mut version_node_map = HashMap::new();
                let mut graph = DiGraph::new();
                for version in data_u16 {
                    if *version == 0 {
                        continue;
                    }

                    let off = *write_offsets_clone.get(version).unwrap();
                    if !version_node_map.contains_key(version) {
                        version_node_map.insert(*version, (graph.add_node(()), off));
                    }
                }

                // add edges to dependency_graph
                let size = data_u16.len();
                for idx in 0..size {
                    let version = data_u16[idx];
                    if version == 0 {
                        continue;
                    }

                    let node = version_node_map.get(&version).unwrap().0;
                    for (other_node, off) in version_node_map.values() {
                        if *other_node == node {
                            continue;
                        }

                        // current node written after other node, add an edge
                        if *off <= idx + offset && idx + offset < write_size + *off {
                            graph.update_edge(node, *other_node, ());
                        }
                    }
                }

                assert!(!is_cyclic_directed(&graph));
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_attr_test() {
    let dsname = "uzfs_attr_test/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    uzfs_env_init().await;

    let ds = Arc::new(
        Dataset::init(
            dsname,
            uzfs_test_env.get_dev_path().unwrap().as_str(),
            DatasetType::Meta,
            4096,
            false,
        )
        .await
        .unwrap(),
    );

    let ntests = 16;
    let nloops = 50;
    let max_key_size = 256;
    let max_value_size = 8192;
    let mut handles = Vec::new();
    for _ in 0..ntests {
        let ds_cloned = ds.clone();
        handles.push(tokio::spawn(async move {
            let inode_type = if rand::thread_rng().gen_bool(0.5) {
                InodeType::DIR
            } else {
                InodeType::FILE
            };
            let ino = ds_cloned.create_inode(inode_type).await.unwrap().0;
            assert!(ds_cloned.get_attr(ino).await.unwrap().reserved.is_empty());

            let mut kvs: HashMap<String, Vec<u8>> = HashMap::new();

            for _ in 0..nloops {
                let mut reserved: Vec<u8> = Vec::new();
                {
                    let mut rng = rand::thread_rng();
                    reserved.resize_with(MAX_RESERVED_SIZE, || rng.gen());
                }
                ds_cloned.set_attr(ino, &reserved).await.unwrap();

                let nkvs = 10;
                for _ in 0..nkvs {
                    let new_key;
                    let key = {
                        let mut rng = rand::thread_rng();
                        new_key = rng.gen_bool(0.3) || kvs.is_empty();
                        if new_key {
                            let key_size = rng.gen_range(1..max_key_size);
                            rng.sample_iter(&Alphanumeric)
                                .take(key_size)
                                .map(char::from)
                                .collect()
                        } else {
                            let key_idx = rng.gen_range(0..kvs.len());
                            kvs.iter().nth(key_idx).unwrap().0.to_owned()
                        }
                    };

                    let value = {
                        let mut rng = rand::thread_rng();
                        if new_key || rng.gen_bool(0.8) {
                            let value_size = rng.gen_range(1..max_value_size);
                            let mut value: Vec<u8> = Vec::new();
                            value.resize_with(value_size, || rng.gen());
                            value
                        } else {
                            Vec::new()
                        }
                    };

                    if value.is_empty() {
                        ds_cloned.remove_kvattr(ino, &key).await.unwrap();
                        kvs.remove(&key).unwrap();
                    } else {
                        ds_cloned
                            .set_kvattr(ino, &key, &value, KvSetOption::HighPriority as u32)
                            .await
                            .unwrap();
                        kvs.insert(key, value);
                    }

                    let mut stored_keys = ds_cloned.list_kvattrs(ino).await.unwrap();
                    stored_keys.sort();
                    let mut keys: Vec<String> = kvs.keys().map(|k| k.to_owned()).collect();
                    keys.sort();
                    assert_eq!(keys, stored_keys);

                    for (k, v) in &kvs {
                        let value = ds_cloned.get_kvattr(ino, k).await.unwrap();
                        assert_eq!(*v, value);
                    }

                    assert_eq!(ds_cloned.get_attr(ino).await.unwrap().reserved, reserved);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}

async fn test_reduce_max(dsname: &str, dev_path: &str) {
    let ds = Dataset::init(dsname, &dev_path, DatasetType::Data, 4096, false)
        .await
        .unwrap();
    let objs = ds.create_objects(4).await.unwrap().0;
    // original max > blksize of obj0 > reduced max, but is not power of 2
    let data0 = vec![1; 3 << 9];
    ds.write_object(objs[0], 0, false, vec![&data0])
        .await
        .unwrap();
    let obj_attr0 = ds.get_object_attr(objs[0]).await.unwrap();
    assert_eq!(obj_attr0.blksize, data0.len() as u32);
    // original max > blksize of obj1 > reduced max, is power of 2
    let data1 = vec![1; 4 << 9];
    ds.write_object(objs[1], 0, false, vec![&data1])
        .await
        .unwrap();
    let blksize1 = ds.get_object_attr(objs[1]).await.unwrap().blksize;
    assert_eq!(blksize1, data1.len() as u32);
    // blksize of obj2 > original max > reduced max
    let data2 = vec![1; 9 << 9];
    ds.write_object(objs[2], 0, false, vec![&data2])
        .await
        .unwrap();
    let blksize2 = ds.get_object_attr(objs[2]).await.unwrap().blksize;
    assert_eq!(blksize2, 4096);
    // original max > reduced max > blksize of obj3
    let data3 = vec![1; 1 << 9];
    ds.write_object(objs[3], 0, false, vec![&data3])
        .await
        .unwrap();
    assert_eq!(
        ds.get_object_attr(objs[3]).await.unwrap().blksize,
        data3.len() as u32
    );
    ds.close().await.unwrap();

    let ds = Dataset::init(dsname, &dev_path, DatasetType::Data, 1024, false)
        .await
        .unwrap();
    ds.write_object(objs[0], data0.len() as u64, false, vec![&data0])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[0]).await.unwrap().blksize, 2048);
    ds.write_object(objs[1], data1.len() as u64, false, vec![&data1])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[1]).await.unwrap().blksize, blksize1);
    ds.write_object(objs[2], data2.len() as u64, false, vec![&data2])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[2]).await.unwrap().blksize, blksize2);
    ds.write_object(objs[3], 0, false, vec![&data2])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[3]).await.unwrap().blksize, 1024);
    ds.close().await.unwrap();
}

async fn test_increase_max(dsname: &str, dev_path: &str) {
    let ds = Dataset::init(dsname, &dev_path, DatasetType::Data, 1024, false)
        .await
        .unwrap();
    let objs = ds.create_objects(3).await.unwrap().0;
    // blksize of obj0 > increased max > original max
    let data0 = vec![1; 9 << 9];
    ds.write_object(objs[0], 0, false, vec![&data0])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[0]).await.unwrap().blksize, 1024);
    // increased max > blksize of obj1 > original max
    let data1 = vec![1; 3 << 9];
    ds.write_object(objs[1], 0, false, vec![&data1])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[1]).await.unwrap().blksize, 1024);
    // increased max > orignal max > blksize of obj2
    let data2 = vec![1; 1 << 9];
    ds.write_object(objs[2], 0, false, vec![&data2])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[2]).await.unwrap().blksize, 512);
    ds.close().await.unwrap();

    let ds = Dataset::init(dsname, &dev_path, DatasetType::Data, 4096, false)
        .await
        .unwrap();
    ds.write_object(objs[0], data0.len() as u64, false, vec![&data0])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[0]).await.unwrap().blksize, 1024);
    ds.write_object(objs[1], data1.len() as u64, false, vec![&data1])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[1]).await.unwrap().blksize, 1024);
    ds.write_object(objs[2], data2.len() as u64, false, vec![&data0])
        .await
        .unwrap();
    assert_eq!(ds.get_object_attr(objs[2]).await.unwrap().blksize, 4096);
    ds.close().await.unwrap();
}

#[tokio::test]
#[serial]
async fn uzfs_block_test() {
    let dsname = "uzfs-test-pool/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    let dev_path = uzfs_test_env.get_dev_path().unwrap();
    uzfs_env_init().await;
    test_reduce_max(dsname, &dev_path).await;
    test_increase_max(dsname, &dev_path).await;
    uzfs_env_fini().await;
}

#[ignore]
#[test]
fn uzfs_sync_test() {
    let mut obj = 0;
    let dev_path = "/tmp/uzfs.img";
    let _ = std::fs::remove_file(dev_path);
    let mut stored_data = Vec::<u64>::new();
    let key = "ababaa";
    let mut stored_value: Vec<u8> = Vec::new();
    let mut stored_mtime = timespec::default();
    for i in 0..100 {
        // smallest write block is 16K
        // file is divided by several 256K blocks
        let file_blocks = rand::thread_rng().gen_range(64..=128);
        let mut write_data = Vec::new();
        if i < 99 {
            for _ in 0..file_blocks {
                let mut write_blocks_remained = 16;
                while write_blocks_remained > 0 {
                    let blocks = rand::thread_rng().gen_range(1..=write_blocks_remained);
                    write_data.push(blocks);
                    write_blocks_remained -= blocks;
                }
            }
            write_data.push(2);
        }
        let mut rng = rand::thread_rng();
        let value: Vec<u8> = (0..64).map(|_| rng.gen()).collect();
        let mtime = timespec {
            tv_sec: rng.gen(),
            tv_nsec: rng.gen(),
        };

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child, .. }) => {
                let status = waitpid(child, None).unwrap();
                if let WaitStatus::Exited(_, res) = status {
                    obj = res as u64;
                } else {
                    println!("{status:?} not expected");
                    abort();
                }
            }
            Ok(ForkResult::Child) => {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let obj = rt.block_on(async move {
                    let dsname = "uzfs_sync_test/ds";
                    if obj == 0 {
                        let mut options = std::fs::OpenOptions::new();
                        options
                            .write(true)
                            .create(true)
                            .open(dev_path)
                            .unwrap()
                            .set_len(128 << 20)
                            .unwrap();
                    }
                    uzfs_env_init().await;
                    let ds = Arc::new(
                        Dataset::init(dsname, dev_path, DatasetType::Data, 262144, false)
                            .await
                            .unwrap(),
                    );

                    if obj == 0 {
                        obj = ds.create_objects(1).await.unwrap().0[0];
                    } else {
                        let read_mtime = ds.get_object_attr(obj).await.unwrap().mtime;
                        assert_eq!(read_mtime.tv_sec, stored_mtime.tv_sec);
                        assert_eq!(read_mtime.tv_nsec, stored_mtime.tv_nsec);
                        let v = ds.get_kvattr(obj, key).await.unwrap();
                        assert_eq!(v, stored_value);
                    }

                    // check data written before
                    let mut offset = 0;
                    for (i, len) in stored_data.into_iter().enumerate() {
                        let size = len << 14;
                        for (idx, ele) in ds
                            .read_object(obj, offset, size)
                            .await
                            .unwrap()
                            .into_iter()
                            .enumerate()
                        {
                            if ele != i as u8 {
                                println!(
                                    "offset: {offset}, size: {size}, idx: {idx}, {ele} != {i}"
                                );
                                abort();
                            }
                        }
                        offset += size;
                    }
                    assert_eq!(ds.get_object_attr(obj).await.unwrap().size, offset);
                    ds.truncate_object(obj, 0, 0).await.unwrap();

                    let mut writer_handles = Vec::<JoinHandle<()>>::with_capacity(write_data.len());
                    offset = 0;
                    for (i, len) in write_data.into_iter().enumerate() {
                        let ds_cloned = ds.clone();
                        let size = len << 14;
                        writer_handles.push(tokio::task::spawn(async move {
                            let data = vec![i as u8; size as usize];
                            let sync = rand::thread_rng().gen_bool(0.5);
                            ds_cloned
                                .write_object(obj, offset, sync, vec![&data])
                                .await
                                .unwrap();
                        }));
                        offset += size;
                    }

                    for handle in writer_handles {
                        handle.await.unwrap();
                    }

                    ds.sync_object(obj).await;
                    ds.set_kvattr(obj, key, &value, KvSetOption::NeedLog as u32)
                        .await
                        .unwrap();
                    ds.set_object_mtime(obj, mtime.tv_sec, mtime.tv_nsec)
                        .await
                        .unwrap();
                    ds.sync_object(obj).await;

                    assert_eq!(ds.get_object_attr(obj).await.unwrap().size, offset);
                    obj
                });
                exit(obj as i32);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }

        stored_data = write_data;
        stored_value = value;
        stored_mtime = mtime;
    }

    let _ = std::fs::remove_file(dev_path);
}
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uzfs_write_read_test() {
    let dsname = "uzfs_write_read_test/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    let dev_path = uzfs_test_env.get_dev_path().unwrap();
    uzfs_env_init().await;

    let concurrency = 64;
    let ds = Arc::new(
        Dataset::init(dsname, &dev_path, DatasetType::Data, 65536, false)
            .await
            .unwrap(),
    );

    for _ in 0..10 {
        let mut handles = Vec::with_capacity(concurrency);
        let obj = ds.create_objects(1).await.unwrap().0[0];
        let blksize = 16 << 10;
        for i in 0..concurrency {
            let ds = ds.clone();
            let offset = blksize * i;
            handles.push(tokio::spawn(async move {
                let data: Vec<_> = (0..blksize).map(|_| rand::thread_rng().gen()).collect();
                ds.write_object(obj, offset as u64, false, vec![&data])
                    .await
                    .unwrap();
                let read = ds
                    .read_object(obj, offset as u64, blksize as u64)
                    .await
                    .unwrap();
                assert!(read == data);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        ds.delete_object(obj).await.unwrap();
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}

#[tokio::test]
#[serial]
async fn uzfs_truncate_test() {
    let dsname = "uzfs-truncate-test-pool/ds";
    let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
    let dev_path = uzfs_test_env.get_dev_path().unwrap();
    uzfs_env_init().await;

    let blksize = 65536;
    let ds = Arc::new(
        Dataset::init(dsname, &dev_path, DatasetType::Data, blksize, false)
            .await
            .unwrap(),
    );

    let iters = 10000;
    let objs = ds.create_objects(iters).await.unwrap().0;
    let max_end_size = blksize * 2;
    let mut total_data: Vec<u8> = vec![0; max_end_size as usize];
    for obj in objs {
        let write_size = rand::thread_rng().gen_range(512..max_end_size) as u64;
        let truncate_size = rand::thread_rng().gen_range(512..max_end_size) as u64;
        total_data.fill(0);

        let data = vec![1; write_size as usize];
        let end_size = if rand::thread_rng().gen_bool(0.5) {
            total_data[..data.len()].copy_from_slice(&data);
            total_data[(truncate_size as usize)..].fill(0);
            ds.write_object(obj, 0, false, vec![&data]).await.unwrap();
            ds.truncate_object(obj, 0, truncate_size).await.unwrap();
            truncate_size
        } else {
            total_data[(truncate_size as usize)..].fill(0);
            total_data[..data.len()].copy_from_slice(&data);
            ds.truncate_object(obj, 0, truncate_size).await.unwrap();
            ds.write_object(obj, 0, false, vec![&data]).await.unwrap();
            std::cmp::max(write_size, truncate_size)
        };

        let read_off = rand::thread_rng().gen_range(0..end_size);
        let read_size = rand::thread_rng().gen_range(0..(max_end_size as u64 - read_off));
        let actually_read = std::cmp::min(end_size - read_off, read_size);
        let data = ds.read_object(obj, read_off, read_size).await.unwrap();
        assert_eq!(actually_read, data.len() as u64);
        assert_eq!(
            data,
            total_data[(read_off as usize)..((read_off + actually_read) as usize)]
        );

        let obj_attr = ds.get_object_attr(obj).await.unwrap();
        assert_eq!(obj_attr.size, end_size);
        let obj_blksize = obj_attr.blksize;
        if obj_blksize < blksize {
            assert!(obj_blksize >= end_size as u32);
        } else {
            assert_eq!(obj_blksize, blksize);
        }

        ds.delete_object(obj).await.unwrap();
    }

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}