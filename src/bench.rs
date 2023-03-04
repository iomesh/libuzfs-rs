use criterion::*;
use std::mem::size_of;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use uzfs::*;
use uzfs_sys as sys;

struct UzfsBenchEnv {
    dataset: Rc<Dataset>,
    _uzfs_env: UzfsTestEnv,
}

impl UzfsBenchEnv {
    pub fn new() -> Self {
        const BENCH_DEV_SIZE: u64 = 500 * 1024 * 1024;

        let env = UzfsTestEnv::new(BENCH_DEV_SIZE);
        let uzfs = Arc::new(Uzfs::init().unwrap());
        let ds = Rc::new(
            Dataset::init("test_pool/ds", env.get_dev_path().unwrap().as_str(), uzfs).unwrap(),
        );

        Self {
            dataset: ds,
            _uzfs_env: env,
        }
    }

    pub fn get_ds(&self) -> Rc<Dataset> {
        self.dataset.clone()
    }
}

pub fn bench_inode_ops(c: &mut Criterion) {
    let mut objs: Vec<u64> = vec![];
    let env = UzfsBenchEnv::new();
    c.bench_function("uzfs::create_inode", |b| {
        b.iter(|| {
            let (obj, _) = env.get_ds().create_inode(InodeType::FILE).unwrap();
            objs.push(obj);
        })
    });

    // because criterion.rs will warm up before real benchmark begins,
    // in order to make uzfs::delete_inode has enough inodes for deleting,
    // we need to create more inodes
    for _ in 0..10000 {
        let (obj, _) = env.get_ds().create_inode(InodeType::FILE).unwrap();
        objs.push(obj);
    }
    c.bench_function("uzfs::delete_inode", |b| {
        b.iter(|| {
            let obj = objs.pop().unwrap();
            env.get_ds().delete_inode(obj, InodeType::FILE).unwrap();
        })
    });
}

pub fn bench_inode_attr(c: &mut Criterion) {
    let env = UzfsBenchEnv::new();
    let (obj, _) = env.get_ds().create_inode(InodeType::FILE).unwrap();
    let attr_size = size_of::<sys::uzfs_attr_t>();
    c.bench_function("uzfs::get_attr", |b| {
        b.iter(|| {
            env.get_ds().get_attr(obj, attr_size as u64).unwrap();
        })
    });

    let mut buf = Vec::<u8>::with_capacity(attr_size);
    buf.resize(attr_size, 0);
    c.bench_function("uzfs::set_attr", |b| {
        b.iter(|| {
            env.get_ds().set_attr(obj, &buf).unwrap();
        })
    });
}

pub fn bench_inode_kvattr(c: &mut Criterion) {
    let env = UzfsBenchEnv::new();
    let basic_value_size: usize = 128;
    for i in [1, 8, 32, 64].iter() {
        let value_size = i * basic_value_size;
        let (obj, _) = env.get_ds().create_inode(InodeType::FILE).unwrap();
        let mut buf = Vec::<u8>::with_capacity(value_size);
        buf.resize(value_size, 0);

        // as stored kv pairs become more, set_kvattr will cost much more time.
        // total_kvs is used to limit total number of kv pairs
        let total_kvs = 1000;
        let mut cur_idx = 0;
        c.bench_function(
            std::format!("uzfs::set_kvattr_{value_size}").as_str(),
            |b| {
                b.iter(|| {
                    let name = cur_idx.to_string();
                    cur_idx = (cur_idx + 1) % total_kvs;
                    env.get_ds()
                        .set_kvattr(obj, name.as_str(), &buf, 0)
                        .unwrap();
                });
            },
        );

        c.bench_function(
            std::format!("uzfs::get_kvattr_{value_size}").as_str(),
            |b| {
                b.iter(|| {
                    cur_idx = (cur_idx + 1) % total_kvs;
                    env.get_ds()
                        .get_kvattr(obj, cur_idx.to_string().as_str(), 0)
                        .unwrap();
                });
            },
        );
        env.get_ds().delete_inode(obj, InodeType::FILE).unwrap();
    }
}

pub fn bench_dir_ops(c: &mut Criterion) {
    let env = UzfsBenchEnv::new();
    let (pino, _) = env.get_ds().create_inode(InodeType::DIR).unwrap();

    let mut total_entries = 0;
    c.bench_function("uzfs::create_dentry", |b| {
        b.iter(|| {
            let name = total_entries.to_string();
            total_entries += 1;
            env.get_ds().create_dentry(pino, name, 1).unwrap();
        });
    });

    // similar to inode bench, create more dentries so that delete entry has enough dentries to delete
    for _ in 0..10000 {
        env.get_ds()
            .create_dentry(pino, total_entries.to_string(), 1)
            .unwrap();
        total_entries += 1;
    }

    let mut cur_lookup_idx = 0;
    c.bench_function("uzfs::lookup_dentry", |b| {
        b.iter(|| {
            let name = cur_lookup_idx.to_string();
            cur_lookup_idx = (cur_lookup_idx + 1) % total_entries;
            env.get_ds().lookup_dentry(pino, name).unwrap();
        });
    });

    let mut cur_delete_idx = 0;
    c.bench_function("uzfs::delete_entry", |b| {
        b.iter(|| {
            env.get_ds()
                .delete_dentry(pino, cur_delete_idx.to_string())
                .unwrap();
            cur_delete_idx += 1;
        });
    });
}

pub fn bench_object_read_write(c: &mut Criterion) {
    let env = UzfsBenchEnv::new();
    let (obj, _) = env.get_ds().create_object().unwrap();
    for size in [4 * 1024, 16 * 1024, 32 * 1024].iter() {
        let mut buf = Vec::<u8>::with_capacity(size.to_owned());
        buf.resize(size.to_owned(), 0);
        let mut i: usize = 0;
        c.bench_function(
            std::format!("uzfs::write_object_nosync_{size}").as_str(),
            |b| {
                b.iter(|| {
                    i = (i + 1) % buf.len();
                    buf[i] = buf[i] + 1;
                    env.get_ds().write_object(obj, 0, false, &buf).unwrap();
                });
            },
        );

        buf.resize(size.to_owned(), 1);
        c.bench_function(
            std::format!("uzfs::write_object_sync_{size}").as_str(),
            |b| {
                b.iter(|| {
                    i = (i + 1) % buf.len();
                    buf[i] = buf[i] + 1;
                    env.get_ds().write_object(obj, 0, true, &buf).unwrap();
                });
            },
        );

        c.bench_function(std::format!("uzfs::read_object_{size}").as_str(), |b| {
            b.iter(|| {
                env.get_ds()
                    .read_object(obj, 0, size.to_owned() as u64)
                    .unwrap();
            });
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).significance_level(0.1);
    targets = bench_inode_ops, bench_inode_attr, bench_inode_kvattr, bench_dir_ops, bench_object_read_write
);
criterion_main!(benches);
