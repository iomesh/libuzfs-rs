# libuzfs-rs

Rust APIs for userworld ZFS(uZFS)
[中文版本](README-cn.md)

## Motivation

ZFS is a powerful file system with excellent features such as Copy-On-Write (COW), snapshots, and ZFS Send/Receive. When developing a new userspace file system, we aim to build upon the existing features of ZFS. Additionally, the clear modular design of ZFS allows us to customize certain components, such as the ZFS VDEV layer.

Rust has become increasingly popular in recent years. Its strict compile-time checks help avoid many concurrency and memory issues that often trouble C/C++ developers. Rust's async ecosystem is well-developed, with powerful runtimes like Tokio, making it easier to build I/O-intensive applications. Moreover, Rust offers a C-compatible ABI, which allows us to seamlessly reuse existing C code.

The primary goal of libuzfs-rs is to provide Rust APIs based on ZFS and efficiently run ZFS code within the Rust async runtime at minimal cost. This makes it easier for developers to quickly build a high-performance userspace file system.

## Get Started

### Environment Setup

#### Ubuntu

1. Dependencies for building ZFS

` sudo apt install uuid-dev libblkid-dev libudev-dev libtirpc-dev`

2. Install Rust

`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

### Run Existing Code

#### Unit Tests

`make test`

#### uzfs-bench

uzfs-bench is a bench for data read and write

1. Compile binaries

`cargo build --all --release`

2. Run uzfs-bench

`./target/release/uzfs-bench <dev_path> false`

`dev_name` can also be file path

### libuzfs-rs Components

#### Dataset

Dataset is ZFS Dataset，but we only create one dataset for one disk

```rust
pub struct Dataset {
    dhp: *mut libuzfs_dataset_handle_t,
    zhp: *mut libuzfs_zpool_handle_t,
    poolname: CString,
    metrics: Box<UzfsMetrics>,
}

pub async fn init(
    dsname: &str,
    dev_path: &str,
    dstype: DatasetType,
    max_blksize: u32,
    already_formatted: bool,
    hostid: Option<String>,
) -> Result<Self> {}
pub async fn close(&self) -> Result<()> {}
```

#### InodeHandle

InodeHandle is a in-memory data structure used for data and meta operations. It works similarly as ZFS znode

```rust
pub struct Dataset {
    dhp: *mut libuzfs_dataset_handle_t,
    zhp: *mut libuzfs_zpool_handle_t,
    poolname: CString,
    metrics: Box<UzfsMetrics>,
}

pub async fn get_inode_handle(&self, ino: u64, gen: u64, is_data_inode: bool) -> Result<InodeHandle> {}
pub async fn release_inode_handle(&self, ino_hdl: &mut InodeHandle) {}
```

#### Meta Operations

Here, the inode refers to a metadata inode. Therefore, the inode created by create_inode can only perform metadata-related operations, such as attribute (attr) and dentry operations.

```rust
pub async fn get_attr(&self, ino_hdl: &InodeHandle) -> Result<InodeAttr> {}
pub async fn set_attr(&self, ino_hdl: &mut InodeHandle, reserved: &[u8]) -> Result<u64> {}
pub async fn create_inode(&self, inode_type: InodeType) -> Result<InodeHandle> {}
pub async fn delete_inode(&self, ino_hdl: &mut InodeHandle, inode_type: InodeType) -> Result<u64> {}
```

#### Data Operations

The objects created by create_objects can only be used for data read and write operations. The naming here can be somewhat misleading. Although read and write operations on objects require passing an InodeHandle, the InodeHandle of the object cannot be used to perform the inode operations mentioned in the previous section.

```rust
pub async fn create_objects(&self, num_objs: usize) -> Result<(Vec<u64>, u64)> {}
pub async fn delete_object(&self, ino_hdl: &mut InodeHandle) -> Result<()> {}
pub async fn read_object(&self, ino_hdl: &InodeHandle, offset: u64, size: u64) -> Result<Vec<u8>> {}
pub async fn write_object(&self, ino_hdl: &InodeHandle, offset: u64, sync: bool, data: Vec<&[u8]>) -> Result<()> {}
```

### Simple Example

```rust
use uzfs::*;

#[tokio::main]
async fn main() {
    uzfs_env_init().await;
    let dev_path = std::env::args().nth(1).unwrap();
    let ds = Dataset::init("testzp/ds", &dev_path, DatasetType::Data, 0, false, None)
        .await
        .unwrap();

    // create a dir inode
    let mut dir_hdl = ds.create_inode(InodeType::DIR).await.unwrap();

    // create a file inode
    let mut file_hdl = ds.create_inode(InodeType::FILE).await.unwrap();

    // create a dentry for the file inode in the dir inode
    ds.create_dentry(&mut dir_hdl, "haha", file_hdl.ino)
        .await
        .unwrap();

    ds.release_inode_handle(&mut dir_hdl).await;
    ds.release_inode_handle(&mut file_hdl).await;

    ds.close().await.unwrap();
    uzfs_env_fini().await;
}
```
