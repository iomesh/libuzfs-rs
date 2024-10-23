# libuzfs-rs

Rust APIs for userworld ZFS(uZFS)
[English Readme](README.md)

## 目标

ZFS 是一款功能强大的文件系统，凭借其出色的特性，如写时复制（COW）、快照功能以及 ZFS Send/Receive 等，赢得了广泛的认可。因此，在开发新的用户态文件系统时，我们希望充分利用 ZFS 现有的优势进行构建。同时，ZFS 模块之间的清晰分层结构也为我们自定义模块，如 ZFS VDEV 层，提供了极大的便利。

近年来，Rust 作为一种新兴编程语言逐渐崭露头角。它通过严格的编译期检查，有效地解决了困扰许多 C/C++ 开发者的并发和内存管理难题。Rust 的异步编程生态系统也日趋完善，其中 Tokio 这类强大的运行时（Runtime）更是让开发者能够轻松开发 IO 密集型应用。此外，Rust 还与 C 语言具有兼容的应用二进制接口（ABI），这让我们能够方便地复用现有的 C 代码。

libuzfs-rs 的核心目标是基于 ZFS 提供 Rust API，并尽量减小将 ZFS 代码运行在 Rust 异步运行时中的额外开销。这样，使用者就能更加迅速地实现一个高性能的用户态文件系统。

## 开始上手

### 环境准备

#### Ubuntu

1. 安装编译 ZFS 需要的依赖

` sudo apt install uuid-dev libblkid-dev libudev-dev libtirpc-dev`

2. 安装 Rust

`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

### 运行已有代码

#### 单测

`make test`

#### uzfs-bench

uzfs-bench 是数据读写吞吐测试

1. 编译二进制

`cargo build --all --release`

2. 运行uzfs-bench

`./target/release/uzfs-bench <dev_path> false`

其中 `dev_name` 也可以是 size > 128GiB 的普通文件

### libuzfs-rs组件

#### Dataset

Dataset 对应 ZFS 中 Dataset，区别在于 libuzfs-rs 中对于一个盘只会创建一个 Dataset

```rust
pub struct Dataset {
    dhp: *mut libuzfs_dataset_handle_t,
    zhp: *mut libuzfs_zpool_handle_t,
    poolname: CString,
    metrics: Box<UzfsMetrics>,
}

pub async fn init(dsname: &str, dev_path: &str, max_blksize: u32) -> Result<Self> {}
pub async fn close(&self) -> Result<()> {}
```

#### InodeHandle

作用类似 ZFS 中的 znode，用于操作元数据和数据

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

#### 元数据操作

这里 inode 指的是元数据的 inode，因此 create_inode 创建的 inode 只能进行元数据操作如 attr 以及 dentry 相关的操作

```rust
pub async fn get_attr(&self, ino_hdl: &InodeHandle) -> Result<InodeAttr> {}
pub async fn set_attr(&self, ino_hdl: &mut InodeHandle, reserved: &[u8]) -> Result<u64> {}
pub async fn create_inode(&self, inode_type: InodeType) -> Result<InodeHandle> {}
pub async fn delete_inode(&self, ino_hdl: &mut InodeHandle, inode_type: InodeType) -> Result<u64> {}
```

#### 数据操作

create_objects 创建的 object 只能用于进行数据读写操作，这里命名有一些令人误解，虽然 object 的读写操作需要传入 InodeHandle，但是并不能对 object 的 InodeHandle 进行上一节中的 inode 的操作

```rust
pub async fn create_objects(&self, num_objs: usize) -> Result<(Vec<u64>, u64)> {}
pub async fn delete_object(&self, ino_hdl: &mut InodeHandle) -> Result<()> {}
pub async fn read_object(&self, ino_hdl: &InodeHandle, offset: u64, size: u64) -> Result<Vec<u8>> {}
pub async fn write_object(&self, ino_hdl: &InodeHandle, offset: u64, sync: bool, data: Vec<&[u8]>) -> Result<()> {}
```

### 简单示例

```rust
use uzfs::*;

#[tokio::main]
async fn main() {
    uzfs_env_init().await;
    let dev_path = std::env::args().nth(1).unwrap();
    let ds = Dataset::init("testzp/ds", &dev_path, DatasetType::Data, 0, false)
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

