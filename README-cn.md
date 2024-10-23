# libuzfs-rs

Rust APIs for userworld ZFS(uZFS)
[English Readme](README.md)

## 目标

ZFS是一个强大的文件系统，其拥有COW、快照、ZFS Send/Receive等优秀的特性，因此在开发一个新的用户态文件系统时，我们想基于ZFS已有的特性进行开发，而且由于ZFS各个模块分层很清晰，这也有利于我们自定义一些模块如ZFS VDEV层。

Rust是近年来逐渐流行的编程语言，其通过严格的编译时期的检查避免了令许多C/C++开发者头疼的并发以及内存问题。Rust Async生态非常完善，其中有Tokio这样强大的Runtime，使得开发者能够比较简单地开发一个IO密集型应用，而且Rust拥有与C兼容的ABI，使得我们能够很简便地服用已有的C代码。

libuzfs-rs的主要目标就是基于ZFS提供Rust API，并以较小的代价将ZFS代码跑在Rust Async Runtime中，进而便于使用者快速地实现一个高效地用户态文件系统。

## 开始上手

### 环境准备

#### Ubuntu

1. 安装编译ZFS需要的依赖

` sudo apt install uuid-dev libblkid-dev libudev-dev libtirpc-dev`

2. 安装Rust

`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

### 运行已有代码

#### 单测

`make test`

#### uzfs-bench

uzfs-bench是数据读写吞吐测试

1. 编译二进制

`cargo build --all --release`

2. 运行uzfs-bench

`./target/release/uzfs-bench <dev_path> false`

其中dev_name也可以是size > 128GiB的普通文件

### libuzfs-rs组件

#### Dataset

Dataset对应ZFS中Dataset，区别在于libuzfs-rs中对于一个盘只会创建一个Dataset

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

作用类似ZFS中的znode，用于操作元数据和数据

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

这里inode指的是元数据的inode，因此create_inode创建的inode只能进行元数据操作如attr以及dentry相关的操作

```rust
pub async fn get_attr(&self, ino_hdl: &InodeHandle) -> Result<InodeAttr> {}
pub async fn set_attr(&self, ino_hdl: &mut InodeHandle, reserved: &[u8]) -> Result<u64> {}
pub async fn create_inode(&self, inode_type: InodeType) -> Result<InodeHandle> {}
pub async fn delete_inode(&self, ino_hdl: &mut InodeHandle, inode_type: InodeType) -> Result<u64> {}
```

#### 数据操作

create_objects创建的object只能用于进行数据读写操作，这里命名有一些令人误解，虽然object的读写操作需要传入InodeHandle，但是并不能对object的InodeHandle进行上一节中的inode的操作

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

