use std::ffi::CStr;
use std::ffi::CString;
use std::io;
use std::io::Error;
use std::io::ErrorKind;
use std::os::raw::{c_char, c_void};
use std::ptr::null_mut;

use cstr_argument::CStrArgument;
use io::Result;
use once_cell::sync::OnceCell;
use tokio::sync::Mutex;

use crate::bindings::async_sys::*;
use crate::bindings::sys::*;
use crate::context::coroutine::CoroutineFuture;
use crate::metrics::{RequestMethod, UzfsMetrics};

pub const DEFAULT_CACHE_FILE: &str = "/tmp/zpool.cache";

static UZFS_INIT_REF: OnceCell<Mutex<u32>> = OnceCell::new();
pub const MAX_RESERVED_SIZE: usize = 192;
const UZFS_DNODESIZE_META: u32 = 1024;
const UZFS_DNODESIZE_DATA: u32 = 512;

/// Configure uzfs parameters.
///
/// - `arc_min`: Minimum ARC memory, set to 1/4 of `mem_max`.
/// - `arc_max`: Maximum ARC memory, set to 3/4 of `mem_max`.
/// - `arc_c`: Current ARC memory limit, dynamically adjustable between `arc_min` and `arc_max`.
/// - `meta_percent`: Percentage of total memory allocated for metadata (e.g., dnodes and dentries).
/// - `enable_compress`: Enables or disables data compression during TXG syncing.
pub fn config_uzfs(mem_max: usize, meta_percent: usize, enable_compress: bool) {
    unsafe { libuzfs_config(mem_max, meta_percent, enable_compress as u32) };
}

///
/// Wakeup background arc evictor
///
pub async fn wakeup_arc_evictor() {
    CoroutineFuture::new(libuzfs_wakeup_arc_evictor_c, 0).await;
}

#[derive(Default)]
pub struct InodeAttr {
    pub gen: u64,
    pub blksize: u32,
    pub blocks: u64,
    pub reserved: Vec<u8>,
}

pub async fn uzfs_debug_main() {
    unsafe { set_libuzfs_ops(None) };
    let mut args: Vec<_> = std::env::args()
        .map(|arg| CString::new(arg).unwrap().into_raw())
        .collect();

    let mut coroutine_arg = LibuzfsDebugArgs {
        argc: args.len() as i32,
        argv: args.as_mut_ptr(),
    };
    let arg_usize = &mut coroutine_arg as *mut _ as usize;
    CoroutineFuture::new_with_stack_size(libuzfs_debug_main_c, arg_usize, 16 << 20).await;

    for arg in args {
        let _ = unsafe { CString::from_raw(arg) };
    }
}

pub async fn uzfs_env_init() {
    unsafe extern "C" fn print_log(buf: *const c_char, new_line: i32) {
        let buf = CStr::from_ptr(buf);
        if new_line != 0 {
            println!("{}", buf.to_bytes().escape_ascii());
        } else {
            print!("{}", buf.to_bytes().escape_ascii());
        }
    }

    let _ = std::fs::remove_file(DEFAULT_CACHE_FILE);
    let mut guard = UZFS_INIT_REF.get_or_init(|| Mutex::new(0)).lock().await;

    if *guard == 0 {
        unsafe { set_libuzfs_ops(Some(print_log)) };
        CoroutineFuture::new(libuzfs_init_c, 0).await;
    }

    *guard += 1;
}

pub fn set_fail_percent(fp: i32) {
    unsafe { libuzfs_set_fail_percent(fp) };
}

#[inline]
pub fn uzfs_set_zpool_cache_path<P: CStrArgument>(path: P) {
    unsafe {
        libuzfs_set_zpool_cache_path(path.into_cstr().as_ref().as_ptr());
    }
}

pub fn enable_debug_msg() {
    unsafe { libuzfs_enable_debug_msg() };
}

pub fn disable_debug_msg() {
    unsafe { libuzfs_disable_debug_msg() };
}

pub async fn uzfs_env_fini() {
    let mut guard = UZFS_INIT_REF.get().unwrap().lock().await;
    if *guard == 1 {
        CoroutineFuture::new(libuzfs_fini_c, 0).await;
    }
    *guard -= 1;
}

pub enum InodeType {
    FILE = libuzfs_inode_type_t_INODE_FILE as isize,
    DIR = libuzfs_inode_type_t_INODE_DIR as isize,
    DATAOBJ = libuzfs_inode_type_t_INODE_DATA_OBJ as isize,
}

pub enum DatasetType {
    Data,
    Meta,
}

#[derive(Debug, PartialEq, Eq)]
#[must_use]
#[warn(unused_must_use)]
pub struct InodeHandle {
    ihp: *mut libuzfs_inode_handle_t,
}

impl Default for InodeHandle {
    fn default() -> Self {
        Self { ihp: null_mut() }
    }
}

unsafe impl Send for InodeHandle {}
unsafe impl Sync for InodeHandle {}

pub enum KvSetOption {
    None = 0,
    HighPriority = 1 << 0,
    NeedLog = 1 << 1,
}

pub struct Dataset {
    dhp: *mut libuzfs_dataset_handle_t,
    zhp: *mut libuzfs_zpool_handle_t,
    poolname: CString,
    metrics: Box<UzfsMetrics>,
}

// metrics
impl Dataset {
    pub fn metrics(&self) -> &UzfsMetrics {
        &self.metrics
    }
}

// control functions
impl Dataset {
    fn dsname_to_poolname(dsname: &str) -> Result<String> {
        // the correct format of dsname is <poolname>/<dsname>, e.g testzp/ds
        let parts: Vec<_> = dsname.split('/').collect();
        if parts.len() != 2 {
            Err(Error::from(ErrorKind::InvalidInput))
        } else {
            Ok(parts[0].to_owned())
        }
    }

    pub async fn init(
        dsname: &str,
        dev_path: &str,
        dstype: DatasetType,
        max_blksize: u32,
        already_formatted: bool,
    ) -> Result<Self> {
        assert!(max_blksize == 0 || (max_blksize & (max_blksize - 1)) == 0);

        let poolname = Self::dsname_to_poolname(dsname)?;
        let metrics = UzfsMetrics::new_boxed();

        let poolname = poolname.into_cstr();
        let dev_path_c = dev_path.into_cstr();
        let dsname = dsname.into_cstr();

        let (dnodesize, enable_autotrim) = match dstype {
            DatasetType::Data => (UZFS_DNODESIZE_DATA, true),
            DatasetType::Meta => (UZFS_DNODESIZE_META, false),
        };

        let mut arg = LibuzfsDatasetInitArg {
            dsname: dsname.as_ref().as_ptr(),
            dev_path: dev_path_c.as_ref().as_ptr(),
            pool_name: poolname.as_ptr() as *const c_char,
            dnodesize,
            max_blksize,
            already_formatted,
            metrics: metrics.as_ref() as *const _ as *const _,
            enable_autotrim,

            ret: 0,
            dhp: std::ptr::null_mut(),
            zhp: std::ptr::null_mut(),
        };

        let arg_usize = &mut arg as *mut LibuzfsDatasetInitArg as usize;

        CoroutineFuture::new(libuzfs_dataset_init_c, arg_usize).await;

        if arg.ret != 0 {
            Err(io::Error::from_raw_os_error(arg.ret))
        } else if arg.dhp.is_null() || arg.zhp.is_null() {
            Err(io::Error::from(io::ErrorKind::InvalidInput))
        } else {
            Ok(Self {
                dhp: arg.dhp,
                zhp: arg.zhp,
                poolname,
                metrics,
            })
        }
    }

    pub async fn expand(&self) -> Result<()> {
        let mut arg = LibuzfsDatasetExpandArg {
            dhp: self.dhp,
            ret: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDatasetExpandArg as usize;

        CoroutineFuture::new(libuzfs_dataset_expand_c, arg_usize).await;

        if arg.ret == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.ret))
        }
    }

    pub async fn start_manual_trim(&self) -> Result<()> {
        let mut arg = LibuzfsDatasetTrimArgs {
            dhp: self.dhp,
            err: 0,
        };

        let arg_usize = &mut arg as *mut _ as usize;
        CoroutineFuture::new(libuzfs_dataset_start_manual_trim_c, arg_usize).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn close(&self) -> Result<()> {
        let mut arg = LibuzfsDatasetFiniArg {
            dhp: self.dhp,
            zhp: self.zhp,
            poolname: self.poolname.as_ptr(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDatasetFiniArg as usize;

        CoroutineFuture::new(libuzfs_dataset_fini_c, arg_usize).await;

        if arg.err != 0 {
            Err(io::Error::from_raw_os_error(arg.err))
        } else {
            Ok(())
        }
    }

    pub fn get_last_synced_txg(&self) -> u64 {
        unsafe { libuzfs_get_last_synced_txg(self.dhp) }
    }

    pub async fn wait_synced(&self) {
        let _guard = self.metrics.record(RequestMethod::WaitSynced, 0);
        let arg_usize = self.dhp as usize;
        CoroutineFuture::new(libuzfs_wait_synced_c, arg_usize).await;
    }
}

// inode handle functions
impl Dataset {
    // when the inode handle is useless, release_inode_handle should be called
    pub async fn get_superblock_inode_handle(&self) -> Result<InodeHandle> {
        let ino = unsafe { libuzfs_dataset_get_superblock_ino(self.dhp) };
        self.get_inode_handle(ino, u64::MAX, false).await
    }

    // when the inode handle is useless, release_inode_handle should be called
    pub async fn get_inode_handle(
        &self,
        ino: u64,
        gen: u64,
        is_data_inode: bool,
    ) -> Result<InodeHandle> {
        let mut arg = LibuzfsInodeHandleGetArgs {
            dhp: self.dhp,
            ino,
            gen,
            is_data_inode,
            ihp: null_mut(),
            err: 0,
        };
        let arg_usize = &mut arg as *mut _ as usize;
        CoroutineFuture::new(libuzfs_inode_handle_get_c, arg_usize).await;

        if arg.err == 0 {
            Ok(InodeHandle { ihp: arg.ihp })
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    // Do not access inode_hanlde after this function returns
    pub async fn release_inode_handle(&self, ino_hdl: &mut InodeHandle) {
        CoroutineFuture::new(libuzfs_inode_handle_rele_c, ino_hdl.ihp as usize).await;
        ino_hdl.ihp = null_mut();
    }
}

// zap functions
impl Dataset {
    pub async fn zap_create(&self) -> Result<(u64, u64)> {
        let (mut handle, ino, gen) = self.create_inode(InodeType::DIR).await?;
        self.release_inode_handle(&mut handle).await;
        Ok((ino, gen))
    }

    pub async fn zap_claim(&self, ino: u64, gen: u64) -> Result<()> {
        self.claim_inode(ino, gen, InodeType::DIR).await
    }

    pub async fn zap_list(&self, zap_obj: u64, limit: usize) -> Result<Vec<(String, Vec<u8>)>> {
        let mut arg = LibuzfsZapListArg {
            dhp: self.dhp,
            obj: zap_obj,
            limit,

            err: 0,
            list: Vec::new(),
        };

        let arg_usize = &mut arg as *mut LibuzfsZapListArg as usize;

        CoroutineFuture::new(libuzfs_zap_list_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.list)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn zap_add<P: CStrArgument>(&self, obj: u64, name: P, value: &[u8]) -> Result<u64> {
        let cname = name.into_cstr();
        let mut arg = LibuzfsZapUpdateArg {
            dhp: self.dhp,
            obj,
            key: cname.as_ref().as_ptr(),
            num_integers: value.len() as u64,
            val: value.as_ptr() as *const c_void,
            only_add: true,
            txg: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsZapUpdateArg as usize;

        CoroutineFuture::new(libuzfs_zap_update_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    // if name exists, zap_update will overwrite original value,
    // if not exists, zap_update will add a new (name, value) like zap_add
    pub async fn zap_update<P: CStrArgument>(
        &self,
        obj: u64,
        name: P,
        value: &[u8],
    ) -> Result<u64> {
        let cname = name.into_cstr();
        let mut arg = LibuzfsZapUpdateArg {
            dhp: self.dhp,
            obj,
            key: cname.as_ref().as_ptr(),
            num_integers: value.len() as u64,
            val: value.as_ptr() as *const c_void,
            only_add: false,
            txg: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsZapUpdateArg as usize;

        CoroutineFuture::new(libuzfs_zap_update_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn zap_remove<P: CStrArgument>(&self, obj: u64, name: P) -> Result<u64> {
        let cname = name.into_cstr();
        let mut arg = LibuzfsZapRemoveArg {
            dhp: self.dhp,
            key: cname.as_ref().as_ptr(),
            obj,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsZapRemoveArg as usize;

        CoroutineFuture::new(libuzfs_zap_remove_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }
}

// object functions
impl Dataset {
    pub async fn create_objects(&self, num_objs: usize) -> Result<(Vec<u64>, u64)> {
        let _guard = self.metrics.record(RequestMethod::CreateObjects, num_objs);
        let mut arg = LibuzfsCreateObjectsArg {
            dhp: self.dhp,
            num_objs,
            err: 0,
            objs: Vec::new(),
            gen: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsCreateObjectsArg as usize;

        CoroutineFuture::new(libuzfs_objects_create_c, arg_usize).await;

        if arg.err == 0 {
            Ok((arg.objs, arg.gen))
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    // delete_object won't wait until synced, wait_log_commit is needed if you want wait sync
    pub async fn delete_object(&self, ino_hdl: &mut InodeHandle) -> Result<()> {
        let _guard = self.metrics.record(RequestMethod::DeleteObject, 0);
        let mut arg = LibuzfsDeleteObjectArg {
            ihp: ino_hdl.ihp,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDeleteObjectArg as usize;

        CoroutineFuture::new(libuzfs_delete_object_c, arg_usize).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn wait_log_commit(&self) {
        let _guard = self.metrics.record(RequestMethod::WaitLogCommit, 0);
        let arg_usize = self.dhp as usize;
        CoroutineFuture::new(libuzfs_wait_log_commit_c, arg_usize).await;
    }

    pub async fn get_object_attr(&self, ino_hdl: &InodeHandle) -> Result<uzfs_object_attr_t> {
        let _guard = self.metrics.record(RequestMethod::GetObjectAttr, 0);
        let mut arg = LibuzfsGetObjectAttrArg {
            ihp: ino_hdl.ihp,
            attr: uzfs_object_attr_t::default(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsGetObjectAttrArg as usize;

        CoroutineFuture::new(libuzfs_get_object_attr_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.attr)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn list_object(&self) -> Result<u64> {
        let mut arg = LibuzfsListObjectArg {
            dhp: self.dhp,
            num_objs: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsListObjectArg as usize;

        CoroutineFuture::new(libuzfs_list_object_c, arg_usize).await;

        Ok(arg.num_objs)
    }

    pub async fn stat_object(&self, obj: u64) -> Result<dmu_object_info_t> {
        let mut arg = LibuzfsStatObjectArg {
            dhp: self.dhp,
            obj,
            doi: dmu_object_info_t::default(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsStatObjectArg as usize;

        CoroutineFuture::new(libuzfs_stat_object_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.doi)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn read_object(
        &self,
        ino_hdl: &InodeHandle,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        let _guard = self
            .metrics
            .record(RequestMethod::ReadObject, size as usize);
        let mut arg = LibuzfsReadObjectArg {
            ihp: ino_hdl.ihp,
            offset,
            size,
            err: 0,
            data: Vec::<u8>::with_capacity(size as usize),
        };

        let arg_usize = &mut arg as *mut LibuzfsReadObjectArg as usize;

        CoroutineFuture::new(libuzfs_read_object_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.data)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn write_object(
        &self,
        ino_hdl: &InodeHandle,
        offset: u64,
        sync: bool,
        data: Vec<&[u8]>,
    ) -> Result<()> {
        let request_size = data.iter().map(|v| v.len()).sum();
        let _guard = self
            .metrics
            .record(RequestMethod::WriteObject, request_size);
        let iovs = data
            .iter()
            .map(|v| iovec {
                iov_base: v.as_ptr() as *mut c_void,
                iov_len: v.len(),
            })
            .collect();
        let mut arg = LibuzfsWriteObjectArg {
            ihp: ino_hdl.ihp,
            offset,
            iovs,
            sync,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsWriteObjectArg as usize;

        CoroutineFuture::new(libuzfs_write_object_c, arg_usize).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn sync_object(&self, ino_hdl: &InodeHandle) {
        let _guard = self.metrics.record(RequestMethod::SyncObject, 0);

        CoroutineFuture::new(libuzfs_sync_object_c, ino_hdl.ihp as usize).await;
    }

    pub async fn truncate_object(
        &self,
        ino_hdl: &mut InodeHandle,
        offset: u64,
        size: u64,
    ) -> Result<()> {
        let mut arg = LibuzfsTruncateObjectArg {
            ihp: ino_hdl.ihp,
            offset,
            size,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsTruncateObjectArg as usize;

        CoroutineFuture::new(libuzfs_truncate_object_c, arg_usize).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn space(&self) -> (u64, u64, u64, u64) {
        let mut arg = LibuzfsDatasetSpaceArg {
            dhp: self.dhp,
            refd_bytes: 0,
            avail_bytes: 0,
            used_objs: 0,
            avail_objs: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDatasetSpaceArg as usize;

        CoroutineFuture::new(libuzfs_dataset_space_c, arg_usize).await;

        (
            arg.refd_bytes,
            arg.avail_bytes,
            arg.used_objs,
            arg.avail_objs,
        )
    }

    pub async fn object_has_hole_in_range(
        &self,
        ino_hdl: &InodeHandle,
        offset: u64,
        size: u64,
    ) -> Result<bool> {
        let mut arg = LibuzfsFindHoleArg {
            ihp: ino_hdl.ihp,
            off: offset,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsFindHoleArg as usize;

        CoroutineFuture::new(libuzfs_object_next_hole_c, arg_usize).await;

        match arg.err {
            0 => Ok(arg.off < offset + size),
            other => Err(io::Error::from_raw_os_error(other)),
        }
    }

    pub async fn object_next_block(
        &self,
        ino_hdl: &InodeHandle,
        offset: u64,
    ) -> Result<Option<(u64, u64)>> {
        let mut arg = LibuzfsNextBlockArg {
            ihp: ino_hdl.ihp,
            off: offset,

            size: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut _ as usize;

        CoroutineFuture::new(libuzfs_object_next_block_c, arg_usize).await;

        match arg.err {
            0 => Ok(Some((arg.off, arg.size))),
            libc::ESRCH => Ok(None),
            other => Err(io::Error::from_raw_os_error(other)),
        }
    }

    pub fn dump_object_doi(obj: u64, doi: dmu_object_info_t) {
        println!("object: {obj}");
        println!("\tdata_block_size: {}", doi.doi_data_block_size);
        println!("\tmetadata_block_size: {}", doi.doi_metadata_block_size);
        println!("\ttype: {}", doi.doi_type);
        println!("\tbonus_type: {}", doi.doi_bonus_type);
        println!("\tbonus_size: {}", doi.doi_bonus_size);
        println!("\tindirection: {}", doi.doi_indirection);
        println!("\tchecksum: {}", doi.doi_checksum);
        println!("\tcompress: {}", doi.doi_compress);
        println!("\tnblkptr: {}", doi.doi_nblkptr);
        println!("\tdnodesize: {}", doi.doi_dnodesize);
        println!("\tphysical_blocks_512: {}", doi.doi_physical_blocks_512);
        println!("\tmax_offset: {}", doi.doi_max_offset);
        println!("\tfill_count: {}", doi.doi_fill_count);
    }

    pub async fn set_object_mtime(
        &self,
        ino_hdl: &mut InodeHandle,
        tv_sec: i64,
        tv_nsec: i64,
    ) -> Result<()> {
        let mut arg = LibuzfsObjectSetMtimeArg {
            ihp: ino_hdl.ihp,
            tv_sec,
            tv_nsec,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsObjectSetMtimeArg as usize;
        CoroutineFuture::new(libuzfs_object_set_mtime, arg_usize).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }
}

#[derive(Default)]
pub struct UzfsDentry {
    pub whence: u64,
    pub name: CString,
    pub value: u64,
}

impl Dataset {
    /// Atomically links an inode to a directory in a filesystem.
    ///
    /// This function performs an atomic operation to link a file (inode) to a specified
    /// directory (parent inode). It ensures consistency and avoids race conditions in a
    /// concurrent environment.
    ///
    /// # Parameters
    /// - `pino_hdl`: A mutable reference to the parent inode handle, representing the directory
    ///   where the new link will be created.
    /// - `pattr`: A vector of bytes representing attributes for the parent inode, such as metadata.
    /// - `name`: A `CString` specifying the name of the link to be created in the parent directory.
    /// - `ino_hdl`: A mutable reference to the inode handle for the file being linked.
    /// - `attr`: A vector of bytes representing attributes for the inode being linked.
    /// - `ino_mask`: A 64-bit mask used in conjunction with the inode number. The result of
    ///   `ino_mask | ino` is stored within the directory entry.
    ///
    /// # Returns
    ///
    /// This function returns a `Result<u64>`:
    /// * `Ok(u64)` - Represents the current sync epoch after the unlink operation. This value can be used to track
    ///   the consistency state of the system.
    /// * `Err(Error)` - Represents an error encountered during the operation.
    #[allow(clippy::too_many_arguments)]
    pub async fn link_inode_atomic<P: CStrArgument>(
        &self,
        pino_hdl: &mut InodeHandle,
        pattr: Vec<u8>,
        name: P,
        ino_hdl: &mut InodeHandle,
        attr: Vec<u8>,
        ino_mask: u64,
    ) -> Result<u64> {
        let name = name.into_cstr().as_ref().to_owned();
        let mut args = InodeLinkArgs {
            dhp: self.dhp,
            dihp: pino_hdl.ihp,
            pattr,
            name,
            ihp: ino_hdl.ihp,
            attr,
            ino_mask,

            txg: 0,
            err: 0,
        };

        let arg_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_inode_link_atomic_c, arg_usize).await;
        if args.err != 0 {
            Err(Error::from_raw_os_error(args.err))
        } else {
            Ok(args.txg)
        }
    }

    /// Atomically creates a new inode in the specified parent directory while ensuring metadata consistency.
    ///
    /// This function performs an atomic operation to create a new inode (file or directory) within a parent directory.
    /// The operation updates the necessary metadata and handles hierarchical (high-priority) and local (low-priority)
    /// key-value attributes for the inode.
    ///
    /// # Arguments
    ///
    /// * `inode_type` - Specifies the type of inode to be created (e.g., file or directory). Typically represented as an enum `InodeType`.
    /// * `phdl` - A mutable reference to the inode handle of the parent directory where the new inode will be created.
    /// * `name` - The name of the new file or directory, represented as a `CString`.
    /// * `attr_func` - this callback filles given attr with caller defined attr
    /// * `pattr` - A vector containing attributes of the parent directory (`phdl`). These attributes may be used
    ///   for validation or metadata updates during the creation process.
    /// * `hp_kvs` - A vector of key-value pairs (`CString`, `Vec<u8>`) representing high-priority attributes to be applied to the new inode.
    ///
    /// # Returns
    ///
    /// This function returns a `Result<InodeHandle>`:
    /// * `Ok(InodeHandle)` - Represents the handle of the newly created inode. This handle can be used to interact
    ///   with the newly created file or directory.
    /// * `Err(Error)` - Represents an error encountered during the operation, such as conflicting names or invalid attributes.
    ///
    /// # Notes
    ///
    /// - The operation ensures atomicity, preventing partial updates or inconsistent states during inode creation.
    /// - The `hp_kvs` and `lp_kvs` attributes are used to configure the new inode. High-priority attributes may take
    ///   precedence in certain metadata updates.
    /// - Ensure that the `name` is unique within the `phdl` parent directory to avoid conflicts.
    /// - This function operates asynchronously and may involve I/O operations or locks.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_inode_atomic<F: FnMut(&mut [u8], &mut usize, &mut u64, u64)>(
        &self,
        inode_type: InodeType,
        phdl: &mut InodeHandle,
        name: CString,
        attr_func: F,
        pattr: Vec<u8>,
        hp_kvs: Vec<(CString, Vec<u8>)>,
    ) -> Result<InodeHandle> {
        let mut args = InodeCreateArgs {
            dhp: self.dhp,
            inode_type: inode_type as u32,
            dihp: phdl.ihp,
            name,
            pattr,
            hp_kvs,

            attr_func,
            ihp: null_mut(),
            err: 0,
        };

        let arg_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_inode_create_atomic_c::<F>, arg_usize).await;
        if args.err == 0 {
            Ok(InodeHandle { ihp: args.ihp })
        } else {
            Err(io::Error::from_raw_os_error(args.err))
        }
    }

    /// Atomically unlinks an inode, removing it from its parent directory while ensuring metadata consistency.
    ///
    /// This function performs an atomic unlink operation, removing a file or directory identified by its inode
    /// from the specified parent directory. The operation ensures consistency in the underlying storage backend
    /// and updates related metadata.
    ///
    /// # Arguments
    ///
    /// * `pino_hdl` - A mutable reference to the inode handle of the parent directory from which the inode will be unlinked.
    /// * `pattr` - A vector of attributes associated with the parent directory (`pino_hdl`). These attributes may be used
    ///   for validation or to update metadata during the unlink operation.
    /// * `name` - The name of the file or directory to be unlinked, represented as a `CString`.
    /// * `ino_hdl` - A mutable reference to the inode handle of the file or directory to be unlinked.
    /// * `attr` - Optional attributes associated with the inode being unlinked:
    ///   - `None` indicates that the file or directory should be deleted during the unlink operation.
    ///   - `Some(Vec<u8>)` represents attributes that may need to be updated or validated during the operation.
    ///
    /// # Returns
    ///
    /// This function returns a `Result<u64>`:
    /// * `Ok(u64)` - Represents the current sync epoch after the unlink operation. This value can be used to track
    ///   the consistency state of the system.
    /// * `Err(Error)` - Represents an error encountered during the operation.
    ///
    /// # Notes
    ///
    /// - The operation ensures atomicity, preventing partial updates or inconsistent states during the unlink process.
    /// - When `attr` is `None`, the function will delete the file or directory represented by the inode.
    /// - If `attr` is provided, ensure it matches the expected state of the inode to avoid conflicts or errors.
    /// - The returned sync epoch (`u64`) indicates the system's state after the unlink operation and can be used for
    ///   higher-level synchronization or consistency checks.
    /// - This function operates asynchronously and may involve I/O operations or locks.
    pub async fn unlink_inode_atomic(
        &self,
        pino_hdl: &mut InodeHandle,
        pattr: Vec<u8>,
        name: CString,
        ino_hdl: &mut InodeHandle,
        attr: Option<Vec<u8>>,
    ) -> Result<u64> {
        let mut args = InodeUnlinkArgs {
            dhp: self.dhp,
            dihp: pino_hdl.ihp,
            pattr,
            name,
            ihp: ino_hdl.ihp,
            attr,

            txg: 0,
            err: 0,
        };

        let args_uzie = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_inode_unlink_atomic_c, args_uzie).await;
        if args.err != 0 {
            Err(Error::from_raw_os_error(args.err))
        } else {
            Ok(args.txg)
        }
    }

    /// Atomically renames an inode, allowing for potential movement across directories.
    ///
    /// This function performs an atomic operation to rename or move a file or directory
    /// identified by an inode. It ensures consistent metadata updates and handles
    /// edge cases such as overwriting existing targets or moving across different parent
    /// directories.
    ///
    /// # Arguments
    ///
    /// * `old_parent` - A mutable reference to the inode handle of the source's parent directory.
    /// * `op_attr` - A vector of attributes associated with the `old_parent` directory, used for validation
    ///   or metadata updates during the operation.
    /// * `src_name` - The name of the source file or directory to be renamed, represented as a `CString`.
    /// * `src_inode` - A mutable reference to the inode handle of the source file or directory.
    /// * `inode_kv` - A key-value pair (`CString`, `Vec<u8>`) representing the attributes of `src_inode` that
    ///   may need to be updated as part of the rename operation.
    /// * `src_attr` - A vector containing attributes of the source inode.
    /// * `new_parent` - An optional tuple with:
    ///   - A mutable reference to the inode handle of the target's parent directory (if moving across directories).
    ///   - A vector of attributes for the target's parent directory.
    ///   - `None` indicates that `old_parent` and `new_parent` are the same, implying no directory movement.
    /// * `target_name` - The new name for the file or directory in the target location, represented as a `CString`.
    /// * `ino_mask` - A 64-bit mask applied to the inode number of the source. The result of this operation
    ///   (`ino_mask | ino`) will be stored in the directory entry of the `new_parent` (or `old_parent` if they are the same).
    /// * `target_inode` - An optional tuple with:
    ///   - A mutable reference to the inode handle of the target (if it exists).
    ///   - Optional attributes associated with the target inode:
    ///     - `None` indicates that the `target_inode` should be deleted.
    ///     - If `target_inode` itself is `None`, it means no target inode exists, and the rename operation will not overwrite any existing entries.
    ///
    /// # Returns
    ///
    /// This function returns a `Result<u64, Error>`:
    /// * `Ok(u64)` - Represents the current sync epoch after the operation. This can be used to track
    ///   the consistency state of the system.
    /// * `Err(Error)` - Represents an error encountered during the operation.
    ///
    /// # Notes
    ///
    /// - `inode_kv` directly represents the key-value attributes of `src_inode` that are involved in the operation.
    /// - The operation ensures atomic updates, including storing the result of `ino_mask | ino` in the directory entry
    ///   of `new_parent` (or `old_parent` if `new_parent` is `None`).
    /// - When `target_inode` is `Some` and its attributes are `None`, the operation will delete the existing `target_inode`.
    /// - This function operates asynchronously and may involve I/O operations or locks.
    /// - Ensure proper handling of `Option` and `Result` types to account for cases like missing targets, overwriting targets, or directory movements.
    /// - Atomicity guarantees depend on the underlying storage backend.
    ///
    /// # Sync Epoch
    ///
    /// The returned `u64` value represents the system's current sync epoch. This value is incremented or updated
    /// to reflect the latest synchronization state after this rename operation. It can be used by higher-level
    /// systems to determine data consistency or trigger further synchronization logic.
    #[allow(clippy::too_many_arguments)]
    pub async fn rename_inode_atomic(
        &self,
        old_parent: &mut InodeHandle,
        op_attr: Vec<u8>,
        src_name: CString,
        src_inode: &mut InodeHandle,
        inode_kv: (CString, Vec<u8>),
        src_attr: Vec<u8>,
        new_parent: Option<(&mut InodeHandle, Vec<u8>)>,
        target_name: CString,
        ino_mask: u64,
        target_inode: Option<(&mut InodeHandle, Option<Vec<u8>>)>,
    ) -> Result<u64> {
        let (target_inode, target_attr) =
            target_inode.map_or_else(|| (null_mut(), None), |(inode, attr)| (inode.ihp, attr));
        let (new_parent, np_attr) = new_parent.map_or_else(
            || (null_mut(), None),
            |(inode, attr)| (inode.ihp, Some(attr)),
        );
        let target_name = target_name.into_cstr().as_ref().to_owned();
        let mut args = InodeRenameArgs {
            dhp: self.dhp,
            old_parent: old_parent.ihp,
            op_attr,
            src_name,
            src_inode: src_inode.ihp,
            key: inode_kv.0,
            value: inode_kv.1,
            src_attr,
            new_parent,
            np_attr,
            target_name,
            ino_mask,
            target_inode,
            target_attr,

            txg: 0,
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_inode_rename_atomic_c, args_usize).await;
        if args.err == 0 {
            Ok(args.txg)
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }
}

// inode functions
impl Dataset {
    pub async fn create_inode(&self, inode_type: InodeType) -> Result<(InodeHandle, u64, u64)> {
        let _guard = self.metrics.record(RequestMethod::CreateInode, 0);
        let mut arg = LibuzfsCreateInode {
            dhp: self.dhp,
            inode_type: inode_type as u32,

            ihp: null_mut(),
            ino: 0,
            txg: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsCreateInode as usize;

        CoroutineFuture::new(libuzfs_create_inode_c, arg_usize).await;

        if arg.err == 0 {
            Ok((InodeHandle { ihp: arg.ihp }, arg.ino, arg.txg))
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn claim_inode(&self, ino: u64, gen: u64, inode_type: InodeType) -> Result<()> {
        let mut arg = LibuzfsClaimInodeArg {
            dhp: self.dhp,
            inode_type: inode_type as u32,
            ino,
            gen,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsClaimInodeArg as usize;

        CoroutineFuture::new(libuzfs_claim_inode_c, arg_usize).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn delete_inode(
        &self,
        ino_hdl: &mut InodeHandle,
        inode_type: InodeType,
    ) -> Result<u64> {
        let _guard = self.metrics.record(RequestMethod::DeleteInode, 0);
        let mut arg = LibuzfsDeleteInode {
            ihp: ino_hdl.ihp,
            inode_type: inode_type as u32,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDeleteInode as usize;

        CoroutineFuture::new(libuzfs_delete_inode_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn get_attr(&self, ino_hdl: &InodeHandle) -> Result<InodeAttr> {
        let _guard = self.metrics.record(RequestMethod::GetAttr, 0);
        let mut attr = InodeAttr::default();
        attr.reserved.reserve(MAX_RESERVED_SIZE);

        let mut arg = LibuzfsGetAttrArg {
            ihp: ino_hdl.ihp,
            reserved: attr.reserved.as_mut_ptr() as *mut i8,
            size: 0,
            attr: uzfs_inode_attr_t::default(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsGetAttrArg as usize;

        CoroutineFuture::new(libuzfs_inode_getattr_c, arg_usize).await;

        if arg.err == 0 {
            unsafe { attr.reserved.set_len(arg.size as usize) };
            (attr.gen, attr.blksize, attr.blocks) =
                (arg.attr.gen, arg.attr.blksize, arg.attr.blocks);
            Ok(attr)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn set_attr(&self, ino_hdl: &mut InodeHandle, reserved: &[u8]) -> Result<u64> {
        let _guard = self.metrics.record(RequestMethod::SetAttr, 0);
        assert!(reserved.len() <= MAX_RESERVED_SIZE);
        let mut arg = LibuzfsSetAttrArg {
            ihp: ino_hdl.ihp,
            reserved: reserved.as_ptr() as *mut i8,
            size: reserved.len() as u32,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsSetAttrArg as usize;

        CoroutineFuture::new(libuzfs_set_attr_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn get_kvattr<P: CStrArgument>(
        &self,
        ino_hdl: &InodeHandle,
        name: P,
    ) -> Result<Vec<u8>> {
        let _guard = self.metrics.record(RequestMethod::GetKvattr, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsGetKvattrArg {
            ihp: ino_hdl.ihp,
            name: cname.as_ref().as_ptr(),
            data: Vec::new(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsGetKvattrArg as usize;

        CoroutineFuture::new(libuzfs_inode_get_kvattr_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.data)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn set_kvattr<P: CStrArgument>(
        &self,
        ino_hdl: &mut InodeHandle,
        name: P,
        value: &[u8],
        option: u32,
    ) -> Result<u64> {
        let _guard = self.metrics.record(RequestMethod::SetKvattr, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsSetKvAttrArg {
            ihp: ino_hdl.ihp,
            name: cname.as_ref().as_ptr(),
            option,
            value: value.as_ptr() as *const c_char,
            size: value.len() as u64,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsSetKvAttrArg as usize;

        CoroutineFuture::new(libuzfs_set_kvattr_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn remove_kvattr<P: CStrArgument>(
        &self,
        ino_hdl: &mut InodeHandle,
        name: P,
    ) -> Result<u64> {
        let cname = name.into_cstr();
        let mut arg = LibuzfsRemoveKvattrArg {
            ihp: ino_hdl.ihp,
            name: cname.as_ref().as_ptr(),
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsRemoveKvattrArg as usize;

        CoroutineFuture::new(libuzfs_remove_kvattr_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn list_kvattrs(&self, ino_hdl: &InodeHandle) -> Result<Vec<String>> {
        let mut arg = LibuzfsListKvAttrsArg {
            ihp: ino_hdl.ihp,
            err: 0,
            names: Vec::new(),
        };

        let arg_usize = &mut arg as *mut LibuzfsListKvAttrsArg as usize;

        CoroutineFuture::new(libuzfs_list_kvattrs_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.names)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn create_dentry<P: CStrArgument>(
        &self,
        ino_hdl: &mut InodeHandle,
        name: P,
        value: u64,
    ) -> Result<u64> {
        let _guard = self.metrics.record(RequestMethod::CreateDentry, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsCreateDentryArg {
            dihp: ino_hdl.ihp,
            name: cname.as_ref().as_ptr(),
            ino: value,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsCreateDentryArg as usize;

        CoroutineFuture::new(libuzfs_create_dentry_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn delete_dentry<P: CStrArgument>(
        &self,
        ino_hdl: &mut InodeHandle,
        name: P,
    ) -> Result<u64> {
        let _guard = self.metrics.record(RequestMethod::DeleteDentry, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsDeleteDentryArg {
            dihp: ino_hdl.ihp,
            name: cname.as_ref().as_ptr(),
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDeleteDentryArg as usize;

        CoroutineFuture::new(libuzfs_delete_entry_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn lookup_dentry<P: CStrArgument>(
        &self,
        ino_hdl: &InodeHandle,
        name: P,
    ) -> Result<u64> {
        let _guard = self.metrics.record(RequestMethod::LookupDentry, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsLookupDentryArg {
            dihp: ino_hdl.ihp,
            name: cname.as_ref().as_ptr(),
            ino: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsLookupDentryArg as usize;

        CoroutineFuture::new(libuzfs_lookup_dentry_c, arg_usize).await;

        if arg.err == 0 {
            Ok(arg.ino)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn iterate_dentry(
        &self,
        ino_hdl: &InodeHandle,
        whence: u64,
        size: u32,
    ) -> Result<(Vec<UzfsDentry>, bool)> {
        let _guard = self.metrics.record(RequestMethod::IterateDentry, 0);
        let mut arg = LibuzfsIterateDentryArg {
            dihp: ino_hdl.ihp,
            whence,
            size,
            err: 0,
            dentries: Vec::new(),
            done: false,
        };

        let arg_usize = &mut arg as *mut LibuzfsIterateDentryArg as usize;

        CoroutineFuture::new(libuzfs_iterate_dentry_c, arg_usize).await;

        if arg.err == 0 {
            Ok((arg.dentries, arg.done))
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }
}

unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}
