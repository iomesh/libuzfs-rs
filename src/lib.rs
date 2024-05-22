pub mod metrics;
use cstr_argument::CStrArgument;
use io::Result;
use metrics::{Method, Metrics};
use once_cell::sync::OnceCell;
use std::ffi::{CStr, CString};
use std::io;
use std::os::raw::{c_char, c_void};
use tempfile::NamedTempFile;
use tokio::sync::Mutex;
use uzfs_sys::async_sys::*;
use uzfs_sys::bindings::{
    self as sys, iovec, libuzfs_set_fail_percent, uzfs_inode_attr_t, uzfs_object_attr_t,
};
use uzfs_sys::coroutine::*;

pub const DEFAULT_CACHE_FILE: &str = "/tmp/zpool.cache";

static UZFS_INIT_REF: OnceCell<Mutex<u32>> = OnceCell::new();
const MAX_RESERVED_SIZE: usize = 192;
const UZFS_DNODESIZE_META: u32 = 1024;
const UZFS_DNODESIZE_DATA: u32 = 512;

#[derive(Default)]
pub struct InodeAttr {
    pub gen: u64,
    pub blksize: u32,
    pub blocks: u64,
    pub reserved: Vec<u8>,
}

#[inline]
pub fn set_coroutine_backtrace_funcs(
    add_bt: fn(u64, String),
    remove_bt: fn(u64),
    add_cp: fn(u64, String),
    remove_cp: fn(u64),
) {
    add_backtrace.get_or_init(|| add_bt);
    remove_backtrace.get_or_init(|| remove_bt);
    add_creation_pos.get_or_init(|| add_cp);
    remove_creation_pos.get_or_init(|| remove_cp);
}

pub fn set_fail_percent(fp: i32) {
    unsafe { libuzfs_set_fail_percent(fp) };
}

pub async fn uzfs_env_init() {
    let _ = std::fs::remove_file(DEFAULT_CACHE_FILE);
    let mut guard = UZFS_INIT_REF.get_or_init(|| Mutex::new(0)).lock().await;

    if *guard == 0 {
        UzfsCoroutineFuture::new(libuzfs_init_c, 0, true, true).await;
    }

    *guard += 1;
}

#[inline]
pub fn uzfs_set_zpool_cache_path<P: CStrArgument>(path: P) {
    unsafe {
        sys::libuzfs_set_zpool_cache_path(path.into_cstr().as_ref().as_ptr());
    }
}

pub fn enable_debug_msg() {
    unsafe { sys::libuzfs_enable_debug_msg() };
}

pub fn disable_debug_msg() {
    unsafe { sys::libuzfs_disable_debug_msg() };
}

pub async fn uzfs_env_fini() {
    let mut guard = UZFS_INIT_REF.get().unwrap().lock().await;
    if *guard == 1 {
        UzfsCoroutineFuture::new(libuzfs_fini_c, 0, true, false).await;
    }
    *guard -= 1;
}

pub enum InodeType {
    FILE = sys::libuzfs_inode_type_t_INODE_FILE as isize,
    DIR = sys::libuzfs_inode_type_t_INODE_DIR as isize,
}

pub enum DatasetType {
    Data,
    Meta,
}

pub enum KvSetOption {
    None = 0,
    HighPriority = 1 << 0,
    NeedLog = 1 << 1,
}

pub struct Dataset {
    dhp: *mut sys::libuzfs_dataset_handle_t,
    zhp: *mut sys::libuzfs_zpool_handle_t,
    poolname: CString,
    pub metrics: Metrics,
}

impl Dataset {
    fn dsname_to_poolname<P: AsRef<CStr>>(dsname: P) -> Result<CString> {
        let s = dsname.as_ref().to_string_lossy().into_owned();
        let v: Vec<&str> = s.split('/').collect();
        if v.len() != 2 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        Ok(CString::new(v[0]).unwrap())
    }

    pub async fn init<P: CStrArgument>(
        dsname: P,
        dev_path: P,
        dstype: DatasetType,
        max_blksize: u32,
        already_formatted: bool,
    ) -> Result<Self> {
        assert!(max_blksize == 0 || (max_blksize & (max_blksize - 1)) == 0);

        let dsname = dsname.into_cstr();
        let poolname = Self::dsname_to_poolname(&dsname)?;
        let dev_path_c = dev_path.into_cstr();

        let dnodesize = match dstype {
            DatasetType::Data => UZFS_DNODESIZE_DATA,
            DatasetType::Meta => UZFS_DNODESIZE_META,
        };

        let mut arg = LibuzfsDatasetInitArg {
            dsname: dsname.as_ref().as_ptr(),
            dev_path: dev_path_c.as_ref().as_ptr(),
            pool_name: poolname.as_ptr() as *const c_char,
            dnodesize,
            max_blksize,
            already_formatted,

            ret: 0,
            dhp: std::ptr::null_mut(),
            zhp: std::ptr::null_mut(),
        };

        let arg_usize = &mut arg as *mut LibuzfsDatasetInitArg as usize;

        UzfsCoroutineFuture::new(libuzfs_dataset_init_c, arg_usize, true, true).await;

        let metrics = Metrics::new();

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

        UzfsCoroutineFuture::new(libuzfs_dataset_expand_c, arg_usize, true, false).await;

        if arg.ret == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.ret))
        }
    }

    // this function should never block
    pub fn get_superblock_ino(&self) -> u64 {
        unsafe { sys::libuzfs_dataset_get_superblock_ino(self.dhp) }
    }

    pub async fn zap_create(&self) -> Result<(u64, u64)> {
        self.create_inode(InodeType::DIR).await
    }

    pub async fn zap_claim(&self, ino: u64, gen: u64) -> Result<()> {
        self.claim_inode(ino, gen, InodeType::DIR).await
    }

    pub async fn zap_list(&self, zap_obj: u64) -> Result<Vec<(String, Vec<u8>)>> {
        let mut arg = LibuzfsZapListArg {
            dhp: self.dhp,
            obj: zap_obj,
            err: 0,
            list: Vec::new(),
        };

        let arg_usize = &mut arg as *mut LibuzfsZapListArg as usize;

        UzfsCoroutineFuture::new(libuzfs_zap_list_c, arg_usize, true, false).await;

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

        UzfsCoroutineFuture::new(libuzfs_zap_update_c, arg_usize, true, false).await;

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

        UzfsCoroutineFuture::new(libuzfs_zap_update_c, arg_usize, true, false).await;

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

        UzfsCoroutineFuture::new(libuzfs_zap_remove_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn create_objects(&self, num_objs: usize) -> Result<(Vec<u64>, u64)> {
        let _guard = self.metrics.record(Method::CreateObjects, num_objs);
        let mut arg = LibuzfsCreateObjectsArg {
            dhp: self.dhp,
            num_objs,
            err: 0,
            objs: Vec::new(),
            gen: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsCreateObjectsArg as usize;

        UzfsCoroutineFuture::new(libuzfs_objects_create_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok((arg.objs, arg.gen))
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    // delete_object won't wait until synced, wait_log_commit is needed if you want wait sync
    pub async fn delete_object(&self, obj: u64) -> Result<()> {
        let _guard = self.metrics.record(Method::DeleteObject, 0);
        let mut arg = LibuzfsDeleteObjectArg {
            dhp: self.dhp,
            obj,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDeleteObjectArg as usize;

        UzfsCoroutineFuture::new(libuzfs_delete_object_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn wait_log_commit(&self) {
        let _guard = self.metrics.record(Method::WaitLogCommit, 0);
        let arg_usize = self.dhp as usize;
        UzfsCoroutineFuture::new(libuzfs_wait_log_commit_c, arg_usize, true, false).await;
    }

    pub async fn get_object_attr(&self, obj: u64) -> Result<uzfs_object_attr_t> {
        let _guard = self.metrics.record(Method::GetObjectAttr, 0);
        let mut arg = LibuzfsGetObjectAttrArg {
            dhp: self.dhp,
            obj,
            attr: uzfs_object_attr_t::default(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsGetObjectAttrArg as usize;

        UzfsCoroutineFuture::new(libuzfs_get_object_attr_c, arg_usize, true, false).await;

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

        UzfsCoroutineFuture::new(libuzfs_list_object_c, arg_usize, true, false).await;

        Ok(arg.num_objs)
    }

    pub async fn stat_object(&self, obj: u64) -> Result<sys::dmu_object_info_t> {
        let mut arg = LibuzfsStatObjectArg {
            dhp: self.dhp,
            obj,
            doi: sys::dmu_object_info_t::default(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsStatObjectArg as usize;

        UzfsCoroutineFuture::new(libuzfs_stat_object_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.doi)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn read_object(&self, obj: u64, offset: u64, size: u64) -> Result<Vec<u8>> {
        let _guard = self.metrics.record(Method::ReadObject, size as usize);
        let mut arg = LibuzfsReadObjectArg {
            dhp: self.dhp,
            obj,
            offset,
            size,
            err: 0,
            data: Vec::<u8>::with_capacity(size as usize),
        };

        let arg_usize = &mut arg as *mut LibuzfsReadObjectArg as usize;

        UzfsCoroutineFuture::new(libuzfs_read_object_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.data)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    // TODO(hping): add unit tests to verify sync write works well in crash scenario
    pub async fn write_object(
        &self,
        obj: u64,
        offset: u64,
        sync: bool,
        data: Vec<&[u8]>,
    ) -> Result<()> {
        let _guard = self.metrics.record(Method::WriteObject, data.len());
        let iovs = data
            .iter()
            .map(|v| iovec {
                iov_base: v.as_ptr() as *mut c_void,
                iov_len: v.len() as u64,
            })
            .collect();
        let mut arg = LibuzfsWriteObjectArg {
            dhp: self.dhp,
            obj,
            offset,
            iovs,
            sync,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsWriteObjectArg as usize;

        UzfsCoroutineFuture::new(libuzfs_write_object_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    // TODO(hping): add ut
    pub async fn sync_object(&self, obj: u64) {
        let _guard = self.metrics.record(Method::SyncObject, 0);
        let mut arg = LibuzfsSyncObjectArg { dhp: self.dhp, obj };

        let arg_usize = &mut arg as *mut LibuzfsSyncObjectArg as usize;

        UzfsCoroutineFuture::new(libuzfs_sync_object_c, arg_usize, true, false).await;
    }

    pub async fn truncate_object(&self, obj: u64, offset: u64, size: u64) -> Result<()> {
        let mut arg = LibuzfsTruncateObjectArg {
            dhp: self.dhp,
            obj,
            offset,
            size,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsTruncateObjectArg as usize;

        UzfsCoroutineFuture::new(libuzfs_truncate_object_c, arg_usize, true, false).await;

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

        UzfsCoroutineFuture::new(libuzfs_dataset_space_c, arg_usize, true, false).await;

        (
            arg.refd_bytes,
            arg.avail_bytes,
            arg.used_objs,
            arg.avail_objs,
        )
    }

    pub async fn object_has_hole_in_range(&self, obj: u64, offset: u64, size: u64) -> Result<bool> {
        let mut arg = LibuzfsFindHoleArg {
            dhp: self.dhp,
            obj,
            off: offset,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsFindHoleArg as usize;

        UzfsCoroutineFuture::new(libuzfs_object_next_hole_c, arg_usize, true, false).await;

        match arg.err {
            0 => Ok(arg.off < offset + size),
            other => Err(io::Error::from_raw_os_error(other)),
        }
    }

    pub fn dump_object_doi(obj: u64, doi: sys::dmu_object_info_t) {
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

    pub async fn create_inode(&self, inode_type: InodeType) -> Result<(u64, u64)> {
        let _guard = self.metrics.record(Method::CreateInode, 0);
        let mut arg = LibuzfsCreateInode {
            dhp: self.dhp,
            inode_type: inode_type as u32,
            ino: 0,
            txg: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsCreateInode as usize;

        UzfsCoroutineFuture::new(libuzfs_create_inode_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok((arg.ino, arg.txg))
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

        UzfsCoroutineFuture::new(libuzfs_claim_inode_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn delete_inode(&self, ino: u64, inode_type: InodeType) -> Result<u64> {
        let _guard = self.metrics.record(Method::DeleteInode, 0);
        let mut arg = LibuzfsDeleteInode {
            dhp: self.dhp,
            ino,
            inode_type: inode_type as u32,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDeleteInode as usize;

        UzfsCoroutineFuture::new(libuzfs_delete_inode_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn get_attr(&self, ino: u64) -> Result<InodeAttr> {
        let _guard = self.metrics.record(Method::GetAttr, 0);
        let mut attr = InodeAttr::default();
        attr.reserved.reserve(MAX_RESERVED_SIZE);

        let mut arg = LibuzfsGetAttrArg {
            dhp: self.dhp,
            ino,
            reserved: attr.reserved.as_mut_ptr() as *mut i8,
            size: 0,
            attr: uzfs_inode_attr_t::default(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsGetAttrArg as usize;

        UzfsCoroutineFuture::new(libuzfs_inode_getattr_c, arg_usize, true, false).await;

        if arg.err == 0 {
            unsafe { attr.reserved.set_len(arg.size as usize) };
            (attr.gen, attr.blksize, attr.blocks) =
                (arg.attr.gen, arg.attr.blksize, arg.attr.blocks);
            Ok(attr)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn set_attr(&self, ino: u64, reserved: &[u8]) -> Result<u64> {
        let _guard = self.metrics.record(Method::SetAttr, 0);
        assert!(reserved.len() <= MAX_RESERVED_SIZE);
        let mut arg = LibuzfsSetAttrArg {
            dhp: self.dhp,
            ino,
            reserved: reserved.as_ptr() as *mut i8,
            size: reserved.len() as u32,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsSetAttrArg as usize;

        UzfsCoroutineFuture::new(libuzfs_set_attr_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn get_kvattr<P: CStrArgument>(&self, ino: u64, name: P) -> Result<Vec<u8>> {
        let _guard = self.metrics.record(Method::GetKvattr, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsGetKvattrArg {
            dhp: self.dhp,
            ino,
            name: cname.as_ref().as_ptr(),
            data: Vec::new(),
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsGetKvattrArg as usize;

        UzfsCoroutineFuture::new(libuzfs_inode_get_kvattr_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.data)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn set_kvattr<P: CStrArgument>(
        &self,
        ino: u64,
        name: P,
        value: &[u8],
        option: u32,
    ) -> Result<u64> {
        let _guard = self.metrics.record(Method::SetKvattr, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsSetKvAttrArg {
            dhp: self.dhp,
            ino,
            name: cname.as_ref().as_ptr(),
            option,
            value: value.as_ptr() as *const c_char,
            size: value.len() as u64,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsSetKvAttrArg as usize;

        UzfsCoroutineFuture::new(libuzfs_set_kvattr_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn remove_kvattr<P: CStrArgument>(&self, ino: u64, name: P) -> Result<u64> {
        let cname = name.into_cstr();
        let mut arg = LibuzfsRemoveKvattrArg {
            dhp: self.dhp,
            ino,
            name: cname.as_ref().as_ptr(),
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsRemoveKvattrArg as usize;

        UzfsCoroutineFuture::new(libuzfs_remove_kvattr_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn list_kvattrs(&self, ino: u64) -> Result<Vec<String>> {
        let mut arg = LibuzfsListKvAttrsArg {
            dhp: self.dhp,
            ino,
            err: 0,
            names: Vec::new(),
        };

        let arg_usize = &mut arg as *mut LibuzfsListKvAttrsArg as usize;

        UzfsCoroutineFuture::new(libuzfs_list_kvattrs_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.names)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn create_dentry<P: CStrArgument>(
        &self,
        pino: u64,
        name: P,
        value: u64,
    ) -> Result<u64> {
        let _guard = self.metrics.record(Method::CreateDentry, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsCreateDentryArg {
            dhp: self.dhp,
            pino,
            name: cname.as_ref().as_ptr(),
            ino: value,
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsCreateDentryArg as usize;

        UzfsCoroutineFuture::new(libuzfs_create_dentry_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn delete_dentry<P: CStrArgument>(&self, pino: u64, name: P) -> Result<u64> {
        let _guard = self.metrics.record(Method::DeleteDentry, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsDeleteDentryArg {
            dhp: self.dhp,
            pino,
            name: cname.as_ref().as_ptr(),
            err: 0,
            txg: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsDeleteDentryArg as usize;

        UzfsCoroutineFuture::new(libuzfs_delete_entry_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.txg)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn lookup_dentry<P: CStrArgument>(&self, pino: u64, name: P) -> Result<u64> {
        let _guard = self.metrics.record(Method::LookupDentry, 0);
        let cname = name.into_cstr();
        let mut arg = LibuzfsLookupDentryArg {
            dhp: self.dhp,
            pino,
            name: cname.as_ref().as_ptr(),
            ino: 0,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsLookupDentryArg as usize;

        UzfsCoroutineFuture::new(libuzfs_lookup_dentry_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(arg.ino)
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn iterate_dentry(
        &self,
        pino: u64,
        whence: u64,
        size: u32,
    ) -> Result<(Vec<u8>, u32)> {
        let _guard = self.metrics.record(Method::IterateDentry, 0);
        let mut arg = LibuzfsIterateDentryArg {
            dhp: self.dhp,
            pino,
            whence,
            size,
            err: 0,
            data: Vec::new(),
            num: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsIterateDentryArg as usize;

        UzfsCoroutineFuture::new(libuzfs_iterate_dentry_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok((arg.data, arg.num))
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub fn get_last_synced_txg(&self) -> u64 {
        unsafe { sys::libuzfs_get_last_synced_txg(self.dhp) }
    }

    pub async fn wait_synced(&self) -> Result<()> {
        let _guard = self.metrics.record(Method::WaitSynced, 0);
        let arg_usize = self.dhp as usize;
        UzfsCoroutineFuture::new(libuzfs_wait_synced_c, arg_usize, true, false).await;
        Ok(())
    }

    pub async fn check_valid(&self, ino: u64, gen: u64) -> Result<()> {
        let mut arg = LibuzfsInodeCheckValidArg {
            dhp: self.dhp,
            ino,
            gen,

            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsInodeCheckValidArg as usize;

        UzfsCoroutineFuture::new(libuzfs_inode_check_valid_c, arg_usize, true, false).await;

        if arg.err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(arg.err))
        }
    }

    pub async fn set_object_mtime(&self, obj: u64, tv_sec: i64, tv_nsec: i64) -> Result<()> {
        let mut arg = LibuzfsObjectSetMtimeArg {
            dhp: self.dhp,
            obj,
            tv_sec,
            tv_nsec,
            err: 0,
        };

        let arg_usize = &mut arg as *mut LibuzfsObjectSetMtimeArg as usize;
        UzfsCoroutineFuture::new(libuzfs_object_set_mtime, arg_usize, true, false).await;

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

        UzfsCoroutineFuture::new(libuzfs_dataset_fini_c, arg_usize, true, true).await;

        if arg.err != 0 {
            Err(io::Error::from_raw_os_error(arg.err))
        } else {
            Ok(())
        }
    }
}

unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}

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

    pub fn get_dev_path(&self) -> Result<String> {
        let dev_path = self.dev_file.path().to_str().unwrap();
        Ok(dev_path.to_owned())
    }

    pub fn set_dev_size(&mut self, new_size: u64) {
        self.dev_file.as_file_mut().set_len(new_size).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::Dataset;
    use super::InodeType;
    use super::UzfsTestEnv;
    use crate::uzfs_env_fini;
    use crate::uzfs_env_init;
    use crate::DatasetType;
    use crate::KvSetOption;
    use crate::MAX_RESERVED_SIZE;
    use cstr_argument::CStrArgument;
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
    use tokio::task::JoinHandle;
    use uzfs_sys::bindings as sys;
    use uzfs_sys::bindings::timespec;

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

        let dsname = "uzfs-test-pool/ds";
        let poolname = "uzfs-test-pool";
        uzfs_env_init().await;
        let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);

        {
            let mut err = 0;
            let hdl = unsafe {
                sys::libuzfs_dataset_open(dsname.into_cstr().as_ptr(), &mut err, 1024, 4096)
            };
            assert!(hdl.is_null());
            let hdl = unsafe { sys::libuzfs_zpool_open(poolname.into_cstr().as_ptr(), &mut err) };
            assert!(hdl.is_null());

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
            let mut err = 0;
            let hdl = unsafe {
                sys::libuzfs_dataset_open(dsname.into_cstr().as_ptr(), &mut err, 1024, 4096)
            };
            assert!(hdl.is_null());
            let hdl = unsafe { sys::libuzfs_zpool_open(poolname.into_cstr().as_ptr(), &mut err) };
            assert!(hdl.is_null());
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
        let dsname = "uzfs-test-pool/ds";
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
        let dsname = "uzfs-test-pool/ds";
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
        let dsname = "uzfs-test-pool/ds";
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
        let dsname = "uzfs-test-pool/ds";
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
                    let my_version =
                        version_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        let dsname = "uzfs-test-pool/ds";
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
                    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
                    let obj = rt.block_on(async move {
                        let dsname = "uzfs-test-pool/ds";
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

                        let mut writer_handles =
                            Vec::<JoinHandle<()>>::with_capacity(write_data.len());
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

    #[tokio::test]
    #[serial]
    async fn uzfs_write_read_test() {
        let dsname = "uzfs-test-pool/ds";
        let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);
        let dev_path = uzfs_test_env.get_dev_path().unwrap();
        uzfs_env_init().await;

        let concurrency = 64;
        let mut handles = Vec::with_capacity(concurrency);
        let ds = Arc::new(
            Dataset::init(dsname, &dev_path, DatasetType::Data, 65536, false)
                .await
                .unwrap(),
        );

        for _ in 0..10000 {
            let obj = ds.create_objects(1).await.unwrap().0[0];
            let blksize = 16 << 10;
            for i in 0..concurrency {
                let ds = ds.clone();
                let offset = blksize * i;
                handles.push(async move {
                    let data: Vec<_> = (0..blksize).map(|_| rand::thread_rng().gen()).collect();
                    ds.write_object(obj, offset as u64, false, vec![&data])
                        .await
                        .unwrap();
                    let read = ds
                        .read_object(obj, offset as u64, blksize as u64)
                        .await
                        .unwrap();
                    assert!(read == data);
                });
            }
            ds.delete_object(obj).await.unwrap();
        }

        ds.close().await.unwrap();
        uzfs_env_fini().await;
    }
}
