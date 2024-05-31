use crate::async_sys::*;
use crate::bindings::{self as sys, iovec, uzfs_inode_attr_t, uzfs_object_attr_t};
use crate::coroutine::*;
use crate::metrics::{Method, Metrics};
use cstr_argument::CStrArgument;
use io::Result;
use once_cell::sync::OnceCell;
use std::ffi::{CStr, CString};
use std::io;
use std::os::raw::{c_char, c_void};
use tokio::sync::Mutex;

pub const DEFAULT_CACHE_FILE: &str = "/tmp/zpool.cache";

static UZFS_INIT_REF: OnceCell<Mutex<u32>> = OnceCell::new();
pub const MAX_RESERVED_SIZE: usize = 192;
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
    ADD_BACKTRACE.get_or_init(|| add_bt);
    REMOVE_BACKTRACE.get_or_init(|| remove_bt);
    ADD_CREATION_POS.get_or_init(|| add_cp);
    REMOVE_CREATION_POS.get_or_init(|| remove_cp);
}

pub async fn uzfs_env_init() {
    let _ = std::fs::remove_file(DEFAULT_CACHE_FILE);
    let mut guard = UZFS_INIT_REF.get_or_init(|| Mutex::new(0)).lock().await;

    if *guard == 0 {
        UzfsCoroutineFuture::new(libuzfs_init_c, 0, true, true).await;
    }

    *guard += 1;
}

pub fn set_fail_percent(fp: i32) {
    unsafe { sys::libuzfs_set_fail_percent(fp) };
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
