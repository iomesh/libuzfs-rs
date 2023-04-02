use cstr_argument::CStrArgument;
use std::ffi::{CStr, CString};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::mem::size_of;
use std::os::raw::c_char;
use std::sync::Arc;
use std::{io, ptr};
use tempfile::NamedTempFile;
use uzfs_sys as sys;

use io::Result;

const MAX_KVATTR_VALUE_SIZE: usize = 8192;
const MAX_KVATTR_KEY_SIZE: usize = 256;
const DEFAULT_CACHE_FILE: &str = "/tmp/zpool.cache";

pub struct Uzfs {
    i: PhantomData<()>,
}

impl Uzfs {
    pub fn init() -> Result<Self> {
        let _ = std::fs::remove_file(DEFAULT_CACHE_FILE);
        unsafe {
            sys::libuzfs_init();
        };
        Ok(Self { i: PhantomData })
    }

    pub fn set_zpool_cache_path<P: CStrArgument>(path: P) {
        unsafe {
            sys::libuzfs_set_zpool_cache_path(path.into_cstr().as_ref().as_ptr());
        }
    }

    pub fn create_zpool<P: CStrArgument>(&self, zpool: P, dev_path: P) -> Result<()> {
        let zpool = zpool.into_cstr();
        let dev_path = dev_path.into_cstr();
        let err = unsafe {
            sys::libuzfs_zpool_create(
                zpool.as_ref().as_ptr(),
                dev_path.as_ref().as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn destroy_zpool<P: CStrArgument>(&self, zpool: P) {
        let zpool = zpool.into_cstr();
        unsafe { sys::libuzfs_zpool_destroy(zpool.as_ref().as_ptr()) };
    }

    pub fn create_dataset<P: CStrArgument>(&self, dsname: P) -> Result<()> {
        let dsname = dsname.into_cstr();
        let err = unsafe { sys::libuzfs_dataset_create(dsname.as_ref().as_ptr()) };
        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn destroy_dataset<P: CStrArgument>(&self, dsname: P) {
        let dsname = dsname.into_cstr();
        unsafe { sys::libuzfs_dataset_destroy(dsname.as_ref().as_ptr()) };
    }
}

impl Drop for Uzfs {
    fn drop(&mut self) {
        unsafe { sys::libuzfs_fini() }
    }
}

unsafe impl Send for Uzfs {}
unsafe impl Sync for Uzfs {}

pub enum InodeType {
    FILE = sys::libuzfs_inode_type_t_INODE_FILE as isize,
    DIR = sys::libuzfs_inode_type_t_INODE_DIR as isize,
}

pub struct Dataset {
    dhp: *mut sys::libuzfs_dataset_handle_t,
    zhp: *mut sys::libuzfs_zpool_handle_t,
    poolname: CString,
    _uzfs: Arc<Uzfs>,
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

    pub fn init<P: CStrArgument>(dsname: P, dev_path: P, uzfs: Arc<Uzfs>) -> Result<Self> {
        let dev_path = dev_path.into_cstr();
        let dsname = dsname.into_cstr();
        let poolname = Self::dsname_to_poolname(&dsname)?;

        let poolname_string = poolname.clone().into_string().unwrap();
        let mut stored_pool_name = vec![0; poolname_string.len()];

        let errno = unsafe {
            sys::libuzfs_zpool_import(
                dev_path.as_ref().as_ptr(),
                stored_pool_name.as_mut_ptr() as *mut c_char,
                poolname_string.len() as i32,
            )
        };

        if errno != 0 {
            let err = io::Error::from_raw_os_error(errno);
            match err.kind() {
                ErrorKind::NotFound => {
                    uzfs.create_zpool(poolname.clone().into_cstr().as_ref(), dev_path.as_ref())
                        .unwrap();
                    uzfs.create_dataset(dsname.as_ref()).unwrap();
                }
                ErrorKind::AlreadyExists => {
                    return Err(err);
                }
                _ => {
                    panic!(
                        "unexpected error while importing zpool, dev_path: {}, dsname: {}, error: {:?}",
                        dev_path.as_ref().to_str().unwrap(),
                        dsname.as_ref().to_str().unwrap(),
                        err
                    );
                }
            }
        } else {
            let stored_pool_name = std::str::from_utf8(&stored_pool_name).unwrap();
            assert_eq!(stored_pool_name, poolname_string);
        }

        let zhp = unsafe { sys::libuzfs_zpool_open(poolname.as_c_str().as_ptr()) };
        if zhp.is_null() {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let dhp = unsafe { sys::libuzfs_dataset_open(dsname.as_ref().as_ptr()) };
        if dhp.is_null() {
            Err(io::Error::from(io::ErrorKind::InvalidInput))
        } else {
            Ok(Self {
                dhp,
                zhp,
                poolname,
                _uzfs: uzfs,
            })
        }
    }

    pub fn get_superblock_ino(&self) -> Result<u64> {
        let mut obj: u64 = 0;
        let obj_ptr: *mut u64 = &mut obj;

        let err = unsafe { sys::libuzfs_dataset_get_superblock_ino(self.dhp, obj_ptr) };

        if err == 0 {
            Ok(obj)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn create_object(&self) -> Result<(u64, u64)> {
        let mut obj: u64 = 0;
        let mut gen: u64 = 0;
        let obj_ptr: *mut u64 = &mut obj;
        let gen_ptr: *mut u64 = &mut gen;

        let err = unsafe { sys::libuzfs_object_create(self.dhp, obj_ptr, gen_ptr) };

        if err == 0 {
            Ok((obj, gen))
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn claim_object(&self, obj: u64) -> Result<()> {
        let err = unsafe { sys::libuzfs_object_claim(self.dhp, obj) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn delete_object(&self, obj: u64) -> Result<()> {
        let err = unsafe { sys::libuzfs_object_delete(self.dhp, obj) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_object_gen(&self, obj: u64) -> Result<u64> {
        let mut gen: u64 = 0;
        let gen_ptr: *mut u64 = &mut gen;

        let err = unsafe { sys::libuzfs_object_get_gen(self.dhp, obj, gen_ptr) };

        if err == 0 {
            Ok(gen)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn list_object(&self) -> Result<u64> {
        let n = unsafe { sys::libuzfs_object_list(self.dhp) };
        Ok(n)
    }

    pub fn stat_object(&self, obj: u64) -> Result<sys::dmu_object_info_t> {
        let doi = Box::<sys::dmu_object_info>::default();
        let doip = Box::into_raw(doi);

        let err = unsafe { sys::libuzfs_object_stat(self.dhp, obj, doip) };
        let doi = unsafe { Box::from_raw(doip) };

        if err == 0 {
            Ok(*doi)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn read_object(&self, obj: u64, offset: u64, size: u64) -> Result<Vec<u8>> {
        let mut data = Vec::<u8>::with_capacity(size as usize);
        data.resize_with(size as usize, Default::default);
        let ptr = data.as_mut_ptr() as *mut c_char;

        let err = unsafe { sys::libuzfs_object_read(self.dhp, obj, offset, size, ptr) };

        if err == 0 {
            Ok(data)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    // TODO(hping): add unit tests to verify sync write works well in crash scenario
    pub fn write_object(&self, obj: u64, offset: u64, sync: bool, data: &[u8]) -> Result<()> {
        let size = data.len() as u64;
        let ptr = data.as_ptr() as *const c_char;
        let sync = sync as u32;
        let err = unsafe { sys::libuzfs_object_write(self.dhp, obj, offset, size, ptr, sync) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    // TODO(hping): add ut
    pub fn sync_object(&self, obj: u64) {
        unsafe { sys::libuzfs_object_sync(self.dhp, obj) };
    }

    pub fn truncate_object(&self, obj: u64, offset: u64, size: u64) -> Result<()> {
        let err = unsafe { sys::libuzfs_object_truncate(self.dhp, obj, offset, size) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
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

    pub fn create_inode(&self, inode_type: InodeType) -> Result<(u64, u64)> {
        let mut ino: u64 = 0;
        let mut txg: u64 = 0;
        let ino_ptr: *mut u64 = &mut ino;
        let txg_ptr: *mut u64 = &mut txg;

        let err =
            unsafe { sys::libuzfs_inode_create(self.dhp, ino_ptr, inode_type as u32, txg_ptr) };

        if err == 0 {
            Ok((ino, txg))
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn claim_inode(&self, ino: u64, inode_type: InodeType) -> Result<()> {
        let err = unsafe { sys::libuzfs_inode_claim(self.dhp, ino, inode_type as u32) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn delete_inode(&self, ino: u64, inode_type: InodeType) -> Result<u64> {
        let mut txg: u64 = 0;
        let txg_ptr: *mut u64 = &mut txg;

        let err = unsafe { sys::libuzfs_inode_delete(self.dhp, ino, inode_type as u32, txg_ptr) };

        if err == 0 {
            Ok(txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_attr(&self, ino: u64, size: u64) -> Result<Vec<u8>> {
        assert!(size_of::<sys::uzfs_attr_t>() == size as usize);
        let mut attr = Vec::<u8>::with_capacity(size as usize);
        attr.resize_with(size as usize, Default::default);
        let attr_ptr = attr.as_mut_ptr() as *mut sys::uzfs_attr_t;

        let err = unsafe { sys::libuzfs_inode_getattr(self.dhp, ino, attr_ptr) };

        if err == 0 {
            Ok(attr)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn set_attr(&self, ino: u64, attr: &[u8]) -> Result<u64> {
        assert!(size_of::<sys::uzfs_attr_t>() == attr.len());
        let attr_ptr = attr.as_ptr() as *mut sys::uzfs_attr_t;
        let mut txg: u64 = 0;
        let txg_ptr: *mut u64 = &mut txg;

        let err = unsafe { sys::libuzfs_inode_setattr(self.dhp, ino, attr_ptr, txg_ptr) };

        if err == 0 {
            Ok(txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_kvattr<P: CStrArgument>(&self, ino: u64, name: P, flags: i32) -> Result<Vec<u8>> {
        let mut data = Vec::<u8>::with_capacity(MAX_KVATTR_VALUE_SIZE);
        data.resize_with(MAX_KVATTR_VALUE_SIZE, Default::default);
        let data_ptr = data.as_mut_ptr() as *mut c_char;
        let cname = name.into_cstr();
        let name_ptr = cname.as_ref().as_ptr() as *const c_char;

        let rc = unsafe {
            sys::libuzfs_inode_get_kvattr(
                self.dhp,
                ino,
                name_ptr,
                data_ptr,
                MAX_KVATTR_VALUE_SIZE as u64,
                flags,
            )
        };

        if rc > 0 {
            data.resize_with(rc as usize, Default::default);
            Ok(data)
        } else {
            Err(io::Error::from_raw_os_error(-rc as i32))
        }
    }

    pub fn set_kvattr<P: CStrArgument>(
        &self,
        ino: u64,
        name: P,
        value: &[u8],
        flags: i32,
    ) -> Result<u64> {
        let mut txg: u64 = 0;
        let txg_ptr: *mut u64 = &mut txg;
        let cname = name.into_cstr();
        let name_ptr = cname.as_ref().as_ptr() as *const c_char;
        let value_ptr = value.as_ptr() as *const c_char;
        let size = value.len() as u64;

        let err = unsafe {
            sys::libuzfs_inode_set_kvattr(self.dhp, ino, name_ptr, value_ptr, size, flags, txg_ptr)
        };

        if err == 0 {
            Ok(txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn remove_kvattr<P: CStrArgument>(&self, ino: u64, name: P) -> Result<u64> {
        let mut txg: u64 = 0;
        let txg_ptr: *mut u64 = &mut txg;
        let cname = name.into_cstr();
        let name_ptr = cname.as_ref().as_ptr() as *const c_char;

        let err = unsafe { sys::libuzfs_inode_remove_kvattr(self.dhp, ino, name_ptr, txg_ptr) };

        if err == 0 {
            Ok(txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn list_kvattrs(&self, ino: u64) -> Result<Vec<String>> {
        let mut err: i32 = 0;
        let iter = unsafe { sys::libuzfs_new_kvattr_iterator(self.dhp, ino, &mut err as *mut i32) };
        if iter.is_null() {
            assert_ne!(err, 0);
            return Err(io::Error::from_raw_os_error(err));
        }

        let mut res: Vec<String> = vec![];
        loop {
            let mut buf = Vec::<u8>::with_capacity(MAX_KVATTR_KEY_SIZE);
            buf.resize_with(MAX_KVATTR_KEY_SIZE, Default::default);
            let rc = unsafe {
                sys::libuzfs_next_kvattr_name(
                    iter,
                    buf.as_mut_ptr() as *mut c_char,
                    MAX_KVATTR_KEY_SIZE as i32,
                )
            };
            assert!(rc >= 0);
            if rc == 0 {
                break;
            }
            buf.resize_with(rc as usize, Default::default);
            let key = String::from_utf8(buf).unwrap();
            res.push(key);
        }

        unsafe { sys::libuzfs_kvattr_iterator_fini(iter) };

        Ok(res)
    }

    pub fn create_dentry<P: CStrArgument>(&self, pino: u64, name: P, value: u64) -> Result<u64> {
        let mut txg: u64 = 0;
        let txg_ptr: *mut u64 = &mut txg;
        let cname = name.into_cstr();
        let name_ptr = cname.as_ref().as_ptr() as *const c_char;

        let err = unsafe { sys::libuzfs_dentry_create(self.dhp, pino, name_ptr, value, txg_ptr) };

        if err == 0 {
            Ok(txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn delete_dentry<P: CStrArgument>(&self, pino: u64, name: P) -> Result<u64> {
        let mut txg: u64 = 0;
        let txg_ptr: *mut u64 = &mut txg;
        let cname = name.into_cstr();
        let name_ptr = cname.as_ref().as_ptr() as *const c_char;

        let err = unsafe { sys::libuzfs_dentry_delete(self.dhp, pino, name_ptr, txg_ptr) };

        if err == 0 {
            Ok(txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn lookup_dentry<P: CStrArgument>(&self, pino: u64, name: P) -> Result<u64> {
        let mut value: u64 = 0;
        let value_ptr: *mut u64 = &mut value;
        let cname = name.into_cstr();
        let name_ptr = cname.as_ref().as_ptr() as *const c_char;

        let err = unsafe { sys::libuzfs_dentry_lookup(self.dhp, pino, name_ptr, value_ptr) };

        if err == 0 {
            Ok(value)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn iterate_dentry(&self, pino: u64, whence: u64, size: u32) -> Result<(Vec<u8>, u32)> {
        let mut data = Vec::<u8>::with_capacity(size as usize);
        data.resize_with(size as usize, Default::default);
        let data_ptr = data.as_mut_ptr() as *mut c_char;
        let mut num: u32 = 0;
        let num_ptr: *mut u32 = &mut num;

        let err =
            unsafe { sys::libuzfs_dentry_iterate(self.dhp, pino, whence, size, data_ptr, num_ptr) };

        if err == 0 {
            Ok((data, num))
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_last_synced_txg(&self) -> Result<u64> {
        let txg = unsafe { sys::libuzfs_get_last_synced_txg(self.dhp) };
        Ok(txg)
    }

    pub fn wait_synced(&self) -> Result<()> {
        unsafe { sys::libuzfs_wait_synced(self.dhp) };
        Ok(())
    }
}

impl Drop for Dataset {
    fn drop(&mut self) {
        unsafe { sys::libuzfs_dataset_close(self.dhp) };
        unsafe { sys::libuzfs_zpool_close(self.zhp) };
        let err = unsafe { sys::libuzfs_zpool_export(self.poolname.as_ref().as_ptr()) };
        assert_eq!(err, 0);
    }
}

unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}

pub struct UzfsTestEnv {
    #[allow(dead_code)]
    dev_file: NamedTempFile,
    cache_file: NamedTempFile,
}

impl UzfsTestEnv {
    /// Create an env for uzfs test.
    ///
    /// If dev_size is not zero, this function will create a temp file as block device,
    /// the size of device is `dev_size` bytes.
    ///
    /// If dev_size is zero, no block device and pool/dataset is created
    ///
    /// A temp cache file is always automacally created, used by uzfs to find zpool.
    ///
    /// All of the resources (temp file, dev_file) will be deleted automatically when the env
    /// goes out of scope
    pub fn new(dev_size: u64) -> Self {
        let mut dev_file = NamedTempFile::new().unwrap();
        let cache_file = NamedTempFile::new().unwrap();
        let cache_path = cache_file.path().to_str().unwrap();
        Uzfs::set_zpool_cache_path(cache_path);

        if dev_size > 0 {
            dev_file.as_file_mut().set_len(dev_size).unwrap();
        }

        UzfsTestEnv {
            dev_file,
            cache_file,
        }
    }

    pub fn get_cache_path(&self) -> Result<String> {
        let cache_path = self.cache_file.path().to_str().unwrap();
        Ok(cache_path.to_owned())
    }

    pub fn get_dev_path(&self) -> Result<String> {
        let dev_path = self.dev_file.path().to_str().unwrap();
        Ok(dev_path.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::Dataset;
    use super::InodeType;
    use super::{Uzfs, UzfsTestEnv};
    use cstr_argument::CStrArgument;
    use serial_test::serial;
    use std::mem::{size_of, transmute};
    use std::sync::Arc;
    use test_log::test;
    use uzfs_sys::{self as sys, uzfs_attr_t as Attr};

    enum FileType {
        FILE = sys::FileType_TYPE_FILE as isize,
        DIR = sys::FileType_TYPE_DIR as isize,
    }

    unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
        ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
    }

    #[serial]
    #[test]
    fn uzfs_test() {
        let rwobj;
        let gen;
        let sb_ino;
        let tmp_ino;
        let tmp_name = "tmp_dir";
        let tmp_dentry_data;
        let s = String::from("Hello uzfs!");
        let t = vec!['H' as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let file_ino;
        let dir_ino;
        let num;
        let mut txg;
        let mut attr: Attr = Attr {
            ..Default::default()
        };
        let key = "acl";
        let value = "root,admin";
        let file_name = "fileA";
        let dentry_data;

        let dsname = "uzfs-test-pool/ds";
        let poolname = "uzfs-test-pool";
        let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);

        {
            Uzfs::set_zpool_cache_path(uzfs_test_env.get_cache_path().unwrap());
            let uzfs = Arc::new(Uzfs::init().unwrap());

            let hdl = unsafe { sys::libuzfs_dataset_open(dsname.into_cstr().as_ptr()) };
            assert!(hdl.is_null());
            let hdl = unsafe { sys::libuzfs_zpool_open(poolname.into_cstr().as_ptr()) };
            assert!(hdl.is_null());

            let ds = Dataset::init(
                dsname,
                uzfs_test_env.get_dev_path().unwrap().as_str(),
                uzfs.clone(),
            )
            .unwrap();

            let second_init = Dataset::init(
                dsname,
                uzfs_test_env.get_dev_path().unwrap().as_str(),
                uzfs.clone(),
            );
            assert!(second_init.is_err());

            sb_ino = ds.get_superblock_ino().unwrap();
            let last_txg = ds.get_last_synced_txg().unwrap();

            txg = ds.set_kvattr(sb_ino, key, value.as_bytes(), 0).unwrap();
            assert!(txg > last_txg);

            let value_read = ds.get_kvattr(sb_ino, key, 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());
            ds.wait_synced().unwrap();

            (tmp_ino, _) = ds.create_inode(InodeType::DIR).unwrap();

            tmp_dentry_data = (FileType::DIR as u64) << 60 | tmp_ino;

            txg = ds.create_dentry(sb_ino, tmp_name, tmp_dentry_data).unwrap();
            ds.wait_synced().unwrap();
            assert!(ds.get_last_synced_txg().unwrap() >= txg);

            let tmp_dentry_data_read = ds.lookup_dentry(sb_ino, tmp_name).unwrap();
            assert_eq!(tmp_dentry_data, tmp_dentry_data_read);

            num = ds.list_object().unwrap();
            (rwobj, gen) = ds.create_object().unwrap();

            assert_eq!(ds.get_object_gen(rwobj).unwrap(), gen);
            assert_eq!(ds.list_object().unwrap(), num + 1);

            let doi = ds.stat_object(rwobj).unwrap();
            Dataset::dump_object_doi(rwobj, doi);

            let data = s.as_bytes();
            let size = s.len() as u64;
            ds.write_object(rwobj, 0, true, data).unwrap();
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), data);

            ds.truncate_object(rwobj, 1, size - 1).unwrap();
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), t);

            (file_ino, _) = ds.create_inode(InodeType::FILE).unwrap();
            (dir_ino, _) = ds.create_inode(InodeType::DIR).unwrap();

            dentry_data = (FileType::FILE as u64) << 60 | file_ino;

            txg = ds.create_dentry(dir_ino, file_name, dentry_data).unwrap();
            let dentry_data_read = ds.lookup_dentry(dir_ino, file_name).unwrap();
            assert_eq!(dentry_data, dentry_data_read);
            ds.wait_synced().unwrap();
            assert!(ds.get_last_synced_txg().unwrap() >= txg);

            let (_, dentry_num) = ds.iterate_dentry(dir_ino, 0, 4096).unwrap();

            // TODO(hping): verify dentry content
            assert_eq!(dentry_num, 1);

            assert_eq!(ds.list_object().unwrap(), num + 3);

            attr.ino = file_ino;
            attr.nlink = 1;
            attr.gen = 101;

            let attr_bytes = unsafe { any_as_u8_slice(&attr) };

            _ = ds.set_attr(file_ino, attr_bytes).unwrap();

            let attr_new = ds.get_attr(file_ino, attr_bytes.len() as u64).unwrap();
            assert_eq!(attr_new.as_slice(), attr_bytes);

            let attr_got =
                unsafe { transmute::<[u8; size_of::<Attr>()], Attr>(attr_new.try_into().unwrap()) };
            assert_eq!(attr_got.ino, attr.ino);
            assert_eq!(attr_got.nlink, attr.nlink);

            _ = ds.set_kvattr(file_ino, key, value.as_bytes(), 0).unwrap();

            let value_read = ds.get_kvattr(file_ino, key, 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            assert_eq!(ds.list_object().unwrap(), num + 3);
        }

        {
            Uzfs::set_zpool_cache_path(uzfs_test_env.get_cache_path().unwrap());
            let uzfs = Arc::new(Uzfs::init().unwrap());

            let hdl = unsafe { sys::libuzfs_dataset_open(dsname.into_cstr().as_ptr()) };
            assert!(hdl.is_null());
            let hdl = unsafe { sys::libuzfs_zpool_open(poolname.into_cstr().as_ptr()) };
            assert!(hdl.is_null());

            let ds = Dataset::init(
                dsname,
                uzfs_test_env.get_dev_path().unwrap().as_str(),
                uzfs.clone(),
            )
            .unwrap();

            assert!(ds.get_last_synced_txg().unwrap() >= txg);
            assert_eq!(ds.list_object().unwrap(), num + 3);

            assert_eq!(ds.get_superblock_ino().unwrap(), sb_ino);

            let value_read = ds.get_kvattr(sb_ino, key, 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            let tmp_dentry_data_read = ds.lookup_dentry(sb_ino, tmp_name).unwrap();
            assert_eq!(tmp_dentry_data, tmp_dentry_data_read);

            assert_eq!(ds.get_object_gen(rwobj).unwrap(), gen);

            let size = s.len() as u64;
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), t);

            ds.delete_object(rwobj).unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 2);

            let dentry_data_read = ds.lookup_dentry(dir_ino, file_name).unwrap();
            assert_eq!(dentry_data, dentry_data_read);

            let (_, dentry_num) = ds.iterate_dentry(dir_ino, 0, 4096).unwrap();
            assert_eq!(dentry_num, 1);

            _ = ds.delete_dentry(dir_ino, file_name).unwrap();

            let (_, dentry_num) = ds.iterate_dentry(dir_ino, 0, 4096).unwrap();
            assert_eq!(dentry_num, 0);

            let attr_bytes = unsafe { any_as_u8_slice(&attr) };
            let attr_new = ds.get_attr(file_ino, attr_bytes.len() as u64).unwrap();
            assert_eq!(attr_new.as_slice(), attr_bytes);

            let value_read = ds.get_kvattr(file_ino, key, 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            txg = ds.remove_kvattr(file_ino, key).unwrap();
            ds.wait_synced().unwrap();
            assert!(ds.get_last_synced_txg().unwrap() >= txg);

            _ = ds.delete_inode(dir_ino, InodeType::DIR).unwrap();
            txg = ds.delete_inode(file_ino, InodeType::FILE).unwrap();
            ds.wait_synced().unwrap();
            assert!(ds.get_last_synced_txg().unwrap() >= txg);

            assert_eq!(ds.list_object().unwrap(), num);

            ds.claim_object(rwobj).unwrap();
            ds.wait_synced().unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 1);
        }

        {
            Uzfs::set_zpool_cache_path(uzfs_test_env.get_cache_path().unwrap());
            let uzfs = Arc::new(Uzfs::init().unwrap());
            let ds = Dataset::init(
                dsname,
                uzfs_test_env.get_dev_path().unwrap().as_str(),
                uzfs.clone(),
            )
            .unwrap();

            let ino = ds.create_inode(InodeType::FILE).unwrap().0;
            let keys = ds.list_kvattrs(ino).unwrap();
            assert!(keys.is_empty());

            let total_kvs: usize = 4096;
            for i in 0..total_kvs {
                let key = i.to_string();
                let mut value: Vec<u8> = vec![];
                let value_size: usize = 256;
                value.resize_with(value_size, Default::default);

                ds.set_kvattr(ino, key.as_str(), &value, 0).unwrap();
            }

            let keys = ds.list_kvattrs(ino).unwrap();
            assert_eq!(keys.len(), total_kvs);

            let mut numbers: Vec<usize> = Vec::<usize>::with_capacity(total_kvs);
            for key in keys {
                numbers.push(key.parse::<usize>().unwrap());
            }
            numbers.sort();

            let expect_vec: Vec<usize> = (0..total_kvs).collect();
            assert_eq!(numbers, expect_vec);

            ds.delete_inode(ino, InodeType::FILE).unwrap();
        }
    }

    #[serial]
    #[test]
    fn uzfs_claim_test() {
        let ino;
        let dsname = "uzfs-test-pool/ds";
        let uzfs_test_env = UzfsTestEnv::new(100 * 1024 * 1024);

        {
            Uzfs::set_zpool_cache_path(uzfs_test_env.get_cache_path().unwrap());
            let uzfs = Arc::new(Uzfs::init().unwrap());

            let ds = Dataset::init(
                dsname,
                uzfs_test_env.get_dev_path().unwrap().as_str(),
                uzfs.clone(),
            )
            .unwrap();

            (ino, _) = ds.create_inode(InodeType::DIR).unwrap();

            ds.wait_synced().unwrap();
        }

        {
            Uzfs::set_zpool_cache_path(uzfs_test_env.get_cache_path().unwrap());
            let uzfs = Arc::new(Uzfs::init().unwrap());

            let ds = Dataset::init(
                dsname,
                uzfs_test_env.get_dev_path().unwrap().as_str(),
                uzfs.clone(),
            )
            .unwrap();

            // test claim when inode exists
            ds.claim_inode(ino, InodeType::DIR).unwrap();

            ds.delete_inode(ino, InodeType::DIR).unwrap();
            ds.wait_synced().unwrap();

            // test claim when inode doesn't exist
            ds.claim_inode(ino, InodeType::DIR).unwrap();
        }
    }
}
