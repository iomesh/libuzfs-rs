use cstr_argument::CStrArgument;
use std::marker::PhantomData;
use std::mem::size_of;
use std::os::raw::c_char;
use std::{io, ptr};
use uzfs_sys as sys;

use io::Result;

const MAX_KVATTR_VALUE_SIZE: usize = 8192;

pub struct Uzfs {
    i: PhantomData<()>,
}

impl Uzfs {
    pub fn init() -> Result<Self> {
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
        unsafe { sys::libuzfs_zpool_destroy(zpool.into_cstr().as_ref().as_ptr()) };
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

    pub fn destroy_dataset<P: CStrArgument>(&self, zpool: P) {
        unsafe { sys::libuzfs_dataset_destroy(zpool.into_cstr().as_ref().as_ptr()) };
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
}

impl Dataset {
    pub fn init<P: CStrArgument>(dsname: P) -> Result<Self> {
        let dhp = unsafe { sys::libuzfs_dataset_open(dsname.into_cstr().as_ref().as_ptr()) };

        if dhp.is_null() {
            Err(io::Error::from(io::ErrorKind::InvalidInput))
        } else {
            Ok(Self { dhp })
        }
    }

    pub fn get_superblock_ino(&self) -> Result<u64> {
        let obj_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe { sys::libuzfs_dataset_get_superblock_ino(self.dhp, obj_ptr) };
        let obj = unsafe { Box::from_raw(obj_ptr) };

        if err == 0 {
            Ok(*obj)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn create_object(&self) -> Result<(u64, u64)> {
        let obj_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));
        let gen_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe { sys::libuzfs_object_create(self.dhp, obj_ptr, gen_ptr) };
        let obj = unsafe { Box::from_raw(obj_ptr) };
        let gen = unsafe { Box::from_raw(gen_ptr) };

        if err == 0 {
            Ok((*obj, *gen))
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
        let gen_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));
        let err = unsafe { sys::libuzfs_object_get_gen(self.dhp, obj, gen_ptr) };
        let gen = unsafe { Box::from_raw(gen_ptr) };

        if err == 0 {
            Ok(*gen)
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
    pub fn write_object(
        &self,
        obj: u64,
        offset: u64,
        size: u64,
        sync: bool,
        data: &[u8],
    ) -> Result<()> {
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
        println!("object: {}", obj);
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
        let ino_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err =
            unsafe { sys::libuzfs_inode_create(self.dhp, ino_ptr, inode_type as u32, txg_ptr) };
        let ino = unsafe { Box::from_raw(ino_ptr) };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok((*ino, *txg))
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
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe { sys::libuzfs_inode_delete(self.dhp, ino, inode_type as u32, txg_ptr) };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok(*txg)
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
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe { sys::libuzfs_inode_setattr(self.dhp, ino, attr_ptr, txg_ptr) };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok(*txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_kvattr(&self, ino: u64, name: &[u8], flags: i32) -> Result<Vec<u8>> {
        let mut data = Vec::<u8>::with_capacity(MAX_KVATTR_VALUE_SIZE);
        data.resize_with(MAX_KVATTR_VALUE_SIZE, Default::default);
        let data_ptr = data.as_mut_ptr() as *mut c_char;
        let name_ptr = name.as_ptr() as *mut c_char;

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

    pub fn set_kvattr(&self, ino: u64, name: &[u8], value: &[u8], flags: i32) -> Result<u64> {
        let name_ptr = name.as_ptr() as *mut c_char;
        let value_ptr = value.as_ptr() as *mut c_char;
        let size = value.len() as u64;
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe {
            sys::libuzfs_inode_set_kvattr(self.dhp, ino, name_ptr, value_ptr, size, flags, txg_ptr)
        };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok(*txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn remove_kvattr(&self, ino: u64, name: &[u8]) -> Result<u64> {
        let name_ptr = name.as_ptr() as *mut c_char;
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe { sys::libuzfs_inode_remove_kvattr(self.dhp, ino, name_ptr, txg_ptr) };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok(*txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn create_dentry(&self, pino: u64, name: &[u8], value: &[u64]) -> Result<u64> {
        let num = value.len() as u64;
        let name_ptr = name.as_ptr() as *mut c_char;
        let value_ptr = value.as_ptr() as *mut u64;
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));

        let err = unsafe {
            sys::libuzfs_dentry_create(self.dhp, pino, name_ptr, value_ptr, num, txg_ptr)
        };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok(*txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn delete_dentry(&self, pino: u64, name: &[u8]) -> Result<u64> {
        let txg_ptr: *mut u64 = Box::into_raw(Box::new(0_u64));
        let err = unsafe {
            sys::libuzfs_dentry_delete(self.dhp, pino, name.as_ptr() as *mut c_char, txg_ptr)
        };
        let txg = unsafe { Box::from_raw(txg_ptr) };

        if err == 0 {
            Ok(*txg)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn lookup_dentry(&self, pino: u64, name: &[u8], size: u64) -> Result<Vec<u64>> {
        let mut value = Vec::<u64>::with_capacity(size as usize);
        value.resize_with(size as usize, Default::default);
        let value_ptr = value.as_mut_ptr() as *mut u64;
        let name_ptr = name.as_ptr() as *mut c_char;

        let err = unsafe { sys::libuzfs_dentry_lookup(self.dhp, pino, name_ptr, value_ptr, size) };

        if err == 0 {
            Ok(value)
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
    }
}

unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}

#[cfg(test)]
mod tests {
    use super::Dataset;
    use super::InodeType;
    use super::Uzfs;
    use std::fs::{self, File};
    use std::mem::{size_of, transmute};
    use uzfs_sys::uzfs_attr_t as Attr;

    unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
        ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
    }

    fn setup(devname: &str, poolname: &str, datasetname: &str, cache_path: &str) {
        let dev_file = File::create(devname.clone()).unwrap();
        dev_file.set_len(100 * 1024 * 1024).unwrap();

        Uzfs::set_zpool_cache_path(cache_path);

        let uzfs = Uzfs::init().unwrap();
        uzfs.create_zpool(poolname.clone(), devname.clone())
            .unwrap();
        uzfs.create_dataset(datasetname.clone()).unwrap();
    }

    fn cleanup(devname: &str, poolname: &str, datasetname: &str, cache_path: &str) {
        Uzfs::set_zpool_cache_path(cache_path);

        let uzfs = Uzfs::init().unwrap();
        uzfs.destroy_dataset(datasetname.clone());
        uzfs.destroy_zpool(poolname.clone());

        fs::remove_file(devname).unwrap();
    }

    #[test]
    fn uzfs_test() {
        let devname = String::from("/tmp/uzfs-test.img");
        let poolname = String::from("uzfs-test-pool1");
        let datasetname = poolname.clone() + "/uzfs";
        let zpool_cache_path = String::from("/tmp/testzpool.cache");

        let rwobj;
        let gen;
        let sb_ino;
        let sb_file_ino;
        let sb_file_name = String::from("sb_file");
        let sb_dentry_data;
        let s = String::from("Hello uzfs!");
        let t = vec!['H' as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let file_ino;
        let dir_ino;
        let num;
        let mut txg;
        let mut attr: Attr = Attr {
            ..Default::default()
        };
        let key = String::from("acl");
        let value = String::from("root,admin");
        let file_name = String::from("fileA");
        let dentry_data;

        setup(&devname, &poolname, &datasetname, &zpool_cache_path);

        {
            Uzfs::set_zpool_cache_path(&zpool_cache_path);
            let _uzfs = Uzfs::init().unwrap();
            let ds = Dataset::init(datasetname.clone()).unwrap();

            sb_ino = ds.get_superblock_ino().unwrap();
            let last_txg = ds.get_last_synced_txg().unwrap();

            txg = ds
                .set_kvattr(sb_ino, key.as_bytes(), value.as_bytes(), 0)
                .unwrap();
            assert!(txg > last_txg);

            let value_read = ds.get_kvattr(sb_ino, key.as_bytes(), 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());
            ds.wait_synced().unwrap();

            (sb_file_ino, _) = ds.create_inode(InodeType::FILE).unwrap();

            sb_dentry_data = [sb_file_ino, 1];

            txg = ds
                .create_dentry(sb_ino, sb_file_name.as_bytes(), &sb_dentry_data)
                .unwrap();
            ds.wait_synced().unwrap();
            assert!(ds.get_last_synced_txg().unwrap() >= txg);

            let sb_dentry_data_read = ds
                .lookup_dentry(sb_ino, sb_file_name.as_bytes(), sb_dentry_data.len() as u64)
                .unwrap();
            assert_eq!(sb_dentry_data.to_vec(), sb_dentry_data_read);

            num = ds.list_object().unwrap();
            (rwobj, gen) = ds.create_object().unwrap();

            assert_eq!(ds.get_object_gen(rwobj).unwrap(), gen);
            assert_eq!(ds.list_object().unwrap(), num + 1);

            let doi = ds.stat_object(rwobj).unwrap();
            Dataset::dump_object_doi(rwobj, doi);

            let data = s.as_bytes();
            let size = s.len() as u64;
            ds.write_object(rwobj, 0, size, true, data).unwrap();
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), data);

            ds.truncate_object(rwobj, 1, size - 1).unwrap();
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), t);

            (file_ino, _) = ds.create_inode(InodeType::FILE).unwrap();
            (dir_ino, _) = ds.create_inode(InodeType::DIR).unwrap();

            dentry_data = [file_ino, 1];

            txg = ds
                .create_dentry(dir_ino, file_name.as_bytes(), &dentry_data)
                .unwrap();
            let dentry_data_read = ds
                .lookup_dentry(dir_ino, file_name.as_bytes(), dentry_data.len() as u64)
                .unwrap();
            assert_eq!(dentry_data.to_vec(), dentry_data_read);
            ds.wait_synced().unwrap();
            assert!(ds.get_last_synced_txg().unwrap() >= txg);

            assert_eq!(ds.list_object().unwrap(), num + 3);

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

            _ = ds
                .set_kvattr(file_ino, key.as_bytes(), value.as_bytes(), 0)
                .unwrap();

            let value_read = ds.get_kvattr(file_ino, key.as_bytes(), 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            assert_eq!(ds.list_object().unwrap(), num + 3);
        }

        {
            Uzfs::set_zpool_cache_path(&zpool_cache_path);
            let _uzfs = Uzfs::init().unwrap();
            let ds = Dataset::init(datasetname.clone()).unwrap();

            assert!(ds.get_last_synced_txg().unwrap() >= txg);
            assert_eq!(ds.list_object().unwrap(), num + 3);

            assert_eq!(ds.get_superblock_ino().unwrap(), sb_ino);

            let value_read = ds.get_kvattr(sb_ino, key.as_bytes(), 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            let sb_dentry_data_read = ds
                .lookup_dentry(sb_ino, sb_file_name.as_bytes(), sb_dentry_data.len() as u64)
                .unwrap();
            assert_eq!(sb_dentry_data.to_vec(), sb_dentry_data_read);

            assert_eq!(ds.get_object_gen(rwobj).unwrap(), gen);

            let size = s.len() as u64;
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), t);

            ds.delete_object(rwobj).unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 2);

            let dentry_data_read = ds
                .lookup_dentry(dir_ino, file_name.as_bytes(), dentry_data.len() as u64)
                .unwrap();
            assert_eq!(dentry_data.to_vec(), dentry_data_read);

            _ = ds.delete_dentry(dir_ino, file_name.as_bytes()).unwrap();

            let attr_bytes = unsafe { any_as_u8_slice(&attr) };
            let attr_new = ds.get_attr(file_ino, attr_bytes.len() as u64).unwrap();
            assert_eq!(attr_new.as_slice(), attr_bytes);

            let value_read = ds.get_kvattr(file_ino, key.as_bytes(), 0).unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            txg = ds.remove_kvattr(file_ino, key.as_bytes()).unwrap();
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

        cleanup(&devname, &poolname, &datasetname, &zpool_cache_path);
    }
}
