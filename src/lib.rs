use cstr_argument::CStrArgument;
use std::marker::PhantomData;
use std::os::raw::{c_char, c_void};
use std::{io, ptr};
use uzfs_sys as sys;

use io::Result;

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

    pub fn create_object(&self, opid: u64) -> Result<u64> {
        let obj = Box::new(0_u64);
        let obj_ptr: *mut u64 = Box::into_raw(obj);

        let err = unsafe { sys::libuzfs_object_create(self.dhp, obj_ptr, opid) };
        let obj = unsafe { Box::from_raw(obj_ptr) };

        if err == 0 {
            Ok(*obj)
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

    pub fn delete_object(&self, obj: u64, opid: u64) -> Result<()> {
        let err = unsafe { sys::libuzfs_object_delete(self.dhp, obj, opid) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn list_object(&self) -> Result<u64> {
        let n = unsafe { sys::libuzfs_object_list(self.dhp) };
        Ok(n)
    }

    pub fn stat_object(&self, obj: u64) -> Result<sys::dmu_object_info_t> {
        let doi = Box::new(sys::dmu_object_info::default());
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

    pub fn write_object(&self, obj: u64, offset: u64, size: u64, data: &[u8]) -> Result<()> {
        let err = unsafe {
            sys::libuzfs_object_write(self.dhp, obj, offset, size, data.as_ptr() as *const c_char)
        };

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

    pub fn create_inode(&self, inode_type: InodeType, opid: u64) -> Result<u64> {
        let ino = Box::new(0_u64);
        let ino_ptr: *mut u64 = Box::into_raw(ino);

        let err = unsafe { sys::libuzfs_inode_create(self.dhp, ino_ptr, inode_type as u32, opid) };
        let ino = unsafe { Box::from_raw(ino_ptr) };

        if err == 0 {
            Ok(*ino)
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

    pub fn delete_inode(&self, ino: u64, inode_type: InodeType, opid: u64) -> Result<()> {
        let err = unsafe { sys::libuzfs_inode_delete(self.dhp, ino, inode_type as u32, opid) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_attr(&self, ino: u64, size: u64) -> Result<Vec<u8>> {
        let mut attr = Vec::<u8>::with_capacity(size as usize);
        attr.resize_with(size as usize, Default::default);
        let attr_ptr = attr.as_mut_ptr() as *mut c_void;

        let err = unsafe { sys::libuzfs_inode_getattr(self.dhp, ino, attr_ptr, size) };

        if err == 0 {
            Ok(attr)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn set_attr(&self, ino: u64, attr: &[u8], opid: u64) -> Result<()> {
        let size = attr.len() as u64;
        let attr_ptr = attr.as_ptr() as *mut c_void;

        let err = unsafe { sys::libuzfs_inode_setattr(self.dhp, ino, attr_ptr, size, opid) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn get_kvattr(&self, ino: u64, name: &[u8], size: u64, flags: i32) -> Result<Vec<u8>> {
        let mut data = Vec::<u8>::with_capacity(size as usize);
        data.resize_with(size as usize, Default::default);
        let data_ptr = data.as_mut_ptr() as *mut c_char;
        let name_ptr = name.as_ptr() as *mut c_char;

        let err = unsafe {
            sys::libuzfs_inode_get_kvattr(self.dhp, ino, name_ptr, data_ptr, size, flags)
        };

        if err == 0 {
            Ok(data)
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn set_kvattr(
        &self,
        ino: u64,
        name: &[u8],
        value: &[u8],
        opid: u64,
        flags: i32,
    ) -> Result<()> {
        let name_ptr = name.as_ptr() as *mut c_char;
        let value_ptr = value.as_ptr() as *mut c_char;
        let size = value.len() as u64;

        let err = unsafe {
            sys::libuzfs_inode_set_kvattr(self.dhp, ino, name_ptr, value_ptr, size, flags, opid)
        };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn remove_kvattr(&self, ino: u64, name: &[u8], opid: u64) -> Result<()> {
        let name_ptr = name.as_ptr() as *mut c_char;

        let err = unsafe { sys::libuzfs_inode_remove_kvattr(self.dhp, ino, name_ptr, opid) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn create_dentry(&self, pino: u64, name: &[u8], value: &[u64], opid: u64) -> Result<()> {
        let num = value.len() as u64;
        let name_ptr = name.as_ptr() as *mut c_char;
        let value_ptr = value.as_ptr() as *mut u64;

        let err =
            unsafe { sys::libuzfs_dentry_create(self.dhp, pino, name_ptr, value_ptr, num, opid) };

        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn delete_dentry(&self, pino: u64, name: &[u8], opid: u64) -> Result<()> {
        let err = unsafe {
            sys::libuzfs_dentry_delete(self.dhp, pino, name.as_ptr() as *mut c_char, opid)
        };

        if err == 0 {
            Ok(())
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

    pub fn get_max_synced_opid(&self) -> Result<u64> {
        let opid = unsafe { sys::libuzfs_get_max_synced_opid(self.dhp) };
        Ok(opid)
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

    struct Attr {
        ino: u64,
        nlink: u64,
    }

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
        let s = String::from("Hello uzfs!");
        let file_ino;
        let dir_ino;
        let num = 0u64;
        let mut opid = 0u64;
        let mut attr: Attr = Attr { ino: 0, nlink: 0 };
        let key = String::from("acl");
        let value = String::from("root,admin");
        let file_name = String::from("fileA");
        let dentry_data;

        setup(&devname, &poolname, &datasetname, &zpool_cache_path);

        {
            Uzfs::set_zpool_cache_path(&zpool_cache_path);
            let _uzfs = Uzfs::init().unwrap();
            let ds = Dataset::init(datasetname.clone()).unwrap();

            let num = ds.list_object().unwrap();
            opid += 1;
            rwobj = ds.create_object(opid).unwrap();
            ds.wait_synced().unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 1);

            let doi = ds.stat_object(rwobj).unwrap();
            Dataset::dump_object_doi(rwobj, doi);

            let data = s.as_bytes();
            let size = s.len() as u64;
            ds.write_object(rwobj, 0, size, data).unwrap();
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), data);

            opid += 1;
            file_ino = ds.create_inode(InodeType::FILE, opid).unwrap();
            opid += 1;
            dir_ino = ds.create_inode(InodeType::DIR, opid).unwrap();
            opid += 1;

            dentry_data = [file_ino, 1];

            ds.create_dentry(dir_ino, file_name.as_bytes(), &dentry_data, opid)
                .unwrap();
            let dentry_data_read = ds
                .lookup_dentry(dir_ino, file_name.as_bytes(), dentry_data.len() as u64)
                .unwrap();
            assert_eq!(dentry_data.to_vec(), dentry_data_read);
            ds.wait_synced().unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 3);

            attr.ino = file_ino;
            attr.nlink = 1;

            let attr_bytes = unsafe { any_as_u8_slice(&attr) };

            opid += 1;
            ds.set_attr(file_ino, attr_bytes, opid).unwrap();

            let attr_new = ds.get_attr(file_ino, attr_bytes.len() as u64).unwrap();
            assert_eq!(attr_new.as_slice(), attr_bytes);

            let attr_got =
                unsafe { transmute::<[u8; size_of::<Attr>()], Attr>(attr_new.try_into().unwrap()) };
            assert_eq!(attr_got.ino, attr.ino);
            assert_eq!(attr_got.nlink, attr.nlink);

            opid += 1;
            ds.set_kvattr(file_ino, key.as_bytes(), value.as_bytes(), opid, 0)
                .unwrap();

            let value_read = ds
                .get_kvattr(file_ino, key.as_bytes(), value.len() as u64, 0)
                .unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            assert_eq!(ds.list_object().unwrap(), num + 4);
            assert_eq!(ds.get_max_synced_opid().unwrap(), opid);
        }

        {
            Uzfs::set_zpool_cache_path(&zpool_cache_path);
            let _uzfs = Uzfs::init().unwrap();
            let ds = Dataset::init(datasetname.clone()).unwrap();

            assert_eq!(ds.get_max_synced_opid().unwrap(), opid);
            assert_eq!(ds.list_object().unwrap(), num + 4);

            let data = s.as_bytes();
            let size = s.len() as u64;
            assert_eq!(ds.read_object(rwobj, 0, size).unwrap(), data);

            opid += 1;
            ds.delete_object(rwobj, opid).unwrap();
            ds.wait_synced().unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 3);

            let dentry_data_read = ds
                .lookup_dentry(dir_ino, file_name.as_bytes(), dentry_data.len() as u64)
                .unwrap();
            assert_eq!(dentry_data.to_vec(), dentry_data_read);

            opid += 1;
            ds.delete_dentry(dir_ino, file_name.as_bytes(), opid)
                .unwrap();

            let attr_bytes = unsafe { any_as_u8_slice(&attr) };
            let attr_new = ds.get_attr(file_ino, attr_bytes.len() as u64).unwrap();
            assert_eq!(attr_new.as_slice(), attr_bytes);

            let value_read = ds
                .get_kvattr(file_ino, key.as_bytes(), value.len() as u64, 0)
                .unwrap();
            assert_eq!(value_read.as_slice(), value.as_bytes());

            opid += 1;
            ds.remove_kvattr(file_ino, key.as_bytes(), opid).unwrap();
            ds.wait_synced().unwrap();

            opid += 1;
            ds.delete_inode(dir_ino, InodeType::DIR, opid).unwrap();
            opid += 1;
            ds.delete_inode(file_ino, InodeType::FILE, opid).unwrap();
            ds.wait_synced().unwrap();

            assert_eq!(ds.list_object().unwrap(), num);
            assert_eq!(ds.get_max_synced_opid().unwrap(), opid);

            ds.claim_object(rwobj).unwrap();
            ds.wait_synced().unwrap();

            assert_eq!(ds.list_object().unwrap(), num + 1);
        }

        cleanup(&devname, &poolname, &datasetname, &zpool_cache_path);
    }
}
