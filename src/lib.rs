use cstr_argument::CStrArgument;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::{io, ptr};
use uzfs_sys as sys;

use io::Result;

pub struct Uzfs {
    i: PhantomData<()>,
}

impl Uzfs {
    pub fn new() -> Result<Self> {
        unsafe {
            sys::libuzfs_init();
        };
        Ok(Self { i: PhantomData })
    }

    pub fn set_zpool_cache_path<P: CStrArgument>(&self, path: P) {
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

    pub fn create_dataset<P: CStrArgument>(&mut self, dsname: P) -> Result<()> {
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

    pub fn get_dataset<P: CStrArgument>(&mut self, dsname: P) -> Result<Dataset> {
        let dsname = dsname.into_cstr();
        let dhp = unsafe { sys::libuzfs_dataset_open(dsname.as_ref().as_ptr()) };

        if dhp.is_null() {
            Err(io::Error::from(io::ErrorKind::InvalidInput))
        } else {
            Dataset::new(dhp)
        }
    }
}

impl Drop for Uzfs {
    fn drop(&mut self) {
        unsafe { sys::libuzfs_fini() }
    }
}

unsafe impl Send for Uzfs {}
unsafe impl Sync for Uzfs {}

pub struct Dataset {
    dhp: *mut sys::libuzfs_dataset_handle_t,
}

impl Dataset {
    pub fn new(dhp: *mut sys::libuzfs_dataset_handle_t) -> Result<Self> {
        Ok(Self { dhp })
    }
}

impl Dataset {
    pub fn create_object(&self) -> Result<u64> {
        let obj = Box::new(0_u64);
        let obj_ptr: *mut u64 = Box::into_raw(obj);
        let err = unsafe { sys::libuzfs_object_create(self.dhp, obj_ptr) };
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

    pub fn delete_object(&self, obj: u64) -> Result<()> {
        let err = unsafe { sys::libuzfs_object_delete(self.dhp, obj) };
        if err == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err))
        }
    }

    pub fn list_object(&self) -> Result<i32> {
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
}

impl Drop for Dataset {
    fn drop(&mut self) {
        unsafe { sys::libuzfs_dataset_close(self.dhp) };
        self.dhp = ptr::null_mut();
    }
}

unsafe impl Send for Dataset {}
unsafe impl Sync for Dataset {}

#[cfg(test)]
mod tests {

    use super::Dataset;
    use super::Uzfs;
    use std::fs::{self, File};

    #[test]
    fn uzfs_test() {
        let dev_name = String::from("/tmp/uzfs-test.img");
        let pool_name = String::from("uzfs-test-pool");
        let zpool_cache_path = String::from("/tmp/zpool.cache");

        let dev_file = File::create(dev_name.clone()).unwrap();
        dev_file.set_len(100 * 1024 * 1024).unwrap();

        let mut uzfs = Uzfs::new().unwrap();
        uzfs.set_zpool_cache_path(zpool_cache_path);
        uzfs.create_zpool(pool_name.clone(), dev_name.clone())
            .unwrap();

        let dsname = pool_name.clone() + "/uzfs";
        uzfs.create_dataset(dsname.clone()).unwrap();

        {
            let ds = uzfs.get_dataset(dsname.clone()).unwrap();
            let num1 = ds.list_object().unwrap();
            let obj = ds.create_object().unwrap();
            let num2 = ds.list_object().unwrap();
            assert_eq!(num1 + 1, num2);
            let doi = ds.stat_object(obj).unwrap();
            Dataset::dump_object_doi(obj, doi);

            let s = String::from("Hello uzfs!");
            let data = s.as_bytes();
            let size = s.len() as u64;
            ds.write_object(obj, 0, size, data).unwrap();
            assert_eq!(ds.read_object(obj, 0, size).unwrap(), data);
            ds.delete_object(obj).unwrap();
            let num3 = ds.list_object().unwrap();
            assert_eq!(num1, num3);
            ds.claim_object(obj).unwrap();
            assert_eq!(num1 + 1, num2);
        }

        uzfs.destroy_dataset(dsname);
        uzfs.destroy_zpool(pool_name.clone());
        fs::remove_file(dev_name).unwrap();
    }
}