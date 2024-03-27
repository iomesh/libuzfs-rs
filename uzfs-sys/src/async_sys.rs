use crate::bindings::*;
use crate::coroutine::*;
use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

const MAX_POOL_NAME_SIZE: i32 = 32;
const MAX_NAME_SIZE: usize = 256;
const MAX_KVATTR_VALUE_SIZE: usize = 8192;

unsafe extern "C" fn print_backtrace() {
    let mut depth = 0;
    backtrace::trace(|frame| {
        backtrace::resolve_frame(frame, |symbol| {
            let name = match symbol.name() {
                Some(name) => name.as_str().unwrap(),
                None => "",
            };

            let file_name = match symbol.filename() {
                Some(path) => path.to_str().unwrap(),
                None => "",
            };

            let line = symbol.lineno().unwrap_or(0);

            println!("#{depth}  {file_name}:{line}:{name}");
            depth += 1;
        });

        true // keep going to the next frame
    });
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_init_c(_: *mut c_void) {
    libuzfs_init(
        Some(thread_create),
        Some(thread_exit),
        Some(thread_join),
        Some(print_backtrace),
    );
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_fini_c(_: *mut c_void) {
    libuzfs_fini();
}

pub struct LibuzfsDatasetInitArg {
    pub dsname: *const c_char,
    pub dev_path: *const c_char,
    pub pool_name: *const c_char,
    pub dnodesize: u32,
    pub max_blksize: u32,

    pub ret: i32,
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub zhp: *mut libuzfs_zpool_handle_t,
}

unsafe impl Send for LibuzfsDatasetInitArg {}
unsafe impl Sync for LibuzfsDatasetInitArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_dataset_init_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDatasetInitArg);
    let mut stored_pool_name = vec![0_u8; MAX_POOL_NAME_SIZE as usize];
    arg.ret = libuzfs_zpool_import(
        arg.dev_path,
        stored_pool_name.as_mut_ptr() as *mut c_char,
        MAX_POOL_NAME_SIZE,
    );

    // import failed, create zpool if not found
    if arg.ret != 0 {
        match arg.ret {
            libc::ENOENT => {
                arg.ret = libuzfs_zpool_create(
                    arg.pool_name,
                    arg.dev_path,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
                if arg.ret == 0 {
                    arg.ret = libuzfs_dataset_create(arg.dsname);
                }

                if arg.ret != 0 {
                    return;
                }
                assert_ne!(arg.ret, libc::EEXIST);
            }
            _ => {
                return;
            }
        }
    } else {
        stored_pool_name.retain(|c| *c != 0);
        let stored_pool_name = std::str::from_utf8(&stored_pool_name).unwrap();
        let c_str = CStr::from_ptr(arg.pool_name);
        assert_eq!(c_str.to_str().unwrap(), stored_pool_name);
    }

    arg.zhp = libuzfs_zpool_open(arg.pool_name, &mut arg.ret);
    if !arg.zhp.is_null() {
        assert_eq!(arg.ret, 0);
        arg.dhp = libuzfs_dataset_open(arg.dsname, &mut arg.ret, arg.dnodesize, arg.max_blksize);
        if arg.dhp.is_null() && arg.ret == libc::ENOENT {
            arg.ret = libuzfs_dataset_create(arg.dsname);
            assert_ne!(arg.ret, libc::EEXIST);
            if arg.ret == 0 {
                arg.dhp =
                    libuzfs_dataset_open(arg.dsname, &mut arg.ret, arg.dnodesize, arg.max_blksize);
            }
        }

        if arg.ret == 0 {
            return;
        }
    }

    if !arg.dhp.is_null() {
        libuzfs_dataset_close(arg.dhp);
    }

    if !arg.zhp.is_null() {
        libuzfs_zpool_close(arg.zhp);
    }

    assert_eq!(libuzfs_zpool_export(arg.pool_name), 0);
}

pub struct LibuzfsDatasetFiniArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub zhp: *mut libuzfs_zpool_handle_t,
    pub poolname: *const c_char,

    pub err: i32,
}

unsafe impl Send for LibuzfsDatasetFiniArg {}
unsafe impl Sync for LibuzfsDatasetFiniArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_dataset_fini_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDatasetFiniArg);
    libuzfs_dataset_close(arg.dhp);
    libuzfs_zpool_close(arg.zhp);
    assert!(!arg.zhp.is_null());
    arg.err = libuzfs_zpool_export(arg.poolname);
}

pub struct LibuzfsDatasetExpandArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ret: i32,
}

unsafe impl Send for LibuzfsDatasetExpandArg {}
unsafe impl Sync for LibuzfsDatasetExpandArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_dataset_expand_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDatasetExpandArg);
    arg.ret = libuzfs_dataset_expand(arg.dhp);
}

pub struct LibuzfsZapListArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub err: i32,
    pub list: Vec<(String, Vec<u8>)>,
}

unsafe impl Send for LibuzfsZapListArg {}
unsafe impl Sync for LibuzfsZapListArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_zap_list_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsZapListArg);
    let iter = libuzfs_new_zap_iterator(arg.dhp, arg.obj, &mut arg.err);

    if iter.is_null() {
        if arg.err == libc::ENOENT {
            arg.err = 0;
        }
        return;
    }

    loop {
        let mut name = Vec::<u8>::with_capacity(MAX_NAME_SIZE + 1);
        let rc =
            libuzfs_zap_iterator_name(iter, name.as_mut_ptr() as *mut c_char, MAX_NAME_SIZE as u64);
        assert!(rc > 0);
        name.set_len(rc as usize);
        // make name end with '\0'
        name.push(0);
        let value_size = libuzfs_zap_iterator_value_size(iter);
        let mut value = Vec::<u8>::with_capacity(value_size as usize);
        arg.err = libuzfs_zap_lookup(
            arg.dhp,
            arg.obj,
            name.as_mut_ptr() as *mut c_char,
            1,
            value_size,
            value.as_mut_ptr() as *mut c_void,
        );
        value.set_len(value_size as usize);
        name.pop();

        if arg.err != 0 {
            break;
        }

        let name = String::from_utf8(name).unwrap();
        arg.list.push((name, value));

        arg.err = libuzfs_zap_iterator_advance(iter);
        if arg.err != 0 {
            if arg.err == libc::ENOENT {
                arg.err = 0;
            }
            break;
        }
    }

    libuzfs_zap_iterator_fini(iter);
}

pub struct LibuzfsZapUpdateArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub key: *const c_char,
    pub num_integers: u64,
    pub val: *const c_void,

    pub only_add: bool,

    pub txg: u64,
    pub err: i32,
}

unsafe impl Send for LibuzfsZapUpdateArg {}
unsafe impl Sync for LibuzfsZapUpdateArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_zap_update_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsZapUpdateArg);
    arg.err = if arg.only_add {
        libuzfs_zap_add(
            arg.dhp,
            arg.obj,
            arg.key,
            1,
            arg.num_integers,
            arg.val,
            &mut arg.txg as *mut u64,
        )
    } else {
        libuzfs_zap_update(
            arg.dhp,
            arg.obj,
            arg.key,
            1,
            arg.num_integers,
            arg.val,
            &mut arg.txg as *mut u64,
        )
    };
}

pub struct LibuzfsZapRemoveArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub key: *const c_char,
    pub obj: u64,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsZapRemoveArg {}
unsafe impl Sync for LibuzfsZapRemoveArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_zap_remove_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsZapRemoveArg);
    arg.err = libuzfs_zap_remove(arg.dhp, arg.obj, arg.key, &mut arg.txg);
}

pub struct LibuzfsCreateObjectsArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub num_objs: usize,

    pub err: i32,
    pub objs: Vec<u64>,
    pub gen: u64,
}

unsafe impl Send for LibuzfsCreateObjectsArg {}
unsafe impl Sync for LibuzfsCreateObjectsArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_objects_create_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsCreateObjectsArg);
    arg.objs = vec![0; arg.num_objs];
    arg.err = libuzfs_objects_create(
        arg.dhp,
        arg.objs.as_mut_ptr(),
        arg.num_objs as i32,
        &mut arg.gen,
    );
    libuzfs_wait_log_commit(arg.dhp);
}

pub struct LibuzfsDeleteObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsDeleteObjectArg {}
unsafe impl Sync for LibuzfsDeleteObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_delete_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDeleteObjectArg);
    arg.err = libuzfs_object_delete(arg.dhp, arg.obj);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_wait_log_commit_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_wait_log_commit(dhp);
}

pub struct LibuzfsGetObjectAttrArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,

    pub attr: uzfs_object_attr_t,
    pub err: i32,
}

unsafe impl Send for LibuzfsGetObjectAttrArg {}
unsafe impl Sync for LibuzfsGetObjectAttrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_get_object_attr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsGetObjectAttrArg);
    arg.err = libuzfs_object_get_attr(arg.dhp, arg.obj, &mut arg.attr as *mut uzfs_object_attr_t)
}

pub struct LibuzfsListObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,

    pub num_objs: u64,
}

unsafe impl Send for LibuzfsListObjectArg {}
unsafe impl Sync for LibuzfsListObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_list_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsListObjectArg);

    arg.num_objs = libuzfs_object_list(arg.dhp);
}

pub struct LibuzfsStatObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,

    pub doi: dmu_object_info_t,
    pub err: i32,
}

unsafe impl Send for LibuzfsStatObjectArg {}
unsafe impl Sync for LibuzfsStatObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_stat_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsStatObjectArg);
    arg.err = libuzfs_object_stat(arg.dhp, arg.obj, &mut arg.doi as *mut dmu_object_info_t);
}

pub struct LibuzfsReadObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub offset: u64,
    pub size: u64,

    pub err: i32,
    pub data: Vec<u8>,
}

unsafe impl Send for LibuzfsReadObjectArg {}
unsafe impl Sync for LibuzfsReadObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_read_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsReadObjectArg);

    let rc = libuzfs_object_read(
        arg.dhp,
        arg.obj,
        arg.offset,
        arg.size,
        arg.data.as_mut_ptr() as *mut c_char,
    );

    if rc >= 0 {
        arg.data.set_len(rc as usize);
        arg.err = 0;
    } else {
        arg.err = -rc;
    }
}

pub struct LibuzfsWriteObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub offset: u64,
    pub iovs: Vec<iovec>,
    pub sync: bool,

    pub err: i32,
}

unsafe impl Send for LibuzfsWriteObjectArg {}
unsafe impl Sync for LibuzfsWriteObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_write_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsWriteObjectArg);

    arg.err = libuzfs_object_write(
        arg.dhp,
        arg.obj,
        arg.offset,
        arg.iovs.as_mut_ptr(),
        arg.iovs.len() as i32,
        arg.sync as u32,
    );
}

pub struct LibuzfsSyncObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
}

unsafe impl Send for LibuzfsSyncObjectArg {}
unsafe impl Sync for LibuzfsSyncObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_sync_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsSyncObjectArg);
    libuzfs_object_sync(arg.dhp, arg.obj);
}

pub struct LibuzfsTruncateObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub offset: u64,
    pub size: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsTruncateObjectArg {}
unsafe impl Sync for LibuzfsTruncateObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_truncate_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsTruncateObjectArg);
    arg.err = libuzfs_object_truncate(arg.dhp, arg.obj, arg.offset, arg.size);
}

pub struct LibuzfsDatasetSpaceArg {
    pub dhp: *mut libuzfs_dataset_handle_t,

    pub refd_bytes: u64,
    pub avail_bytes: u64,
    pub used_objs: u64,
    pub avail_objs: u64,
}

unsafe impl Send for LibuzfsDatasetSpaceArg {}
unsafe impl Sync for LibuzfsDatasetSpaceArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_dataset_space_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDatasetSpaceArg);
    libuzfs_dataset_space(
        arg.dhp,
        &mut arg.refd_bytes,
        &mut arg.avail_bytes,
        &mut arg.used_objs,
        &mut arg.avail_objs,
    );
}

pub struct LibuzfsFindHoleArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub off: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsFindHoleArg {}
unsafe impl Sync for LibuzfsFindHoleArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_object_next_hole_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsFindHoleArg);
    arg.err = libuzfs_object_next_hole(arg.dhp, arg.obj, &mut arg.off);
}

pub struct LibuzfsCreateInode {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub inode_type: libuzfs_inode_type_t,

    pub ino: u64,
    pub txg: u64,
    pub err: i32,
}

unsafe impl Send for LibuzfsCreateInode {}
unsafe impl Sync for LibuzfsCreateInode {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_create_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsCreateInode);
    arg.err = libuzfs_inode_create(arg.dhp, &mut arg.ino, arg.inode_type, &mut arg.txg);
}

pub struct LibuzfsClaimInodeArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub inode_type: libuzfs_inode_type_t,
    pub ino: u64,
    pub gen: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsClaimInodeArg {}
unsafe impl Sync for LibuzfsClaimInodeArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_claim_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsClaimInodeArg);
    arg.err = libuzfs_inode_claim(arg.dhp, arg.ino, arg.gen, arg.inode_type);
}

pub struct LibuzfsDeleteInode {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub inode_type: libuzfs_inode_type_t,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsDeleteInode {}
unsafe impl Sync for LibuzfsDeleteInode {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_delete_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDeleteInode);
    arg.err = libuzfs_inode_delete(arg.dhp, arg.ino, arg.inode_type, &mut arg.txg);
}

pub struct LibuzfsGetAttrArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub reserved: *mut i8,
    pub size: i32,

    pub attr: uzfs_inode_attr_t,
    pub err: i32,
}

unsafe impl Send for LibuzfsGetAttrArg {}
unsafe impl Sync for LibuzfsGetAttrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_inode_getattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsGetAttrArg);
    arg.err = libuzfs_inode_getattr(
        arg.dhp,
        arg.ino,
        &mut arg.attr as *mut uzfs_inode_attr_t,
        arg.reserved,
        &mut arg.size as *mut i32,
    );
}

pub struct LibuzfsSetAttrArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub reserved: *const i8,
    pub size: u32,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsSetAttrArg {}
unsafe impl Sync for LibuzfsSetAttrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_set_attr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsSetAttrArg);
    arg.err = libuzfs_inode_setattr(arg.dhp, arg.ino, arg.reserved, arg.size, &mut arg.txg);
}

pub struct LibuzfsGetKvattrArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub name: *const c_char,

    pub data: Vec<u8>,
    pub err: i32,
}

unsafe impl Send for LibuzfsGetKvattrArg {}
unsafe impl Sync for LibuzfsGetKvattrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_inode_get_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsGetKvattrArg);
    arg.data = Vec::<u8>::with_capacity(MAX_KVATTR_VALUE_SIZE);
    let rc = libuzfs_inode_get_kvattr(
        arg.dhp,
        arg.ino,
        arg.name,
        arg.data.as_mut_ptr() as *mut i8,
        MAX_KVATTR_VALUE_SIZE as u64,
    );

    if rc < 0 {
        arg.err = -rc as i32;
    } else {
        arg.err = 0;
        arg.data.set_len(rc as usize);
    }
}

pub struct LibuzfsSetKvAttrArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub name: *const c_char,
    pub value: *const c_char,
    pub size: u64,
    pub option: u32,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsSetKvAttrArg {}
unsafe impl Sync for LibuzfsSetKvAttrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_set_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsSetKvAttrArg);
    arg.err = libuzfs_inode_set_kvattr(
        arg.dhp,
        arg.ino,
        arg.name,
        arg.value,
        arg.size,
        &mut arg.txg,
        arg.option,
    );
}

pub struct LibuzfsRemoveKvattrArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub name: *const c_char,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsRemoveKvattrArg {}
unsafe impl Sync for LibuzfsRemoveKvattrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_remove_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsRemoveKvattrArg);

    arg.err = libuzfs_inode_remove_kvattr(arg.dhp, arg.ino, arg.name, &mut arg.txg);
}

pub struct LibuzfsListKvAttrsArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,

    pub err: i32,
    pub names: Vec<String>,
}

unsafe impl Send for LibuzfsListKvAttrsArg {}
unsafe impl Sync for LibuzfsListKvAttrsArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_list_kvattrs_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsListKvAttrsArg);
    let iter = libuzfs_new_kvattr_iterator(arg.dhp, arg.ino, &mut arg.err);
    if iter.is_null() {
        return;
    }

    loop {
        let mut buf = Vec::<u8>::with_capacity(MAX_NAME_SIZE);
        let rc =
            libuzfs_next_kvattr_name(iter, buf.as_mut_ptr() as *mut c_char, MAX_NAME_SIZE as i32);
        assert!(rc >= 0);
        if rc == 0 {
            break;
        }
        buf.set_len(rc as usize);
        arg.names.push(String::from_utf8(buf).unwrap());
    }

    libuzfs_kvattr_iterator_fini(iter);
}

pub struct LibuzfsCreateDentryArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub pino: u64,
    pub name: *const c_char,
    pub ino: u64,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsCreateDentryArg {}
unsafe impl Sync for LibuzfsCreateDentryArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_create_dentry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsCreateDentryArg);

    arg.err = libuzfs_dentry_create(arg.dhp, arg.pino, arg.name, arg.ino, &mut arg.txg);
}

pub struct LibuzfsDeleteDentryArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub pino: u64,
    pub name: *const c_char,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsDeleteDentryArg {}
unsafe impl Sync for LibuzfsDeleteDentryArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_delete_entry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDeleteDentryArg);

    arg.err = libuzfs_dentry_delete(arg.dhp, arg.pino, arg.name, &mut arg.txg);
}

pub struct LibuzfsLookupDentryArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub pino: u64,
    pub name: *const c_char,

    pub ino: u64,
    pub err: i32,
}

unsafe impl Send for LibuzfsLookupDentryArg {}
unsafe impl Sync for LibuzfsLookupDentryArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_lookup_dentry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsLookupDentryArg);
    arg.err = libuzfs_dentry_lookup(arg.dhp, arg.pino, arg.name, &mut arg.ino);
}

pub struct LibuzfsIterateDentryArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub pino: u64,
    pub whence: u64,
    pub size: u32,

    pub err: i32,
    pub data: Vec<u8>,
    pub num: u32,
}

unsafe impl Send for LibuzfsIterateDentryArg {}
unsafe impl Sync for LibuzfsIterateDentryArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_iterate_dentry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsIterateDentryArg);

    arg.data.reserve(arg.size as usize);

    arg.err = libuzfs_dentry_iterate(
        arg.dhp,
        arg.pino,
        arg.whence,
        arg.size,
        arg.data.as_mut_ptr() as *mut c_char,
        &mut arg.num,
    );

    arg.data.set_len(arg.size as usize);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_wait_synced_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_wait_synced(dhp);
}

pub struct LibuzfsInodeCheckValidArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub gen: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsInodeCheckValidArg {}
unsafe impl Sync for LibuzfsInodeCheckValidArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_inode_check_valid_c(arg: *mut c_void) {
    let arg = (arg as *mut LibuzfsInodeCheckValidArg).as_mut().unwrap();
    arg.err = libuzfs_inode_check_valid(arg.dhp, arg.ino, arg.gen);
}
