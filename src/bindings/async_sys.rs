use std::ffi::CStr;
use std::mem::size_of;
use std::os::raw::{c_char, c_void};

use super::sys::*;
use crate::context::coroutine_c::*;
use crate::context::taskq;
use crate::io::async_io_c::*;
use crate::metrics;
use crate::metrics::stats::*;
use crate::sync::sync_c::*;
use crate::UzfsDentry;

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

pub(crate) unsafe fn set_libuzfs_ops(log_func: Option<unsafe extern "C" fn(*const c_char, i32)>) {
    let co_ops = coroutine_ops_t {
        coroutine_key_create: Some(co_create_key),
        coroutine_getkey: Some(co_get_key),
        coroutine_setkey: Some(co_set_key),
        uzfs_coroutine_self: Some(co_self),
        coroutine_sched_yield: Some(co_sched_yield),
        coroutine_sleep: Some(co_sleep),
    };

    let mutex_ops = co_mutex_ops {
        co_mutex_held: Some(co_mutex_held),
        co_mutex_init: Some(co_mutex_init),
        co_mutex_destroy: Some(co_mutex_destroy),
        co_mutex_lock: Some(co_mutex_lock),
        co_mutex_trylock: Some(co_mutex_trylock),
        co_mutex_unlock: Some(co_mutex_unlock),
    };

    let cond_ops = co_cond_ops {
        co_cond_init: Some(co_cond_init),
        co_cond_destroy: Some(co_cond_destroy),
        co_cond_wait: Some(co_cond_wait),
        co_cond_timedwait: Some(co_cond_timedwait),
        co_cond_signal: Some(co_cond_signal),
        co_cond_broadcast: Some(co_cond_broadcast),
    };

    let rwlock_ops = co_rwlock_ops {
        co_rw_lock_read_held: Some(co_rwlock_read_held),
        co_rw_lock_write_held: Some(co_rwlock_write_held),
        co_rw_lock_init: Some(co_rwlock_init),
        co_rw_lock_destroy: Some(co_rwlock_destroy),
        co_rw_lock_read: Some(co_rw_lock_read),
        co_rw_lock_write: Some(co_rw_lock_write),
        co_rw_lock_try_read: Some(co_rwlock_try_read),
        co_rw_lock_try_write: Some(co_rwlock_try_write),
        co_rw_lock_exit: Some(co_rw_unlock),
    };

    let aio_ops = aio_ops {
        register_aio_fd: Some(register_fd),
        unregister_aio_fd: Some(unregister_fd),
        submit_aio_read: Some(submit_read),
        submit_aio_write: Some(submit_write),
        submit_aio_fsync: Some(submit_fsync),
    };

    let thread_ops = thread_ops {
        uthread_create: Some(thread_create),
        uthread_exit: Some(thread_exit),
        uthread_join: Some(thread_join),
    };

    let taskq_ops = taskq_ops {
        taskq_create: Some(taskq::taskq_create),
        taskq_dispatch: Some(taskq::taskq_dispatch),
        taskq_delay_dispatch: Some(taskq::taskq_delay_dispatch),
        taskq_member: Some(taskq::taskq_is_member),
        taskq_of_curthread: Some(taskq::taskq_of_curthread),
        taskq_wait: Some(taskq::taskq_wait),
        taskq_destroy: Some(taskq::taskq_destroy),
        taskq_wait_id: Some(taskq::taskq_wait_id),
        taskq_cancel_id: Some(taskq::taskq_cancel_id),
        taskq_is_empty: Some(taskq::taskq_is_empty),
        taskq_nalloc: Some(taskq::taskq_nalloc),
    };

    let stat_ops = stat_ops {
        print_log: log_func,
        kstat_install: Some(install_stat),
        kstat_uinstall: Some(uninstall_stat),
        backtrace: Some(print_backtrace),
        record_txg_delays: Some(metrics::record_txg_delay),
        record_zio: Some(metrics::record_zio),
    };

    unsafe {
        libuzfs_set_ops(
            &co_ops,
            &mutex_ops,
            &cond_ops,
            &rwlock_ops,
            &aio_ops,
            &thread_ops,
            &taskq_ops,
            &stat_ops,
        )
    };
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_init_c(_: *mut c_void) {
    libuzfs_init();
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
    pub already_formatted: bool,
    pub metrics: *const c_void,
    pub enable_autotrim: bool,

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

    if arg.ret == 0 {
        stored_pool_name.retain(|c| *c != 0);
        let stored_pool_name = std::str::from_utf8(&stored_pool_name).unwrap();
        let c_str = CStr::from_ptr(arg.pool_name);
        assert_eq!(c_str.to_str().unwrap(), stored_pool_name);
    }

    // only when already_formatted is false and no labels on disk can we format
    // this device, this is for data integrety
    if arg.ret == libc::ENOENT && !arg.already_formatted {
        arg.ret = libuzfs_zpool_create(arg.pool_name, arg.dev_path);
        if arg.ret == 0 {
            arg.ret = libuzfs_dataset_create(arg.dsname);
        }
        assert_ne!(arg.ret, libc::EEXIST, "{:?}", CStr::from_ptr(arg.pool_name));
    }

    if arg.ret != 0 {
        return;
    }

    arg.zhp = libuzfs_zpool_open(arg.pool_name, &mut arg.ret, arg.enable_autotrim as u32);
    if !arg.zhp.is_null() {
        assert_eq!(arg.ret, 0);
        arg.dhp = libuzfs_dataset_open(
            arg.dsname,
            &mut arg.ret,
            arg.dnodesize,
            arg.max_blksize,
            arg.metrics,
        );
        if arg.dhp.is_null() && arg.ret == libc::ENOENT && !arg.already_formatted {
            arg.ret = libuzfs_dataset_create(arg.dsname);
            assert_ne!(arg.ret, libc::EEXIST);
            if arg.ret == 0 {
                arg.dhp = libuzfs_dataset_open(
                    arg.dsname,
                    &mut arg.ret,
                    arg.dnodesize,
                    arg.max_blksize,
                    arg.metrics,
                );
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

pub struct LibuzfsDatasetTrimArgs {
    pub dhp: *mut libuzfs_dataset_handle_t,

    pub err: i32,
}

unsafe impl Send for LibuzfsDatasetTrimArgs {}
unsafe impl Sync for LibuzfsDatasetTrimArgs {}

pub unsafe extern "C" fn libuzfs_dataset_start_manual_trim_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDatasetTrimArgs);
    arg.err = libuzfs_start_manual_trim(arg.dhp);
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
    if !arg.zhp.is_null() {
        libuzfs_zpool_close(arg.zhp);
        assert!(!arg.zhp.is_null());
        arg.err = libuzfs_zpool_export(arg.poolname);
    }
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
    pub limit: usize,

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
        let rc = libuzfs_zap_iterator_name(iter, name.as_mut_ptr() as *mut c_char, MAX_NAME_SIZE);
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
            value_size as u64,
            value.as_mut_ptr() as *mut c_void,
        );
        value.set_len(value_size as usize);
        name.pop();

        if arg.err != 0 {
            break;
        }

        let name = String::from_utf8(name).unwrap();
        arg.list.push((name, value));

        if arg.list.len() >= arg.limit {
            break;
        }

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
    pub ihp: *mut libuzfs_inode_handle_t,

    pub err: i32,
}

unsafe impl Send for LibuzfsDeleteObjectArg {}
unsafe impl Sync for LibuzfsDeleteObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_delete_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDeleteObjectArg);
    arg.err = libuzfs_object_delete(arg.ihp);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_wait_log_commit_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_wait_log_commit(dhp);
}

pub struct LibuzfsGetObjectAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,

    pub attr: uzfs_object_attr_t,
    pub err: i32,
}

unsafe impl Send for LibuzfsGetObjectAttrArg {}
unsafe impl Sync for LibuzfsGetObjectAttrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_get_object_attr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsGetObjectAttrArg);
    arg.err = libuzfs_object_get_attr(arg.ihp, &mut arg.attr as *mut uzfs_object_attr_t)
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
    pub ihp: *mut libuzfs_inode_handle_t,
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
        arg.ihp,
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
    pub ihp: *mut libuzfs_inode_handle_t,
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
        arg.ihp,
        arg.offset,
        arg.iovs.as_mut_ptr(),
        arg.iovs.len() as i32,
        arg.sync as u32,
    );
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_sync_object_c(arg: *mut c_void) {
    libuzfs_object_sync(arg as *mut libuzfs_inode_handle_t);
}

pub struct LibuzfsTruncateObjectArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub offset: u64,
    pub size: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsTruncateObjectArg {}
unsafe impl Sync for LibuzfsTruncateObjectArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_truncate_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsTruncateObjectArg);
    arg.err = libuzfs_object_truncate(arg.ihp, arg.offset, arg.size);
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
    pub ihp: *mut libuzfs_inode_handle_t,
    pub off: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsFindHoleArg {}
unsafe impl Sync for LibuzfsFindHoleArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_object_next_hole_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsFindHoleArg);
    arg.err = libuzfs_object_next_hole(arg.ihp, &mut arg.off);
}

pub struct LibuzfsNextBlockArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub off: u64,
    pub size: u64,

    pub err: i32,
}

unsafe impl Send for LibuzfsNextBlockArg {}
unsafe impl Sync for LibuzfsNextBlockArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_object_next_block_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsNextBlockArg);
    arg.err = libuzfs_object_next_block(arg.ihp, &mut arg.off, &mut arg.size);
}

pub struct LibuzfsCreateInode {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub inode_type: libuzfs_inode_type_t,

    pub ihp: *mut libuzfs_inode_handle_t,
    pub ino: u64,
    pub txg: u64,
    pub err: i32,
}

unsafe impl Send for LibuzfsCreateInode {}
unsafe impl Sync for LibuzfsCreateInode {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_create_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsCreateInode);
    arg.err = libuzfs_inode_create(
        arg.dhp,
        &mut arg.ino,
        arg.inode_type,
        &mut arg.ihp,
        &mut arg.txg,
    );
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
    pub ihp: *mut libuzfs_inode_handle_t,
    pub inode_type: libuzfs_inode_type_t,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsDeleteInode {}
unsafe impl Sync for LibuzfsDeleteInode {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_delete_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDeleteInode);
    arg.err = libuzfs_inode_delete(arg.ihp, arg.inode_type, &mut arg.txg);
}

pub struct LibuzfsGetAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
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
        arg.ihp,
        &mut arg.attr as *mut uzfs_inode_attr_t,
        arg.reserved,
        &mut arg.size as *mut i32,
    );
}

pub struct LibuzfsSetAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
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
    arg.err = libuzfs_inode_setattr(arg.ihp, arg.reserved, arg.size, &mut arg.txg);
}

pub struct LibuzfsGetKvattrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
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
        arg.ihp,
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
    pub ihp: *mut libuzfs_inode_handle_t,
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
        arg.ihp,
        arg.name,
        arg.value,
        arg.size,
        &mut arg.txg,
        arg.option,
    );
}

pub struct LibuzfsRemoveKvattrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsRemoveKvattrArg {}
unsafe impl Sync for LibuzfsRemoveKvattrArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_remove_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsRemoveKvattrArg);

    arg.err = libuzfs_inode_remove_kvattr(arg.ihp, arg.name, &mut arg.txg);
}

pub struct LibuzfsListKvAttrsArg {
    pub ihp: *mut libuzfs_inode_handle_t,

    pub err: i32,
    pub names: Vec<String>,
}

unsafe impl Send for LibuzfsListKvAttrsArg {}
unsafe impl Sync for LibuzfsListKvAttrsArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_list_kvattrs_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsListKvAttrsArg);
    let iter = libuzfs_new_kvattr_iterator(arg.ihp, &mut arg.err);
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
    pub dihp: *mut libuzfs_inode_handle_t,
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

    arg.err = libuzfs_dentry_create(arg.dihp, arg.name, arg.ino, &mut arg.txg);
}

pub struct LibuzfsDeleteDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for LibuzfsDeleteDentryArg {}
unsafe impl Sync for LibuzfsDeleteDentryArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_delete_entry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsDeleteDentryArg);

    arg.err = libuzfs_dentry_delete(arg.dihp, arg.name, &mut arg.txg);
}

pub struct LibuzfsLookupDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub ino: u64,
    pub err: i32,
}

unsafe impl Send for LibuzfsLookupDentryArg {}
unsafe impl Sync for LibuzfsLookupDentryArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_lookup_dentry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsLookupDentryArg);
    arg.err = libuzfs_dentry_lookup(arg.dihp, arg.name, &mut arg.ino);
}

pub struct LibuzfsIterateDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub whence: u64,
    pub size: u32,

    pub err: i32,
    pub done: bool,
    pub dentries: Vec<UzfsDentry>,
}

unsafe impl Send for LibuzfsIterateDentryArg {}
unsafe impl Sync for LibuzfsIterateDentryArg {}

const DEFAULT_NDENTRIES: usize = 128;

unsafe extern "C" fn dir_emit(
    arg: *mut c_void,
    whence: u64,
    name: *const c_char,
    value: u64,
) -> i32 {
    let arg = &mut *(arg as *mut LibuzfsIterateDentryArg);
    let size = (libc::strlen(name) + size_of::<u64>() * 2) as u32;
    if arg.size < size {
        return 1;
    }

    arg.size -= size;

    let name = CStr::from_ptr(name).to_owned();
    arg.dentries.push(UzfsDentry {
        whence,
        name,
        value,
    });

    0
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_iterate_dentry_c(arg: *mut c_void) {
    let arg_ptr = arg;
    let arg = &mut *(arg as *mut LibuzfsIterateDentryArg);

    arg.dentries.reserve(DEFAULT_NDENTRIES);

    arg.err = libuzfs_dentry_iterate(arg.dihp, arg.whence, arg_ptr, Some(dir_emit));

    if arg.err == libc::ENOENT {
        arg.done = true;
        arg.err = 0;
    }
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_wait_synced_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_wait_synced(dhp);
}

pub struct LibuzfsObjectSetMtimeArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub tv_sec: i64,
    pub tv_nsec: i64,

    pub err: i32,
}

unsafe impl Send for LibuzfsObjectSetMtimeArg {}
unsafe impl Sync for LibuzfsObjectSetMtimeArg {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_object_set_mtime(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsObjectSetMtimeArg);
    let mtime = timespec {
        tv_sec: arg.tv_sec,
        tv_nsec: arg.tv_nsec,
    };
    arg.err = libuzfs_object_setmtime(arg.ihp, &mtime, false as u32);
}

pub struct LibuzfsInodeHandleGetArgs {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub gen: u64,
    pub is_data_inode: bool,

    pub ihp: *mut libuzfs_inode_handle_t,
    pub err: i32,
}

unsafe impl Send for LibuzfsInodeHandleGetArgs {}
unsafe impl Sync for LibuzfsInodeHandleGetArgs {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_inode_handle_get_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsInodeHandleGetArgs);
    arg.err = libuzfs_inode_handle_get(
        arg.dhp,
        arg.is_data_inode as u32,
        arg.ino,
        arg.gen,
        &mut arg.ihp,
    );
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_inode_handle_rele_c(arg: *mut c_void) {
    let arg = arg as *mut libuzfs_inode_handle_t;
    libuzfs_inode_handle_rele(arg);
}

pub struct LibuzfsDebugArgs {
    pub argv: *mut *mut c_char,
    pub argc: i32,
}

unsafe impl Send for LibuzfsDebugArgs {}
unsafe impl Sync for LibuzfsDebugArgs {}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_debug_main_c(arg: *mut c_void) {
    let arg = &*(arg as *mut LibuzfsDebugArgs);
    libuzfs_debug_main(arg.argc, arg.argv);
}

pub struct LibuzfsShowStatsArgs {
    pub stat_ptr: *mut c_void,
    pub stat_type: i32,

    pub formatted_strings: Vec<Vec<u8>>,
}

unsafe impl Send for LibuzfsShowStatsArgs {}
unsafe impl Sync for LibuzfsShowStatsArgs {}

unsafe extern "C" fn generate(arg: *mut c_void, sf: *mut seq_file) {
    let buf_size = 1024;
    let arg = &mut *(arg as *mut Vec<Vec<u8>>);
    arg.push(Vec::with_capacity(buf_size));
    let last = arg.last_mut().unwrap();
    last.resize(buf_size, 0);
    *sf = seq_file {
        buf: last.as_ptr() as *mut _,
        size: buf_size as i32,
    };
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_show_stats_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LibuzfsShowStatsArgs);
    let generator = seq_file_generator {
        generate: Some(generate),
        arg: &mut arg.formatted_strings as *mut _ as *mut _,
    };

    libuzfs_show_stats(arg.stat_ptr, arg.stat_type, &generator);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn libuzfs_wakeup_arc_evictor_c(_arg: *mut c_void) {
    libuzfs_wakeup_arc_evictor();
}
