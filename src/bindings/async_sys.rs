use std::collections::HashMap;
use std::ffi::{CStr, CString};
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
        submit_aio: Some(submit_aio),
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

pub unsafe extern "C" fn libuzfs_init_c(_: *mut c_void) {
    libuzfs_init();
}

pub unsafe extern "C" fn libuzfs_fini_c(_: *mut c_void) {
    libuzfs_fini();
}

pub struct ZpoolOpenArg {
    pub pool_name: *const c_char,
    pub dev_paths: Vec<*const c_char>,
    pub metrics: *const c_void,
    pub enable_autotrim: bool,
    pub create: bool,

    pub res: i32,
    pub zhp: *mut libuzfs_zpool_handle_t,
}

unsafe impl Send for ZpoolOpenArg {}
unsafe impl Sync for ZpoolOpenArg {}

pub unsafe extern "C" fn libuzfs_zpool_open_c(args: *mut c_void) {
    let args = &mut *(args as *mut ZpoolOpenArg);
    args.res = libuzfs_zpool_open(
        args.dev_paths.as_ptr(),
        args.dev_paths.len() as u32,
        args.pool_name,
        &mut args.zhp,
        args.create as u32,
        args.enable_autotrim as u32,
        args.metrics,
    );
}

pub struct ManualTrimArg {
    pub zhp: *mut libuzfs_zpool_handle_t,

    pub res: i32,
}

unsafe impl Send for ManualTrimArg {}
unsafe impl Sync for ManualTrimArg {}

pub unsafe extern "C" fn libuzfs_start_manual_trim_c(args: *mut c_void) {
    let args = &mut *(args as *mut ManualTrimArg);
    args.res = libuzfs_zpool_start_trim(args.zhp);
}

pub unsafe extern "C" fn libuzfs_zpool_close_c(arg: *mut c_void) {
    libuzfs_zpool_close(arg as *mut libuzfs_zpool_handle_t);
}

pub struct ZpoolExpandArg {
    pub zhp: *mut libuzfs_zpool_handle_t,
    pub dev_path: *const c_char,

    pub res: i32,
}

unsafe impl Send for ZpoolExpandArg {}
unsafe impl Sync for ZpoolExpandArg {}

pub unsafe extern "C" fn libuzfs_zpool_expand_c(args: *mut c_void) {
    let args = &mut *(args as *mut ZpoolExpandArg);
    args.res = libuzfs_zpool_expand_vdev(args.zhp, args.dev_path);
}

pub struct ZpoolAddDevArg {
    pub zhp: *mut libuzfs_zpool_handle_t,
    pub dev_path: *const c_char,

    pub res: i32,
}

unsafe impl Send for ZpoolAddDevArg {}
unsafe impl Sync for ZpoolAddDevArg {}

pub unsafe extern "C" fn libuzfs_zpool_add_dev_c(args: *mut c_void) {
    let args = &mut *(args as *mut ZpoolAddDevArg);
    args.res = libuzfs_zpool_dev_add(args.zhp, args.dev_path);
}

pub struct DatasetListArgs {
    pub zhp: *mut libuzfs_zpool_handle_t,
    pub dnodesize: u32,
    pub max_blksize: u32,

    pub dsnames: Vec<CString>,
    pub dhps: HashMap<String, *mut libuzfs_dataset_handle_t>,
    pub err: i32,
}

unsafe impl Send for DatasetListArgs {}
unsafe impl Sync for DatasetListArgs {}

unsafe extern "C" fn ds_emit(args: *mut c_void, dsname: *const c_char) {
    let args = &mut *(args as *mut DatasetListArgs);

    // Ignore hidden ($FREE, $MOS & $ORIGIN) objsets.
    if *dsname == '$' as c_char {
        return;
    }

    let dsname = CStr::from_ptr(dsname).to_owned();
    args.dsnames.push(dsname);
}

pub unsafe extern "C" fn libuzfs_dataset_list_c(args_void: *mut c_void) {
    let args = &mut *(args_void as *mut DatasetListArgs);
    args.err = libuzfs_dataset_iterate(args.zhp, args_void, Some(ds_emit));
    if args.err != 0 {
        return;
    }

    for dsname in &args.dsnames {
        let dhp = libuzfs_dataset_open(
            args.zhp,
            dsname.as_ptr(),
            &mut args.err,
            args.dnodesize,
            args.max_blksize,
            false as boolean_t,
        );

        assert!(args.err != libc::ENOENT);
        if args.err != 0 {
            break;
        }

        let replaced = args.dhps.insert(dsname.to_str().unwrap().to_owned(), dhp);
        assert!(replaced.is_none());
    }

    if args.err != 0 {
        for dhp in args.dhps.values() {
            libuzfs_dataset_close(*dhp);
        }
    }
}

pub struct DatasetOpenArgs {
    pub zhp: *mut libuzfs_zpool_handle_t,
    pub dsname: *const c_char,
    pub dnode_size: u32,
    pub max_blksize: u32,
    pub create: bool,

    pub res: i32,
    pub dhp: *mut libuzfs_dataset_handle_t,
}

unsafe impl Send for DatasetOpenArgs {}
unsafe impl Sync for DatasetOpenArgs {}

pub unsafe extern "C" fn libuzfs_dataset_open_c(args: *mut c_void) {
    let args = &mut *(args as *mut DatasetOpenArgs);
    args.dhp = libuzfs_dataset_open(
        args.zhp,
        args.dsname,
        &mut args.res,
        args.dnode_size,
        args.max_blksize,
        args.create as u32,
    );
}

pub unsafe extern "C" fn libuzfs_dataset_close_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_dataset_close(dhp);
}

pub struct DatasetDestroyArgs {
    pub zhp: *mut libuzfs_zpool_handle_t,
    pub dsname: *const c_char,

    pub err: i32,
}

pub unsafe extern "C" fn libuzfs_dataset_destroy_c(args: *mut c_void) {
    let args = &mut *(args as *mut DatasetDestroyArgs);
    args.err = libuzfs_dataset_destroy(args.zhp, args.dsname);
}

pub struct ZapListArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub limit: usize,

    pub err: i32,
    pub list: Vec<(String, Vec<u8>)>,
}

unsafe impl Send for ZapListArg {}
unsafe impl Sync for ZapListArg {}

pub unsafe extern "C" fn libuzfs_zap_list_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ZapListArg);
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

pub struct ZapUpdateArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,
    pub key: *const c_char,
    pub num_integers: u64,
    pub val: *const c_void,

    pub only_add: bool,

    pub txg: u64,
    pub err: i32,
}

unsafe impl Send for ZapUpdateArg {}
unsafe impl Sync for ZapUpdateArg {}

pub unsafe extern "C" fn libuzfs_zap_update_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ZapUpdateArg);
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

pub struct ZapRemoveArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub key: *const c_char,
    pub obj: u64,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for ZapRemoveArg {}
unsafe impl Sync for ZapRemoveArg {}

pub unsafe extern "C" fn libuzfs_zap_remove_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ZapRemoveArg);
    arg.err = libuzfs_zap_remove(arg.dhp, arg.obj, arg.key, &mut arg.txg);
}

pub struct CreateObjectsArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub num_objs: usize,

    pub err: i32,
    pub objs: Vec<u64>,
    pub gen: u64,
}

unsafe impl Send for CreateObjectsArg {}
unsafe impl Sync for CreateObjectsArg {}

pub unsafe extern "C" fn libuzfs_objects_create_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut CreateObjectsArg);
    arg.objs = vec![0; arg.num_objs];
    arg.err = libuzfs_objects_create(
        arg.dhp,
        arg.objs.as_mut_ptr(),
        arg.num_objs as i32,
        &mut arg.gen,
    );
    libuzfs_wait_log_commit(arg.dhp);
}

pub struct DeleteObjectArg {
    pub ihp: *mut libuzfs_inode_handle_t,

    pub err: i32,
}

unsafe impl Send for DeleteObjectArg {}
unsafe impl Sync for DeleteObjectArg {}

pub unsafe extern "C" fn libuzfs_delete_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut DeleteObjectArg);
    arg.err = libuzfs_object_delete(arg.ihp);
}

pub unsafe extern "C" fn libuzfs_wait_log_commit_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_wait_log_commit(dhp);
}

pub struct GetObjectAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,

    pub attr: uzfs_object_attr_t,
    pub err: i32,
}

unsafe impl Send for GetObjectAttrArg {}
unsafe impl Sync for GetObjectAttrArg {}

pub unsafe extern "C" fn libuzfs_get_object_attr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut GetObjectAttrArg);
    arg.err = libuzfs_object_get_attr(arg.ihp, &mut arg.attr as *mut uzfs_object_attr_t)
}

pub struct ListObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,

    pub num_objs: u64,
}

unsafe impl Send for ListObjectArg {}
unsafe impl Sync for ListObjectArg {}

pub unsafe extern "C" fn libuzfs_list_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ListObjectArg);

    arg.num_objs = libuzfs_object_list(arg.dhp);
}

pub struct StatObjectArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub obj: u64,

    pub doi: dmu_object_info_t,
    pub err: i32,
}

unsafe impl Send for StatObjectArg {}
unsafe impl Sync for StatObjectArg {}

pub unsafe extern "C" fn libuzfs_stat_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut StatObjectArg);
    arg.err = libuzfs_object_stat(arg.dhp, arg.obj, &mut arg.doi as *mut dmu_object_info_t);
}

pub struct ReadObjectArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub offset: u64,
    pub size: u64,

    pub err: i32,
    pub data: Vec<u8>,
}

unsafe impl Send for ReadObjectArg {}
unsafe impl Sync for ReadObjectArg {}

pub unsafe extern "C" fn libuzfs_read_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ReadObjectArg);

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

pub struct WriteObjectArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub offset: u64,
    pub iovs: Vec<iovec>,
    pub sync: bool,

    pub err: i32,
}

unsafe impl Send for WriteObjectArg {}
unsafe impl Sync for WriteObjectArg {}

pub unsafe extern "C" fn libuzfs_write_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut WriteObjectArg);

    arg.err = libuzfs_object_write(
        arg.ihp,
        arg.offset,
        arg.iovs.as_mut_ptr(),
        arg.iovs.len() as i32,
        arg.sync as u32,
    );
}

pub unsafe extern "C" fn libuzfs_sync_object_c(arg: *mut c_void) {
    libuzfs_object_sync(arg as *mut libuzfs_inode_handle_t);
}

pub struct TruncateObjectArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub offset: u64,
    pub size: u64,

    pub err: i32,
}

unsafe impl Send for TruncateObjectArg {}
unsafe impl Sync for TruncateObjectArg {}

pub unsafe extern "C" fn libuzfs_truncate_object_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut TruncateObjectArg);
    arg.err = libuzfs_object_truncate(arg.ihp, arg.offset, arg.size);
}

pub struct DatasetSpaceArg {
    pub dhp: *mut libuzfs_dataset_handle_t,

    pub refd_bytes: u64,
    pub avail_bytes: u64,
    pub used_objs: u64,
    pub avail_objs: u64,
}

unsafe impl Send for DatasetSpaceArg {}
unsafe impl Sync for DatasetSpaceArg {}

pub unsafe extern "C" fn libuzfs_dataset_space_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut DatasetSpaceArg);
    libuzfs_dataset_space(
        arg.dhp,
        &mut arg.refd_bytes,
        &mut arg.avail_bytes,
        &mut arg.used_objs,
        &mut arg.avail_objs,
    );
}

pub struct FindHoleArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub off: u64,

    pub err: i32,
}

unsafe impl Send for FindHoleArg {}
unsafe impl Sync for FindHoleArg {}

pub unsafe extern "C" fn libuzfs_object_next_hole_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut FindHoleArg);
    arg.err = libuzfs_object_next_hole(arg.ihp, &mut arg.off);
}

pub struct NextBlockArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub off: u64,
    pub size: u64,

    pub err: i32,
}

unsafe impl Send for NextBlockArg {}
unsafe impl Sync for NextBlockArg {}

pub unsafe extern "C" fn libuzfs_object_next_block_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut NextBlockArg);
    arg.err = libuzfs_object_next_block(arg.ihp, &mut arg.off, &mut arg.size);
}

pub struct CreateInode {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub inode_type: libuzfs_inode_type_t,

    pub ihp: *mut libuzfs_inode_handle_t,
    pub ino: u64,
    pub txg: u64,
    pub err: i32,
}

unsafe impl Send for CreateInode {}
unsafe impl Sync for CreateInode {}

pub unsafe extern "C" fn libuzfs_create_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut CreateInode);
    arg.err = libuzfs_inode_create(
        arg.dhp,
        &mut arg.ino,
        arg.inode_type,
        &mut arg.ihp,
        &mut arg.txg,
    );
}

pub struct ClaimInodeArg {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub inode_type: libuzfs_inode_type_t,
    pub ino: u64,
    pub gen: u64,

    pub err: i32,
}

unsafe impl Send for ClaimInodeArg {}
unsafe impl Sync for ClaimInodeArg {}

pub unsafe extern "C" fn libuzfs_claim_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ClaimInodeArg);
    arg.err = libuzfs_inode_claim(arg.dhp, arg.ino, arg.gen, arg.inode_type);
}

pub struct DeleteInode {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub inode_type: libuzfs_inode_type_t,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for DeleteInode {}
unsafe impl Sync for DeleteInode {}

pub unsafe extern "C" fn libuzfs_delete_inode_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut DeleteInode);
    arg.err = libuzfs_inode_delete(arg.ihp, arg.inode_type, &mut arg.txg);
}

pub struct GetAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub reserved: *mut i8,
    pub size: i32,

    pub attr: uzfs_inode_attr_t,
    pub err: i32,
}

unsafe impl Send for GetAttrArg {}
unsafe impl Sync for GetAttrArg {}

pub unsafe extern "C" fn libuzfs_inode_getattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut GetAttrArg);
    arg.err = libuzfs_inode_getattr(
        arg.ihp,
        &mut arg.attr as *mut uzfs_inode_attr_t,
        arg.reserved,
        &mut arg.size as *mut i32,
    );
}

pub struct SetAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub reserved: *const i8,
    pub size: u32,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for SetAttrArg {}
unsafe impl Sync for SetAttrArg {}

pub unsafe extern "C" fn libuzfs_set_attr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut SetAttrArg);
    arg.err = libuzfs_inode_setattr(arg.ihp, arg.reserved, arg.size, &mut arg.txg);
}

pub struct GetKvattrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub data: Vec<u8>,
    pub err: i32,
}

unsafe impl Send for GetKvattrArg {}
unsafe impl Sync for GetKvattrArg {}

pub unsafe extern "C" fn libuzfs_inode_get_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut GetKvattrArg);
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

pub struct SetKvAttrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,
    pub value: *const c_char,
    pub size: u64,
    pub option: u32,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for SetKvAttrArg {}
unsafe impl Sync for SetKvAttrArg {}

pub unsafe extern "C" fn libuzfs_set_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut SetKvAttrArg);
    arg.err = libuzfs_inode_set_kvattr(
        arg.ihp,
        arg.name,
        arg.value,
        arg.size,
        &mut arg.txg,
        arg.option,
    );
}

pub struct RemoveKvattrArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for RemoveKvattrArg {}
unsafe impl Sync for RemoveKvattrArg {}

pub unsafe extern "C" fn libuzfs_remove_kvattr_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut RemoveKvattrArg);

    arg.err = libuzfs_inode_remove_kvattr(arg.ihp, arg.name, &mut arg.txg);
}

pub struct ListKvAttrsArg {
    pub ihp: *mut libuzfs_inode_handle_t,

    pub err: i32,
    pub names: Vec<String>,
}

unsafe impl Send for ListKvAttrsArg {}
unsafe impl Sync for ListKvAttrsArg {}

pub unsafe extern "C" fn libuzfs_list_kvattrs_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ListKvAttrsArg);
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

pub struct CreateDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,
    pub ino: u64,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for CreateDentryArg {}
unsafe impl Sync for CreateDentryArg {}

pub unsafe extern "C" fn libuzfs_create_dentry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut CreateDentryArg);

    arg.err = libuzfs_dentry_create(arg.dihp, arg.name, arg.ino, &mut arg.txg);
}

pub struct DeleteDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub err: i32,
    pub txg: u64,
}

unsafe impl Send for DeleteDentryArg {}
unsafe impl Sync for DeleteDentryArg {}

pub unsafe extern "C" fn libuzfs_delete_entry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut DeleteDentryArg);

    arg.err = libuzfs_dentry_delete(arg.dihp, arg.name, &mut arg.txg);
}

pub struct LookupDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub name: *const c_char,

    pub ino: u64,
    pub err: i32,
}

unsafe impl Send for LookupDentryArg {}
unsafe impl Sync for LookupDentryArg {}

pub unsafe extern "C" fn libuzfs_lookup_dentry_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut LookupDentryArg);
    arg.err = libuzfs_dentry_lookup(arg.dihp, arg.name, &mut arg.ino);
}

pub struct IterateDentryArg {
    pub dihp: *mut libuzfs_inode_handle_t,
    pub whence: u64,
    pub size: u32,

    pub err: i32,
    pub done: bool,
    pub dentries: Vec<UzfsDentry>,
}

unsafe impl Send for IterateDentryArg {}
unsafe impl Sync for IterateDentryArg {}

const DEFAULT_NDENTRIES: usize = 128;

unsafe extern "C" fn dir_emit(
    arg: *mut c_void,
    whence: u64,
    name: *const c_char,
    value: u64,
) -> i32 {
    let arg = &mut *(arg as *mut IterateDentryArg);
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

pub unsafe extern "C" fn libuzfs_iterate_dentry_c(arg: *mut c_void) {
    let arg_ptr = arg;
    let arg = &mut *(arg as *mut IterateDentryArg);

    arg.dentries.reserve(DEFAULT_NDENTRIES);

    arg.err = libuzfs_dentry_iterate(arg.dihp, arg.whence, arg_ptr, Some(dir_emit));

    if arg.err == libc::ENOENT {
        arg.done = true;
        arg.err = 0;
    }
}

pub unsafe extern "C" fn libuzfs_wait_synced_c(arg: *mut c_void) {
    let dhp = arg as *mut libuzfs_dataset_handle_t;
    libuzfs_wait_synced(dhp);
}

pub struct ObjectSetMtimeArg {
    pub ihp: *mut libuzfs_inode_handle_t,
    pub tv_sec: i64,
    pub tv_nsec: i64,

    pub err: i32,
}

unsafe impl Send for ObjectSetMtimeArg {}
unsafe impl Sync for ObjectSetMtimeArg {}

pub unsafe extern "C" fn libuzfs_object_set_mtime(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ObjectSetMtimeArg);
    let mtime = timespec {
        tv_sec: arg.tv_sec,
        tv_nsec: arg.tv_nsec,
    };
    arg.err = libuzfs_object_setmtime(arg.ihp, &mtime, false as u32);
}

pub struct InodeHandleGetArgs {
    pub dhp: *mut libuzfs_dataset_handle_t,
    pub ino: u64,
    pub gen: u64,
    pub is_data_inode: bool,

    pub ihp: *mut libuzfs_inode_handle_t,
    pub err: i32,
}

unsafe impl Send for InodeHandleGetArgs {}
unsafe impl Sync for InodeHandleGetArgs {}

pub unsafe extern "C" fn libuzfs_inode_handle_get_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut InodeHandleGetArgs);
    arg.err = libuzfs_inode_handle_get(
        arg.dhp,
        arg.is_data_inode as u32,
        arg.ino,
        arg.gen,
        &mut arg.ihp,
    );
}

pub unsafe extern "C" fn libuzfs_inode_handle_rele_c(arg: *mut c_void) {
    let arg = arg as *mut libuzfs_inode_handle_t;
    libuzfs_inode_handle_rele(arg);
}

pub struct DebugArgs {
    pub argv: *mut *mut c_char,
    pub argc: i32,
}

unsafe impl Send for DebugArgs {}
unsafe impl Sync for DebugArgs {}

pub unsafe extern "C" fn libuzfs_debug_main_c(arg: *mut c_void) {
    let arg = &*(arg as *mut DebugArgs);
    libuzfs_debug_main(arg.argc, arg.argv);
}

pub struct ShowStatsArgs {
    pub stat_ptr: *mut c_void,
    pub stat_type: i32,

    pub formatted_strings: Vec<Vec<u8>>,
}

unsafe impl Send for ShowStatsArgs {}
unsafe impl Sync for ShowStatsArgs {}

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

pub unsafe extern "C" fn libuzfs_show_stats_c(arg: *mut c_void) {
    let arg = &mut *(arg as *mut ShowStatsArgs);
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
