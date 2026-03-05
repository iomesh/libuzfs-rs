use std::ffi::{c_void, CString};
use std::io::{Error, ErrorKind, Result};
use std::mem::take;
use std::ptr::{null, null_mut};
use std::time::Duration;
use std::{future::Future, sync::Arc};

use chrono::Local;
use cstr_argument::CStrArgument;
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};

use crate::metrics::RequestMetrics;
use crate::ReadBufReleaser;
use crate::{
    bindings::{async_sys::*, sys::*},
    context::coroutine::CoroutineFuture,
    metrics::ZpoolMetrics,
    Dataset,
};

pub enum ZpoolType {
    Meta,
    Data,
}

pub struct ZpoolOpenOptions {
    create: bool,
    autotrim: bool,
    max_blksize: u32,
    dnodesize: u32,
}

impl ZpoolOpenOptions {
    pub fn new(zp_type: ZpoolType) -> Self {
        let (autotrim, dnodesize, max_blksize) = match zp_type {
            ZpoolType::Data => (true, 512, 64 << 10),
            ZpoolType::Meta => (false, 1024, 0),
        };

        Self {
            create: false,
            autotrim,
            max_blksize,
            dnodesize,
        }
    }

    pub fn max_blksize(mut self, max_blksize: u32) -> Self {
        self.max_blksize = max_blksize;
        self
    }

    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    pub async fn open<FS: FileSystem>(
        self,
        poolname: &str,
        dev_paths: &[&str],
    ) -> Result<Zpool<FS>> {
        Zpool::open(
            poolname,
            dev_paths,
            self.create,
            self.autotrim,
            self.dnodesize,
            self.max_blksize,
        )
        .await
    }
}

pub trait FileSystem: Send + Sync + 'static + Sized {
    fn init(
        ds: Dataset,
        fsid: u32,
        snapid: Option<u32>,
        pool_name: &str,
    ) -> impl Future<Output = Result<Self>> + Send;
    fn close(&mut self) -> impl Future<Output = Result<()>> + Send;
}

pub struct Zpool<FS> {
    zhp: *mut libuzfs_zpool_handle_t,
    pub metrics: Box<ZpoolMetrics>,

    locks: DashMap<u32, Arc<Mutex<()>>>,
    filesystems: DashMap<u32, Arc<FS>>,
    snapshots: DashMap<(u32, u32), Arc<FS>>,

    dnodesize: u32,
    max_blksize: u32,

    dev_lock: RwLock<()>,

    poolname: String,
}

unsafe impl<FS> Send for Zpool<FS> {}
unsafe impl<FS> Sync for Zpool<FS> {}

impl<FS: FileSystem> Zpool<FS> {
    async fn open(
        poolname: &str,
        dev_paths: &[&str],
        create: bool,
        enable_autotrim: bool,
        dnodesize: u32,
        max_blksize: u32,
    ) -> Result<Self> {
        let poolname_cstr = poolname.into_cstr();
        let metrics = ZpoolMetrics::new_boxed();
        let dev_paths_cstr: Vec<_> = dev_paths
            .iter()
            .map(|dev_path| dev_path.into_cstr())
            .collect();
        let mut args = ZpoolOpenArg {
            pool_name: poolname_cstr.as_ptr(),
            dev_paths: dev_paths_cstr
                .iter()
                .map(|dev_path| dev_path.as_ptr())
                .collect(),
            metrics: metrics.as_ref() as *const _ as *const _,
            create,
            enable_autotrim,
            res: 0,
            zhp: null_mut(),
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_zpool_open_c, args_usize).await;

        if args.zhp.is_null() {
            assert_ne!(args.res, 0);
            Err(Error::from_raw_os_error(args.res))
        } else {
            Ok(Self {
                zhp: args.zhp,
                metrics,
                locks: DashMap::new(),
                filesystems: DashMap::new(),
                snapshots: DashMap::new(),
                dnodesize,
                max_blksize,
                dev_lock: RwLock::new(()),
                poolname: poolname.to_owned(),
            })
        }
    }

    pub async fn start_manual_trim(&self) -> Result<()> {
        let _guard = self.dev_lock.read().await;
        let mut args = ManualTrimArg {
            zhp: self.zhp,
            res: 0,
        };
        let arg_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_start_manual_trim_c, arg_usize).await;

        if args.res != 0 {
            Err(Error::from_raw_os_error(args.res))
        } else {
            Ok(())
        }
    }

    pub async fn close(&mut self) {
        for (fsid, mut fs) in take(&mut self.filesystems) {
            self.do_close_filesystem(&mut fs, fsid.to_string(), "zpool closing")
                .await;
        }

        for ((fsid, snapid), mut fs) in take(&mut self.snapshots) {
            self.do_close_filesystem(
                &mut fs,
                Self::cloned_snapshot_name(fsid, snapid),
                "zpool closing",
            )
            .await;
        }

        CoroutineFuture::new(libuzfs_zpool_close_c, self.zhp as usize)
            .record_pending_time()
            .await;
    }

    pub async fn expand_dev(&self, dev_path: &str) -> Result<()> {
        let _guard = self.dev_lock.read().await;
        let dev_path_cstr = dev_path.into_cstr();

        let mut args = ZpoolExpandArg {
            zhp: self.zhp,
            dev_path: dev_path_cstr.as_ptr(),
            res: 0,
        };

        let arg_usize = &mut args as *mut _ as usize;

        CoroutineFuture::new(libuzfs_zpool_expand_c, arg_usize).await;
        if args.res != 0 {
            Err(Error::from_raw_os_error(args.res))
        } else {
            Ok(())
        }
    }

    pub async fn add_dev(&self, dev_path: &str) -> Result<()> {
        let _guard = self.dev_lock.write().await;
        let dev_path_cstr = dev_path.into_cstr();

        let mut args = ZpoolAddDevArg {
            zhp: self.zhp,
            dev_path: dev_path_cstr.as_ptr(),
            res: 0,
        };

        let arg_usize = &mut args as *mut _ as usize;

        CoroutineFuture::new(libuzfs_zpool_add_dev_c, arg_usize).await;
        if args.res != 0 {
            Err(Error::from_raw_os_error(args.res))
        } else {
            Ok(())
        }
    }

    async fn open_dataset(&self, dsname: String, create: bool) -> Result<Dataset> {
        let dsname = dsname.into_cstr();
        let mut args = DatasetOpenArgs {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            dnode_size: self.dnodesize,
            max_blksize: self.max_blksize,
            create,

            res: 0,
            dhp: null_mut(),
        };

        let arg_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_dataset_open_c, arg_usize).await;

        if args.res == 0 {
            Ok(Dataset {
                dhp: args.dhp,
                metrics: RequestMetrics::new(),
                buf_releaser: ReadBufReleaser::new(),
            })
        } else {
            Err(Error::from_raw_os_error(args.res))
        }
    }

    fn find_or_create_lock(&self, fsid: u32) -> Arc<Mutex<()>> {
        self.locks
            .entry(fsid)
            .or_insert(Arc::new(Mutex::new(())))
            .clone()
    }

    pub async fn get_or_open_filesystem(&self, fsid: u32, create: bool) -> Result<Arc<FS>> {
        if let Some(fs) = self.filesystems.get(&fsid).map(|v| v.clone()) {
            return Ok(fs);
        }

        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;
        if let Some(fs) = self.filesystems.get(&fsid).map(|v| v.clone()) {
            return Ok(fs);
        }

        let ds = self.open_dataset(fsid.to_string(), create).await?;
        let fs = Arc::new(FS::init(ds, fsid, None, &self.poolname).await?);

        self.filesystems.insert(fsid, fs.clone());
        Ok(fs)
    }

    async fn do_close_filesystem(&self, filesystem: &mut Arc<FS>, fsname: String, debug_str: &str) {
        loop {
            if let Some(fs) = Arc::get_mut(filesystem) {
                fs.close().await.unwrap();
                break;
            }
            println!(
                "[{}] {debug_str}. filesystem {fsname} in ZPool {} still has {} inflight io, wait..",
                Local::now(),
                self.poolname,
                Arc::strong_count(filesystem)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn do_destroy_filesystem(&self, dsname: String) -> Result<()> {
        let dsname = dsname.into_cstr();
        let mut args = DatasetDestroyArgs {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),

            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_dataset_destroy_c, args_usize).await;

        if args.err == 0 {
            Ok(())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }

    pub async fn destroy_filesystem(&self, fsid: u32) -> Result<()> {
        let lock = self.find_or_create_lock(fsid);
        let guard = lock.lock().await;

        if let Some((_key, mut filesystem)) = self.filesystems.remove(&fsid) {
            self.do_close_filesystem(&mut filesystem, fsid.to_string(), "destroy")
                .await;
        };

        for snapid in self.list_snapshots(fsid).await? {
            self.do_destroy_snapshot(fsid, snapid).await?;
        }

        self.do_destroy_filesystem(fsid.to_string()).await?;

        drop(guard);
        self.locks.remove(&fsid);
        Ok(())
    }
}

// Basic Snapshot operations
impl<FS: FileSystem> Zpool<FS> {
    pub async fn create_snapshot(&self, fsid: u32, snapid: u32) -> Result<()> {
        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;
        self.do_create_snapshot(fsid, snapid).await?;
        self.clone_snapshot(fsid, snapid).await
    }

    fn cloned_snapshot_name(fsid: u32, snapid: u32) -> String {
        format!("{fsid}-{snapid}")
    }

    async fn open_snapshot(&self, fsid: u32, snapid: u32) -> Result<Arc<FS>> {
        let ds = self
            .open_dataset(Self::cloned_snapshot_name(fsid, snapid), false)
            .await?;
        let fs = Arc::new(FS::init(ds, fsid, Some(snapid), &self.poolname).await?);

        self.snapshots.insert((fsid, snapid), fs.clone());

        Ok(fs)
    }

    pub async fn get_or_open_snapshot(&self, fsid: u32, snapid: u32) -> Result<Arc<FS>> {
        if let Some(fs) = self.snapshots.get(&(fsid, snapid)).map(|v| v.clone()) {
            return Ok(fs);
        }

        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;

        if let Some(fs) = self.snapshots.get(&(fsid, snapid)).map(|v| v.clone()) {
            return Ok(fs);
        }

        self.open_snapshot(fsid, snapid).await
    }

    async fn clone_snapshot(&self, fsid: u32, snapid: u32) -> Result<()> {
        let dsname = fsid.to_string().into_cstr();
        let snapname_cstr = snapid.to_string().into_cstr();
        let clone_dsname = CString::new(Self::cloned_snapshot_name(fsid, snapid)).unwrap();

        let mut args = SnapshotCloneArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            snapname: snapname_cstr.as_ptr(),
            clone: clone_dsname.as_ptr(),
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_snapshot_clone_c, args_usize).await;

        if args.err == 0 {
            Ok(())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }

    async fn do_create_snapshot(&self, fsid: u32, snapid: u32) -> Result<()> {
        let dsname = fsid.to_string().into_cstr();
        let snapname_cstr = snapid.to_string().into_cstr();

        let mut args = SnapshotCreateArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            snapname: snapname_cstr.as_ptr(),
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_snapshot_create_c, args_usize).await;

        if args.err == 0 {
            Ok(())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }

    async fn do_destroy_snapshot(&self, fsid: u32, snapid: u32) -> Result<()> {
        if let Some((_, mut fs)) = self.snapshots.remove(&(fsid, snapid)) {
            self.do_close_filesystem(
                &mut fs,
                Self::cloned_snapshot_name(fsid, snapid),
                "destroy snapshot",
            )
            .await;
        }

        let res = self
            .do_destroy_filesystem(Self::cloned_snapshot_name(fsid, snapid))
            .await;
        if let Err(err) = res {
            if err.kind() != ErrorKind::NotFound {
                return Err(err);
            }
        }

        let dsname = fsid.to_string().into_cstr();
        let snapname_cstr = snapid.to_string().into_cstr();

        let mut args = SnapshotDestroyArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            snapname: snapname_cstr.as_ptr(),
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_snapshot_destroy_c, args_usize).await;

        if args.err == 0 {
            Ok(())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }

    pub async fn destroy_snapshot(&self, fsid: u32, snapid: u32) -> Result<()> {
        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;
        self.do_destroy_snapshot(fsid, snapid).await
    }

    pub async fn rollback_to_snapshot(&self, fsid: u32, snapid: u32) -> Result<()> {
        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;

        // Close the current filesystem before rollback
        if let Some((_, mut fs)) = self.filesystems.remove(&fsid) {
            self.do_close_filesystem(&mut fs, fsid.to_string(), "rollback")
                .await;
        }

        let dsname = fsid.to_string().into_cstr();
        let snapname_cstr = snapid.to_string().into_cstr();

        let mut args = SnapshotRollbackArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            snapname: snapname_cstr.as_ptr(),
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_snapshot_rollback_c, args_usize).await;

        if args.err == 0 {
            Ok(())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }

    async fn list_snapshots(&self, fsid: u32) -> Result<Vec<u32>> {
        let dsname = fsid.to_string().into_cstr();
        let mut args = SnapshotListArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            snapnames: Vec::new(),
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_snapshot_list_c, args_usize).await;

        if args.err == 0 {
            Ok(args
                .snapnames
                .into_iter()
                .map(|name| name.parse().unwrap())
                .collect())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResumeInfo {
    pub resume_object: u64,
    pub resume_offset: u64,
}

#[derive(Debug, Clone)]
pub struct ReceiveSnapshotOptions {
    pub force: bool,
    pub resumable: bool,
}

impl Default for ReceiveSnapshotOptions {
    fn default() -> Self {
        Self {
            force: false,
            resumable: true,
        }
    }
}

struct SendCallback<F, Fut>
where
    F: Fn(&[u8]) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    callback: F,
}

impl<F, Fut> SendCallback<F, Fut>
where
    F: Fn(&[u8]) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
{
    fn new(callback: F) -> Self {
        Self { callback }
    }
}

unsafe extern "C" fn send_callback_wrapper<F, Fut>(
    arg: *const c_void,
    buf: *const c_void,
    len: usize,
) -> i32
where
    F: Fn(&[u8]) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
{
    let context = &mut *(arg as *mut SendCallback<F, Fut>);
    let data = std::slice::from_raw_parts(buf as *const u8, len);

    match CoroutineFuture::poll_until_ready((context.callback)(data)) {
        Ok(()) => 0,
        Err(e) => e.raw_os_error().unwrap_or(libc::EIO),
    }
}

// Snapshot send
impl<FS: FileSystem> Zpool<FS> {
    pub async fn send_snapshot<F, Fut>(
        &self,
        fsid: u32,
        to_snapid: u32,
        from_snapid: Option<u32>,
        callback: F,
        ri: Option<ResumeInfo>,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        let dsname = fsid.to_string().into_cstr();
        let snapname_cstr = to_snapid.to_string().into_cstr();
        let from_snapname_cstr = from_snapid.map(|s| s.to_string().into_cstr());
        let from_snap = from_snapname_cstr.as_ref().map_or(null(), |s| s.as_ptr());

        let context = SendCallback::new(callback);
        let ri = ri.unwrap_or(ResumeInfo::default());
        let sendargs = libuzfs_send_args_t {
            resume_object: ri.resume_object,
            resume_offset: ri.resume_offset,
            send_cb: Some(send_callback_wrapper::<F, Fut>),
            arg: &context as *const _ as _,
        };

        let mut args = SendSnapshotArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            to_snap: snapname_cstr.as_ptr(),
            from_snap,
            sendargs,
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_send_snapshot_c, args_usize).await;

        if args.err == 0 {
            Ok(())
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }
}

struct ReceiveCallback<F, Fut>
where
    F: FnMut(&mut [u8]) -> Fut,
    Fut: Future<Output = Result<usize>>,
{
    callback: F,
}

unsafe extern "C" fn receive_callback_wrapper<F, Fut>(
    arg: *mut c_void,
    buf: *mut c_void,
    len: usize,
    nread: *mut usize,
) -> i32
where
    F: FnMut(&mut [u8]) -> Fut,
    Fut: Future<Output = Result<usize>>,
{
    let context = &mut *(arg as *mut ReceiveCallback<F, Fut>);
    let data = std::slice::from_raw_parts_mut(buf as *mut u8, len);

    match crate::context::coroutine::CoroutineFuture::poll_until_ready((context.callback)(data)) {
        Ok(n) => {
            *nread = n;
            0
        }
        Err(e) => e.raw_os_error().unwrap_or(libc::EIO),
    }
}

// Snapshot receive
impl<FS: FileSystem> Zpool<FS> {
    pub async fn receive_snapshot<F, Fut>(&self, fsid: u32, snapid: u32, callback: F) -> Result<()>
    where
        F: FnMut(&mut [u8]) -> Fut + Send,
        Fut: Future<Output = Result<usize>> + Send,
    {
        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;

        match self.open_snapshot(fsid, snapid).await {
            Err(err) if err.kind() == ErrorKind::NotFound => (),
            Err(err) => return Err(err),
            Ok(_) => return Ok(()),
        }

        if self.filesystems.contains_key(&fsid) {
            return Err(Error::new(
                ErrorKind::ResourceBusy,
                format!("{fsid} is working"),
            ));
        }

        let dsname = fsid.to_string().into_cstr();
        let snapname_cstr = snapid.to_string().into_cstr();

        let mut context = ReceiveCallback { callback };
        let mut args = ReceiveSnapshotArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            to_snap: snapname_cstr.as_ptr(),
            callback: Some(receive_callback_wrapper::<F, Fut>),
            callback_arg: &mut context as *mut _ as *mut c_void,
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_receive_snapshot_c, args_usize).await;

        if args.err == 0 || args.err == libc::EEXIST {
            self.clone_snapshot(fsid, snapid).await
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }

    pub async fn get_receive_resume_info(&self, fsid: u32) -> Result<Option<ResumeInfo>> {
        let lock = self.find_or_create_lock(fsid);
        let _guard = lock.lock().await;

        let dsname = fsid.to_string().into_cstr();

        let mut info = libuzfs_receive_resume_info_t {
            has_resume: 0,
            object: 0,
            offset: 0,
        };

        let mut args = GetReceiveResumeInfoArg {
            zhp: self.zhp,
            dsname: dsname.as_ptr(),
            info: &mut info as *mut _,
            err: 0,
        };

        let args_usize = &mut args as *mut _ as usize;
        CoroutineFuture::new(libuzfs_get_receive_resume_info_c, args_usize).await;

        if args.err == 0 {
            let ri = (info.has_resume != 0).then_some(ResumeInfo {
                resume_object: info.object,
                resume_offset: info.offset,
            });
            Ok(ri)
        } else {
            Err(Error::from_raw_os_error(args.err))
        }
    }
}
