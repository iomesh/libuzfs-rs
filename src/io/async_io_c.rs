use super::async_io::{AioCallback, AioContex, IoContent, IoType};
use crate::context::coroutine::AsyncCoroutine;
use once_cell::sync::OnceCell;
use tokio::{
    runtime::Runtime,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinHandle,
};

struct AioHandle {
    sender: UnboundedSender<AioCallback>,
    task_handle: JoinHandle<()>,
}

const DEFAULT_NR_REQUESTS: u32 = 128;
const ENV_NR_REQUESTS: &str = "NR_REQUESTS";

static IO_RUNTIME: OnceCell<Runtime> = OnceCell::new();

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn register_fd(
    fd: i32,
    cb: Option<unsafe extern "C" fn(arg: *mut libc::c_void, res: i64)>,
) -> *mut libc::c_void {
    let nr_requests =
        std::env::var(ENV_NR_REQUESTS).map_or_else(|_| DEFAULT_NR_REQUESTS, |s| s.parse().unwrap());
    let (sender, receiver) = unbounded_channel();
    let mut aio_context = AioContex::new(fd, nr_requests, receiver, cb.unwrap()).unwrap();
    let rt = IO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });
    let task_handle = rt.spawn(async move {
        let _ = aio_context.submit_and_reap_tasks().await;
    });
    let handle = Box::new(AioHandle {
        sender,
        task_handle,
    });

    Box::into_raw(handle) as *mut libc::c_void
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn unregister_fd(aio_hdl: *mut libc::c_void) {
    let aio_hdl = Box::from_raw(aio_hdl as *mut AioHandle);
    aio_hdl.task_handle.abort();
    AsyncCoroutine::tls_coroutine()
        .poll_until_ready(aio_hdl.task_handle)
        .unwrap_err();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_read(
    aio_hdl: *const libc::c_void,
    offset: u64,
    buf: *mut i8,
    size: u64,
    arg: *mut libc::c_void,
) {
    let aio_hdl = &*(aio_hdl as *const AioHandle);
    let io_content = IoContent {
        data_ptr: buf as usize,
        offset,
        size,
    };
    aio_hdl
        .sender
        .send(AioCallback {
            io_type: IoType::Read,
            io_content,
            arg,
        })
        .unwrap();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_write(
    aio_hdl: *const libc::c_void,
    offset: u64,
    buf: *const i8,
    size: u64,
    arg: *mut libc::c_void,
) {
    let aio_hdl = &*(aio_hdl as *const AioHandle);
    let io_content = IoContent {
        data_ptr: buf as usize,
        offset,
        size,
    };
    aio_hdl
        .sender
        .send(AioCallback {
            io_type: IoType::Write,
            io_content,
            arg,
        })
        .unwrap();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_fsync(aio_hdl: *const libc::c_void, arg: *mut libc::c_void) {
    let aio_hdl = &*(aio_hdl as *const AioHandle);
    let io_content = IoContent::default();
    aio_hdl
        .sender
        .send(AioCallback {
            io_type: IoType::Sync,
            io_content,
            arg,
        })
        .unwrap();
}
