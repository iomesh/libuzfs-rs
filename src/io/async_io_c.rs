use super::async_io::{AioCallback, AioContex, IoContent, IoType};
use crate::context::coroutine::AsyncCoroutine;
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinHandle,
};

struct AioHandle {
    sender: UnboundedSender<AioCallback>,
    task_handle: JoinHandle<()>,
}

static FD_CONTEXT_MAP: OnceCell<DashMap<i32, Arc<AioHandle>>> = OnceCell::new();

const DEFAULT_NR_REQUESTS: u32 = 128;
const ENV_NR_REQUESTS: &str = "NR_REQUESTS";

#[allow(clippy::missing_safety_doc)]
pub(crate) unsafe extern "C" fn register_fd(
    fd: i32,
    cb: Option<unsafe extern "C" fn(arg: *mut libc::c_void, res: i64)>,
) {
    let nr_requests =
        std::env::var(ENV_NR_REQUESTS).map_or_else(|_| DEFAULT_NR_REQUESTS, |s| s.parse().unwrap());
    let (sender, receiver) = unbounded_channel();
    let mut aio_context = AioContex::new(fd, nr_requests, receiver, cb.unwrap()).unwrap();
    let task_handle = tokio::spawn(async move {
        let _ = aio_context.submit_and_reap_tasks().await;
    });
    let handle = Arc::new(AioHandle {
        sender,
        task_handle,
    });
    FD_CONTEXT_MAP.get_or_init(DashMap::new).insert(fd, handle);
}

#[allow(clippy::missing_safety_doc)]
pub(crate) unsafe extern "C" fn unregister_fd(fd: i32) {
    if let Some((_, mut aio_ctx)) = FD_CONTEXT_MAP.get().unwrap().remove(&fd) {
        let fut = async move {
            aio_ctx.task_handle.abort();
            let handle = &mut Arc::get_mut(&mut aio_ctx).unwrap().task_handle;
            handle.await.unwrap_err();
        };

        AsyncCoroutine::tls_coroutine().poll_until_ready(fut);
    }
}

#[allow(clippy::missing_safety_doc)]
pub(crate) unsafe extern "C" fn submit_read(
    fd: i32,
    offset: u64,
    buf: *mut i8,
    size: u64,
    arg: *mut libc::c_void,
) {
    let ctx = FD_CONTEXT_MAP.get().unwrap().get(&fd).unwrap().clone();
    let io_content = IoContent {
        data_ptr: buf as usize,
        offset,
        size,
    };
    ctx.sender
        .send(AioCallback {
            io_type: IoType::Read,
            io_content,
            arg,
        })
        .unwrap();
}

#[allow(clippy::missing_safety_doc)]
pub(crate) unsafe extern "C" fn submit_write(
    fd: i32,
    offset: u64,
    buf: *const i8,
    size: u64,
    arg: *mut libc::c_void,
) {
    let ctx = FD_CONTEXT_MAP.get().unwrap().get(&fd).unwrap().clone();
    let io_content = IoContent {
        data_ptr: buf as usize,
        offset,
        size,
    };
    ctx.sender
        .send(AioCallback {
            io_type: IoType::Write,
            io_content,
            arg,
        })
        .unwrap();
}

#[allow(clippy::missing_safety_doc)]
pub(crate) unsafe extern "C" fn submit_fsync(fd: i32, arg: *mut libc::c_void) {
    let ctx = FD_CONTEXT_MAP.get().unwrap().get(&fd).unwrap().clone();
    let io_content = IoContent::default();
    ctx.sender
        .send(AioCallback {
            io_type: IoType::Sync,
            io_content,
            arg,
        })
        .unwrap();
}
