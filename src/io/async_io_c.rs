use super::async_io::{AioCallback, AioContext, IoContent, IoType};

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn register_fd(
    fd: i32,
    cb: Option<unsafe extern "C" fn(arg: *mut libc::c_void, res: i64)>,
) -> *mut libc::c_void {
    let aio_context = Box::new(AioContext::start(fd, cb.unwrap()).unwrap());
    Box::into_raw(aio_context) as *mut libc::c_void
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn unregister_fd(aio_hdl: *mut libc::c_void) {
    drop(Box::from_raw(aio_hdl as *mut AioContext));
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_read(
    aio_hdl: *const libc::c_void,
    offset: u64,
    buf: *mut libc::c_char,
    size: u64,
    arg: *mut libc::c_void,
) {
    let aio_hdl = &*(aio_hdl as *const AioContext);
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
    buf: *const libc::c_char,
    size: u64,
    arg: *mut libc::c_void,
) {
    let aio_hdl = &*(aio_hdl as *const AioContext);
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
    let aio_hdl = &*(aio_hdl as *const AioContext);
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
