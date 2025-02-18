use crate::{
    bindings::sys::{aio_done_func_t, init_io_args_func_t},
    context::coroutine::CoroutineFuture,
};

use super::async_io::AioContext;

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn aio_init(
    next_off: usize,
    io_done: aio_done_func_t,
    init_io_args: init_io_args_func_t,
) -> *mut libc::c_void {
    let aio_context =
        Box::new(AioContext::start(io_done.unwrap(), next_off, init_io_args.unwrap()).unwrap());
    Box::into_raw(aio_context) as *mut libc::c_void
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn aio_fini(aio_hdl: *mut libc::c_void) {
    let aio_handle = Box::from_raw(aio_hdl as *mut AioContext);
    CoroutineFuture::poll_until_ready(aio_handle.exit());
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn submit_aio(aio_hdl: *const libc::c_void, arg: *mut libc::c_void) {
    let aio_hdl = &*(aio_hdl as *const AioContext);
    aio_hdl.task_list.push(arg);
}
