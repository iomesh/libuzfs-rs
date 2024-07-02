use futures::ready;
use std::{
    io::Error,
    mem::transmute,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{unix::AsyncFd, AsyncRead, AsyncReadExt, ReadBuf};

pub(super) struct EventFd(AsyncFd<i32>);

impl EventFd {
    pub(super) fn new(init: u32) -> Result<Self, Error> {
        let fd = unsafe { libc::eventfd(init, libc::EFD_CLOEXEC) };
        if fd < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(Self(AsyncFd::new(fd)?))
        }
    }

    pub(super) fn raw_fd(&self) -> i32 {
        self.0.get_ref().to_owned()
    }

    pub(super) async fn read_events(&mut self) -> Result<u64, Error> {
        let mut nevents: u64 = 0;
        let buf: &mut [u8; 8] = unsafe { transmute(&mut nevents) };
        self.read_exact(buf).await?;
        Ok(nevents)
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        if unsafe { libc::close(self.raw_fd()) } < 0 {
            panic!(
                "unexpected error when closing eventfd, {}",
                Error::last_os_error()
            );
        }
    }
}

impl AsyncRead for EventFd {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), Error>> {
        let guard = ready!(self.0.poll_read_ready(cx)?);
        let unfilled = buf.initialize_unfilled();
        assert_eq!(unfilled.len(), 8);
        let fd = *guard.get_inner();
        let res = unsafe { libc::read(fd, unfilled.as_mut_ptr() as *mut libc::c_void, 8) };
        let res = if res < 0 {
            Err(Error::last_os_error())
        } else {
            buf.advance(8);
            assert_eq!(res, 8);
            Ok(())
        };

        Poll::Ready(res)
    }
}
