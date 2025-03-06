use std::{
    io::{Error, Result},
    os::fd::AsRawFd,
    pin::Pin,
    ptr::null_mut,
    task::{ready, Context, Poll},
};

use libc::{itimerspec, timespec};
use tokio::io::{unix::AsyncFd, AsyncRead, Interest, ReadBuf};

struct TimerFd(i32);

impl TimerFd {
    fn new() -> Result<Self> {
        let fd = unsafe {
            libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::O_CLOEXEC | libc::O_NONBLOCK)
        };
        if fd < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(Self(fd))
        }
    }

    fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let nread = unsafe { libc::read(self.0, buf.as_mut_ptr() as *mut _, buf.len()) };
        if nread < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(nread as usize)
        }
    }
}

impl Drop for TimerFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

impl AsRawFd for TimerFd {
    fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
        self.0
    }
}

pub(super) struct AsyncTimerFd(AsyncFd<TimerFd>);

impl AsyncTimerFd {
    pub(super) fn new() -> Result<Self> {
        let fd = TimerFd::new()?;
        let async_fd = AsyncFd::with_interest(fd, Interest::READABLE)?;
        Ok(Self(async_fd))
    }

    pub(super) unsafe fn set_next_wakeup(&mut self, wakeup: timespec) -> Result<()> {
        let res = libc::timerfd_settime(
            self.0.as_raw_fd(),
            libc::TFD_TIMER_ABSTIME,
            &itimerspec {
                it_interval: timespec {
                    tv_nsec: 0,
                    tv_sec: 0,
                },
                it_value: wakeup,
            },
            null_mut(),
        );

        if res < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl AsyncRead for AsyncTimerFd {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.0.poll_read_ready(cx))?;

            let unfilled = buf.initialize_unfilled();
            match guard.try_io(|inner| inner.get_ref().read(unfilled)) {
                Ok(Ok(len)) => {
                    buf.advance(len);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(_would_block) => continue,
            }
        }
    }
}
