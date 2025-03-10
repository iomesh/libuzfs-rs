use std::{
    io::{Error, Result},
    os::fd::AsRawFd,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use timerfd::{ClockId, SetTimeFlags, TimerFd, TimerState};
use tokio::io::{unix::AsyncFd, AsyncRead, Interest, ReadBuf};

pub(super) struct AsyncTimerFd(AsyncFd<TimerFd>);

impl AsyncTimerFd {
    pub(super) fn new() -> Result<Self> {
        let fd = TimerFd::new_custom(ClockId::Monotonic, true, true)?;
        let async_fd = AsyncFd::with_interest(fd, Interest::READABLE)?;
        Ok(Self(async_fd))
    }

    pub(super) fn set_next_wakeup(&mut self, duration: Duration) {
        self.0
            .get_mut()
            .set_state(TimerState::Oneshot(duration), SetTimeFlags::Default);
    }
}

fn read_timerfd(timerfd: &AsyncFd<TimerFd>, buf: &mut [u8]) -> Result<usize> {
    let ret = unsafe { libc::read(timerfd.as_raw_fd(), buf.as_mut_ptr() as *mut _, buf.len()) };
    if ret < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(ret as usize)
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
            match guard.try_io(|inner| read_timerfd(inner, unfilled)) {
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
