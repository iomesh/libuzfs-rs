use std::{io::Result, time::Duration};

use timerfd::{ClockId, SetTimeFlags, TimerFd, TimerState};
use tokio::io::{unix::AsyncFd, Interest};

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

    pub(super) async fn wait_until_wakeup(&mut self) {
        self.0.readable().await.unwrap().clear_ready();
    }
}
