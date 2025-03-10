pub(crate) mod driver;
mod timer_fd;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use driver::{Timer, TimerManager};
use once_cell::sync::OnceCell;
use pin_project::pin_project;

static TIMER_MANAGER: OnceCell<TimerManager> = OnceCell::new();

pub(crate) fn init_timer() {
    TIMER_MANAGER.get_or_init(TimerManager::new);
}

pub struct Sleep {
    timer: Arc<Timer>,
    registered: bool,
    finished: bool,
}

impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.finished || self.timer.expired() {
            Poll::Ready(())
        } else if self.registered {
            let mut waker = self.timer.waker.lock().unwrap();
            match waker.as_mut() {
                None => {
                    drop(waker);
                    self.finished = true;
                    Poll::Ready(())
                }
                Some(waker) => {
                    if !waker.will_wake(cx.waker()) {
                        *waker = cx.waker().clone();
                    }
                    Poll::Pending
                }
            }
        } else {
            let old = self.timer.waker.lock().unwrap().replace(cx.waker().clone());
            assert!(old.is_none());

            TIMER_MANAGER
                .get()
                .unwrap()
                .register_timer(self.timer.clone());
            self.registered = true;
            Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if self.registered && !self.finished && Arc::strong_count(&self.timer) > 1 {
            TIMER_MANAGER
                .get()
                .unwrap()
                .unregister_timer(self.timer.clone());
        }
    }
}

pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        timer: TIMER_MANAGER.get().unwrap().new_timer(duration),
        registered: false,
        finished: false,
    }
}

#[pin_project]
pub struct Timeout<F> {
    #[pin]
    fut: F,
    #[pin]
    sleep: Sleep,
}

pub fn timeout<F: Future>(duration: Duration, fut: F) -> Timeout<F> {
    Timeout {
        fut,
        sleep: sleep(duration),
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        match me.fut.poll(cx) {
            Poll::Pending => match me.sleep.poll(cx) {
                Poll::Ready(_) => Poll::Ready(None),
                _ => Poll::Pending,
            },
            Poll::Ready(ret) => Poll::Ready(Some(ret)),
        }
    }
}
