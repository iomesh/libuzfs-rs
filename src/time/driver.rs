use std::{
    cmp::Ordering,
    collections::BTreeSet,
    sync::{atomic::AtomicU64, Arc, Mutex},
    task::Waker,
    time::{Duration, Instant},
};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::timer_fd::AsyncTimerFd;

pub(super) struct Timer {
    expiration: Instant,
    id: u64,
    pub(super) waker: Mutex<Option<Waker>>,
}

impl Timer {
    pub(super) fn expired(&self) -> bool {
        Instant::now() >= self.expiration
    }

    pub(super) fn check_bias(&self) {
        let bias = Instant::now().saturating_duration_since(self.expiration);
        if bias.as_secs() > 1 {
            println!("bias: {bias:?}");
        }
    }
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Timer {}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.expiration.cmp(&other.expiration) {
            Ordering::Equal => self.id.cmp(&other.id),
            other => other,
        }
    }
}

enum TimerEvent {
    Register(Arc<Timer>),
    Unregister(Arc<Timer>),
}

struct TimerDriver {
    set: BTreeSet<Arc<Timer>>,
    event_receiver: UnboundedReceiver<TimerEvent>,
}

impl TimerDriver {
    async fn handle_timer_events(&mut self) {
        let mut events = Vec::with_capacity(256);
        self.event_receiver.recv_many(&mut events, 256).await;
        for event in events {
            match event {
                TimerEvent::Register(timer) => assert!(self.set.insert(timer)),
                TimerEvent::Unregister(timer) => {
                    self.set.remove(&timer);
                }
            }
        }
    }

    async fn serve(mut self, mut timerfd: AsyncTimerFd) {
        loop {
            tokio::select! {
                _ = self.handle_timer_events() => {},
                _ = timerfd.wait_until_wakeup() => {}
            }

            let now = Instant::now();
            while let Some(first) = self.set.first() {
                if let Some(duration) = first.expiration.checked_duration_since(now) {
                    timerfd.set_next_wakeup(duration);
                    break;
                }

                first.waker.lock().unwrap().take().unwrap().wake();
                self.set.pop_first();
            }
        }
    }
}

pub(super) struct TimerManager {
    event_sender: UnboundedSender<TimerEvent>,
    cur_id: AtomicU64,
}

impl TimerManager {
    pub(super) fn new() -> Self {
        let timerfd = AsyncTimerFd::new().unwrap();
        let (event_sender, event_receiver) = unbounded_channel();
        let driver = TimerDriver {
            event_receiver,
            set: BTreeSet::new(),
        };
        #[cfg(debug_assertions)]
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(driver.serve(timerfd))
        });

        #[cfg(not(debug_assertions))]
        tokio::spawn(driver.serve(timerfd));

        Self {
            event_sender,
            cur_id: AtomicU64::new(0),
        }
    }

    pub(super) fn new_timer(&self, duration: Duration) -> Arc<Timer> {
        let id = self
            .cur_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Arc::new(Timer {
            expiration: Instant::now().checked_add(duration).unwrap(),
            id,
            waker: Mutex::new(None),
        })
    }

    pub(super) fn register_timer(&self, timer: Arc<Timer>) {
        self.event_sender.send(TimerEvent::Register(timer)).unwrap();
    }

    pub(super) fn unregister_timer(&self, timer: Arc<Timer>) {
        self.event_sender
            .send(TimerEvent::Unregister(timer))
            .unwrap();
    }
}
