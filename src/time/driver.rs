use std::{
    cmp::Ordering,
    collections::BTreeSet,
    mem::MaybeUninit,
    sync::{atomic::AtomicU64, Arc, Mutex},
    task::Waker,
};

use libc::timespec;
use tokio::{
    io::AsyncReadExt,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

use super::timer_fd::AsyncTimerFd;

const NANOSECS: i64 = 1000000000;

fn compare_ts(me: &timespec, other: &timespec) -> Ordering {
    match me.tv_sec.cmp(&other.tv_sec) {
        Ordering::Equal => me.tv_nsec.cmp(&other.tv_nsec),
        other => other,
    }
}

pub(super) fn duration_from_now(duration: &timespec) -> timespec {
    let mut ts = MaybeUninit::uninit();
    let res = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, ts.as_mut_ptr()) };

    assert_eq!(res, 0);
    let mut ts = unsafe { ts.assume_init() };

    ts.tv_sec += duration.tv_sec;
    ts.tv_nsec += duration.tv_nsec;
    ts.tv_sec += ts.tv_nsec / NANOSECS;
    ts.tv_nsec %= NANOSECS;

    ts
}

pub(super) struct Timer {
    expiration: timespec,
    id: u64,
    pub(super) waker: Mutex<Option<Waker>>,
}

impl Timer {
    pub(super) fn expired(&self) -> bool {
        let now = duration_from_now(&timespec {
            tv_sec: 0,
            tv_nsec: 0,
        });
        compare_ts(&now, &self.expiration).is_ge()
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
        match compare_ts(&self.expiration, &other.expiration) {
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
                _ = timerfd.read_u64() => {}
            }

            let now = duration_from_now(&timespec {
                tv_sec: 0,
                tv_nsec: 0,
            });
            while let Some(first) = self.set.first() {
                if compare_ts(&now, &first.expiration).is_ge() {
                    first.waker.lock().unwrap().take().unwrap().wake_by_ref();
                    self.set.pop_first();
                } else {
                    break;
                }
            }

            if let Some(first) = self.set.first() {
                unsafe { timerfd.set_next_wakeup(first.expiration).unwrap() };
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

    pub(super) fn new_timer(&self, expiration: timespec) -> Arc<Timer> {
        let id = self
            .cur_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Arc::new(Timer {
            expiration,
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
