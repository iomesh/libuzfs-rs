use super::aio::*;
use crate::context::coroutine::CoroutineFuture;
use libc::{c_char, c_int, c_void, timespec};
use std::{
    io::{Error, ErrorKind},
    ptr::null_mut,
    sync::{
        atomic::{fence, AtomicBool, AtomicPtr, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
};
use tokio::runtime::Handle;

type DoneFunc = unsafe extern "C" fn(arg: *mut libc::c_void, res: i64);
type InitArgsFunc =
    unsafe extern "C" fn(*mut c_void, *mut u64, *mut *mut c_char, *mut usize) -> c_int;

#[inline]
unsafe fn io_setup(nr_events: i64) -> Result<aio_context_t, Error> {
    let mut io_ctx: aio_context_t = 0;
    let res = libc::syscall(
        libc::SYS_io_setup,
        nr_events,
        &mut io_ctx as *mut aio_context_t,
    );

    if res == 0 {
        Ok(io_ctx)
    } else {
        Err(Error::last_os_error())
    }
}

#[inline]
unsafe fn io_destroy(io_ctx: aio_context_t) -> Result<(), Error> {
    if libc::syscall(libc::SYS_io_destroy, io_ctx) != 0 {
        Err(Error::last_os_error())
    } else {
        Ok(())
    }
}

#[inline]
unsafe fn io_submit(io_ctx: aio_context_t, iocbs: &[iocb]) -> Result<(), Error> {
    if iocbs.is_empty() {
        return Ok(());
    }

    let iocb_ptrs: Vec<_> = iocbs.iter().map(|iocb| iocb as *const iocb).collect();
    let mut nsubmitted = 0;
    while nsubmitted < iocb_ptrs.len() {
        let ret = libc::syscall(
            libc::SYS_io_submit,
            io_ctx,
            1,
            iocb_ptrs.as_ptr().add(nsubmitted),
        );
        assert!(ret != 0);
        if ret > 0 {
            nsubmitted += ret as usize;
            continue;
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted && err.kind() != ErrorKind::WouldBlock {
            return Err(err);
        }
    }

    Ok(())
}

const SEM_CLOSED: usize = usize::MAX;

struct Semaphore {
    count: Mutex<usize>,
    cond: Condvar,
}

impl Semaphore {
    fn new(count: usize) -> Self {
        Self {
            count: Mutex::new(count),
            cond: Condvar::new(),
        }
    }

    fn post(&self) {
        let mut count = self.count.lock().unwrap();
        *count += 1;
        self.cond.notify_all();
    }

    fn close(&self) {
        let mut count = self.count.lock().unwrap();
        *count = SEM_CLOSED;
        self.cond.notify_all();
    }

    fn acquire_all(&self) -> Result<(), ()> {
        let mut count = self.count.lock().unwrap();
        loop {
            if *count == SEM_CLOSED {
                return Err(());
            }

            if *count > 0 {
                *count = 0;
                return Ok(());
            }

            count = self.cond.wait(count).unwrap();
        }
    }
}

const AIO_READ: c_int = 0;
const AIO_WRITE: c_int = 1;
const AIO_FSYNC: c_int = 2;

pub(super) struct TaskList {
    head: AtomicPtr<c_void>,
    sem: Semaphore,
    next_off: usize,
    init_io_arg: InitArgsFunc,
    io_fd: i32,
}

impl TaskList {
    pub(super) fn new_arc(next_off: usize, init_io_arg: InitArgsFunc, io_fd: i32) -> Arc<Self> {
        Arc::new(Self {
            head: AtomicPtr::new(null_mut()),
            sem: Semaphore::new(0),
            next_off,
            init_io_arg,
            io_fd,
        })
    }

    #[inline]
    pub(super) unsafe fn push(&self, arg: *mut c_void) {
        let next = arg.byte_add(self.next_off) as *mut *mut c_void;

        // Fetch the current head and update it with the new task node
        // `Ordering::Release` ensures that the writes to the task node (like `(*task).next`)
        // happen before updating the head pointer.
        // This prevents other threads from seeing an incomplete task node with an uninitialized `next`.
        let was_empty = self
            .head
            .fetch_update(Ordering::Release, Ordering::Relaxed, |ptr| {
                *next = ptr;
                Some(arg)
            })
            .unwrap()
            .is_null();

        if was_empty {
            self.sem.post();
        }
    }

    fn close(&self) {
        self.sem.close();
    }

    #[inline]
    unsafe fn pop_all(&self) -> Option<Vec<iocb>> {
        if self.sem.acquire_all().is_err() {
            return None;
        }

        // Use `swap` with `Ordering::Acquire` to read and clear the head pointer.
        // `Ordering::Acquire` ensures that all operations before this load (such as the writes to the task list)
        // are visible to this thread. Without this, we might see stale data or incomplete updates to the list.
        let mut cur = self.head.swap(null_mut(), Ordering::Acquire);

        let mut res = Vec::with_capacity(256);
        while !cur.is_null() {
            let mut off = 0;
            let mut data = null_mut();
            let mut len = 0;
            let io_type = (self.init_io_arg)(cur, &mut off, &mut data, &mut len);

            let opcode = match io_type {
                AIO_READ => IOCB_CMD_PREAD,
                AIO_WRITE => IOCB_CMD_PWRITE,
                AIO_FSYNC => IOCB_CMD_FSYNC,
                _ => unimplemented!(),
            };

            res.push(iocb {
                aio_data: cur as u64,
                aio_key: 0,
                aio_rw_flags: 0,
                aio_lio_opcode: opcode as u16,
                aio_reqprio: 0,
                aio_fildes: self.io_fd as u32,
                aio_buf: data as u64,
                aio_nbytes: len as u64,
                aio_offset: off as i64,
                aio_reserved2: 0,
                aio_flags: 0,
                aio_resfd: 0,
            });

            cur = *(cur.byte_add(self.next_off) as *mut *mut c_void);
        }

        Some(res)
    }
}

impl Drop for TaskList {
    fn drop(&mut self) {
        assert!(self.head.load(Ordering::Relaxed).is_null());
    }
}

unsafe impl Send for TaskList {}
unsafe impl Sync for TaskList {}

struct IoCompletions {
    completions: Vec<io_event>,
    io_done: DoneFunc,
}

unsafe impl Send for IoCompletions {}
unsafe impl Sync for IoCompletions {}

unsafe extern "C" fn process_completion(arg: *mut libc::c_void) {
    let completions = &*(arg as *const IoCompletions);
    for completion in &completions.completions {
        (completions.io_done)(completion.data as *mut libc::c_void, completion.res);
    }
}

pub(super) struct AioContext {
    pub(super) task_list: Arc<TaskList>,
    reaper: Option<JoinHandle<()>>,
    submitter: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    io_ctx: aio_context_t,
}

const MAX_EVENTS: usize = 4096;
const MAX_IDLE_MILLS: u64 = 10;

impl AioContext {
    pub(super) fn submit(task_list: Arc<TaskList>, io_ctx: aio_context_t) {
        while let Some(tasks) = unsafe { task_list.pop_all() } {
            unsafe { io_submit(io_ctx, &tasks).unwrap() };
        }
    }

    pub fn reap(
        io_ctx: aio_context_t,
        stop: Arc<AtomicBool>,
        handle: Handle,
        io_done: DoneFunc,
    ) -> Result<(), Error> {
        while !stop.load(Ordering::Acquire) {
            let mut ts = timespec {
                tv_sec: 0,
                tv_nsec: MAX_IDLE_MILLS as i64 * 1000000,
            };

            let mut completions: Vec<io_event> = Vec::with_capacity(MAX_EVENTS);
            let ret = unsafe {
                libc::syscall(
                    libc::SYS_io_getevents,
                    io_ctx,
                    1,
                    MAX_EVENTS,
                    completions.as_mut_ptr(),
                    &mut ts,
                )
            };

            if ret > 0 {
                handle.block_on(async move {
                    unsafe { completions.set_len(ret as usize) };
                    let mut completions = IoCompletions {
                        completions,
                        io_done,
                    };
                    let arg = &mut completions as *mut _ as usize;
                    CoroutineFuture::new(process_completion, arg).await;
                });
                continue;
            }

            if ret == 0 {
                continue;
            }

            let err = Error::last_os_error();
            if err.kind() != ErrorKind::Interrupted
                && err.kind() != ErrorKind::WouldBlock
                && err.kind() != ErrorKind::TimedOut
            {
                return Err(err);
            }
        }

        Ok(())
    }

    pub(super) fn start(
        io_fd: i32,
        io_done: DoneFunc,
        next_off: usize,
        init_io_args: InitArgsFunc,
    ) -> Result<Self, Error> {
        let io_ctx = unsafe { io_setup(256)? };
        let task_list = TaskList::new_arc(next_off, init_io_args, io_fd);
        let task_list_cloned = task_list.clone();
        let submitter = std::thread::spawn(move || Self::submit(task_list_cloned, io_ctx));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_cloned = stop.clone();
        let handle = Handle::current();
        let reaper = std::thread::spawn(move || {
            Self::reap(io_ctx, stop_cloned, handle, io_done).unwrap();
        });
        Ok(Self {
            task_list: task_list,
            reaper: Some(reaper),
            submitter: Some(submitter),
            stop,
            io_ctx,
        })
    }
}

impl Drop for AioContext {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.task_list.close();
        fence(Ordering::SeqCst);
        self.submitter.take().unwrap().join().unwrap();
        self.reaper.take().unwrap().join().unwrap();
        unsafe { io_destroy(self.io_ctx).unwrap() }
    }
}
