use super::aio::*;
use crate::context::coroutine::AsyncCoroutine;
use kanal::*;
use libc::timespec;
use std::{
    io::{Error, ErrorKind},
    sync::{
        atomic::{fence, AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
};
use tokio::runtime::Handle;

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
            iocb_ptrs.len() - nsubmitted,
            iocb_ptrs.as_ptr().add(nsubmitted),
        );
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

#[derive(Default)]
pub(super) struct IoContent {
    pub(super) data_ptr: usize,
    pub(super) offset: u64,
    pub(super) size: u64,
}

pub(super) enum IoType {
    Read,
    Write,
    Sync,
}

pub(super) struct AioCallback {
    pub(super) io_type: IoType,
    pub(super) io_content: IoContent,
    pub(super) arg: *mut libc::c_void,
}

unsafe impl Send for AioCallback {}
unsafe impl Sync for AioCallback {}

impl AioCallback {
    fn into_iocb(self, io_fd: i32) -> iocb {
        let opcode = match self.io_type {
            IoType::Read => IOCB_CMD_PREAD,
            IoType::Write => IOCB_CMD_PWRITE,
            IoType::Sync => IOCB_CMD_FSYNC,
        };

        iocb {
            aio_data: self.arg as u64,
            aio_key: 0,
            aio_rw_flags: 0,
            aio_lio_opcode: opcode as u16,
            aio_reqprio: 0,
            aio_fildes: io_fd as u32,
            aio_buf: self.io_content.data_ptr as u64,
            aio_nbytes: self.io_content.size,
            aio_offset: self.io_content.offset as i64,
            aio_reserved2: 0,
            aio_flags: 0,
            aio_resfd: 0,
        }
    }
}

struct IoCompletions {
    completions: Vec<io_event>,
    cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
}

unsafe impl Send for IoCompletions {}
unsafe impl Sync for IoCompletions {}

unsafe extern "C" fn process_completion(arg: *mut libc::c_void) {
    let completions = &*(arg as *const IoCompletions);
    for completion in &completions.completions {
        (completions.cb)(completion.data as *mut libc::c_void, completion.res);
    }
}

pub(super) struct AioContex {
    pub(super) sender: Sender<AioCallback>,
    reaper: Option<JoinHandle<()>>,
    submitter: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    io_ctx: aio_context_t,
}

const MAX_EVENTS: usize = 4096;
const MAX_IDLE_MILLS: u64 = 10;

impl AioContex {
    pub(super) fn submit(io_fd: i32, receiver: Receiver<AioCallback>, io_ctx: aio_context_t) {
        loop {
            match receiver.recv() {
                Ok(task) => {
                    let mut iocbs = Vec::with_capacity(MAX_EVENTS);
                    iocbs.push(task.into_iocb(io_fd));

                    while let Ok(Some(task)) = receiver.try_recv() {
                        iocbs.push(task.into_iocb(io_fd));
                    }

                    unsafe { io_submit(io_ctx, &iocbs).unwrap() };
                }
                _ => return,
            }
        }
    }

    pub fn reap(
        io_ctx: aio_context_t,
        stop: Arc<AtomicBool>,
        handle: Handle,
        cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
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
                    let mut completions = IoCompletions { completions, cb };
                    let arg = &mut completions as *mut _ as usize;
                    AsyncCoroutine::new(process_completion, arg, true).await;
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
        cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
    ) -> Result<Self, Error> {
        let io_ctx = unsafe { io_setup(256)? };
        let (sender, receiver) = unbounded();
        let submitter = std::thread::spawn(move || Self::submit(io_fd, receiver, io_ctx));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_cloned = stop.clone();
        let handle = Handle::current();
        let reaper = std::thread::spawn(move || {
            Self::reap(io_ctx, stop_cloned, handle, cb).unwrap();
        });
        Ok(Self {
            sender,
            reaper: Some(reaper),
            submitter: Some(submitter),
            stop,
            io_ctx,
        })
    }
}

impl Drop for AioContex {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.sender.close();
        fence(Ordering::SeqCst);
        self.submitter.take().unwrap().join().unwrap();
        self.reaper.take().unwrap().join().unwrap();
        unsafe { io_destroy(self.io_ctx).unwrap() }
    }
}
