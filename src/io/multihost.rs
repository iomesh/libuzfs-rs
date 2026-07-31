use std::{
    io::{Error, ErrorKind, Result},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::{
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, RawFd},
        unix::fs::FileExt,
    },
};

use crc32fast::Hasher;
use tokio::{
    sync::{Mutex, Notify},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::scsi::RECORD_OFFSET;
use super::scsi::{AlignedScsiBuffer, ScsiDevFile, RECORD_SIZE};

const MAX_HOSTID_LEN: usize = 128;

const MULTIHOST_VERSION: u64 = 1;

const MULTIHOST_MAGIC: &[u8; 8] = b"MULTHST\0";

const UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const IMPORT_DELAY: Duration = Duration::from_secs(8);
const FAILED_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    hostid: String,
    timestamp: u64,
    active: bool,
}

impl Record {
    fn decode(mut buffer: &[u8]) -> Result<Option<Self>> {
        buffer = &buffer[..RECORD_SIZE];

        if buffer.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }

        let expected_checksum = u32::from_be_bytes(buffer[..4].try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(&buffer[4..]);
        let checksum = hasher.finalize();
        if checksum != expected_checksum {
            return Err(Error::new(ErrorKind::InvalidData, "invalid checksum"));
        }

        buffer = &buffer[4..];
        if &buffer[..8] != MULTIHOST_MAGIC {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid multihost magic",
            ));
        }

        buffer = &buffer[8..];
        let version = u64::from_be_bytes(buffer[..8].try_into().unwrap());
        if version != MULTIHOST_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid multihost version",
            ));
        }

        buffer = &buffer[8..];
        let timestamp = u64::from_be_bytes(buffer[..8].try_into().unwrap());
        buffer = &buffer[8..];
        let active = u32::from_be_bytes(buffer[..4].try_into().unwrap()) != 0;
        buffer = &buffer[4..];
        let name_len = u32::from_be_bytes(buffer[..4].try_into().unwrap());
        if name_len > MAX_HOSTID_LEN as u32 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "hostid length exceeds maximum",
            ));
        }

        buffer = &buffer[4..];
        let hostid = String::from_utf8(buffer[..name_len as usize].to_vec())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        Ok(Some(Self {
            hostid,
            timestamp,
            active,
        }))
    }

    fn encode_to(&self, mut buffer: &mut [u8]) {
        debug_assert!(buffer.len() >= RECORD_SIZE);
        buffer = &mut buffer[..RECORD_SIZE];

        let mut offset = 4;
        buffer[offset..offset + 8].copy_from_slice(MULTIHOST_MAGIC);
        offset += 8;

        buffer[offset..offset + 8].copy_from_slice(&MULTIHOST_VERSION.to_be_bytes());
        offset += 8;

        buffer[offset..offset + 8].copy_from_slice(&self.timestamp.to_be_bytes());
        offset += 8;

        let active: u32 = if self.active { 1 } else { 0 };
        buffer[offset..offset + 4].copy_from_slice(&active.to_be_bytes());
        offset += 4;

        let name_len = self.hostid.len() as u32;
        buffer[offset..offset + 4].copy_from_slice(&name_len.to_be_bytes());
        offset += 4;

        buffer[offset..(offset + name_len as usize)].copy_from_slice(self.hostid.as_bytes());

        let mut hasher = Hasher::new();
        hasher.update(&buffer[4..]);
        let checksum = hasher.finalize();
        buffer[0..4].copy_from_slice(&checksum.to_be_bytes());
    }
}

#[cfg(test)]
struct FileLockGuard {
    fd: RawFd,
}

#[cfg(test)]
impl FileLockGuard {
    fn acquire(file: &File, operation: libc::c_int) -> Result<Self> {
        let fd = file.as_raw_fd();
        if unsafe { libc::flock(fd, operation) } < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(Self { fd })
        }
    }
}

#[cfg(test)]
impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.fd, libc::LOCK_UN) };
    }
}

#[cfg(test)]
struct TestFileDevice {
    file: File,
}

#[cfg(test)]
impl TestFileDevice {
    fn new(path: &str) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() < RECORD_OFFSET + RECORD_SIZE as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "file device is too small for the multihost record",
            ));
        }
        Ok(Self { file })
    }

    fn read_exact_at(&self, mut buffer: &mut [u8], mut offset: u64) -> Result<()> {
        while !buffer.is_empty() {
            match self.file.read_at(buffer, offset) {
                Ok(0) => return Err(Error::from(ErrorKind::UnexpectedEof)),
                Ok(read) => {
                    buffer = &mut buffer[read..];
                    offset += read as u64;
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn write_all_at(&self, mut buffer: &[u8], mut offset: u64) -> Result<()> {
        while !buffer.is_empty() {
            match self.file.write_at(buffer, offset) {
                Ok(0) => return Err(Error::from(ErrorKind::WriteZero)),
                Ok(written) => {
                    buffer = &buffer[written..];
                    offset += written as u64;
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn read_block(&self) -> Result<AlignedScsiBuffer> {
        let _guard = FileLockGuard::acquire(&self.file, libc::LOCK_SH)?;
        let mut transfer = AlignedScsiBuffer::new(RECORD_SIZE * 2);
        self.read_exact_at(transfer.slice_mut_from(0, RECORD_SIZE), RECORD_OFFSET)?;
        Ok(transfer)
    }

    fn compare_and_write_block(&self, transfer: AlignedScsiBuffer) -> Result<()> {
        let _guard = FileLockGuard::acquire(&self.file, libc::LOCK_EX)?;
        let mut current = [0_u8; RECORD_SIZE];
        self.read_exact_at(&mut current, RECORD_OFFSET)?;
        if current != transfer.slice_from(0, RECORD_SIZE) {
            return Err(Error::from_raw_os_error(libc::EREMOTEIO));
        }
        self.write_all_at(transfer.slice_from(RECORD_SIZE, RECORD_SIZE), RECORD_OFFSET)
    }
}

enum MultihostDevice {
    Scsi(ScsiDevFile),
    #[cfg(test)]
    File(TestFileDevice),
}

impl MultihostDevice {
    async fn new(path: &str) -> Result<Option<Self>> {
        if let Some(file) = ScsiDevFile::new(path).await? {
            return Ok(Some(Self::Scsi(file)));
        }

        #[cfg(test)]
        {
            return Ok(Some(Self::File(TestFileDevice::new(path)?)));
        }

        #[cfg(not(test))]
        Ok(None)
    }

    fn logical_block_size(&self) -> usize {
        match self {
            Self::Scsi(file) => file.logical_block_size,
            #[cfg(test)]
            Self::File(_) => RECORD_SIZE,
        }
    }

    async fn read_block(&self) -> Result<AlignedScsiBuffer> {
        match self {
            Self::Scsi(file) => file.sg_read_block().await,
            #[cfg(test)]
            Self::File(file) => file.read_block(),
        }
    }

    async fn compare_and_write_block(&self, transfer: AlignedScsiBuffer) -> Result<()> {
        match self {
            Self::Scsi(file) => file.sg_compare_and_write_block(transfer).await,
            #[cfg(test)]
            Self::File(file) => file.compare_and_write_block(transfer),
        }
    }
}

struct Checker {
    hostid: String,
    file: Option<MultihostDevice>,
}

impl Checker {
    async fn new(hostid: String, path: &str) -> Result<Self> {
        if hostid.len() > MAX_HOSTID_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "hostid length exceeds maximum",
            ));
        }

        let file = MultihostDevice::new(path).await?;

        Ok(Self { hostid, file })
    }

    async fn check(mut self) -> Result<Option<Writer>> {
        let Some(file) = self.file.take() else {
            return Ok(None);
        };

        let mut transfer = file.read_block().await?;
        let mut record = Record::decode(transfer.slice_from(0, RECORD_SIZE))?;

        let before = Instant::now();
        while before.elapsed() < IMPORT_DELAY {
            match &record {
                Some(record) if record.hostid != self.hostid && record.active => {}
                _ => break,
            }

            println!("current record: {:?}, waiting", record);

            sleep(UPDATE_INTERVAL).await;
            let current_transfer = file.read_block().await?;
            let current_record = Record::decode(current_transfer.slice_from(0, RECORD_SIZE))?;
            if current_record != record {
                return Err(Error::from_raw_os_error(libc::EREMOTEIO));
            }

            transfer = current_transfer;
            record = current_record;
        }

        let last_ts = record.map(|r| r.timestamp).unwrap_or(0);

        let record = Record {
            hostid: self.hostid.clone(),
            timestamp: last_ts + 1,
            active: true,
        };
        let logical_block_size = file.logical_block_size();
        record.encode_to(transfer.slice_mut_from(logical_block_size, logical_block_size));

        file.compare_and_write_block(transfer).await?;

        Ok(Some(Writer {
            hostid: self.hostid,
            file,
            next_id: last_ts + 2,
        }))
    }
}

struct Writer {
    hostid: String,
    file: MultihostDevice,
    next_id: u64,
}

impl Writer {
    async fn write_record(&mut self, active: bool) -> Result<()> {
        let lbs = self.file.logical_block_size();
        let mut old_buffer = AlignedScsiBuffer::new(lbs * 2);
        let record = Record {
            hostid: self.hostid.clone(),
            timestamp: self.next_id - 1,
            active: true,
        };
        record.encode_to(old_buffer.slice_mut_from(0, lbs));

        let mut buffer = AlignedScsiBuffer::new(lbs);
        let record = Record {
            hostid: self.hostid.clone(),
            timestamp: self.next_id,
            active,
        };
        self.next_id += 1;
        record.encode_to(buffer.slice_mut_from(lbs, lbs));

        self.file.compare_and_write_block(buffer).await
    }

    async fn update_record(mut self, token: CancellationToken, notify: Arc<Notify>) {
        let mut last_success = Instant::now();
        let fut = async {
            loop {
                let now = Instant::now();
                let Some(remained) = FAILED_DELAY.checked_sub(now - last_success) else {
                    eprintln!(
                        "failed to update record for host {} after {:?}",
                        self.hostid, FAILED_DELAY
                    );
                    std::process::abort();
                };

                let res = timeout(remained, self.write_record(true)).await;
                match res {
                    Ok(Ok(_)) => {
                        last_success = now;
                        sleep(UPDATE_INTERVAL).await;
                    }
                    Err(_) => {
                        eprintln!(
                            "failed to update record for host {} after {:?}",
                            self.hostid, FAILED_DELAY
                        );
                        std::process::abort();
                    }
                    Ok(Err(err)) => {
                        if err.raw_os_error().unwrap() != libc::EREMOTEIO {
                            self.next_id -= 1;
                        }
                        eprintln!("failed to update record for host {}: {err}", self.hostid);
                    }
                }
            }
        };

        token.run_until_cancelled(fut).await;
        let _ = self.write_record(false).await;
        notify.notify_one();
    }
}

pub(crate) struct MultiHostProtector {
    notify: Mutex<Option<Arc<Notify>>>,
    token: CancellationToken,
}

impl MultiHostProtector {
    pub(crate) async fn new(hostid: String, path: &str) -> Result<Self> {
        let checker = Checker::new(hostid, path).await?;
        let token = CancellationToken::new();
        let Some(writer) = checker.check().await? else {
            return Ok(Self {
                notify: Mutex::new(None),
                token,
            });
        };

        let notify = Arc::new(Notify::new());

        let token2 = token.clone();
        let notify2 = notify.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(writer.update_record(token2, notify2));
        });

        Ok(Self {
            notify: Mutex::new(Some(notify)),
            token,
        })
    }

    pub async fn exit(&self) {
        self.token.cancel();
        if let Some(notify) = self.notify.lock().await.take() {
            notify.notified().await;
        }
    }
}
