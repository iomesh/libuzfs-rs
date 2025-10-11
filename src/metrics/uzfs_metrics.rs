use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::{
    family::Family,
    gauge::Gauge,
    histogram::{exponential_buckets, Histogram},
};
use std::ptr::slice_from_raw_parts;
use std::time::Instant;
use strum_macros::Display;
use strum_macros::EnumIter;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue, EnumIter, Display)]
pub enum RequestMethod {
    CreateObjects,
    DeleteObject,
    WaitLogCommit,
    GetObjectAttr,
    ReadObject,
    WriteObject,
    SyncObject,
    CreateInode,
    DeleteInode,
    GetAttr,
    SetAttr,
    GetKvattr,
    SetKvattr,
    CreateDentry,
    DeleteDentry,
    LookupDentry,
    IterateDentry,
    WaitSynced,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    pub method: RequestMethod,
}

#[derive(Clone)]
pub struct RequestMetrics {
    pub request_size_bytes: Family<RequestLabels, Histogram>,
    pub request_latency_ns: Family<RequestLabels, Histogram>,
    pub request_concurrency: Family<RequestLabels, Gauge>,
}

impl RequestMetrics {
    fn new() -> Self {
        let request_size_bytes = Family::<RequestLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 32))
        });
        let request_latency_ns = Family::<RequestLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 40))
        });
        let request_concurrency =
            Family::<RequestLabels, Gauge>::new_with_constructor(Gauge::default);

        Self {
            request_size_bytes,
            request_latency_ns,
            request_concurrency,
        }
    }
}

pub(crate) struct MetricsGuard<'a> {
    metrics: &'a RequestMetrics,
    begin_instant: Instant,
    request_size: usize,
    method: RequestMethod,
}

impl<'a> MetricsGuard<'a> {
    fn new(method: RequestMethod, request_size: usize, metrics: &'a RequestMetrics) -> Self {
        metrics
            .request_concurrency
            .get_or_create(&RequestLabels { method })
            .inc();

        MetricsGuard {
            metrics,
            begin_instant: Instant::now(),
            request_size,
            method,
        }
    }
}

impl Drop for MetricsGuard<'_> {
    fn drop(&mut self) {
        let label = &RequestLabels {
            method: self.method,
        };
        self.metrics.request_concurrency.get_or_create(label).dec();
        self.metrics
            .request_size_bytes
            .get_or_create(label)
            .observe(self.request_size as f64);
        self.metrics
            .request_latency_ns
            .get_or_create(label)
            .observe(self.begin_instant.elapsed().as_nanos() as f64);
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue, EnumIter, Display)]
pub enum TxgStage {
    Birth,
    Open,
    Quiece,
    WaitForSync,
    Sync,
    Committed,
}

impl TryFrom<i32> for TxgStage {
    type Error = i32;
    fn try_from(value: i32) -> Result<Self, i32> {
        match value {
            0 => Ok(Self::Birth),
            1 => Ok(Self::Open),
            2 => Ok(Self::Quiece),
            3 => Ok(Self::WaitForSync),
            4 => Ok(Self::Sync),
            5 => Ok(Self::Committed),
            other => Err(other),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TxgLabel {
    pub stage: TxgStage,
}

const ZIO_STAGES: usize = 25;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue, EnumIter, Display)]
pub enum ZioStage {
    Open = 0,
    ReadBpInit,
    WriteBpInit,
    FreeBpInit,
    IssueAsync,
    WriteCompress,
    Encrypt,
    ChecksumGenerate,
    NopWrite,
    DdtReadStart,
    DdtReadDone,
    DdtWrite,
    DdtFree,
    GangAssemble,
    GangIssue,
    DvaThrottle,
    DvaAllocate,
    DvaFree,
    DvaClaim,
    Ready,
    VdevIoStart,
    VdevIoDone,
    VdevIoAssess,
    ChecksumVerify,
    Done,
}

impl TryFrom<usize> for ZioStage {
    type Error = usize;
    fn try_from(value: usize) -> Result<Self, usize> {
        match value {
            0 => Ok(Self::Open),
            1 => Ok(Self::ReadBpInit),
            2 => Ok(Self::WriteBpInit),
            3 => Ok(Self::FreeBpInit),
            4 => Ok(Self::IssueAsync),
            5 => Ok(Self::WriteCompress),
            6 => Ok(Self::Encrypt),
            7 => Ok(Self::ChecksumGenerate),
            8 => Ok(Self::NopWrite),
            9 => Ok(Self::DdtReadStart),
            10 => Ok(Self::DdtReadDone),
            11 => Ok(Self::DdtWrite),
            12 => Ok(Self::DdtFree),
            13 => Ok(Self::GangAssemble),
            14 => Ok(Self::GangIssue),
            15 => Ok(Self::DvaThrottle),
            16 => Ok(Self::DvaAllocate),
            17 => Ok(Self::DvaFree),
            18 => Ok(Self::DvaClaim),
            19 => Ok(Self::Ready),
            20 => Ok(Self::VdevIoStart),
            21 => Ok(Self::VdevIoDone),
            22 => Ok(Self::VdevIoAssess),
            23 => Ok(Self::ChecksumVerify),
            24 => Ok(Self::Done),
            other => Err(other),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ZioLabel {
    pub zio_stage: ZioStage,
}

#[derive(Clone)]
pub struct IoMetrics {
    pub txg_delays: Family<TxgLabel, Histogram>,
    pub read_delays: Family<ZioLabel, Histogram>,
    pub write_delays: Family<ZioLabel, Histogram>,
}

impl IoMetrics {
    fn new() -> Self {
        let txg_delays = Family::<TxgLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 40))
        });
        let read_delays = Family::<ZioLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 40))
        });
        let write_delays = Family::<ZioLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 30))
        });

        Self {
            txg_delays,
            read_delays,
            write_delays,
        }
    }

    fn record_txg_delay(&self, stage: TxgStage, delay_ns: u64) {
        self.txg_delays
            .get_or_create(&TxgLabel { stage })
            .observe(delay_ns as f64);
    }

    fn record_zio(&self, stages: &[i64], read: bool) {
        let mut lastidx = 0;
        let mut last = stages[lastidx];
        for (i, stage_start) in stages.iter().enumerate() {
            if i <= lastidx {
                continue;
            }

            let start = *stage_start;
            if start > 0 {
                let label = ZioLabel {
                    zio_stage: lastidx.try_into().unwrap(),
                };
                let histogram = if read {
                    &self.read_delays
                } else {
                    &self.write_delays
                };
                histogram
                    .get_or_create(&label)
                    .observe((start - last) as f64);
                lastidx = i;
                last = start;
            }
        }
    }
}

#[derive(Clone)]
pub struct UzfsMetrics {
    pub io_metrics: IoMetrics,
    pub req_metrics: RequestMetrics,
}

impl UzfsMetrics {
    pub fn new_boxed() -> Box<Self> {
        let io_metrics = IoMetrics::new();
        let req_metrics = RequestMetrics::new();

        Box::new(Self {
            io_metrics,
            req_metrics,
        })
    }

    pub(crate) fn record(&'_ self, method: RequestMethod, request_size: usize) -> MetricsGuard<'_> {
        MetricsGuard::new(method, request_size, &self.req_metrics)
    }
}

pub(crate) unsafe extern "C" fn record_txg_delay(
    metrics: *const libc::c_void,
    txg_stage: i32,
    delay_ns: u64,
) {
    let metrics = &*(metrics as *const UzfsMetrics);
    metrics
        .io_metrics
        .record_txg_delay(txg_stage.try_into().unwrap(), delay_ns);
}

pub(crate) unsafe extern "C" fn record_zio(
    metrics: *const libc::c_void,
    stages: *const i64,
    read: i32,
) {
    let metrics = &*(metrics as *const UzfsMetrics);
    let stages = &*slice_from_raw_parts(stages, ZIO_STAGES);
    metrics.io_metrics.record_zio(stages, read != 0);
}
