use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::{
    family::Family,
    gauge::Gauge,
    histogram::{exponential_buckets, Histogram},
};
use std::time::Instant;
use strum_macros::Display;
use strum_macros::EnumIter;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue, EnumIter, Display)]
pub enum Method {
    ReadObject,
    WriteObject,
    GetAttr,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct Labels {
    pub method: Method,
}
pub struct Metrics {
    pub request_size_bytes: Family<Labels, Histogram>,
    pub request_latency_ns: Family<Labels, Histogram>,
    pub request_concurrency: Family<Labels, Gauge>,
}
impl Metrics {
    pub(crate) fn record(&self, method: Method, request_size: usize) -> MetricsGuard {
        MetricsGuard::new(method, request_size, self)
    }
    pub(crate) fn new() -> Self {
        // let mut registry = <Registry>::default();
        let request_size_bytes = Family::<Labels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 32))
        });
        let request_latency_ns = Family::<Labels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 2.0, 40))
        });
        let request_concurrency = Family::<Labels, Gauge>::new_with_constructor(Gauge::default);
        /*
        registry.register(
            "request_size_bytes",
            "request size in bytes",
            request_size_bytes.clone(),
        );
        registry.register(
            "request_latency_ns",
            "request latency in nanoseconds",
            request_latency_ns.clone(),
        );
        registry.register(
            "request_concurrency",
            "number of concurrent in-flight requests",
            request_concurrency.clone(),
        );

        let registry = Arc::new(registry);
        */

        Metrics {
            request_size_bytes,
            request_latency_ns,
            request_concurrency,
        }
    }
}

pub(crate) struct MetricsGuard {
    request_size_bytes: Histogram,
    request_latency_ns: Histogram,
    request_concurrency: Family<Labels, Gauge>,
    begin_instant: Instant,
    request_size: usize,
    method: Method,
}

impl MetricsGuard {
    fn new(method: Method, request_size: usize, server: &Metrics) -> Self {
        let begin_instant = Instant::now();
        let request_size_bytes = server
            .request_size_bytes
            .get_or_create(&Labels { method })
            .clone();
        let request_latency_ns = server
            .request_latency_ns
            .get_or_create(&Labels { method })
            .clone();
        // Unlike Histogram, cloning a Gauge doesn't work because the inner numerical value is cloned.
        // Thus, we clone Family of Gauge, which essentially only clones an Arc.
        let request_concurrency = server.request_concurrency.clone();
        request_concurrency.get_or_create(&Labels { method }).inc();

        MetricsGuard {
            request_size_bytes,
            request_latency_ns,
            request_concurrency,
            begin_instant,
            request_size,
            method,
        }
    }
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        self.request_concurrency
            .get_or_create(&Labels {
                method: self.method,
            })
            .dec();
        self.request_size_bytes.observe(self.request_size as f64);
        self.request_latency_ns
            .observe(self.begin_instant.elapsed().as_nanos() as f64);
    }
}
