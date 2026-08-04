//! # Prometheus remote-write streaming output
//!
//! Pushes samples to a Prometheus remote-write endpoint
//! (e.g. `http://localhost:9090/api/v1/write`) as snappy-compressed
//! protobuf `WriteRequest` batches during the load test.
//!
//! Samples are **aggregated per time series per flush window**: raw
//! per-request samples would (a) push the same `(series, timestamp)` twice
//! when two requests land in the same millisecond — remote-write rejects
//! that with a 400 — and (b) balloon the payload to one sample per request.
//! Instead, each series collapses to ONE sample per flush: counters/rates
//! sum, points take the last value, and trends emit `_count`/`_sum`
//! sub-series (the Prometheus summary convention), all stamped with the
//! flush time.
//!
//! Samples are flushed every `FLUSH_INTERVAL` or when the number of series
//! exceeds `MAX_BUFFERED_SERIES`. A final flush happens when the sample
//! stream closes (test end).
//!
//! The wire format is the standard Prometheus remote-write protocol:
//! `Content-Encoding: snappy` + `Content-Type: application/x-protobuf` with
//! `X-Prometheus-Remote-Write-Version: 0.1.0`. The protobuf encoding for the
//! three small messages (`WriteRequest`, `TimeSeries`, `Label`, `Sample`) is
//! hand-rolled (no `prost`/`protobuf` dependency needed for such a tiny,
//! stable schema) and covered by round-trip decode tests.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tropel_core::types::{Sample, SampleType};
use tropel_core::{Result, TropelError};

use crate::output::TagPolicy;
use crate::Output;

/// How often buffered samples are flushed to the endpoint.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Max distinct series before a forced flush (aggregation keeps one sample
/// per series per window, so this bounds payload by cardinality, not volume).
const MAX_BUFFERED_SERIES: usize = 10_000;
/// Remote-write URL path appended when the user gives only a base URL.
const REMOTE_WRITE_PATH: &str = "/api/v1/write";

/// Prometheus remote-write output.
///
/// Create one with [`PrometheusRemoteWriteOutput::new`] and either drive it
/// through the [`Output`] trait or spawn the engine-facing consumer task
/// with [`PrometheusRemoteWriteOutput::spawn`] (the pattern used by
/// [`crate::StreamingStdoutOutput`]).
pub struct PrometheusRemoteWriteOutput {
    url: String,
    client: reqwest::Client,
    /// Per-series aggregation accumulated during the current flush window.
    series: Mutex<HashMap<SeriesKey, SeriesAgg>>,
    /// Number of series currently buffered (fast read without the lock).
    total_buffered: AtomicUsize,
    /// Tag forwarding policy (allowlist + cardinality cap).
    tag_policy: TagPolicy,
}

/// Per-series aggregation for the current flush window.
#[derive(Debug, Clone)]
struct SeriesAgg {
    sample_type: SampleType,
    count: u64,
    sum: f64,
    last: f64,
}

/// A single time series identity: metric name plus sorted (name, value) labels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    metric: String,
    /// Sorted label pairs including `__name__` = metric. `__name__` is NOT
    /// hardcoded first — it is part of the sort, because a tag whose name
    /// byte-sorts before it (e.g. `__custom`, `_a`) must precede it for the
    /// label set to be in canonical Prometheus order (sorted by name).
    labels: Vec<(String, String)>,
}

impl SeriesKey {
    fn from_parts(metric: &str, tags: &tropel_core::types::TagMap) -> Self {
        let mut labels: Vec<(String, String)> = tags
            .iter()
            .filter(|(k, _)| k != &"__name__") // avoid duplicate __name__ label
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        labels.push(("__name__".to_string(), metric.to_string()));
        labels.sort();
        Self {
            metric: metric.to_string(),
            labels,
        }
    }
}

impl PrometheusRemoteWriteOutput {
    /// Create a new remote-write output pushing to `url`.
    ///
    /// The URL may be a full endpoint (`http://host:9090/api/v1/write`) or a
    /// bare base — `/api/v1/write` is appended when missing.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: normalize_remote_write_url(&url.into()),
            client: reqwest::Client::new(),
            series: Mutex::new(HashMap::new()),
            total_buffered: AtomicUsize::new(0),
            tag_policy: TagPolicy::default(),
        }
    }

    /// Set the tag forwarding policy (allowlist + cardinality cap).
    pub fn with_tag_policy(mut self, policy: TagPolicy) -> Self {
        self.tag_policy = policy;
        self
    }

    /// Spawn a consumer task that pushes samples to the endpoint.
    ///
    /// Subscribes to the metrics collector's broadcast stream, buffers
    /// samples, flushes every `FLUSH_INTERVAL` (and when the number of
    /// series exceeds `MAX_BUFFERED_SERIES`), and performs a final flush
    /// when the channel closes (test end). Returns a `JoinHandle` that
    /// completes when the sender is dropped.
    pub fn spawn(
        mut rx: broadcast::Receiver<Sample>,
        url: String,
        tag_policy: TagPolicy,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let output = PrometheusRemoteWriteOutput::new(url).with_tag_policy(tag_policy);
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(sample) => {
                            output.buffer(&sample);
                            if output.total_buffered.load(Ordering::Relaxed) >= MAX_BUFFERED_SERIES {
                                if let Err(e) = output.flush().await {
                                    tracing::warn!("prometheus remote-write flush failed: {e}");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("prometheus output dropped {n} samples (consumer lag)");
                        }
                    },
                    _ = tick.tick() => {
                        if output.total_buffered.load(Ordering::Relaxed) > 0 {
                            if let Err(e) = output.flush().await {
                                tracing::warn!("prometheus remote-write flush failed: {e}");
                            }
                        }
                    }
                }
            }

            // Final flush on stream close.
            if let Err(e) = output.flush().await {
                tracing::warn!("prometheus remote-write final flush failed: {e}");
            }
        })
    }

    /// Buffer a sample into its per-series aggregation for this flush window.
    /// The sample's tags pass through the tag policy first (allowlist + cap).
    /// `total_buffered` tracks DISTINCT series (only bumped on first sight of
    /// a series), so the forced-flush threshold bounds cardinality — the
    /// thing that actually determines payload size after aggregation.
    fn buffer(&self, sample: &Sample) {
        let tags = self.tag_policy.apply(&sample.tags);
        let key = SeriesKey::from_parts(&sample.metric, &tags);
        let mut series = self.series.lock().unwrap();
        match series.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let agg = e.get_mut();
                agg.count += 1;
                agg.sum += sample.value;
                agg.last = sample.value;
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(SeriesAgg {
                    sample_type: sample.sample_type.clone(),
                    count: 1,
                    sum: sample.value,
                    last: sample.value,
                });
                self.total_buffered.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Drain the buffer, encode a snappy-compressed `WriteRequest`, and POST
    /// it to the remote-write endpoint. Non-2xx responses are logged, not
    /// fatal (the run must not fail because a dashboard is down).
    async fn flush(&self) -> Result<()> {
        let series = {
            let mut guard = self.series.lock().unwrap();
            let taken = std::mem::take(&mut *guard);
            // Reset the counter inside the lock so a concurrent `buffer()`
            // cannot push a series after `mem::take` but before the reset,
            // leaving the map non-empty with the counter at 0 (the 5s tick's
            // `> 0` check would then skip a flush until the next sample).
            self.total_buffered.store(0, Ordering::Relaxed);
            taken
        };
        if series.is_empty() {
            return Ok(());
        }

        // Aggregate per series for THIS flush window into the wire map. One
        // sample per series (stamped at flush time) means a remote-write
        // request can never contain duplicate (series, timestamp) — the 400
        // cause — and the payload is bounded by cardinality, not request
        // volume.
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let wire_series: HashMap<SeriesKey, Vec<(f64, i64)>> = series
            .iter()
            .flat_map(|(key, agg)| expand_series(key, agg, ts_ms))
            .collect();

        let payload = encode_write_request(&wire_series);
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&payload)
            .map_err(|e| TropelError::Report(format!("snappy compress failed: {e}")))?;

        let resp = self
            .client
            .post(&self.url)
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(compressed)
            .send()
            .await
            .map_err(|e| TropelError::Http(format!("remote-write POST failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!("remote-write rejected ({status}): {body}");
        }
        Ok(())
    }
}

#[async_trait]
impl Output for PrometheusRemoteWriteOutput {
    fn name(&self) -> &str {
        "prometheus-remote-write"
    }

    async fn sample(&self, samples: &[Sample]) -> Result<()> {
        for sample in samples {
            self.buffer(sample);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.flush().await
    }
}

/// Append `/api/v1/write` to a bare base URL.
fn normalize_remote_write_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with(REMOTE_WRITE_PATH) {
        trimmed.to_string()
    } else {
        format!("{trimmed}{REMOTE_WRITE_PATH}")
    }
}

/// Expand one flush-window aggregation into wire series (one sample per
/// series, all stamped with the flush time `ts_ms`):
/// - Counter/Rate → the window sum
/// - Point → the last observed value
/// - Trend → two series, `{metric}_count` and `{metric}_sum` (the Prometheus
///   summary convention, so backends can derive mean/p50-style aggregates)
fn expand_series(
    key: &SeriesKey,
    agg: &SeriesAgg,
    ts_ms: i64,
) -> Vec<(SeriesKey, Vec<(f64, i64)>)> {
    match agg.sample_type {
        SampleType::Counter | SampleType::Rate => {
            vec![(key.clone(), vec![(agg.sum, ts_ms)])]
        }
        SampleType::Point => {
            vec![(key.clone(), vec![(agg.last, ts_ms)])]
        }
        SampleType::Trend => {
            // `{metric}_count` — labels share everything except __name__,
            // whose VALUE becomes `{metric}_count`. The label can sit anywhere
            // in the sorted set (a `__custom` tag sorts before it), so locate
            // it by name rather than assuming index 0.
            let mut count_labels = key.labels.clone();
            if let Some(entry) = count_labels.iter_mut().find(|(n, _)| n == "__name__") {
                entry.1 = format!("{}_count", key.metric);
            }
            let count_key = SeriesKey {
                metric: format!("{}_count", key.metric),
                labels: count_labels,
            };
            // `{metric}_sum` — same, with the sum value.
            let mut sum_labels = key.labels.clone();
            if let Some(entry) = sum_labels.iter_mut().find(|(n, _)| n == "__name__") {
                entry.1 = format!("{}_sum", key.metric);
            }
            let sum_key = SeriesKey {
                metric: format!("{}_sum", key.metric),
                labels: sum_labels,
            };
            vec![
                (count_key, vec![(agg.count as f64, ts_ms)]),
                (sum_key, vec![(agg.sum, ts_ms)]),
            ]
        }
    }
}

// ---------------------------------------------------------------------------
// Hand-rolled protobuf encoding (remote-write wire format)
// ---------------------------------------------------------------------------

/// Encode a `WriteRequest` message from buffered series.
///
/// Schema:
/// ```protobuf
/// message WriteRequest { repeated TimeSeries timeseries = 1; }
/// message TimeSeries  { repeated Label labels = 1; repeated Sample samples = 2; }
/// message Label       { string name = 1; string value = 2; }
/// message Sample      { double value = 1; int64 timestamp = 2; } // ms
/// ```
fn encode_write_request(series: &HashMap<SeriesKey, Vec<(f64, i64)>>) -> Vec<u8> {
    let mut out = Vec::new();
    for (key, samples) in series {
        let mut ts = Vec::new();
        for (name, value) in &key.labels {
            let mut label = Vec::new();
            write_string_field(&mut label, 1, name);
            write_string_field(&mut label, 2, value);
            write_bytes_field(&mut ts, 1, &label);
        }
        for (value, ts_ms) in samples {
            let mut sample = Vec::new();
            write_double_field(&mut sample, 1, *value);
            write_varint_field(&mut sample, 2, *ts_ms as u64);
            write_bytes_field(&mut ts, 2, &sample);
        }
        write_bytes_field(&mut out, 1, &ts);
    }
    out
}

/// Write a base-128 varint.
fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Write a field key: `(field_number << 3) | wire_type`.
fn write_key(buf: &mut Vec<u8>, field: u32, wire_type: u8) {
    write_varint(buf, ((field as u64) << 3) | wire_type as u64);
}

/// Write a length-delimited (wire type 2) field.
fn write_bytes_field(buf: &mut Vec<u8>, field: u32, data: &[u8]) {
    write_key(buf, field, 2);
    write_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// Write a string (length-delimited) field.
fn write_string_field(buf: &mut Vec<u8>, field: u32, s: &str) {
    write_bytes_field(buf, field, s.as_bytes());
}

/// Write a fixed64 (wire type 1) field — used for `double`.
fn write_double_field(buf: &mut Vec<u8>, field: u32, v: f64) {
    write_key(buf, field, 1);
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Write a varint (wire type 0) field — used for `int64`.
fn write_varint_field(buf: &mut Vec<u8>, field: u32, v: u64) {
    write_key(buf, field, 0);
    write_varint(buf, v);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tropel_core::types::{Sample, SampleType, TagMap};

    fn sample(metric: &str, value: f64, tags: TagMap) -> Sample {
        sample_typed(metric, value, tags, SampleType::Point)
    }

    fn sample_typed(metric: &str, value: f64, tags: TagMap, sample_type: SampleType) -> Sample {
        Sample {
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: std::sync::Arc::new(tags),
            timestamp: SystemTime::now(),
            sample_type,
        }
    }

    /// A minimal protobuf reader used only to validate the encoder output
    /// (round-trip). Handles the three remote-write messages.
    mod decode {
        use std::collections::HashMap;

        /// Decoded payload: series key (sorted label pairs) → samples.
        pub type Decoded = HashMap<Vec<(String, String)>, Vec<(f64, i64)>>;

        pub fn read_varint(buf: &[u8], pos: &mut usize) -> u64 {
            let mut result = 0u64;
            let mut shift = 0u32;
            loop {
                let byte = buf[*pos];
                *pos += 1;
                result |= ((byte & 0x7f) as u64) << shift;
                if byte & 0x80 == 0 {
                    return result;
                }
                shift += 7;
            }
        }

        pub fn read_bytes(buf: &[u8], pos: &mut usize) -> Vec<u8> {
            let len = read_varint(buf, pos) as usize;
            let start = *pos;
            *pos += len;
            buf[start..start + len].to_vec()
        }

        #[derive(Debug, PartialEq)]
        pub struct Label {
            pub name: String,
            pub value: String,
        }

        fn parse_label(buf: &[u8]) -> Label {
            let mut pos = 0usize;
            let mut name = String::new();
            let mut value = String::new();
            while pos < buf.len() {
                let key = read_varint(buf, &mut pos);
                let field = key >> 3;
                let wire = key & 0x7;
                match (field, wire) {
                    (1, 2) => name = String::from_utf8(read_bytes(buf, &mut pos)).unwrap(),
                    (2, 2) => value = String::from_utf8(read_bytes(buf, &mut pos)).unwrap(),
                    _ => panic!("unexpected label field {field}/{wire}"),
                }
            }
            Label { name, value }
        }

        fn parse_sample(buf: &[u8]) -> (f64, i64) {
            let mut pos = 0usize;
            let mut value = 0.0f64;
            let mut ts = 0i64;
            while pos < buf.len() {
                let key = read_varint(buf, &mut pos);
                let field = key >> 3;
                let wire = key & 0x7;
                match (field, wire) {
                    (1, 1) => {
                        let start = pos;
                        pos += 8;
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(&buf[start..start + 8]);
                        value = f64::from_le_bytes(arr);
                    }
                    (2, 0) => ts = read_varint(buf, &mut pos) as i64,
                    _ => panic!("unexpected sample field {field}/{wire}"),
                }
            }
            (value, ts)
        }

        pub fn decode(buf: &[u8]) -> Decoded {
            let mut out = HashMap::new();
            let mut pos = 0usize;
            while pos < buf.len() {
                let key = read_varint(buf, &mut pos);
                assert_eq!(key >> 3, 1, "expected timeseries field");
                assert_eq!(key & 0x7, 2, "expected length-delimited");
                let ts_bytes = read_bytes(buf, &mut pos);
                let mut tpos = 0usize;
                let mut labels = Vec::new();
                let mut samples = Vec::new();
                while tpos < ts_bytes.len() {
                    let tkey = read_varint(&ts_bytes, &mut tpos);
                    let field = tkey >> 3;
                    let wire = tkey & 0x7;
                    match (field, wire) {
                        (1, 2) => {
                            let lb = parse_label(&read_bytes(&ts_bytes, &mut tpos));
                            labels.push((lb.name, lb.value));
                        }
                        (2, 2) => samples.push(parse_sample(&read_bytes(&ts_bytes, &mut tpos))),
                        _ => panic!("unexpected timeseries field {field}/{wire}"),
                    }
                }
                out.insert(labels, samples);
            }
            out
        }
    }

    /// Aggregate a list of samples through the same path `buffer()`+`flush()`
    /// use (policy applied, per-series aggregation, flush-time stamping).
    fn aggregate(samples: &[Sample], ts_ms: i64) -> HashMap<SeriesKey, Vec<(f64, i64)>> {
        let mut series: HashMap<SeriesKey, SeriesAgg> = HashMap::new();
        for s in samples {
            let key = SeriesKey::from_parts(&s.metric, &s.tags);
            let agg = series.entry(key).or_insert(SeriesAgg {
                sample_type: s.sample_type.clone(),
                count: 0,
                sum: 0.0,
                last: 0.0,
            });
            agg.count += 1;
            agg.sum += s.value;
            agg.last = s.value;
        }
        series
            .iter()
            .flat_map(|(k, a)| expand_series(k, a, ts_ms))
            .collect()
    }

    #[test]
    fn remote_write_roundtrip() {
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        tags.insert("method", "GET");

        let s1 = sample_typed("http_req_duration", 123.5, tags.clone(), SampleType::Trend);
        let s2 = sample_typed("http_req_duration", 200.0, tags.clone(), SampleType::Trend);
        let s3 = sample_typed("http_reqs", 1.0, TagMap::new(), SampleType::Counter);

        // Two raw trend samples in one flush window aggregate to ONE sample
        // per sub-series (the whole point: no duplicate (series, ts) → 400).
        let series = aggregate(&[s1, s2, s3], 1000);

        let encoded = encode_write_request(&series);
        let decoded = decode::decode(&encoded);

        // http_req_duration_count, http_req_duration_sum, http_reqs
        assert_eq!(decoded.len(), 3, "three series expected after aggregation");

        // Trend → _count/_sum sub-series with correct aggregates.
        let mut found_count = None;
        let mut found_sum = None;
        for (labels, samples) in &decoded {
            let map: HashMap<&str, &str> = labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            match map.get("__name__") {
                Some(&"http_req_duration_count") => {
                    assert_eq!(map.get("status"), Some(&"200"));
                    assert_eq!(map.get("method"), Some(&"GET"));
                    assert_eq!(samples, &vec![(2.0, 1000)]);
                    found_count = Some(());
                }
                Some(&"http_req_duration_sum") => {
                    assert_eq!(samples, &vec![(323.5, 1000)]); // 123.5 + 200.0
                    found_sum = Some(());
                }
                _ => {}
            }
        }
        assert!(found_count.is_some(), "_count sub-series missing");
        assert!(found_sum.is_some(), "_sum sub-series missing");

        // Counter http_reqs → single series, sum, just the __name__ label.
        // (The metric name lives in the __name__ VALUE — comparing label
        // NAMES to "http_reqs" would never match, so the series went
        // unverified before.)
        let mut found_counter = false;
        for (labels, samples) in &decoded {
            if labels
                .iter()
                .find(|(n, _)| n == "__name__")
                .is_some_and(|(_, v)| v == "http_reqs")
            {
                assert_eq!(labels.len(), 1, "counter series must have only __name__");
                assert_eq!(samples, &vec![(1.0, 1000)]);
                found_counter = true;
            }
        }
        assert!(found_counter, "http_reqs counter series missing from output");
    }

    #[test]
    fn name_label_sorted_with_tags_not_hardcoded_first() {
        // `__name__` used to be inserted at index 0 AFTER sorting the tags, so
        // a tag that byte-sorts before `__name__` (e.g. `__custom`, `_a`) sat
        // AFTER it — a non-canonical label set. All labels including
        // `__name__` must be sorted together.
        let mut tags = TagMap::new();
        tags.insert("__custom", "v");
        tags.insert("zebra", "z");
        let key = SeriesKey::from_parts("http_reqs", &tags);
        assert_eq!(
            key.labels,
            vec![
                ("__custom".to_string(), "v".to_string()),
                ("__name__".to_string(), "http_reqs".to_string()),
                ("zebra".to_string(), "z".to_string()),
            ],
            "labels must be in canonical sorted order with __name__ among them"
        );
    }

    #[test]
    fn trend_expansion_finds_name_label_not_at_index_zero() {
        // expand_series previously rewrote labels[0] assuming __name__ was
        // first. With a `__custom` tag it is NOT first — the __name__ entry
        // must be found by name and its value rewritten for the _count/_sum
        // sub-series.
        let mut tags = TagMap::new();
        tags.insert("__custom", "v");
        let key = SeriesKey::from_parts("http_req_duration", &tags);
        let agg = SeriesAgg {
            sample_type: SampleType::Trend,
            count: 3,
            sum: 45.0,
            last: 45.0,
        };
        let expanded = expand_series(&key, &agg, 1000);
        assert_eq!(expanded.len(), 2);
        for (k, samples) in &expanded {
            // __custom tag survives on the sub-series.
            assert!(k.labels.iter().any(|(n, v)| n == "__custom" && v == "v"));
            // __name__ value is the suffixed metric name, wherever it sits.
            let name_val = k
                .labels
                .iter()
                .find(|(n, _)| n == "__name__")
                .map(|(_, v)| v.as_str())
                .unwrap();
            assert!(
                name_val == "http_req_duration_count"
                    || name_val == "http_req_duration_sum",
                "unexpected __name__ value {name_val}"
            );
            // One sample per sub-series at flush time.
            assert_eq!(samples.len(), 1);
        }
    }

    #[test]
    fn aggregation_collapses_duplicate_timestamps() {
        // Regression: two raw samples with the SAME series + ms timestamp
        // previously produced a duplicate (series, ts) pair → remote-write
        // rejects with 400. Aggregation must collapse them to one sample.
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        let s1 = sample_typed("http_req_duration", 100.0, tags.clone(), SampleType::Trend);
        let s2 = sample_typed("http_req_duration", 250.0, tags.clone(), SampleType::Trend);

        let series = aggregate(&[s1, s2], 5000);
        // Two sub-series (count + sum), each with exactly ONE sample.
        assert_eq!(series.len(), 2);
        for samples in series.values() {
            assert_eq!(samples.len(), 1, "one aggregated sample per series per window");
        }
    }

    #[test]
    fn tag_policy_applies_allowlist_and_cap() {
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        tags.insert("method", "GET");
        tags.insert("url", "https://x/y?z=1");

        // Allowlist: only status survives.
        let policy = TagPolicy {
            allowlist: vec!["status".to_string()],
            max_tags: None,
        };
        let filtered = policy.apply(&tags);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.get("status"), Some("200"));
        assert!(filtered.get("method").is_none());
        assert!(filtered.get("url").is_none());

        // Cap: at most 2 tag keys (sorted, deterministic: method, status).
        let policy = TagPolicy {
            allowlist: Vec::new(),
            max_tags: Some(2),
        };
        let capped = policy.apply(&tags);
        assert_eq!(capped.len(), 2);
        let mut keys: Vec<&str> = capped.iter().map(|(k, _)| k).collect();
        keys.sort();
        assert_eq!(keys, vec!["method", "status"]);
    }

    #[test]
    fn snappy_roundtrip() {
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        let mut series = HashMap::new();
        let key = SeriesKey::from_parts("http_req_duration", &tags);
        series.insert(key, vec![(42.0, 1000)]);

        let encoded = encode_write_request(&series);
        let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();
        let decompressed = snap::raw::Decoder::new().decompress_vec(&compressed).unwrap();
        assert_eq!(encoded, decompressed);
    }

    #[test]
    fn url_normalization() {
        assert_eq!(
            normalize_remote_write_url("http://localhost:9090"),
            "http://localhost:9090/api/v1/write"
        );
        assert_eq!(
            normalize_remote_write_url("http://localhost:9090/"),
            "http://localhost:9090/api/v1/write"
        );
        assert_eq!(
            normalize_remote_write_url("http://host:9090/api/v1/write"),
            "http://host:9090/api/v1/write"
        );
    }

    /// End-to-end: buffer samples, flush to a live TCP server, and decode the
    /// received snappy+protobuf payload.
    #[tokio::test]
    async fn flush_posts_to_endpoint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Read the HTTP request head.
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            // Extract Content-Length and read the body.
            let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
            let content_length: usize = head
                .lines()
                .find_map(|l| {
                    let l = l.trim();
                    let lower = l.to_lowercase();
                    lower.strip_prefix("content-length:").and_then(|v| v.trim().parse().ok())
                })
                .unwrap_or(0);
            while buf.len() < head_end + content_length {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let body = &buf[head_end..head_end + content_length];

            // Echo the raw body back for the test to decode.
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-protobuf\r\nConnection: close\r\n\r\n",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.write_all(body).await.unwrap();
            sock.flush().await.unwrap();
            body.to_vec()
        });

        let output = PrometheusRemoteWriteOutput::new(format!("http://{addr}"));
        let s = sample("http_reqs", 1.0, TagMap::new());
        output.sample(&[s]).await.unwrap();
        output.stop().await.unwrap();

        let received = server.await.unwrap();
        assert!(!received.is_empty(), "server received nothing");
        let decompressed = snap::raw::Decoder::new().decompress_vec(&received).unwrap();
        let decoded = decode::decode(&decompressed);
        assert!(!decoded.is_empty());
        let series = decoded.values().next().unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].0, 1.0);
    }
}
