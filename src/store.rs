use std::{collections::{HashMap, HashSet}, fs::{File, create_dir_all}, path::{Path, PathBuf}, sync::Arc, error::Error};

use async_nats::{self, jetstream};
use flatbuffers::root;
use futures_util::StreamExt;

use chrono::{DateTime, Utc, TimeZone, Datelike, Timelike};
use serde::{Deserialize, Serialize};

use arrow_array::{ArrayRef, Float64Builder, TimestampMillisecondBuilder, RecordBatch};
use arrow_schema::{Schema, Field, DataType, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

mod sample_data_generated {
    #![allow(dead_code, unused_imports)]
    include!("data_generated.rs");
}
use sample_data_generated::sampler;


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SampleConfig {
    scans_per_read: i32,
    suggested_scan_rate: f64,
    channels: Vec<u8>,     
    asset_number: u32,
    nats_url: String,
    nats_subject: String,  // prefix, e.g. "labjack"
    nats_stream: String,   // not used here but present
}

fn pad_asset(n: u32) -> String { format!("{n:03}") }
fn extract_channel(subject: &str) -> Option<u8> {
    // expects ... .data.chXX
    subject.split('.').last()
        .and_then(|tok| tok.strip_prefix("ch"))
        .and_then(|num| num.parse::<u8>().ok())
}

// ---------------- Parquet (wide, nullable) ------------------

fn make_schema() -> Arc<Schema> {
    let mut fields = vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
    ];
    for i in 0..14 {
        fields.push(Field::new(&format!("ch{:02}", i), DataType::Float64, true)); // nullable
    }
    Arc::new(Schema::new(fields))
}

struct WideBuilders {
    ts: TimestampMillisecondBuilder,
    ch: [Float64Builder; 14],
}
impl WideBuilders {
    fn new() -> Self {
        Self {
            ts: TimestampMillisecondBuilder::new(),
            ch: [
                Float64Builder::new(), Float64Builder::new(), Float64Builder::new(),
                Float64Builder::new(), Float64Builder::new(), Float64Builder::new(),
                Float64Builder::new(), Float64Builder::new(), Float64Builder::new(),
                Float64Builder::new(), Float64Builder::new(), Float64Builder::new(),
                Float64Builder::new(), Float64Builder::new(),
            ],
        }
    }
    fn clear(&mut self) {
        // Re-init builders after finishing a row group
        *self = Self::new();
    }
}

struct ParquetShard {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    builders: WideBuilders,
    rowgroup_target: usize,
    rows_in_buffer: usize,
    // paths for sidecar metadata
    meta_path: PathBuf,
    active_channels: Vec<u8>,
}

impl ParquetShard {
    fn create(root: &Path, asset: u32, ts: DateTime<Utc>, active_channels: &[u8], rowgroup_target: usize) -> parquet::errors::Result<Self> {
        let schema = make_schema();

        // partition dirs
        let mut dir = PathBuf::from(root);
        dir.push(format!("asset={:03}", asset));
        dir.push(format!("date={:04}-{:02}-{:02}", ts.year(), ts.month(), ts.day()));
        dir.push(format!("hour={:02}", ts.hour()));
        create_dir_all(&dir).expect("make partition dirs");

        // filename aligns to the 15-minute window start
        let minute_window = (ts.minute() / 15) * 15;
        let file_stem = format!("part-{}{:02}{:02}{:02}{:02}", ts.year(), ts.month(), ts.day(), ts.hour(), minute_window);

        let mut parquet_path = dir.clone(); parquet_path.push(format!("{file_stem}.parquet"));
        let mut meta_path = dir.clone();    meta_path.push(format!("{file_stem}.meta.json"));

        let file = File::create(&parquet_path).expect("create parquet file");
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // sidecar metadata (active channels)
        let meta = serde_json::json!({
            "active_channels": active_channels,
            "window_start_utc": format!("{:04}-{:02}-{:02}T{:02}:{:02}:00Z",
                ts.year(), ts.month(), ts.day(), ts.hour(), minute_window),
            "asset": format!("{:03}", asset)
        });
        std::fs::write(&meta_path, serde_json::to_vec_pretty(&meta).unwrap()).ok();

        Ok(Self {
            writer,
            schema,
            builders: WideBuilders::new(),
            rowgroup_target,
            rows_in_buffer: 0,
            meta_path,
            active_channels: active_channels.to_vec(),
        })
    }

    fn push_row(&mut self, t_ms: i64, row_vals: [Option<f64>; 14]) -> parquet::errors::Result<()> {
        self.builders.ts.append_value(t_ms);
        for i in 0..14 {
            match row_vals[i] {
                Some(v) => self.builders.ch[i].append_value(v),
                None    => self.builders.ch[i].append_null(),
            }
        }
        self.rows_in_buffer += 1;
        if self.rows_in_buffer >= self.rowgroup_target {
            self.flush_rowgroup()?;
        }
        Ok(())
    }

    fn flush_rowgroup(&mut self) -> parquet::errors::Result<()> {
        if self.rows_in_buffer == 0 { return Ok(()); }
        let arrays: Vec<ArrayRef> = {
            let mut v: Vec<ArrayRef> = Vec::with_capacity(1 + 14);
            v.push(Arc::new(self.builders.ts.finish()));
            for i in 0..14 {
                v.push(Arc::new(self.builders.ch[i].finish()));
            }
            v
        };
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).expect("record batch");
        self.writer.write(&batch)?;
        self.rows_in_buffer = 0;
        self.builders.clear();
        Ok(())
    }

    fn close(mut self) -> parquet::errors::Result<()> {
        let _ = self.flush_rowgroup();
        let _ = self.writer.close(); // finalize footer & stats
        Ok(())
    }
}


#[derive(Default)]
struct PendingBatch {
    // per-channel values for this batch timestamp
    per_channel: [Option<Vec<f64>>; 14],
    seen: HashSet<u8>,
    expected: HashSet<u8>,
}

impl PendingBatch {
    fn new(expected_channels: &[u8]) -> Self {
        Self {
            per_channel: Default::default(),
            seen: HashSet::new(),
            expected: expected_channels.iter().copied().collect(),
        }
    }
    fn insert(&mut self, ch: u8, vals: Vec<f64>) {
        self.per_channel[ch as usize] = Some(vals);
        self.seen.insert(ch);
    }
    fn ready(&self) -> bool { self.seen == self.expected }
    fn take(self) -> [Option<Vec<f64>>; 14] { self.per_channel }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://0.0.0.0:4222".into());
    let bucket   = std::env::var("CFG_BUCKET").unwrap_or_else(|_| "sampler_cfg".into());
    let key      = std::env::var("CFG_KEY").unwrap_or_else(|_| "active".into());

    let nc = async_nats::connect(&nats_url).await?;
    let js = jetstream::new(nc.clone());
    let store = js.get_key_value(&bucket).await
        .or_else(|_| async { js.create_key_value(jetstream::kv::Config { bucket: bucket.clone(), ..Default::default() }).await })?;
    let cfg_entry = store.entry(&key).await?.ok_or("config KV key missing")?;
    let cfg: SampleConfig = serde_json::from_slice(&cfg_entry.value)?;

    println!("Loaded config from KV {bucket}:{key}: {:?}", cfg);

    let asset = cfg.asset_number;
    let active_channels = cfg.channels.clone();               // subset or all 0..13
    let active_set: HashSet<u8> = active_channels.iter().copied().collect();
    let subject = format!("{}.{}.data.*", cfg.nats_subject, pad_asset(asset));
    let period_ms = 1000.0 / cfg.suggested_scan_rate.max(1.0); // sample period per index
    let rowgroup_target = 500_000usize; // ~tunable

    let mut sub = nc.subscribe(subject.clone()).await?;
    println!("Subscribed: {subject}");

    let parquet_root = PathBuf::from("parquet");

    let mut current_shard: Option<ParquetShard> = None;

    let mut pending: HashMap<String, PendingBatch> = HashMap::new();

    while let Some(msg) = sub.next().await {
        let ch = match extract_channel(&msg.subject) {
            Some(c) if active_set.contains(&c) => c,
            _ => continue, // ignore channels not in this run
        };

        let scan = match root::<sampler::Scan>(&msg.payload) {
            Ok(s) => s,
            Err(_) => { eprintln!("Failed to decode flatbuffer for {}", msg.subject); continue; }
        };

        let batch_ts_str = match scan.timestamp() {
            Some(s) => s,
            None => { eprintln!("missing timestamp"); continue; }
        };
        let values = scan.values().map(|v| v.iter().collect::<Vec<f64>>()).unwrap_or_default();

        // insert into pending batch keyed by batch_ts string
        let entry = pending.entry(batch_ts_str.to_string()).or_insert_with(|| PendingBatch::new(&active_channels));
        entry.insert(ch, values);

        // if we have all channels for this batch timestamp → flush wide rows
        if entry.ready() {
            // parse ts
            let batch_dt = DateTime::parse_from_rfc3339(batch_ts_str).ok()
                .map(|t| t.with_timezone(&Utc))
                .unwrap_or_else(|| Utc::now());
            let per_channel = pending.remove(batch_ts_str).unwrap().take();

            // determine shard window start (15 min)
            let window_min = (batch_dt.minute() / 15) * 15;
            let window_start = Utc
                .with_ymd_and_hms(batch_dt.year(), batch_dt.month(), batch_dt.day(), batch_dt.hour(), window_min, 0)
                .unwrap();

            // rotate shard if needed
            let shard_needs_rotate = match &current_shard {
                None => true,
                Some(_) => {
                    // Compare current file window to new window
                    let cur_min = (batch_dt.minute() / 15) * 15; // quick compare by minute bucket
                    let now_min = cur_min; // just a marker; actual shard holds path keyed by window_start
                    // We hold no direct window in struct; simplest: rotate if file missing or window minute differs from its meta file.
                    // For simplicity, rotate when minute bucket changes from last write:
                    true // we’ll force re-create shard when window changes below using Option::take
                }
            };

            // crude window tracking: rebuild shard if its 15-min bucket mismatches last writer's minute
            let mut must_recreate = false;
            if let Some(sh) = &current_shard {
                // Peek at current meta filename minute vs new
                if let Some(fname) = sh.meta_path.file_stem().and_then(|s| s.to_str()) {
                    // part-YYYYMMDDHHmm
                    let mm = &fname[fname.len()-2..];
                    let new_mm = format!("{:02}", window_min);
                    if mm != new_mm { must_recreate = true; }
                }
            } else {
                must_recreate = true;
            }
            if must_recreate {
                if let Some(sh) = current_shard.take() {
                    let _ = sh.close();
                }
                current_shard = Some(ParquetShard::create(
                    &parquet_root, asset, window_start, &active_channels, rowgroup_target
                )?);
            }

            // now emit rows for this batch: row i = timestamp_ms + 14 optionals
            let base_ms = batch_dt.timestamp_millis();
            if let Some(sh) = current_shard.as_mut() {
                // find batch length (all channels same len)
                let batch_len = per_channel.iter().filter_map(|o| o.as_ref()).map(|v| v.len()).max().unwrap_or(0);
                for i in 0..batch_len {
                    let t_ms = base_ms + (i as f64 * period_ms).round() as i64;
                    let mut row: [Option<f64>; 14] = Default::default();
                    for ch_idx in 0..14 {
                        if let Some(vs) = &per_channel[ch_idx] {
                            row[ch_idx] = vs.get(i).copied();
                        } else {
                            row[ch_idx] = None; // channel inactive this run
                        }
                    }
                    let _ = sh.push_row(t_ms, row);
                }
            }
        }
    }

    if let Some(sh) = current_shard.take() {
        let _ = sh.close();
    }
    Ok(())
}
