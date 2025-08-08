use tokio::{fs, signal, task, time};
use tokio::sync::{Mutex, watch, mpsc};
use std::sync::Arc;
use std::time::Instant;
use serde::Deserialize;
use flatbuffers::FlatBufferBuilder;
use tokio::io::AsyncWriteExt;
use ljmrs::LJMLibrary;

use spr4918::stream_generated; // from lib.rs
use stream_generated::stream::{ScanBatch, ScanBatchArgs};

#[derive(Debug, Deserialize, Clone, PartialEq)]
struct StreamConfig {
    scans_per_read: usize,
    suggested_scan_rate: f64,
    channels: Vec<i32>,
}


enum ConsumerMsg {
    Data(Vec<u8>),
    Rollover(StreamConfig),
    Shutdown,
}

#[async_trait::async_trait]
trait Consumer: Send {
    async fn init(&mut self, cfg: &StreamConfig);
    async fn consume(&mut self, batch: Vec<u8>);
    async fn rollover(&mut self, cfg: &StreamConfig);
    async fn shutdown(&mut self) {}
}

struct DiskConsumer {
    file: Option<tokio::fs::File>,
}

impl DiskConsumer {
    fn new() -> Self { Self { file: None } }
}

#[async_trait::async_trait]
impl Consumer for DiskConsumer {
    async fn init(&mut self, cfg: &StreamConfig) {
        let filename = generate_bin_filename(cfg);
        let f = fs::OpenOptions::new().create(true).append(true).open(filename).await
            .expect("DiskConsumer: open failed");
        println!("DiskConsumer: file opened.");
        self.file = Some(f);
    }
    async fn consume(&mut self, batch: Vec<u8>) {
        if let Some(file) = &mut self.file {
            file.write_all(&batch).await.unwrap();
        }
    }
    async fn rollover(&mut self, cfg: &StreamConfig) {
        self.file = None;
        let filename = generate_bin_filename(cfg);
        let f = fs::OpenOptions::new().create(true).append(true).open(filename).await
            .expect("DiskConsumer: rollover open failed");
        println!("DiskConsumer: rolled over to new file.");
        self.file = Some(f);
    }
    async fn shutdown(&mut self) {
        self.file = None;
        println!("DiskConsumer: shutdown complete.");
    }
}

/* Example NATS consumer if needed later
struct NatsConsumer {
    client: Option<async_nats::Client>,
    subject: String,
}
impl NatsConsumer { fn new(subject: impl Into<String>) -> Self { Self { client: None, subject: subject.into() } } }
#[async_trait::async_trait]
impl Consumer for NatsConsumer {
    async fn init(&mut self, _cfg: &StreamConfig) {
        let client = async_nats::connect("nats://127.0.0.1:4222").await.unwrap();
        self.client = Some(client);
        println!("NatsConsumer: connected.");
    }
    async fn consume(&mut self, batch: Vec<u8>) {
        if let Some(cl) = &self.client {
            cl.publish(self.subject.clone().into(), batch.into()).await.unwrap();
        }
    }
    async fn rollover(&mut self, _cfg: &StreamConfig) { println!("NatsConsumer: rollover (no-op)"); }
    async fn shutdown(&mut self) { self.client = None; }
}
*/

async fn load_config(path: &str) -> Option<StreamConfig> {
    if let Ok(file) = fs::read_to_string(path).await {
        serde_json::from_str(&file).ok()
    } else {
        None
    }
}

fn generate_bin_filename(cfg: &StreamConfig) -> String {
    let now = chrono::Utc::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let time_str = now.format("%H%M%S").to_string();
    let channel_str = cfg.channels.iter().map(|c| format!("AIN{}", c)).collect::<Vec<_>>().join("_");
    format!("{}_{}_{}Hz_{}scan_{}.bin", date_str, time_str, cfg.suggested_scan_rate, cfg.scans_per_read, channel_str)
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "staticlib")]
    unsafe { LJMLibrary::init().unwrap(); }

    let open_call = LJMLibrary::open_jack(
        ljmrs::DeviceType::ANY,
        ljmrs::ConnectionType::ANY,
        "ANY".to_string(),
    ).expect("Failed to open LabJack device");
    println!("Opened LabJack. Handle: {}", open_call);
    if let Err(e) = LJMLibrary::stream_stop(open_call) {
        // It's fine if this returns "not active" — we just ignore it
        eprintln!("(preflight) stream_stop: ignoring: {:?}", e);
    }

    let config_path = "config.json";
    let initial_config = load_config(config_path).await.unwrap_or(StreamConfig {
        scans_per_read: 100,
        suggested_scan_rate: 1000.0,
        channels: vec![0,1,2,3],
    });

    let (tx_config, mut rx_config) = watch::channel(initial_config.clone());
    let shared_config = Arc::new(Mutex::new(initial_config));

    {
        let tx_config = tx_config.clone();
        let shared_config = Arc::clone(&shared_config);
        let config_path = config_path.to_string();
        task::spawn(async move {
            let mut last = None;
            loop {
                if let Ok(meta) = fs::metadata(&config_path).await {
                    if let Ok(modified) = meta.modified() {
                        if Some(modified) != last {
                            if let Some(new_cfg) = load_config(&config_path).await {
                                println!("Configuration updated: {:?}", new_cfg);
                                *shared_config.lock().await = new_cfg.clone();
                                let _ = tx_config.send(new_cfg.clone());
                                last = Some(modified);
                            }
                        }
                    }
                }
                time::sleep(time::Duration::from_secs(2)).await;
            }
        });
    }

    let shutdown = task::spawn(async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("\nCtrl+C detected! Stopping stream...");
    });

    let (tx_msg, mut rx_msg) = mpsc::channel::<ConsumerMsg>(200);

    // Choose your consumer (DiskConsumer by default)
    let mut dyn_consumer: Box<dyn Consumer> = Box::new(DiskConsumer::new());

    let consumer_task = task::spawn(async move {
        let mut buffer: Vec<Vec<u8>> = Vec::new();
        let mut flush_timer = Instant::now();
        let mut initialized = false;

        while let Some(msg) = rx_msg.recv().await {
            match msg {
                ConsumerMsg::Data(b) => {
                    if !initialized {
                        // If someone forgot to send init/rollover first, ignore until init.
                        continue;
                    }
                    buffer.push(b);
                    if buffer.len() >= 20 || flush_timer.elapsed().as_secs_f64() >= 1.0 {
                        for chunk in buffer.drain(..) {
                            dyn_consumer.consume(chunk).await;
                        }
                        flush_timer = Instant::now();
                    }
                }
                ConsumerMsg::Rollover(cfg) => {
                    if !initialized {
                        dyn_consumer.init(&cfg).await;
                        initialized = true;
                    } else {
                        for chunk in buffer.drain(..) {
                            dyn_consumer.consume(chunk).await;
                        }
                        dyn_consumer.rollover(&cfg).await;
                    }
                }
                ConsumerMsg::Shutdown => {
                    for chunk in buffer.drain(..) {
                        dyn_consumer.consume(chunk).await;
                    }
                    dyn_consumer.shutdown().await;
                    break;
                }
            }
        }
    });

    // tell consumer to open initial output
    {
        let cfg = shared_config.lock().await.clone();
        tx_msg.send(ConsumerMsg::Rollover(cfg)).await.unwrap();
    }

    let mut total_scans = 0usize;
    let mut rate_timer = Instant::now();

    'outer: loop {
        if shutdown.is_finished() { break; }

        let cfg = shared_config.lock().await.clone();
        println!(
            "Starting stream: channels={:?}, scans/read={}, rate={:.2}Hz",
            cfg.channels, cfg.scans_per_read, cfg.suggested_scan_rate
        );

        if let Err(e) = LJMLibrary::stream_start(
            open_call,
            cfg.scans_per_read as i32,
            cfg.suggested_scan_rate,
            cfg.channels.clone(),
        ) {
            eprintln!("Failed to start streaming: {:?}. Retrying...", e);
            time::sleep(time::Duration::from_secs(2)).await;
            continue;
        }
        println!("Streaming started.");

        let mut fbb = FlatBufferBuilder::with_capacity(16 * 1024);
        let mut batch_accumulator: Vec<f64> = Vec::new(); // f64 to match ljmrs
        let mut batch_timestamp = chrono::Utc::now().to_rfc3339();
        let batch_reads_target = 10;

        loop {
            if shutdown.is_finished() {
                LJMLibrary::stream_stop(open_call).unwrap();
                println!("Stream stopped gracefully.");
                tx_msg.send(ConsumerMsg::Shutdown).await.ok();
                consumer_task.await.ok();
                return;
            }

            if rx_config.has_changed().unwrap() {
                // Consume the change flag and get the new value
                let new_cfg = rx_config.borrow_and_update().clone();

                if new_cfg != cfg {
                    println!("Config changed! Restarting stream + rolling output...");
                    LJMLibrary::stream_stop(open_call).unwrap();

                    tx_msg.send(ConsumerMsg::Rollover(new_cfg)).await.unwrap();

                    time::sleep(time::Duration::from_millis(500)).await;
                    continue 'outer;
                } else {
                    // Same config; do nothing (we just cleared the flag)
                }
            }


            match LJMLibrary::stream_read(open_call) {
                Ok(read_value) => {
                    let num_channels = cfg.channels.len();
                    let samples_per_read = read_value.len() / num_channels;

                    if batch_accumulator.is_empty() {
                        batch_timestamp = chrono::Utc::now().to_rfc3339();
                    }
                    batch_accumulator.extend_from_slice(&read_value);
                    total_scans += samples_per_read;

                    if batch_accumulator.len() >= num_channels * samples_per_read * batch_reads_target {
                        fbb.reset();
                        let fb_timestamp = fbb.create_string(&batch_timestamp);
                        let fb_channels = fbb.create_vector(&cfg.channels);
                        let fb_values = fbb.create_vector(&batch_accumulator);

                        let batch = ScanBatch::create(
                            &mut fbb,
                            &ScanBatchArgs {
                                timestamp: Some(fb_timestamp),
                                channel_ids: Some(fb_channels),
                                values: Some(fb_values),
                                scans_in_batch: (batch_accumulator.len() / num_channels) as i32,
                                channels_per_scan: num_channels as i32,
                            },
                        );

                        fbb.finish_size_prefixed(batch, None);
                        tx_msg.send(ConsumerMsg::Data(fbb.finished_data().to_vec())).await.unwrap();
                        batch_accumulator.clear();
                    }

                    if rate_timer.elapsed().as_secs_f64() >= 1.0 {
                        let actual_rate = total_scans as f64 / rate_timer.elapsed().as_secs_f64();
                        println!("Actual rate: {:.2} Hz", actual_rate);
                        total_scans = 0;
                        rate_timer = Instant::now();
                    }
                }
                Err(e) => {
                    eprintln!("Stream read failed: {:?}. Restarting...", e);
                    LJMLibrary::stream_stop(open_call).unwrap();
                    time::sleep(time::Duration::from_millis(500)).await;
                    break;
                }
            }
        }
    }

    tx_msg.send(ConsumerMsg::Shutdown).await.ok();
    consumer_task.await.ok();
    println!("Streamer exiting.");
}
