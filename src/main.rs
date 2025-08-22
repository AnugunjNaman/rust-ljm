use tokio::sync::{watch, mpsc};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use chrono::{Utc, TimeZone};
use chrono_tz::America::New_York;
use notify::{Watcher, RecursiveMode, recommended_watcher};
use tokio::time::{Duration, Instant};

use ljmrs::{LJMLibrary, LJMError};
use ljmrs::handle::{DeviceType, ConnectionType};

use flatbuffers::FlatBufferBuilder;
use async_nats::jetstream::{self, stream::Config as StreamConfig};

mod sample_data_generated {
    #![allow(dead_code, unused_imports)]
    include!("data_generated.rs");
}
use sample_data_generated::sampler::{self, ScanArgs};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SampleConfig {
    scans_per_read: i32,
    suggested_scan_rate: f64,
    channels: Vec<u8>,
    nats_url: String,
    nats_subject: String,
    nats_stream: String,
}

struct LabJackGuard {
    handle: i32,
}

impl Drop for LabJackGuard {
    fn drop(&mut self) {
        let _ = LJMLibrary::stream_stop(self.handle);
        let _ = LJMLibrary::close_jack(self.handle);
    }
}

#[tokio::main]
async fn main() -> Result<(), LJMError> {
    let config_path = "config/sample.json";
    let cfg = load_config(config_path)
        .map_err(|e| LJMError::LibraryError(format!("Failed to load config: {}", e)))?;

    let (config_tx, config_rx) = watch::channel(cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Initialize LJM
    #[cfg(feature = "staticlib")]
    unsafe { LJMLibrary::init()?; }
    #[cfg(all(feature = "dynlink", not(feature = "staticlib")))]
    unsafe {
        let path = std::env::var("LJM_PATH").ok();
        LJMLibrary::init(path)?;
    }

    // Spawn sampler
    tokio::spawn(run_sampler(config_rx.clone(), shutdown_rx.clone()));

    // Spawn config watcher
    tokio::spawn(watch_config_file(config_path.to_string(), config_tx, shutdown_rx.clone()));

    // Wait for Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| LJMError::LibraryError(format!("Failed to listen for Ctrl+C: {}", e)))?;

    println!("Shutting down...");
    let _ = shutdown_tx.send(true);

    // Let background tasks shut down ????
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(())
}

fn load_config<P: AsRef<Path>>(path: P) -> Result<SampleConfig, std::io::Error> {
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("JSON parse error: {}", e)))?)
}

async fn watch_config_file(
    path: String,
    config_tx: watch::Sender<SampleConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let (tx, mut rx) = mpsc::channel(8);

    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();

    let blocking_jh = tokio::task::spawn_blocking({
        let path_clone = path.clone();
        move || {
            let mut watcher = recommended_watcher(move |res| {
                let _ = tx.blocking_send(res);
            }).expect("Failed to init watcher");

            watcher
                .watch(Path::new(&path_clone), RecursiveMode::NonRecursive)
                .expect("Failed to watch config file");

            // Block here until told to stop
            let _ = stop_rx.recv();
            // watcher is dropped automatically here
        }
    });

    let mut last_update = Instant::now();

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                if let Some(res) = maybe {
                    if res.is_ok() && last_update.elapsed() > Duration::from_millis(200) {
                        last_update = Instant::now();
                        match load_config(&path) {
                            Ok(cfg) => {
                                if cfg != *config_tx.borrow() {
                                    println!("Config updated: {:?}", cfg);
                                    let _ = config_tx.send(cfg);
                                }
                            }
                            Err(e) => eprintln!("Failed to reload config: {:?}", e),
                        }
                    }
                } else {
                    break; // channel closed
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    println!("[watch_config_file] Stopping watcher...");
                    let _ = stop_tx.send(()); // Tell blocking thread to exit
                    let _ = blocking_jh.await;
                    break;
                }
            }
        }
    }
}


async fn run_sampler(
    mut config_rx: watch::Receiver<SampleConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut run_id = 0;
    loop {
        if *shutdown_rx.borrow() {
            println!("[run_sampler] Sampler shutting down...");
            break;
        }

        run_id += 1;
        let cfg = config_rx.borrow().clone();
        println!("[run_sampler] Starting sampler run #{run_id} with {:?}", cfg);

        if let Err(e) = sample_with_config(run_id, cfg, &mut config_rx, &mut shutdown_rx).await {
            eprintln!("[run_sampler] Sampler error: {:?}", e);
        }

        if *shutdown_rx.borrow() {
            println!("[run_sampler] Shutdown detected after sampler error/config change");
            break;
        }

        println!("[run_sampler] Restarting sampler after config change...");
    }
}

async fn ensure_stream_exists(
    js: &jetstream::Context,
    stream_name: &str,
    subject: &str,
) -> Result<(), LJMError> {
    if js.get_stream(stream_name).await.is_ok() {
        println!("JetStream stream '{}' already exists.", stream_name);
        return Ok(());
    }

    println!("Creating JetStream stream '{}' for subject '{}'", stream_name, subject);

    let config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        storage: jetstream::stream::StorageType::File,
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_consumers: -1,
        max_messages: -1,
        max_bytes: -1,
        discard: jetstream::stream::DiscardPolicy::Old,
        ..Default::default()
    };

    js.create_stream(config)
        .await
        .map_err(|e| LJMError::LibraryError(format!("Failed to create JetStream stream: {}", e)))?;

    Ok(())
}

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};

async fn sample_with_config(
    run_id: usize,
    cfg: SampleConfig,
    config_rx: &mut watch::Receiver<SampleConfig>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), LJMError> {
    println!("[run #{run_id}] Connecting to NATS at {}", cfg.nats_url);
    let nc = async_nats::connect(cfg.nats_url.clone())
        .await
        .map_err(|e| LJMError::LibraryError(format!("Failed to connect to NATS: {}", e)))?;
    let js = async_nats::jetstream::new(nc);

    ensure_stream_exists(&js, &cfg.nats_stream, &cfg.nats_subject).await?;

    let handle = LJMLibrary::open_jack(DeviceType::ANY, ConnectionType::ANY, "ANY")?;
    let _guard = LabJackGuard { handle };

    let info = LJMLibrary::get_handle_info(handle)?;
    println!(
        "[run #{run_id}] Connected to {:?} (serial {})",
        info.device_type, info.serial_number
    );

    if matches!(info.device_type, DeviceType::T7) {
        LJMLibrary::write_name(handle, "AIN_ALL_NEGATIVE_CH", 199_u32)?;
    }
    LJMLibrary::write_name(handle, "AIN_ALL_RANGE", 10.0_f64)?;
    LJMLibrary::write_name(handle, "AIN_ALL_RESOLUTION_INDEX", 0_u32)?;
    LJMLibrary::write_name(handle, "STREAM_SETTLING_US", 0_u32)?;

    let channel_addresses: Result<Vec<i32>, LJMError> = cfg.channels.iter()
        .map(|ch| {
            LJMLibrary::name_to_address(&format!("AIN{}", ch))
                .map(|(addr, _)| addr)
                .map_err(|e| LJMError::LibraryError(format!("Invalid channel {}: {:?}", ch, e)))
        })
        .collect();
    let channel_addresses = channel_addresses?;
    let num_channels = channel_addresses.len();

    let actual_rate = LJMLibrary::stream_start(
        handle,
        cfg.scans_per_read,
        cfg.suggested_scan_rate,
        channel_addresses.clone(),
    )?;
    println!(
        "[run #{run_id}] Streaming started: {} scans/read @ {} Hz",
        cfg.scans_per_read, actual_rate
    );

    let (scan_tx, mut scan_rx) = mpsc::channel::<Vec<f64>>(32);

    // Shared running flag for stopping the blocking loop
    let running = Arc::new(AtomicBool::new(true));
    let running_reader = running.clone();
    let scan_tx_reader = scan_tx.clone();

    // Single long-lived blocking task for reading
    let read_handle = tokio::task::spawn_blocking(move || {
        while running_reader.load(Ordering::Relaxed) {
            match LJMLibrary::stream_read(handle) {
                Ok(batch) => {
                    if scan_tx_reader.blocking_send(batch).is_err() {
                        break; // receiver gone
                    }
                }
                Err(e) => {
                    eprintln!("[run #{run_id}] Error reading stream: {:?}", e);
                    break;
                }
            }
        }
    });

    let mut builder = FlatBufferBuilder::new();

    loop {
        tokio::select! {
            Some(batch) = scan_rx.recv() => {
                let batch_timestamp = New_York.from_utc_datetime(&Utc::now().naive_utc()).to_rfc3339();
                let scans = batch.chunks(num_channels);
                for scan in scans {
                    builder.reset();
                    let ts_fb = builder.create_string(&batch_timestamp);
                    let values_fb = builder.create_vector(scan);
                    let scan_args = ScanArgs { timestamp: Some(ts_fb), values: Some(values_fb) };
                    let scan_offset = sampler::Scan::create(&mut builder, &scan_args);
                    builder.finish(scan_offset, None);
                    let data = builder.finished_data().to_vec();
                    if let Err(e) = js.publish(cfg.nats_subject.clone(), data.into()).await {
                        eprintln!("[run #{run_id}] Failed to publish to NATS: {}", e);
                    }
                }
            }
            _ = config_rx.changed() => {
                println!("[run #{run_id}] Config change detected. Stopping stream...");
                running.store(false, Ordering::Relaxed);
                let _ = LJMLibrary::stream_stop(handle);
                drop(scan_tx);
                let _ = read_handle.await;
                return Ok(());
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    println!("[run #{run_id}] Shutdown signal received. Stopping stream...");
                    running.store(false, Ordering::Relaxed);
                    let _ = LJMLibrary::stream_stop(handle);
                    drop(scan_tx);
                    let _ = read_handle.await;
                    return Ok(());
                }
            }
        }
    }
}
