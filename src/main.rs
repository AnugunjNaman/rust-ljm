use tokio::sync::watch;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use chrono::{Utc};
use chrono::TimeZone;
use chrono_tz::America::New_York;
use notify::{Watcher, RecursiveMode, recommended_watcher};
use std::sync::mpsc::channel;

use ljmrs::{LJMLibrary, LJMError};
use ljmrs::handle::{DeviceType, ConnectionType};

use flatbuffers::FlatBufferBuilder;
use async_nats::jetstream::{self, stream::Config as StreamConfig};

mod sample_data_generated {
    #![allow(dead_code, unused_imports)]
    include!("data_generated.rs");
}

use sample_data_generated::sampler::{self, ScanArgs};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SampleConfig {
    scans_per_read: i32,
    suggested_scan_rate: f64,
    channels: Vec<u8>,
    nats_url: String,
    nats_subject: String,
    nats_stream: String
}

struct LabJackGuard {
    handle: i32
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

    // shutdown signal channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Initialize LJM
    #[cfg(feature = "staticlib")]
    unsafe { LJMLibrary::init()?; }
    #[cfg(all(feature = "dynlink", not(feature = "staticlib")))]
    unsafe {
        let path = std::env::var("LJM_PATH").ok();
        LJMLibrary::init(path)?;
    }

    tokio::spawn(run_sampler(config_rx.clone(), shutdown_rx.clone()));
    tokio::spawn(watch_config_file(config_path.to_string(), config_tx));

    // Wait for Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| LJMError::LibraryError(format!("Failed to listen for Ctrl+C: {}", e)))?;

    println!("Shutting down...");
    let _ = shutdown_tx.send(true); // notify all tasks

    Ok(())
}

fn load_config<P: AsRef<Path>>(path: P) -> Result<SampleConfig, std::io::Error> {
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("JSON parse error: {}", e))
    })?)
}

async fn watch_config_file(path: String, config_tx: watch::Sender<SampleConfig>) {
    let (tx, rx) = channel();
    let mut watcher = recommended_watcher(move |res| {
        let _ = tx.send(res);
    }).expect("Failed to init watcher");

    watcher.watch(Path::new(&path), RecursiveMode::NonRecursive)
        .expect("Failed to watch config file");

    for res in rx {
        if res.is_ok() {
            match load_config(&path) {
                Ok(cfg) => {
                    println!("Config updated: {:?}", cfg);
                    let _ = config_tx.send(cfg);
                }
                Err(e) => eprintln!("Failed to reload config: {:?}", e),
            }
        }
    }
}

async fn run_sampler(
    mut config_rx: watch::Receiver<SampleConfig>,
    mut shutdown_rx: watch::Receiver<bool>
) {
    loop {
        if *shutdown_rx.borrow() {
            println!("Sampler shutting down...");
            break;
        }

        let cfg = config_rx.borrow().clone();
        println!("Starting sampler with {:?}", cfg);

        if let Err(e) = sample_with_config(cfg.clone(), &mut config_rx, &mut shutdown_rx).await {
            eprintln!("Sampler error: {:?}", e);
        }

        if *shutdown_rx.borrow() {
            println!("Shutdown detected after sampler error/config change");
            break;
        }

        println!("Restarting sampler after config change...");
    }
}

async fn ensure_stream_exists(
    js: &jetstream::Context,
    stream_name: &str,
    subject: &str
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

async fn sample_with_config(
    cfg: SampleConfig,
    config_rx: &mut watch::Receiver<SampleConfig>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), LJMError> {
    // Connect to NATS & JetStream
    let nc = async_nats::connect(cfg.nats_url.clone())
        .await
        .map_err(|e| LJMError::LibraryError(format!("Failed to connect to NATS: {}", e)))?;
    let js = async_nats::jetstream::new(nc);

    // Ensure stream exists
    ensure_stream_exists(&js, &cfg.nats_stream, &cfg.nats_subject).await?;

    // Open LabJack device
    let handle = LJMLibrary::open_jack(DeviceType::ANY, ConnectionType::ANY, "ANY")?;
    let _guard = LabJackGuard { handle };

    let info = LJMLibrary::get_handle_info(handle)?;
    println!("Connected to {:?} (serial {})", info.device_type, info.serial_number);

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
    println!("Streaming started: {} scans/read @ {} Hz", cfg.scans_per_read, actual_rate);

    let mut builder = FlatBufferBuilder::new();

    loop {
        tokio::select! {
            _ = config_rx.changed() => {
                println!("Config change detected. Restarting stream...");
                return Ok(());
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    println!("Shutdown signal received. Stopping stream...");
                    return Ok(());
                }
            }
            result = tokio::task::spawn_blocking({
                let handle = handle;
                move || LJMLibrary::stream_read(handle)
            }) => {
                let batch = result.map_err(|e| LJMError::LibraryError(format!("Task join error: {}", e)))??;
                let batch_timestamp = New_York.from_utc_datetime(&Utc::now().naive_utc()).to_rfc3339();
                let scans = batch.chunks(num_channels);

                for scan in scans {
                    builder.reset();

                    let ts_fb = builder.create_string(&batch_timestamp);
                    let values_fb = builder.create_vector(scan);

                    let scan_args = ScanArgs {
                        timestamp: Some(ts_fb),
                        values: Some(values_fb),
                    };
                    let scan_offset = sampler::Scan::create(&mut builder, &scan_args);
                    builder.finish(scan_offset, None);

                    let data = builder.finished_data().to_vec();

                    js.publish(cfg.nats_subject.clone(), data.into())
                        .await
                        .map_err(|e| LJMError::LibraryError(format!("Failed to publish to NATS: {}", e)))?;
                }
            }
        }
    }
}
