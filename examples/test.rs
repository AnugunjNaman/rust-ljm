extern crate ljmrs; 
// This imports the LabJack library crate, which lets us talk to the LabJack device.

use std::fs::{File, OpenOptions, metadata}; 
// File handling: we use this to read config.json, create CSV files, and check if the config file changed.

use std::io::BufReader; 
// BufReader helps us read files efficiently, like config.json.

use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}; 
// Arc + Mutex = share data safely between threads (like config data).
// AtomicBool = a special boolean that is thread-safe (we use it to handle Ctrl+C exit).

use std::thread; 
// Lets us spawn extra threads (we use one thread just to watch the config.json for changes).

use std::time::{Duration, Instant}; 
// Duration = for delays (sleep).
// Instant = for timing things (like calculating actual sampling rate every second).

use serde::Deserialize; 
// Serde helps us convert JSON data into our Rust structs.

use csv::Writer; 
// Writer lets us write sensor data into CSV files.

use ljmrs::LJMLibrary; 
// The main API we use to control and stream data from the LabJack device.


// ========== CONFIG STRUCT ==========
#[derive(Debug, Deserialize, Clone)]
struct StreamConfig {
    scans_per_read: usize,      // How many samples we grab in one read from the device.
    suggested_scan_rate: f64,   // How fast we *ask* LabJack to sample (Hz).
}


// Reads config.json and turns it into a StreamConfig struct.
// If file doesn't exist or is broken, we return None.
fn load_config(path: &str) -> Option<StreamConfig> {
    if let Ok(file) = File::open(path) {           // Try to open file.
        let reader = BufReader::new(file);         // Wrap it in a buffered reader (faster).
        serde_json::from_reader(reader).ok()       // Deserialize JSON -> StreamConfig.
    } else {
        None                                       // If file missing or error, return None.
    }
}


// ========== CREATE CSV WRITER ==========
fn create_csv(channels: &Vec<i32>, cfg: &StreamConfig) -> Writer<File> {
    // Grab the current date and time for naming the file.
    let now = chrono::Utc::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let time_str = now.format("%H%M%S").to_string();

    // Filename format: date_time_scanRate_scanPerRead.csv
    let filename = format!(
        "{}_{}_{}Hz_{}scan.csv",
        date_str,
        time_str,
        cfg.suggested_scan_rate,
        cfg.scans_per_read
    );

    // Create a brand new CSV file for this run.
    let file = OpenOptions::new()
        .create(true)  // If file doesn't exist, create it.
        .write(true)   // Open for writing (not appending because new run).
        .open(&filename)
        .expect("Failed to create new CSV file");

    println!("New CSV file created: {}", filename);

    // Wrap the file in a CSV writer.
    let mut writer = Writer::from_writer(file);

    // Write header row: "timestamp, AIN0, AIN1, ..."
    let mut header = vec!["timestamp".to_string()];
    for ch in channels {
        header.push(format!("AIN{}", ch));
    }
    writer.write_record(&header).unwrap();

    writer
}


// ========== MAIN STREAM FUNCTION ==========
fn stream() {
    #[cfg(feature = "staticlib")]
    unsafe { LJMLibrary::init().unwrap(); }
    // Initialize LabJack if we built the library statically.

    // Open a connection to ANY connected LabJack.
    let open_call = LJMLibrary::open_jack(
        ljmrs::DeviceType::ANY,          // Any LabJack model.
        ljmrs::ConnectionType::ANY,      // USB/Ethernet auto-detect.
        "ANY".to_string(),               // Just pick the first device found.
    ).expect("Failed to open LabJack device");

    println!("Successfully opened LabJack device. Handle: {}", open_call);

    // The analog input channels we want to read (AIN0 to AIN7).
    let channels = vec![0, 1, 2, 3, 4, 5, 6, 7];
    let num_channels = channels.len(); // Save the count.

    // Load config.json, or use defaults if file missing.
    let config_path = "config.json";
    let config = load_config(config_path).unwrap_or(StreamConfig {
        scans_per_read: 100,     // Default: grab 100 scans per read.
        suggested_scan_rate: 1000.0, // Default: aim for 1000 Hz sampling.
    });

    // Shared config state between threads (main loop + config watcher thread).
    let shared_config = Arc::new(Mutex::new(config));

    // Shutdown flag for Ctrl+C.
    let shutdown = Arc::new(AtomicBool::new(false));

    // ========= HANDLE CTRL+C =========
    {
        let shutdown_clone = Arc::clone(&shutdown);
        ctrlc::set_handler(move || {
            println!("\nCtrl+C detected! Stopping stream...");
            shutdown_clone.store(true, Ordering::SeqCst); // Flip shutdown flag.
        }).expect("Error setting Ctrl+C handler");
    }

    // ========= WATCH CONFIG FILE FOR CHANGES (in another thread) =========
    {
        let shared_config_clone = Arc::clone(&shared_config);
        thread::spawn(move || {
            let mut last_modified = metadata(config_path).ok().and_then(|m| m.modified().ok());
            loop {
                if let Ok(meta) = metadata(config_path) {
                    if let Ok(modified) = meta.modified() {
                        // If file changed since last check...
                        if Some(modified) != last_modified {
                            if let Some(new_config) = load_config(config_path) {
                                let mut cfg = shared_config_clone.lock().unwrap();
                                *cfg = new_config.clone(); // Update shared config.
                                println!("Configuration updated: {:?}", new_config);
                                last_modified = Some(modified);
                            }
                        }
                    }
                }
                thread::sleep(Duration::from_secs(2)); // Check every 2 seconds.
            }
        });
    }

    // ========= CREATE INITIAL CSV FILE =========
    let mut csv_writer;
    {
        let cfg = shared_config.lock().unwrap().clone();
        csv_writer = create_csv(&channels, &cfg);
    }

    // We'll store rows temporarily here before flushing to CSV (faster).
    let mut buffer: Vec<Vec<String>> = Vec::new();

    // Track how many scans we processed in the last second (for actual rate).
    let mut total_scans = 0;
    let mut rate_timer = Instant::now();

    // ========= MAIN STREAMING LOOP =========
    loop {
        // If Ctrl+C pressed, break out.
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Grab latest config settings.
        let cfg = { shared_config.lock().unwrap().clone() };
        println!(
            "Starting streaming: {} scans per read at {:.2} scans/sec",
            cfg.scans_per_read, cfg.suggested_scan_rate
        );

        // Start streaming on the LabJack device.
        match LJMLibrary::stream_start(
            open_call,
            cfg.scans_per_read as i32,
            cfg.suggested_scan_rate,
            channels.clone(),
        ) {
            Ok(_) => println!("Streaming started."),
            Err(e) => {
                // If starting failed, wait 2 seconds and try again.
                eprintln!("Failed to start streaming: {:?}. Retrying...", e);
                thread::sleep(Duration::from_secs(2));
                continue;
            }
        }

        // ========= INNER LOOP: ACTUAL STREAMING =========
        loop {
            // If Ctrl+C pressed, stop streaming gracefully.
            if shutdown.load(Ordering::SeqCst) {
                LJMLibrary::stream_stop(open_call).unwrap();
                println!("Stream stopped gracefully.");
                return;
            }

            // Check if config changed mid-stream (e.g., user edited config.json).
            let current_config = { shared_config.lock().unwrap().clone() };
            if current_config.scans_per_read != cfg.scans_per_read
                || current_config.suggested_scan_rate != cfg.suggested_scan_rate
            {
                println!("Config changed! Stopping stream and creating new CSV...");
                LJMLibrary::stream_stop(open_call).unwrap();
                thread::sleep(Duration::from_millis(500));

                // Flush any leftover buffered rows to CSV.
                for rec in buffer.drain(..) {
                    csv_writer.write_record(&rec).unwrap();
                }
                csv_writer.flush().unwrap();

                // Create a new CSV file for the new config settings.
                csv_writer = create_csv(&channels, &current_config);

                // Reset counters for new session.
                total_scans = 0;
                rate_timer = Instant::now();
                break; // Restart loop with new config.
            }

            // ========= READ DATA FROM LABJACK =========
            match LJMLibrary::stream_read(open_call) {
                Ok(read_value) => {
                    // read_value is a flat vector like: [AIN0, AIN1, AIN2..., AIN0, AIN1, ...]
                    let samples_per_read = read_value.len() / num_channels;
                    let timestamp = chrono::Utc::now().to_rfc3339();

                    // Loop through each scan in this read.
                    for scan_idx in 0..samples_per_read {
                        let mut record = vec![timestamp.clone()]; // First column: timestamp.
                        let start_idx = scan_idx * num_channels;

                        // Loop through all channels (AIN0...AIN7) and add them to row.
                        for ch in 0..num_channels {
                            record.push(format!("{:.6}", read_value[start_idx + ch]));
                        }
                        buffer.push(record); // Add row to buffer.
                    }

                    total_scans += samples_per_read; // Count scans for rate calc.

                    // ========= ONCE PER SECOND: FLUSH CSV + PRINT ACTUAL RATE =========
                    if rate_timer.elapsed().as_secs_f64() >= 1.0 {
                        // Write buffered rows to disk.
                        for rec in buffer.drain(..) {
                            csv_writer.write_record(&rec).unwrap();
                        }
                        csv_writer.flush().unwrap();

                        // Calculate actual achieved sampling rate.
                        let actual_rate = total_scans as f64 / rate_timer.elapsed().as_secs_f64();
                        println!("Actual sampling rate: {:.2} Hz", actual_rate);

                        // Reset counter and timer.
                        total_scans = 0;
                        rate_timer = Instant::now();
                    }
                }
                Err(e) => {
                    // If reading fails (e.g., device hiccup), stop and restart.
                    eprintln!("Stream read failed: {:?}. Restarting...", e);
                    LJMLibrary::stream_stop(open_call).unwrap();
                    thread::sleep(Duration::from_millis(500));
                    break;
                }
            }
        }
    }

    println!("Streaming stopped. Exiting program.");
}

// Entry point of the program.
fn main() {
    stream();
}
