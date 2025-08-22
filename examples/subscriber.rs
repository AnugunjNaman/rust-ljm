use async_nats;
use flatbuffers::root;
use futures_util::stream::StreamExt; // for .next()
use serde_json::json;
use std::error::Error;
use std::fs::{OpenOptions};
use std::io::Write;


// Import your generated FlatBuffers schema
mod sample_data_generated {
    #![allow(dead_code, unused_imports)]
    include!("../src/data_generated.rs"); // path relative to examples/
}
use sample_data_generated::sampler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let nats_url = "nats://0.0.0.0:4222";
    let subject = "labjack.data";

    // Open CSV in append mode, create if missing
    let mut csv_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("labjack_data.csv")?;

    // If the file is empty, write the header
    if csv_file.metadata()?.len() == 0 {
        writeln!(csv_file, "timestamp,values")?;
    }

    // Connect to NATS
    let nc = async_nats::connect(nats_url).await?;
    println!("Connected to NATS at {}", nats_url);

    // Subscribe
    let mut sub = nc.subscribe(subject.to_string()).await?;
    println!("Subscribed to '{}'", subject);

    // Receive loop
    while let Some(msg) = sub.next().await {
        match root::<sampler::Scan>(&msg.payload) {
            Ok(scan) => {
                let timestamp = scan.timestamp().unwrap_or("<no ts>");
                let values = scan.values().unwrap();
                let values_vec: Vec<f64> = values.iter().collect();

                // Print to console
                println!("Timestamp: {}", timestamp);
                println!("Values: {:?}", values_vec);

                // Optional JSON debug
                let json_obj = json!({
                    "timestamp": timestamp,
                    "values": values_vec
                });
                println!("As JSON: {}", json_obj);

                // Append to CSV
                let values_str = values_vec.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(";");
                writeln!(csv_file, "{},{}", timestamp, values_str)?;
                csv_file.flush()?;
            }
            Err(_) => {
                eprintln!("Failed to decode FlatBuffer payload");
            }
        }
    }

    Ok(())
}
