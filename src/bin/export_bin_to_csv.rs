use std::fs::File;
use std::io::{Read, BufReader};
use std::path::Path;
use csv::Writer;

use spr4918::stream_generated; // <- your crate/lib name
use stream_generated::stream::ScanBatch;

/// Read size-prefixed FlatBuffer frames from file
fn read_size_prefixed_frames<P: AsRef<Path>>(path: P) -> Vec<Vec<u8>> {
    let mut reader = BufReader::new(File::open(path).expect("open .bin failed"));
    let mut all = Vec::new();
    reader.read_to_end(&mut all).expect("read .bin failed");

    let mut frames = Vec::new();
    let mut off = 0usize;

    while off + 4 <= all.len() {
        let len = u32::from_le_bytes(all[off..off+4].try_into().unwrap()) as usize;
        let end = off + 4 + len;
        if end > all.len() { break; }
        frames.push(all[off..end].to_vec()); // keep size prefix included
        off = end;
    }
    frames
}

fn export_to_csv(bin_path: &str, csv_path: &str) {
    let frames = read_size_prefixed_frames(bin_path);
    if frames.is_empty() {
        println!("No frames.");
        return;
    }

    let mut writer = Writer::from_path(csv_path).expect("create CSV failed");

    // Header from first batch
    let first = &frames[0];
    let batch = flatbuffers::size_prefixed_root::<ScanBatch>(first).expect("parse first batch");
    let channel_ids = batch.channel_ids().expect("missing channel_ids");
    let mut header = vec!["timestamp".to_string()];
    for ch in channel_ids {
        header.push(format!("AIN{}", ch));
    }
    writer.write_record(&header).unwrap();

    // Rows
    for frame in frames {
        let batch = flatbuffers::size_prefixed_root::<ScanBatch>(&frame).expect("parse batch");
        let ts = batch.timestamp().unwrap_or_default();
        let scans = batch.scans_in_batch() as usize;
        let chans = batch.channels_per_scan() as usize;
        let values = batch.values().expect("missing values"); // Vector<'a, f64>

        for s in 0..scans {
            let start = s * chans;
            let end = start + chans;

            let mut row = vec![ts.to_string()];
            // FlatBuffers Vector doesn't support slicing; use .get(i)
            for i in start..end {
                let v = values.get(i); // f64
                row.push(format!("{:.6}", v));
            }
            writer.write_record(&row).unwrap();
        }
    }

    writer.flush().unwrap();
    println!("CSV export complete: {}", csv_path);
}

fn main() {
    // Example:
    let bin_file = "2025-08-05_165123_2000Hz_200scan_AIN0_AIN1_AIN5.bin";
    let csv_file = "exported_data.csv";
    export_to_csv(bin_file, csv_file);
}
