use std::fs::File;
use std::io::{Read, BufReader};
use std::path::{Path, PathBuf};
use std::env;
use csv::Writer;
use serde::Deserialize;

use spr4918::stream_generated; // your crate/lib name
use stream_generated::stream::ScanBatch;

#[derive(Debug, Deserialize)]
struct ExportConfig {
    // single-file legacy
    input_bin: Option<String>,
    output_csv: Option<String>,
    // multi-file
    input_bins: Option<Vec<String>>,
    output_csvs: Option<Vec<String>>,
    // optional output directory prefix
    output_dir: Option<String>,
}

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
        frames.push(all[off..end].to_vec()); // include size prefix + buffer
        off = end;
    }
    frames
}

fn default_csv_from_bin_with_dir(bin_path: &str, output_dir: Option<&str>) -> String {
    let p = Path::new(bin_path);
    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("export");
    if let Some(dir) = output_dir {
        let mut out = PathBuf::from(dir);
        out.push(format!("{stem}.csv"));
        out.to_string_lossy().into_owned()
    } else {
        let parent = p.parent().unwrap_or_else(|| Path::new("."));
        let mut out = PathBuf::from(parent);
        out.push(format!("{stem}.csv"));
        out.to_string_lossy().into_owned()
    }
}

fn load_export_config() -> Option<ExportConfig> {
    let path = Path::new("export.json");
    if !path.exists() { return None; }
    let f = File::open(path).ok()?;
    serde_json::from_reader::<_, ExportConfig>(f).ok()
}

fn export_single(bin_path: &str, csv_path: &str) {
    let frames = read_size_prefixed_frames(bin_path);
    if frames.is_empty() {
        eprintln!("No frames in {bin_path} — skipping.");
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
        let values = batch.values().expect("missing values"); // Vector<'_, f64>

        for s in 0..scans {
            let start = s * chans;
            let end = start + chans;

            let mut row = vec![ts.to_string()];
            for i in start..end {
                let v = values.get(i); // f64
                row.push(format!("{:.6}", v));
            }
            writer.write_record(&row).unwrap();
        }
    }

    writer.flush().unwrap();
    println!("OK: {bin_path} -> {csv_path}");
}

fn export_multi(input_bins: &[String], output_csvs: Option<&[String]>, output_dir: Option<&str>) {
    if let Some(csvs) = output_csvs {
        if csvs.len() != input_bins.len() {
            eprintln!("ERROR: output_csvs length ({}) does not match input_bins length ({})",
                csvs.len(), input_bins.len());
            return;
        }
        for (bin, csv) in input_bins.iter().zip(csvs.iter()) {
            export_single(bin, csv);
        }
    } else {
        for bin in input_bins {
            let csv = default_csv_from_bin_with_dir(bin, output_dir);
            export_single(bin, &csv);
        }
    }
}

fn main() {
    // Priority: CLI args (single file) > export_config.json (single or multi) > defaults
    let args: Vec<String> = env::args().collect();

    if args.len() >= 2 {
        // CLI: single file mode
        let bin = args[1].clone();
        let csv = if args.len() >= 3 {
            args[2].clone()
        } else {
            default_csv_from_bin_with_dir(&bin, None)
        };
        println!("Input BIN: {bin}");
        println!("Output CSV: {csv}");
        export_single(&bin, &csv);
        return;
    }

    if let Some(cfg) = load_export_config() {
        if let Some(list) = cfg.input_bins.as_ref() {
            // Multi-file batch
            export_multi(
                list,
                cfg.output_csvs.as_deref(),
                cfg.output_dir.as_deref()
            );
            return;
        }

        // Single-file legacy
        let bin = cfg.input_bin.unwrap_or_else(|| "stream_output.bin".to_string());
        let csv = cfg.output_csv.unwrap_or_else(|| default_csv_from_bin_with_dir(&bin, cfg.output_dir.as_deref()));
        println!("Input BIN: {bin}");
        println!("Output CSV: {csv}");
        export_single(&bin, &csv);
        return;
    }

    // Nothing provided: default single
    let bin = "stream_output.bin".to_string();
    let csv = default_csv_from_bin_with_dir(&bin, None);
    println!("Input BIN: {bin}");
    println!("Output CSV: {csv}");
    export_single(&bin, &csv);
}
