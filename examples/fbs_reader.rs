use std::env;
use std::fs::File;
use std::io::{self, Read, BufReader};
use std::thread;
use std::time::Duration;

use flatbuffers::root;

// Reuse the generated code from `flatc --rust sample_data.fbs`
mod sample_data_generated {
    #![allow(dead_code, unused_imports)]
    include!("../src/data_generated.rs");
}
use sample_data_generated::sampler::Scan;

fn usage() -> ! {
    eprintln!(
        "Usage: fbs_reader <path-to.fbsdata> [--follow]\n\
         Example: fbs_reader outputs/fbs/2025-08-10_12-00-00_scans100_rate1000_ch0-1-2.fbsdata --follow"
    );
    std::process::exit(1);
}

/// Read exactly `buf.len()` bytes into `buf`.
/// If `follow` is true, this will wait for more bytes when EOF is reached, like `tail -f`.
fn read_exact_with_follow<R: Read>(r: &mut R, mut buf: &mut [u8], follow: bool) -> io::Result<()> {
    while !buf.is_empty() {
        match r.read(buf) {
            Ok(0) => {
                if follow {
                    thread::sleep(Duration::from_millis(200));
                    continue;
                } else {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
                }
            }
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || args.len() > 2 {
        usage();
    }
    let follow = args.iter().any(|a| a == "--follow");
    let path = args.iter().find(|a| *a != "--follow").unwrap();

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    loop {
        // Read 4-byte little-endian size prefix
        let mut len_buf = [0u8; 4];
        if let Err(e) = read_exact_with_follow(&mut reader, &mut len_buf, follow) {
            // If not following and we hit EOF, we just stop gracefully.
            if e.kind() == io::ErrorKind::UnexpectedEof && !follow {
                break;
            } else if !follow {
                return Err(e);
            } else {
                // In follow mode, the helper already waited; loop continues.
                continue;
            }
        }

        let len = u32::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        if let Err(e) = read_exact_with_follow(&mut reader, &mut data, follow) {
            if e.kind() == io::ErrorKind::UnexpectedEof && !follow {
                break;
            } else if !follow {
                return Err(e);
            } else {
                // In follow mode, we’ll wait and retry next loop.
                continue;
            }
        }

        // Parse FlatBuffer root
        match root::<Scan>(&data) {
            Ok(scan) => {
                let ts = scan.timestamp().unwrap_or("<missing-ts>");
                // `values()` returns a vector-like object; use `iter()` to collect.
                if let Some(vals) = scan.values() {
                    let vals: Vec<f64> = vals.iter().collect();
                    println!("{} -> {:?}", ts, vals);
                } else {
                    println!("{} -> []", ts);
                }
            }
            Err(e) => eprintln!("Failed to parse Scan flatbuffer: {e}"),
        }
    }

    Ok(())
}
