extern crate ljmrs;

use std::time::Instant;
use ljmrs::LJMLibrary;

/// Main function to demonstrate streaming data from multiple analog input channels (AIN0–AIN7)
/// on a LabJack device using the LJM library.
fn stream() {
    
    #[cfg(feature = "staticlib")]
    unsafe {
        LJMLibrary::init().unwrap();
    }

    // Attempt to open a connection to any available LabJack device
    // Parameters:
    //   - DeviceType::ANY: Connects to any supported LabJack (T7, T8, etc.)
    //   - ConnectionType::ANY: Automatically detects USB, Ethernet, or other interface
    //   - "ANY": Let LJM auto-discover the first connected device
    let open_call = LJMLibrary::open_jack(
        ljmrs::DeviceType::ANY,
        ljmrs::ConnectionType::ANY,
        "ANY".to_string(),
    )
    .expect("Failed to open LabJack device");

    // Print success message with the handle (device ID) returned by LJM
    println!("Successfully opened LabJack device. Handle: {}", open_call);
    
    // Define the list of analog input channels to stream
    // Use integer indices: AIN0 = 0, AIN1 = 1, ..., AIN7 = 7
    let channels = vec![0, 1, 2, 3, 4, 5, 6, 7];
    let num_channels = channels.len();

    // Set the number of scans to read per call to the stream_read function
    // This controls how many samples are returned in each batch
    let scans_per_read = 1000;

    // Set the desired scan rate (samples per second) for streaming
    let suggested_scan_rate = 10_000.0;

    // Print configuration details before starting stream
    println!(
        "Starting streaming session: {} scans per read at {:.2} scans/sec",
        scans_per_read, suggested_scan_rate
    );

    // Start the streaming process using the configured parameters
    // Arguments:
    //   - open_call: Handle to the opened LabJack device
    //   - scans_per_read: Number of samples to return in each stream_read call
    //   - suggested_scan_rate: Target sampling frequency (in Hz)
    //   - channels: Vector of channel indices to read (must be i32)
    LJMLibrary::stream_start(open_call, scans_per_read, suggested_scan_rate, channels)
        .expect("Failed to start streaming");

    // Confirm stream is active
    println!("Streaming has started successfully.");


    // Use a timer to measure elapsed time during streaming
    let start_time = Instant::now();

    // Begin reading data in batches until 50 batches are received
    // This simulates a short test run (e.g., 50 * 1000 = 50,000 samples)
    let mut batch_count = 0;
    while batch_count < 50 {
        // Read one batch of data from the streaming buffer
        // Returns a flat vector of calibrated volts for all channels in order
        let read_value = LJMLibrary::stream_read(open_call)
            .expect("Failed to read streaming data");

        // Calculate how many samples were returned per channel
        // Total elements divided by number of channels
        let samples_per_read = read_value.len() / num_channels;

        // Print batch header with current batch index and time elapsed
        println!("\n--- Batch {} ({} scans) ---", batch_count + 1, samples_per_read);
        println!("Elapsed since start: {:.3} seconds", start_time.elapsed().as_secs_f64());

        // Process each individual scan in the batch
        for scan_index in 0..samples_per_read {
            // Compute starting index in the flat vector for this scan
            let start_idx = scan_index * num_channels;

            // Print scan number with alignment
            print!("  Scan {:>4} | ", scan_index);

            // Iterate through each channel and convert raw value to voltage
            for channel_index in 0..num_channels {
                // Extract the raw ADC count for this channel from the flat vector
                let volts = read_value[start_idx + channel_index];
                print!("AIN{}: {:.4} mV/V | ", channel_index, volts);
            }

            // End the line after printing all channels for this scan
            println!();
        }

        // Increment batch counter
        batch_count += 1;
    }

    // Print final summary after streaming completes
    let total_elapsed = start_time.elapsed();
    println!(
        "\nStreaming session completed after {:.3} seconds.",
        total_elapsed.as_secs_f64()
    );

    // Stop the streaming process to free resources
    LJMLibrary::stream_stop(open_call)
        .expect("Failed to stop streaming");

    // Final confirmation message
    println!("Streaming has been successfully stopped.");
}

// Entry point of the program
fn main() {
    // Call the streaming function to begin data acquisition
    stream();
}