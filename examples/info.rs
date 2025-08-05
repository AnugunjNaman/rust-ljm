// Import the external crate `ljmrs` which provides bindings to LabJack's LJM (LabJack M Series) library.
extern crate ljmrs;

// Import necessary modules from `ljmrs` and standard library.
use ljmrs::{LJMLibrary, DeviceType, ConnectionType};
use std::net::Ipv4Addr;

// Function: Convert a 32-bit signed integer (i32) representing an IP in network byte order into an Ipv4Addr.
fn u32_to_ipv4(ip_i32: i32) -> Ipv4Addr {
    let bytes = [
        ((ip_i32 >> 24) & 0xFF) as u8,
        ((ip_i32 >> 16) & 0xFF) as u8,
        ((ip_i32 >> 8) & 0xFF) as u8,
        (ip_i32 & 0xFF) as u8,
    ];
    Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])
}

// Function: Retrieve and display information about a connected LabJack device, including per-channel config.
fn info() {
    #[cfg(feature = "staticlib")]
    unsafe {
        // Initialize the LJM library
        LJMLibrary::init().unwrap();
    }

    // Attempt to open a connection to any available LabJack device
    // Parameters:
    //   - DeviceType::ANY: Connects to any supported LabJack (T7, T8, etc.)
    //   - ConnectionType::ANY: Automatically detects USB, Ethernet, or other interface
    //   - "ANY": Let LJM auto-discover the first connected device
    let open_call = LJMLibrary::open_jack(
        DeviceType::ANY,
        ConnectionType::ANY,
        "ANY".to_string(),
    ).expect("Could not open LabJack");

    println!("Opened LabJack, got handle: {}", open_call);

    // Retrieve device info
    let info = LJMLibrary::get_handle_info(open_call).expect("Handle verification failed.");

    // Extract IP address and convert to Ipv4Addr
    let ip_u32 = info.ip_address;
    let ip_addr = u32_to_ipv4(ip_u32);

    // Print basic device info
    println!("Device Type: {}", info.device_type);
    println!("IP Address: {}", ip_addr);
    println!("Port: {}", info.port);
    println!("Connection Type: {}", info.connection_type);
    println!("Max Bytes per Megabyte: {}", info.max_bytes_per_megabyte);
}

// Main entry point of the program
fn main() {
    // Get info about LabJack
    info();
}