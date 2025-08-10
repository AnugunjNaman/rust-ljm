extern crate ljmrs;

use ljmrs::{LJMLibrary, DeviceType, ConnectionType};
use std::net::Ipv4Addr;

fn u32_to_ipv4(ip_i32: i32) -> Ipv4Addr {
    let bytes = [
        ((ip_i32 >> 24) & 0xFF) as u8,
        ((ip_i32 >> 16) & 0xFF) as u8,
        ((ip_i32 >> 8) & 0xFF) as u8,
        (ip_i32 & 0xFF) as u8,
    ];
    Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])
}

fn info() {
    #[cfg(feature = "staticlib")]
    unsafe {
        // Initialize the LJM library
        LJMLibrary::init().unwrap();
    }

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

fn main() {
    // Get info about LabJack
    info();
}