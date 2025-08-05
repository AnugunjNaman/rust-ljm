extern crate ljmrs;

use ljmrs::{LJMLibrary, DeviceType, ConnectionType};

fn read_ain_config() {
    #[cfg(feature = "staticlib")]
    unsafe {
        LJMLibrary::init().unwrap();
    }

    // Open connection to any LabJack device
    let handle = LJMLibrary::open_jack(
        DeviceType::ANY,
        ConnectionType::ANY,
        "ANY".to_string(),
    ).expect("Could not open LabJack");

    println!("Opened LabJack, got handle: {}", handle);

    for x in 0..8 {
        let str_val = format!("AIN{}_RANGE", x);
        let value = LJMLibrary::read_name(handle, str_val);
        match value {
            Ok(v) => println!("AIN{}_RANGE = {}", x, v),
            Err(e) => eprintln!("Error reading AIN{}_RANGE: {:?}", x, e),
        }
    }

}

fn main() {
    read_ain_config();
}