import asyncio
import datetime
import json
import shelve
import signal
import socket
from asyncio import Queue

from filelock import FileLock
from jsonschema import ValidationError, validate
from labjack import ljm
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrNoServers
from nats.js.errors import BucketNotFoundError, KeyNotFoundError

NATS_SERVER_IP = ["nats://127.0.0.1:4222"]

def handle_exit_signal(loop, sig):
    """Handles exit signals like Ctrl+C."""
    print(f"\nReceived exit signal {sig.name}. Cleaning up...")
    for task in tasks.values():
        task.cancel()
    loop.stop()
    
# Mapping for connection types
connection_type_map = {
    0: "ANY",
    1: "USB",
    2: "TCP",
    3: "Ethernet",
    4: "WiFi"
}

# Mapping for device types
device_type_map = {
    0: "ANY",
    4: "T4",
    7: "T7",
    8: "T8",
    84: "T-SERIES",
    200: "DIGIT"
}

# Global variables to keep track of running tasks and queues
tasks = {}  # To store tasks, keyed by serial number
queues = {}  # To store queues, keyed by serial number
max_labjack_config_retries = 3

config_schema = {
    "type": "object",
    "properties": {
        "scan_rate": {
            "type": "integer",
            "minimum": 1,
            "maximum": 40000
        },
        "gain": {
            "type": "number",
            "enum": [0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128]
        }
        ,
        "STREAM_SETTLING_US": {
            "type": "integer",
            "minimum": 0,  # Minimum settling time in microseconds
            "maximum": 50000  # Maximum settling time in microseconds (50 ms)
        },
        "STREAM_RESOLUTION_INDEX": {
            "type": "integer",
            "minimum": 0,  # Minimum index (auto)
            "maximum": 12  # Maximum index (highest resolution)
        },
        "channels_enabled": {
            "type": "object",
            "patternProperties": {
                "^[0-9]+$": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["SCS", "PC", "SG"]
                        },
                        "name": {"type": "string"},
                        "raw_data": {
                            "type": "string",
                            "enum": ["volt", "cal"]
                        },
                        "report_unit": {
                            "type": "string",
                            "enum": [
                                "V", "mV", "muV",  # For raw_data: "volt"
                                "m", "mm", "mum",  # For raw_data: "cal", type: "SCS"
                                "Pa", "KPa",  # For raw_data: "cal", type: "PC"
                                "microstrain"  # For raw_data: "cal", type: "SG"
                            ]
                        },
                        "data_type": {
                            "type": "string",
                            "enum": ["FLOAT32", "FLOAT16", "EXP"]
                        },
                        "nats_stream_rate": {
                            "type": "integer",
                            "minimum": 100,
                            "maximum": 100000,
                            "multipleOf": 100
                        }
                    },
                    "required": ["type", "name"]
                }
            },
            "additionalProperties": False
        }
    },
    "required": ["scan_rate", "gain", "STREAM_SETTLING_US", "STREAM_RESOLUTION_INDEX"]
}

async def start_labjack_sample(queue):
    try:
        print("start_labjack_sample function started")
        stream_config, channel_details = await queue.get()
        print(f"{stream_config.get('handle')} - Received stream_config: {stream_config}")
        print(f"{stream_config.get('handle')} - Received channel_details: {channel_details}")
        
        queue.task_done()  # Mark the queue item as processed

        serial_number = stream_config.get("serial_number")
        handle = stream_config.get("handle")
        scan_rate = stream_config["scan_rate"]
        num_addresses = stream_config["num_addresses"]
        scansPerRead = int(scan_rate / num_addresses)
        aScanList = stream_config["aScanList"]
        
        print(f"{handle} - scansPerRead : {scansPerRead}")
        # Extract channel information directly from channel_details
        file_path = f"all_data_{serial_number}"
        file_lock = FileLock(f"all_data_{serial_number}.lock")
        with shelve.open(file_path, flag='n') as shelf:
            print(f"Deleted existing file for all_data")
        actual_scan_rate = await asyncio.to_thread(ljm.eStreamStart, handle, int(scan_rate / num_addresses), num_addresses, aScanList, scan_rate)
        print(f"{handle} - Stream started with an actual scan rate of {actual_scan_rate} Hz.")

        # Initialize counters for skipped samples and loop index
        i = 0
        j = 0
        print(f"{handle} - Starting data sampling loop...")

        # Start reading data
        while True:
            #print(f"{handle} - Reading data iteration {i}")
        
            # Asynchronously read data from the LabJack device
            try:
                start = datetime.datetime.now()
                ret = await asyncio.to_thread(ljm.eStreamRead, handle)
               
                if not ret or len(ret) < 1:
                    print(f"{handle} - Error or empty data returned from eStreamRead.")
                    continue
                aData = ret[0]
                
                with file_lock:
                    with shelve.open(file_path, writeback=True) as shelf:
                        key = start.isoformat()
                        shelf[key] = [start, aData]

            except Exception as e:
                print(f"{handle} - Error during eStreamRead or processing: {e}")
                await asyncio.to_thread(ljm.eStreamStop,handle)
                i = 0
                j = j+1
                await asyncio.sleep(0.2) 
                await asyncio.to_thread(ljm.eStreamStart, handle, int(scan_rate / num_addresses), num_addresses, aScanList, scan_rate)
            i += 1
            print(f"{handle} - Iteration {i,j} completed.")
    except asyncio.CancelledError:
        ljm.eStreamStop(handle)
        ljm.close(handle)
        print(f"{handle} - cleaning handle:")
        raise

async def set_labjack_config(serial_number, scan_rate, gain, stream_settling_us, stream_resolution_index, channels):
    """
    Configures a LabJack T7 device asynchronously and starts sampling.
    
    Args:
        serial_number (str): The serial number of the LabJack device.
        scan_rate (int): The desired scan rate in Hz.
        gain (int): Gain level (1, 10, 100, 1000) for input range configuration.
        stream_settling_us (int): Settling time in microseconds.
        stream_resolution_index (int): Resolution index (0 to 12).
        channels (list of str): List of analog input channels to scan (e.g., ["AIN0", "AIN1"]).
    
    Returns:
        handle: The handle to the configured LabJack device.
    """
    #scan_rate = 1000
    # Validate gain input and map it to voltage range
    gain_to_range = {
    0.125: 11.0,   # ±11.0V
    0.25: 9.6,     # ±9.6V
    0.5: 4.8,      # ±4.8V
    1: 2.4,        # ±2.4V
    2: 1.2,        # ±1.2V
    4: 0.6,        # ±0.6V
    8: 0.3,        # ±0.3V
    16: 0.15,      # ±0.15V
    32: 0.075,     # ±0.075V
    64: 0.036,     # ±0.036V
    128: 0.018 }    # ±0.018V}
    if gain not in gain_to_range:
        raise ValueError("Invalid gain. Allowed values are 1, 10, 100.")
    
    if not channels or not isinstance(channels, list):
        raise ValueError("Invalid channels. Provide a non-empty list of channel names (e.g., ['AIN0', 'AIN1']).")

    # Open the specified LabJack T7
    handle = await asyncio.to_thread(ljm.openS, "ANY", "ETHERNET", str(serial_number))
    print(f"Connected to LabJack T7 with serial number: {serial_number}")

    try:
        # Stream Configuration
        aScanListNames = channels  # Channels to stream (user-specified)
        num_addresses = len(aScanListNames)
        aScanList = await asyncio.to_thread(ljm.namesToAddresses, num_addresses, aScanListNames)
        aScanList = aScanList[0]  # Get addresses from the result

        # Write configuration settings
        aNames = [
            "AIN_ALL_RANGE",  # Input range for each channel
            "STREAM_SETTLING_US",                       # Settling time
            "STREAM_RESOLUTION_INDEX",                  # Resolution index
            "STREAM_TRIGGER_INDEX",                     # Ensure triggered stream is disabled
            "STREAM_CLOCK_SOURCE",                       # Internally-clocked stream
            #"STREAM_BUFFER_SIZE_BYTES"
        ]
        aValues = [
            gain_to_range[gain],  # Range based on gain
            stream_settling_us,                              # Settling time
            stream_resolution_index,                         # Resolution index
            0,                                               # Trigger index (disabled)
            0,                                               # Clock source (internal) 
            #32768,                                           #16384,32768, 262144
        ]
        
        print(f"aNames = {aNames}")
        print(f"aNames = {aValues}")
        await asyncio.to_thread(ljm.eWriteNames, handle, len(aNames), aNames, aValues)
        print("Stream configuration written successfully.")

        # Prepare the stream configuration to be returned
        stream_config = {
            "serial_number":serial_number,
            "handle": handle,
            "num_addresses": num_addresses,
            "aScanList": aScanList,
            "scan_rate": scan_rate,
        }

        return stream_config

    except ljm.LJMError as e:
        print(f"LJMError: {e}")
        await asyncio.to_thread(ljm.eStreamStop, handle)
        await asyncio.to_thread(ljm.close, handle)
        raise
    except Exception as e:
        print(f"Error: {e}")
        await asyncio.to_thread(ljm.close, handle)
        raise

async def configure_each_labjack(serial_number, config):
    try:
        # Extract LabJack configuration details
        scan_rate = config["scan_rate"]
        gain = config["gain"]
        settling_time = config.get("STREAM_SETTLING_US", 0)  # Default to 0 if not provided
        resolution_index = config.get("STREAM_RESOLUTION_INDEX", 0)  # Default to 0 if not provided
        channels = config.get("channels_enabled", {})  # Get channels as a dictionary
        
        # Convert channel indices to channel names (e.g., "AIN0", "AIN1")
        channel_names = [f"AIN{channel}" for channel in channels.keys()]
        
        channel_details = {
            channel: {
                "type": details["type"],
                "name": details["name"],
                "raw_data": details["raw_data"],
                "report_unit": details["report_unit"],
                "data_type": details["data_type"],
                "nats_stream_rate": details["nats_stream_rate"],
            }
            for channel, details in channels.items()
        }

        # Configure and start LabJack asynchronously
        stream_config = await set_labjack_config(
            serial_number=serial_number,
            scan_rate=scan_rate,
            gain=gain,
            stream_settling_us=settling_time,
            stream_resolution_index=resolution_index,
            channels=channel_names,
        )
        print(f"Successfully configured LabJack with serial number {serial_number}.")
        
        return stream_config, channel_details
    except Exception as e:
        print(f"Failed to configure LabJack serial_number: {e}")
        return None, None  # Explicitly return a fallback value

async def send_ping(nc):
    """Send a ping to the NATS server every second to keep the connection alive."""
    while True:
        try:
            # Flush to the NATS server to simulate a ping-like action
            await nc.flush()
            print("Connection is alive, flush sent to NATS server.")
        except Exception as e:
            print(f"Error during flush: {e}")
        await asyncio.sleep(15)  # Wait for 15 second before sending the next "ping"
        
# Function to manage LabJack devices and keys
async def init_key_and_config():
    # Initialize NATS client
    nc = NATS()

    try:
        await nc.connect(servers=NATS_SERVER_IP)  # Connect to the local NATS server
        print("connecting to jetstream")
        asyncio.create_task(send_ping(nc))
        js = nc.jetstream()
        try:
            await js.account_info()  # This will fail if JetStream is not started
        except Exception as e:
            print("NATS JetStream has not started. Please start it.")
            await nc.close()
            return
        global queues
        # DeviceType and ConnectionType
        device_type = ljm.constants.dtANY  # Search for any device type
        connection_type = ljm.constants.ctETHERNET #ljm.constants.ctANY  # Search for any connection type

        # Get connected LabJack devices
        num_found, device_types, connection_types, serial_numbers, ip_addresses = ljm.listAll(device_type, connection_type)

        print(f"Number of devices found: {num_found}")
        
        # Dictionary to store valid configurations
        last_revisions = {}  # Tracks the last revision of each key
        active_configs = {}  # Tracks valid configurations
        
        labjacks = []
        for i in range(num_found):
            # Map the numerical values to descriptive names
            device_type_name = device_type_map.get(device_types[i], f"Unknown ({device_types[i]})")
            connection_type_name = connection_type_map.get(connection_types[i], f"Unknown ({connection_types[i]})")
            
            # Handle negative IP address values by treating them as unsigned 32-bit integers
            ip_as_unsigned = ip_addresses[i] & 0xFFFFFFFF
            ip_address_str = socket.inet_ntoa(ip_as_unsigned.to_bytes(4, 'big'))

            # Collect LabJack device info
            labjack_info = {
                "DeviceType": device_type_name,
                "ConnectionType": connection_type_name,
                "SerialNumber": serial_numbers[i],
                "IPAddress": ip_address_str
            }
            labjacks.append(labjack_info)
            
            # Publish active LabJacks to NATS
            subject = "labjackd.active"
            await nc.publish(subject, json.dumps(labjacks).encode("utf-8"))
            print(f"Published active LabJacks to {subject}: {json.dumps(labjacks, indent=2)}")

            bucket_name = "labjackd_config"

            # Ensure the bucket exists
            try:
                kv = await js.key_value(bucket_name)
                print(f"Bucket '{bucket_name}' already exists.")
            except BucketNotFoundError:
                # Create the bucket if it doesn't exist
                kv = await js.create_key_value(bucket=bucket_name)
                print(f"Created bucket '{bucket_name}'.")
            
            # Manage Key-Value entry for the LabJack
            key_name = f"labjackd.config.{serial_numbers[i]}"
            try:
                existing_value = await kv.get(key_name)
                print(f"Key '{key_name}' already exists with value: {existing_value.value.decode()}")
            except KeyNotFoundError:
                # Create an empty key if it doesn't exist
                await kv.put(key_name, b"{}")  # Blank JSON-like key
                print(f"Created empty key '{key_name}'.")
            
            serial_number = key_name.split('.')[-1]
            if serial_number not in queues:
                print(f"Creating queue for serial {serial_number}")
                queues[serial_number] = Queue()
        
        if(num_found):
            print(f"All keys are set up for the active LabJack devices.")
            # Watch for changes in all keys
            print("Watching for configuration changes...")
            while True:  # Keep watching indefinitely
                watcher = await kv.watch("labjackd.config.*")  # Await to get the async iterator
                async for update in watcher:
                    #print(f"update.key : {update.key}")
                    if update.operation == "DEL":  # Check if the key was deleted
                        print(f"Key {update.key} was deleted.")
                        # Remove the key from last_revisions and active_configs
                        last_revisions.pop(update.key, None)
                        active_configs.pop(update.key, None)
                        continue
                    
                    # Check if the revision has changed
                    if last_revisions.get(update.key) == update.revision:
                        #print(f"No change in revision for key '{update.key}'. Skipping processing.")
                        await asyncio.sleep(5)
                        continue
                    try:
                        # Update the last seen revision
                        last_revisions[update.key] = update.revision

                        key_name = update.key
                        key_value = update.value.decode()
                        print(f"Key '{key_name}' updated to revision {update.revision}: {key_value}")

                        # Check if the key is empty
                        if not key_value.strip() or key_value == "{}":  # Empty or blank JSON
                            print(f"Key '{key_name}' is empty. Skipping processing.")
                            continue
                        try:
                            config = json.loads(key_value)
                            validate(instance=config, schema=config_schema)
                        except (ValidationError, json.JSONDecodeError) as e:
                            print(f"Invalid configuration for key '{update.key}': {e}")
                            continue    
                            
                        print(f"Key '{key_name}' is valid.")
                        active_configs[key_name] = config  # Store for later processing
                        # Configure LabJack with the updated valid configuration
                       
                        serial_number = key_name.split('.')[-1]
                        if serial_number in tasks:
                            print(f"Task for serial {serial_number} was already running")
                            task = tasks[serial_number]
                            if not task.done():
                                print(f"Cancelling the existing task for serial {serial_number}")
                                task.cancel()
                                try:
                                    # Await task cancellation to ensure it completes properly
                                    await task
                                except asyncio.CancelledError:
                                    print(f"Task for serial {serial_number} was cancelled")
                            # Remove the task from the dictionary
                            del tasks[serial_number]   
                        #stream_config, channel_details = await configure_each_labjack(serial_number,active_configs[key_name])
                        if serial_number in queues:
                            for attempt in range(max_labjack_config_retries):
                                stream_config, channel_details = await configure_each_labjack(serial_number,active_configs[key_name])
                                if stream_config is not None and stream_config.get("handle") is not None:
                                    print("LabJack configuration obtained successfully.")
                                    print("stream_config : ", stream_config)
                                    print("Channel Details : ", channel_details)
                                    print(f"Starting labjack_sample for serial number {serial_number}")
                                    queue = queues[serial_number]
                                    queue.put_nowait((stream_config, channel_details))
                                    tasks[serial_number] = asyncio.create_task(start_labjack_sample(queue))
                                    break
                                else:
                                    print(f"Failed to configure LabJack for serial {serial_number} (attempt {attempt + 1}). Retrying...")
                                    await asyncio.sleep(1)  # Adjust the delay between retries as needed
                        else:
                            print(f"skipping for labjack {serial_number} as it is not detected")

                        if attempt == max_labjack_config_retries - 1:
                            print(f"Failed to configure LabJack for serial {serial_number} after {max_labjack_config_retries} attempts. Skipping.")
     
                    except Exception as e:
                        print(f"An unexpected error occurred: {e}")
    except ErrNoServers:
        print("Could not connect to NATS server. Ensure that the NATS server is running.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if not nc.is_closed:
            await nc.close()

    
async def main():
    await init_key_and_config()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    # Register the signal handlers using signal.signal (works on Windows)
    signal.signal(signal.SIGINT, lambda sig, frame: handle_exit_signal(loop))
    signal.signal(signal.SIGTERM, lambda sig, frame: handle_exit_signal(loop))

    try:
        loop.run_until_complete(main())
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()
