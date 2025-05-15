from labjack import ljm
import socket
import asyncio
from asyncio import Queue
import json
from jsonschema import validate, ValidationError
import nats
from nats.aio.client import Client as NATS
from nats.js.errors import KeyNotFoundError, BucketNotFoundError
from nats.aio.errors import ErrNoServers
import signal
import datetime
import pyarrow as pa
import os
from filelock import FileLock
import pickle
import shelve
import pyarrow.ipc  # To serialize the RecordBatch to a byte stream
import pyarrow.parquet as pq
import io
import zlib
import msgpack
import csv

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

async def nats_publish(topic: str, payload: bytes, headers: dict):
    """
    Connects to the NATS server and publishes a message to the specified topic.
    
    Args:
        topic (str): The topic to publish the message to.
        payload (pa.Buffer): The message to be published (byte buffer).
        headers (dict): Sampling info of the message
    """
    # Connect to NATS
    try:
        # Establish a connection to the NATS server
        nc = await nats.connect(NATS_SERVER_IP)
        #print(f"Connected to NATS server at {nc.connected_url}")

        # Publish the message to the specified topic
        await nc.publish(topic, payload,headers=headers)
        print(f"NATS Message published to {topic} with headers: {headers}")

        # Close the connection
        await nc.close()

    except Exception as e:
        print(f"Error while connecting or publishing to NATS: {e}")
        
async def check_buffer_and_prepare_publish(queue):
    """
    This function will publish data to NATS. It will check if there is enough data in the buffer,
    create the RecordBatch, and publish the data to NATS.
    """
    stream_config, channel_details = await queue.get()
    queue.task_done()  # Mark the queue item as processed

    serial_number = stream_config.get("serial_number")
    scan_rate = stream_config["scan_rate"]
    num_addresses = stream_config["num_addresses"]
    scansPerRead = int(scan_rate / num_addresses)
    channel_data = {channel['name']: [] for channel in channel_details.values()}
    timestamp_data = {channel['name']: [] for channel in channel_details.values()}
    schemas = {}
    print(f"Running check_buffer_and_prepare_publish task for {serial_number}")
    for channel in channel_details.values():
        schema = pa.schema([pa.field(channel['name'], pa.float32())])
        schemas[channel['name']] = schema
    print(f"{serial_number} - Created schemas for all channels: {schemas}")
    is_first_sample = True
    file_lock = FileLock(f"all_data_{serial_number}.lock")  # Create a file lock for the current channel
    try:
        while True:
            #if os.path.exists(f"{channel_name}_data.dat") and os.path.getsize(f"{channel_name}_data.dat") > 0:
            if os.path.exists(f"all_data_{serial_number}.dat") and os.path.getsize(f"all_data_{serial_number}.dat") > 0:
                print(f"File all_data_{serial_number}.dat exists, starting to process data...")
                
                with file_lock:
                    with shelve.open(f"all_data_{serial_number}", flag='c') as shelf:
                        #print(list(shelf.values()))
                        all_data_samples = list(shelf.values())
                        #print(f"Reading from file all_data_{serial_number}...")
                               
                    with shelve.open(f"all_data_{serial_number}", flag='n') as shelf:
                        pass
                # If data_samples is not empty, process the data
                if all_data_samples:
                    #print(f"Processing {len(all_data_samples)} data samples...")
                    for data_sample in all_data_samples:
                        start_timestamp, aData = data_sample
                        #print(f"Processing data sample: {start_timestamp}, {aData[:5]}...")  # Show first 5 data points
                        #timestamp_all_channel = [start_timestamp + datetime.timedelta(seconds=sample_idx / scan_rate) for sample_idx in range(len(aData))]
                        
                        if is_first_sample:
                            each_channel_start_timestamp = [start_timestamp + datetime.timedelta(seconds=sample_idx / scan_rate) for sample_idx in range(num_addresses)]
                            per_channel_time_delta = datetime.timedelta(seconds=num_addresses / scan_rate)
                            sample_interval = per_channel_time_delta.total_seconds()
                            is_first_sample = False
                        for j, channel in enumerate(channel_details.values()):
                            channel_name = channel['name']
                            nats_stream_rate = channel['nats_stream_rate']

                            each_channel_data_samples = aData[j::num_addresses]
                            
                            #print(f"Distributing data for channel {channel_name}...")  # Debug print
                            channel_data[channel_name].extend(each_channel_data_samples)  # Distribute data across channels

                            #print(f"Channel {channel_name}: {len(channel_data[channel_name])} data samples accumulated.")  # Debug print
                            
                            while len(channel_data[channel_name]) >= nats_stream_rate:
                                #print(f"Processing batch for channel {channel_name}, size: {nats_stream_rate}")  # Debug print
                                batch_data = channel_data[channel_name][:nats_stream_rate]
                                start_time_current_batch = each_channel_start_timestamp[j]
                                each_channel_start_timestamp[j] = each_channel_start_timestamp[j] + (nats_stream_rate-1)*per_channel_time_delta
                                channel_data[channel_name] = channel_data[channel_name][nats_stream_rate:]
                                #print(f"Start timestamp for batch {j}: {each_channel_start_timestamp[j]}")
                                # Slice the data for publication
                                
                                serialized_data = msgpack.packb(batch_data)
                                compressed_data = zlib.compress(serialized_data)
                                nats_topic = f"channel.{channel_name}"
                                payload_info = {'start_timestamp' : start_time_current_batch.isoformat(), 
                                                'sample_interval': str(sample_interval), 
                                                'length' : str(nats_stream_rate)} 
                                #print(f"Serializing and publishing {len(compressed_data)} bytes of data to {nats_topic} with header {payload_info}.")  
                                await nats_publish(nats_topic, compressed_data, payload_info)
                                
                                '''
                                timestamp_current_channel = [start_time_current_batch + datetime.timedelta(seconds=i * sample_interval) for i in range(nats_stream_rate)]
                                csv_filename = f"{channel_name}.csv"
                                with open(csv_filename, mode='a', newline='') as file:
                                    writer = csv.writer(file)
                                    for timestamp, value in zip(timestamp_current_channel, batch_data):
                                        writer.writerow([timestamp, value])
                                print(f"Batch data for channel {channel_name} written to {csv_filename}")
                                '''
            else:
                #print(f"Labjack {serial_number} not started yet")
                await asyncio.sleep(0.5) #added to ensure other labjack tasks start/continue if no file exists
                
    except Exception as e:
        print(f"Error while creating and publishing data: {e}")                    
                       
async def get_each_labjack_config(serial_number, config):
    # Extract LabJack configuration details
    scan_rate = config["scan_rate"]
    channels = config.get("channels_enabled", {})  # Get channels as a dictionary
    
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

    num_addresses = len(channels)
    
    stream_config = {
        "serial_number":serial_number,
        "num_addresses": num_addresses,
        "scan_rate": scan_rate,
    }
    
    return stream_config, channel_details

async def send_ping(nc):
    """Send a ping to the NATS server every second to keep the connection alive."""
    while True:
        try:
            # Flush to the NATS server to simulate a ping-like action
            await nc.flush()
            print("Connection is alive, flush sent to NATS server.")
        except Exception as e:
            print(f"Error during flush: {e}")
        await asyncio.sleep(10)  # Wait for 15 second before sending the next "ping"
        
async def monitor_bucket():
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
                            
                        if serial_number in queues:
                            stream_config, channel_details = await get_each_labjack_config(serial_number,active_configs[key_name])
                            if stream_config is not None:
                                print("LabJack configuration obtained successfully.")
                                print("stream_config : ", stream_config)
                                print("Channel Details : ", channel_details)
                                queue = queues[serial_number]
                                queue.put_nowait((stream_config, channel_details))
                                tasks[serial_number] = asyncio.create_task(check_buffer_and_prepare_publish(queue))
                            else:
                                print(f"Failed to get configuration for LabJack with serial {serial_number})")
                        else:
                            print(f"skipping for labjack {serial_number} as it is not detected")
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
    await monitor_bucket()

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
