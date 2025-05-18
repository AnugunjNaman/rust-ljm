import asyncio
import datetime
import zlib

import msgpack
import pyarrow as pa
from nats.aio.client import Client as NATS

# Buffer to store up to 20 batches per channel
BATCH_SIZE = 5

async def run():
    # Connect to NATS
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # Dictionary to hold separate buffers for each channel
    channel_buffers = {}

    async def message_handler(msg):
        nonlocal channel_buffers

        subject = msg.subject
        compressed_data = msg.data
        channel_name = subject.split('.')[1]  # Assumes topic format "channel.channel_name"

        # Decompress the data
        decompressed_data = zlib.decompress(compressed_data)
        # Deserialize the data using MessagePack
        data = msgpack.unpackb(decompressed_data)

        # Extract headers
        headers = msg.headers
        start_timestamp_str = headers.get('start_timestamp', None)
        sample_interval_str = headers.get('sample_interval', None)
        length_str = headers.get('length', None)

        if start_timestamp_str is None or sample_interval_str is None or length_str is None:
            print("Missing required header data.")
            return

        # Parse header values
        start_timestamp = datetime.datetime.fromisoformat(start_timestamp_str)  # Start timestamp as ISO 8601 string
        sample_interval = float(sample_interval_str)  # Sample interval in seconds (converted to float)
        length = int(length_str)  # Length of data (number of samples)

        # Step 4: Calculate timestamps for all samples
        timestamps = [
            start_timestamp + datetime.timedelta(seconds=i * sample_interval)
            for i in range(length)
        ]

        # Assuming 'data' is a list of values (e.g., floats), we need to extract 'values'
        values = data  # Data contains the list of sample values

        # If the channel doesn't have a buffer, create one
        if channel_name not in channel_buffers:
            channel_buffers[channel_name] = []

        # Store the batch along with the first timestamp
        channel_buffers[channel_name].append((timestamps, values))

        # If the buffer for this channel has 5 batches, process them
        if len(channel_buffers[channel_name]) >= BATCH_SIZE:
            print(f"Sorting and writing data for channel {channel_name}...")

            # Sort batches based on the first timestamp
            channel_buffers[channel_name].sort(key=lambda x: x[0][0])  # Sort based on the first timestamp in the batch

            # Prepare Arrow arrays (timestamps and values)
            timestamp_array = pa.array([timestamp for batch in channel_buffers[channel_name] for timestamp in batch[0]])
            value_array = pa.array([value for batch in channel_buffers[channel_name] for value in batch[1]], type=pa.float32())

            # Create an Arrow Table from the arrays
            table = pa.Table.from_arrays([timestamp_array, value_array], names=['timestamp', 'value'])

            # Write the table to an Arrow file (e.g., channel_name.arrow)
            arrow_filename = f"{channel_name}.arrow"
            with pa.OSFile(arrow_filename, 'wb') as sink:
                with pa.ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)

            print(f"Data written to Arrow file {arrow_filename} for channel {channel_name}.")

            # Clear the buffer after writing to Arrow file for this channel
            channel_buffers[channel_name].clear()

        print(f"Batch processed for channel {channel_name}")

    # Subscribe to the topic and keep the subscription active
    await nc.subscribe("channel.*", cb=message_handler)

    try:
        # Keep the coroutine running indefinitely
        await asyncio.Future()  # This will block forever unless cancelled
    except KeyboardInterrupt:
        print("Received exit signal, shutting down...")
    finally:
        # Perform any cleanup here
        print("Closing NATS connection...")
        await nc.close()
        print("NATS connection closed.")

if __name__ == '__main__':
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Program terminated by user.")
