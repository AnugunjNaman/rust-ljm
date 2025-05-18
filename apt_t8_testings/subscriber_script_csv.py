import asyncio
import csv
import datetime
import zlib

import msgpack
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
        channel_name = subject.split(".")[
            1
        ]  # Assumes topic format "channel.channel_name"

        # Decompress the data
        decompressed_data = zlib.decompress(compressed_data)
        # Deserialize the data using MessagePack
        data = msgpack.unpackb(decompressed_data)

        # Extract headers
        headers = msg.headers
        start_timestamp_str = headers.get("start_timestamp", None)
        sample_interval_str = headers.get("sample_interval", None)
        length_str = headers.get("length", None)

        if (
            start_timestamp_str is None
            or sample_interval_str is None
            or length_str is None
        ):
            print("Missing required header data.")
            return

        # Parse header values
        start_timestamp = datetime.datetime.fromisoformat(
            start_timestamp_str
        )  # Start timestamp as ISO 8601 string
        sample_interval = float(
            sample_interval_str
        )  # Sample interval in seconds (converted to float)
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

        # If the buffer for this channel has 20 batches, process them
        if len(channel_buffers[channel_name]) >= BATCH_SIZE:
            print(f"Sorting and writing data for channel {channel_name}...")

            # Sort batches based on the first timestamp
            channel_buffers[channel_name].sort(
                key=lambda x: x[0][0]
            )  # Sort based on the first timestamp in the batch

            # Write sorted batches to CSV for this channel
            csv_filename = f"{channel_name}.csv"
            with open(csv_filename, mode="a", newline="") as file:
                writer = csv.writer(file)
                for timestamps, values in channel_buffers[channel_name]:
                    for timestamp, value in zip(timestamps, values):
                        writer.writerow([timestamp, value])

            # Clear the buffer after writing to CSV for this channel
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


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Program terminated by user.")
