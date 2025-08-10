import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter
import matplotlib.dates as mdates

# === CONFIG ===
csv_file = "okay.csv"   # path to your CSV file
window_length = 11           # must be odd, choose based on smoothing
polyorder = 3                # polynomial order for filter
channels_to_plot = None      # None = all channels, or e.g. [1, 3, 5]
# ==============

# Load CSV
df = pd.read_csv(csv_file, header=None)
df.columns = ["timestamp"] + [f"ch{i}" for i in range(1, len(df.columns))]

# Parse timestamps
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Select channels
if channels_to_plot is None:
    channels_to_plot = list(range(1, len(df.columns)))  # skip timestamp col

plt.figure(figsize=(12, 6))

for ch_idx in channels_to_plot:
    ch_name = f"ch{ch_idx}"
    y = df[ch_name].values

    # Apply Savitzky-Golay filter
    if len(y) >= window_length:
        y_smooth = savgol_filter(y, window_length=window_length, polyorder=polyorder)
    else:
        y_smooth = y  # not enough points for filter

    plt.plot(df["timestamp"], y_smooth, label=ch_name)

plt.xlabel("Time")
plt.ylabel("Value")
plt.title("Channels with Savitzky–Golay Smoothing")
plt.legend()
plt.grid(True)

# Format x-axis nicely for time
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
plt.gcf().autofmt_xdate()

plt.tight_layout()
plt.show()
