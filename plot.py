import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter
import matplotlib.dates as mdates

csv_file = "labjack_data.csv"
window_length = 11
polyorder = 3
channels_to_plot = None

df = pd.read_csv(csv_file)
df["timestamp"] = pd.to_datetime(df["timestamp"])
value_cols = df["values"].str.split(";", expand=True)
value_cols = value_cols.apply(pd.to_numeric, errors="coerce")
value_cols.columns = [f"ch{i}" for i in range(value_cols.shape[1])]
df = pd.concat([df["timestamp"], value_cols], axis=1)

if channels_to_plot is None:
    channels_to_plot = list(range(len(value_cols.columns)))

plt.figure(figsize=(12, 6))
for ch_idx in channels_to_plot:
    ch_name = f"ch{ch_idx}"
    y = df[ch_name].values
    if len(y) >= window_length:
        y_smooth = savgol_filter(y, window_length=window_length, polyorder=polyorder)
    else:
        y_smooth = y
    plt.plot(df["timestamp"], y_smooth, label=ch_name)

plt.xlabel("Time")
plt.ylabel("Value")
plt.title("LabJack Channels with Savitzky–Golay Smoothing")
plt.legend()
plt.grid(True)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
plt.gcf().autofmt_xdate()
plt.tight_layout()
plt.show()
