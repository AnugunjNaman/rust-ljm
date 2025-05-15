import pandas as pd
import matplotlib.pyplot as plt

# Define the file name dynamically
file_name = 'StrainGauge2.csv'  # Replace with the name of your file or pass dynamically

# Read the CSV file
df = pd.read_csv(file_name, header=None, names=['time', 'raw_voltage'], delimiter=",")

# Convert the time column to datetime format (adjusting the format to handle full datetime)
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S.%f')

# Sort the dataframe by the time column (in case it's not sorted)
df = df.sort_values(by='time')

# Calculate the time difference in seconds relative to the first timestamp
df['time_diff'] = (df['time'] - df['time'].iloc[0]).dt.total_seconds()

# Plot the raw voltage vs. time difference in seconds
plt.plot(df['time_diff'], df['raw_voltage'])
plt.xlabel('Time (seconds)')
plt.ylabel('Raw Voltage')
plt.title(f'{file_name} Raw Voltage vs. Time')  # Dynamically set the title based on the file name
plt.grid(True)
plt.show()
