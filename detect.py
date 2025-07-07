from labjack import ljm

try:
    num, dt, ct, serials, ips = ljm.listAll(ljm.constants.dtANY, ljm.constants.ctANY)
    if num > 0:
        print(f"{num} LabJack(s) connected:")
        for i in range(num):
            print(f"  Serial: {serials[i]}, Type: {dt[i]}, Conn: {ct[i]}, IP: {ips[i] & 0xFFFFFFFF}")
    else:
        print("No LabJack devices detected.")
except Exception as e:
    print(f"Error communicating with LabJack: {e}")
