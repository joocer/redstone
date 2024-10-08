

import socket
import datetime
import orso
import re
from typing import Dict


def parse_syslog_entry(entry: str, host: str, port: int, current_year: int = datetime.datetime.now().year) -> dict:
    """
    Parses a syslog entry into its components.
    
    Parameters:
        entry (str): The syslog entry string.
        host (str): The host from which the log was received.
        port (int): The port from which the log was received.
        current_year (int): The current year for constructing the timestamp.
        
    Returns:
        dict: A dictionary containing parsed components of the syslog.
    """
    
    # Remove the code at the start (e.g., <34>)
    if entry.startswith('<') and '>' in entry:
        entry = entry.split('>', 1)[1].strip()
    
    # Collapse multiple spaces to handle inconsistent spacing
    entry = re.sub(r'\s+', ' ', entry)
    
    # Split by spaces to get the timestamp, hostname, and the remaining message
    parts = entry.split(' ', 4)
    
    # Properly zero-fill the day part
    month, day, time = parts[0], parts[1].zfill(2), parts[2]
    timestamp_str = f"{month} {day} {time}"
    hostname = parts[3]

    # Parse the timestamp string into a datetime object
    timestamp = datetime.datetime.strptime(f"{current_year} {timestamp_str}", "%Y %b %d %H:%M:%S")
    
    # The remaining part should contain process, pid, and the actual message
    process_part, message = parts[4].split(': ', 1)
    
    # Process part may contain process name and pid in the format "process[pid]"
    if '[' in process_part and ']' in process_part:
        process_name, pid = process_part.split('[')
        pid = pid.rstrip(']')
    else:
        process_name = process_part
        pid = None
    
    return {
        "timestamp": timestamp,
        "hostname": hostname,
        "process_name": process_name,
        "pid": pid,
        "message": message,
        "host": host,
        "port": port
    }

def purge_frame(frame: orso.DataFrame):
    #from pyarrow import parquet

    #table = frame.arrow()
    #parquet.write_table(table, "file.parquet")

    return orso.DataFrame(schema=["timestamp", "hostname", "process_name", "pid", "message", "host", "port"])

def syslog_listener(host: str = '0.0.0.0', port: int = 1111):
    """
    A simple syslog listener that listens for syslog messages on the specified host and port.

    Parameters:
        host (str): The host IP address to listen on. Defaults to '0.0.0.0'.
        port (int): The UDP port to listen on. Defaults to 514.
    """
    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Bind the socket to the specified host and port
    sock.bind((host, port))
    
    print(f"Listening for syslog messages on {host}:{port}...")
    df = orso.DataFrame(schema=["timestamp", "hostname", "process_name", "pid", "message", "host", "port"])

    try:
        while True:
            # Receive data from the socket
            data, (host, port) = sock.recvfrom(2048)  # Buffer size is 2048 bytes
            try:
                entry = parse_syslog_entry(data.decode(), host, port)
                print("\n", entry)
                df.append(entry)
            except Exception as err:
                print(f"X ({err})", end="", flush=True)
                #print(data)



            if df.rowcount >= 50_000:
                print(".")
                df = purge_frame(df)
                print("<")
            

    except KeyboardInterrupt:
        print("Syslog listener stopped.")
    finally:
        sock.close()

if __name__ == "__main__":
    syslog_listener()