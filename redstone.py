

import socket
import datetime
import orso

def parse_syslog_entry(entry: str, host:str, port:int, current_year: int = datetime.datetime.now().year) -> dict:
    """
    Parses a syslog entry into its components.

    Parameters:
        entry (str): The syslog entry string.
    
    Returns:
        dict: A dictionary containing parsed components of the syslog.
    """
    # Split by spaces to get the timestamp, hostname, and the remaining message
    parts = entry.split(' ', 4)
    timestamp_str = f"{parts[0]} {parts[1]} {parts[2]}"
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

def syslog_listener(host: str = '0.0.0.0', port: int = 514):
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
            entry = parse_syslog_entry(data.decode(), host, port)
            df.append(entry)

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