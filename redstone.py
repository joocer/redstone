

import socket
import datetime
import orso
import re
from typing import Dict

def parse_syslog_entry(log: str) -> Dict[str, Dict]:
    """
    Parse a syslog entry and categorize fields into mandatory (parent dict) and optional (child dict).

    Parameters:
        log: str
            The syslog entry as a string.

    Returns:
        Dict[str, Dict]: A dictionary with mandatory fields in the parent dictionary 
                         and optional fields in a child dictionary under the "log" key.
    """
    
    # Regular expression to match key="value" or key=value patterns
    regex_pattern = r'(\w+)="([^"]*)"|(\w+)=([^"\s]+)'

    # Fields considered mandatory and will be placed in the parent dictionary
    mandatory_fields = {"device_name", "timestamp", "severity"}
    
    # Initialize parent and child dictionaries
    parent_dict = {}
    child_dict = {}

    # Extract key-value pairs using regex
    matches = re.findall(regex_pattern, log)
    
    for match in matches:
        # Extract key and value from regex groups
        key = match[0] if match[0] else match[2]
        value = match[1] if match[1] else match[3]
        
        # Add to parent dictionary if the key is mandatory, otherwise to child dictionary
        if key in mandatory_fields:
            parent_dict[key] = value
        else:
            child_dict[key] = value

    # Combine mandatory fields with optional fields in the final dictionary
    parsed_log = parent_dict
    parsed_log["log"] = child_dict
    
    if parsed_log["device_name"] != 'SFW':
        print(parsed_log)

    return parsed_log

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
            entry = parse_syslog_entry(data.decode())
            entry["host"] = host
            entry["port"] = port
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