import csv
import os
import threading
from datetime import datetime, UTC


# Lock for thread-safe CSV file writing
_csv_lock = threading.Lock()


def save_to_csv(data, csv_filename, receive_time=None):

    if receive_time is None:
        receive_time = datetime.now(UTC).isoformat()
    
    # Add the receiving time to the data
    data_with_receive_time = data.copy()
    data_with_receive_time['receive_time'] = receive_time
    data_with_receive_time.setdefault('status', 'ok')
    
    # Use lock to ensure thread-safe file writing
    with _csv_lock:
        # Create directory if it doesn't exist
        csv_path = os.path.dirname(csv_filename)
        if csv_path and not os.path.exists(csv_path):
            os.makedirs(csv_path, exist_ok=True)
        
        file_exists = os.path.exists(csv_filename)
        
        with open(csv_filename, 'a', newline='') as csvfile:
            fieldnames = ['device_id', 'timestamp', 'protocol', 'sensor', 'value', 'status', 'receive_time', 'event_id']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Ensure event_id is present in data (default to empty string if not)
            if 'event_id' not in data_with_receive_time:
                data_with_receive_time['event_id'] = ''
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            # Write the data row
            writer.writerow(data_with_receive_time)
        
        print(f"[STORAGE] Saved data to {csv_filename}: {data_with_receive_time}")

