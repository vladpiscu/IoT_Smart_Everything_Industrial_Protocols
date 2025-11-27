"""Thread-safe logger for device sent events using a queue and background thread."""

import atexit
import csv
import os
import queue
import threading
from datetime import datetime, UTC
from typing import Dict, Any, Optional

# Thread-safe queue for sent events
_sent_events_queue: queue.Queue = queue.Queue()
_CSV_FILENAME = "sent_events.csv"
_logger_thread: Optional[threading.Thread] = None
_logger_stop_event = threading.Event()
_logger_lock = threading.Lock()


def set_csv_filename(filename: str) -> None:
    """Set the CSV filename for sent events logging.
    
    Args:
        filename: Path to the CSV file where sent events will be logged
    """
    global _CSV_FILENAME
    with _logger_lock:
        _CSV_FILENAME = filename

# Register cleanup function to run on exit
def _cleanup_on_exit():
    """Ensure logger is stopped and flushed when program exits."""
    if _logger_thread is not None and _logger_thread.is_alive():
        stop_logger()

atexit.register(_cleanup_on_exit)


def log_sent_event(event_data: Dict[str, Any]) -> None:
    """Add a sent event to the queue for logging.
    
    Args:
        event_data: Dictionary containing event information with keys:
            - event_id: The event ID (required)
            - device_id: Device identifier (optional but recommended)
            - protocol: Protocol used (optional but recommended)
            - timestamp: When the event was sent (optional, will be set if missing)
            - Additional fields will be logged as-is
    """
    # Ensure timestamp is present
    if "timestamp" not in event_data:
        event_data["timestamp"] = datetime.now(UTC).isoformat()
    
    # Put event in queue (non-blocking, queue size should handle normal load)
    try:
        _sent_events_queue.put(event_data, block=False)
    except queue.Full:
        # Queue is full, log error but don't block
        print(f"[SENT EVENTS] Warning: Queue full, dropped event: {event_data.get('event_id', 'unknown')}")


def _logger_worker() -> None:
    """Background thread worker that writes events from queue to CSV file."""
    file_exists = os.path.exists(_CSV_FILENAME)
    
    while True:
        try:
            # If stop event is set, try to get all remaining events without timeout
            # Otherwise, use timeout to check stop event periodically
            timeout = None if _logger_stop_event.is_set() else 1.0
            
            try:
                event_data = _sent_events_queue.get(timeout=timeout)
            except queue.Empty:
                # If stop is set and queue is empty, we're done
                if _logger_stop_event.is_set():
                    break
                continue
            
            # Write to CSV file
            with _logger_lock:
                # Create directory if it doesn't exist
                csv_path = os.path.dirname(_CSV_FILENAME)
                if csv_path and not os.path.exists(csv_path):
                    os.makedirs(csv_path, exist_ok=True)
                
                file_exists = os.path.exists(_CSV_FILENAME)
                
                with open(_CSV_FILENAME, "a", newline="") as csvfile:
                    # Define fieldnames based on what we expect
                    fieldnames = ["timestamp", "event_id", "device_id", "protocol", "sensor", "value"]
                    
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
                    
                    # Write header if file is new
                    if not file_exists:
                        writer.writeheader()
                    
                    # Write the data row
                    writer.writerow(event_data)
            
            _sent_events_queue.task_done()
            
        except Exception as exc:
            print(f"[SENT EVENTS] Error writing event: {exc}")
            # Mark task as done even on error to prevent deadlock
            try:
                _sent_events_queue.task_done()
            except ValueError:
                pass  # Queue might be empty, ignore


def start_logger() -> None:
    """Start the background thread for logging sent events."""
    global _logger_thread
    
    with _logger_lock:
        if _logger_thread is not None and _logger_thread.is_alive():
            return  # Already running
        
        _logger_stop_event.clear()
        _logger_thread = threading.Thread(target=_logger_worker, daemon=True, name="SentEventsLogger")
        _logger_thread.start()
        print("[SENT EVENTS] Logger thread started")


def flush_logger(timeout: float = 5.0) -> None:
    """Wait for all events in the queue to be written to disk.
    
    Args:
        timeout: Maximum time to wait for queue to empty (seconds)
    """
    import time
    
    start_time = time.time()
    while _sent_events_queue.qsize() > 0:
        if time.time() - start_time > timeout:
            print(f"[SENT EVENTS] Warning: Flush timeout reached, {_sent_events_queue.qsize()} events still queued")
            break
        time.sleep(0.1)
    
    # Wait for queue tasks to complete
    try:
        _sent_events_queue.join()
    except Exception:
        pass


def stop_logger() -> None:
    """Stop the background thread and flush remaining events."""
    global _logger_thread
    
    with _logger_lock:
        if _logger_thread is None or not _logger_thread.is_alive():
            return
        
        # Signal stop (worker will continue processing until queue is empty)
        _logger_stop_event.set()
    
    # Wait for queue to be processed (outside lock to avoid deadlock)
    print("[SENT EVENTS] Flushing remaining events...")
    
    # Wait for all currently queued items to be processed
    _sent_events_queue.join()
    
    # Wait for thread to finish processing
    with _logger_lock:
        if _logger_thread and _logger_thread.is_alive():
            _logger_thread.join(timeout=10.0)  # Increased timeout to allow all events to be written
            if _logger_thread.is_alive():
                print("[SENT EVENTS] Warning: Logger thread did not stop within timeout")
        
        remaining = _sent_events_queue.qsize()
        if remaining > 0:
            print(f"[SENT EVENTS] Warning: {remaining} events still in queue after shutdown")
        else:
            print("[SENT EVENTS] All events flushed successfully")
        
        _logger_thread = None
        print("[SENT EVENTS] Logger thread stopped")

