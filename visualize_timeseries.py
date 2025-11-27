"""
Time-series visualization script for IoT device data.
Plots sensor values over time for at least two devices.
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import List, Optional


# Configure plotting style
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except OSError:
    try:
        plt.style.use('seaborn-darkgrid')
    except OSError:
        plt.style.use('default')
sns.set_palette("husl")


def parse_timestamp(series: pd.Series) -> pd.Series:
    """Parse ISO format timestamps."""
    return pd.to_datetime(series, errors='coerce', utc=True)


def load_and_prepare_data(csv_path: str) -> pd.DataFrame:
    """Load CSV data and prepare for plotting."""
    df = pd.read_csv(csv_path)
    
    # Parse timestamp
    if 'timestamp' in df.columns:
        df['ts'] = parse_timestamp(df['timestamp'])
    elif 'receive_time' in df.columns:
        df['ts'] = parse_timestamp(df['receive_time'])
    else:
        raise ValueError("CSV must contain either 'timestamp' or 'receive_time' column")
    
    # Remove rows with invalid timestamps
    df = df.dropna(subset=['ts']).sort_values('ts')
    
    return df


def plot_timeseries(
    csv_path: str,
    device_ids: Optional[List[str]] = None,
    output_path: Optional[str] = None,
    output_dir: str = "plots"
) -> Path:
    """
    Create time-series plots for at least two devices.
    
    Args:
        csv_path: Path to CSV file with device data
        device_ids: List of device IDs to plot (if None, plots first 2+ devices found)
        output_path: Optional output file path
        output_dir: Directory to save plots
    """
    # Load data
    df = load_and_prepare_data(csv_path)
    
    if df.empty:
        raise ValueError("No valid data found in CSV file")
    
    # Determine which devices to plot
    if device_ids is None:
        # Get unique device IDs
        if 'device_id' not in df.columns:
            raise ValueError("CSV must contain 'device_id' column")
        
        unique_devices = df['device_id'].unique()
        if len(unique_devices) < 2:
            # If only one device, we can still plot it, but warn
            print(f"Warning: Only found {len(unique_devices)} device(s). Plotting all available.")
            device_ids = list(unique_devices)
        else:
            # Plot at least two devices
            device_ids = list(unique_devices[:max(2, len(unique_devices))])
    
    # Filter data for selected devices
    df_filtered = df[df['device_id'].isin(device_ids)].copy()
    
    if df_filtered.empty:
        raise ValueError(f"No data found for devices: {device_ids}")
    
    # Create figure with subplots
    num_devices = len(device_ids)
    fig, axes = plt.subplots(num_devices, 1, figsize=(14, 4 * num_devices), sharex=True)
    
    # Handle single device case
    if num_devices == 1:
        axes = [axes]
    
    # Plot each device
    for idx, device_id in enumerate(device_ids):
        ax = axes[idx]
        device_data = df_filtered[df_filtered['device_id'] == device_id]
        
        if device_data.empty:
            ax.text(0.5, 0.5, f'No data for {device_id}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f"Device: {device_id}")
            continue
        
        # Get protocol and sensor info for labeling
        protocol = device_data['protocol'].iloc[0] if 'protocol' in device_data.columns else 'unknown'
        sensor = device_data['sensor'].iloc[0] if 'sensor' in device_data.columns else 'value'
        
        # Plot time series
        ax.plot(device_data['ts'], device_data['value'], 
               marker='o', linewidth=1.5, markersize=4, label=f'{device_id} ({protocol})')
        
        ax.set_ylabel(f'{sensor.replace("_", " ").title()}', fontsize=11)
        ax.set_title(f"Device: {device_id} | Protocol: {protocol} | Sensor: {sensor}", 
                    fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper right')
    
    # Set x-axis label on bottom subplot
    axes[-1].set_xlabel('Time (UTC)', fontsize=11)
    
    # Overall title
    fig.suptitle('Time-Series Plot: Device Sensor Readings', 
                fontsize=14, fontweight='bold', y=0.995)
    
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    
    # Save plot
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(exist_ok=True)
    
    if output_path is None:
        csv_name = Path(csv_path).stem
        output_path = output_dir_path / f"timeseries_{csv_name}.png"
    else:
        output_path = Path(output_path)
    
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f"Time-series plot saved to: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description='Create time-series plots for IoT devices',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python visualize_timeseries.py --csv gateway_data.csv
  python visualize_timeseries.py --csv gateway_data.csv --devices device1 device2
  python visualize_timeseries.py --csv gateway_data.csv --output-dir plots
        """
    )
    parser.add_argument(
        '--csv',
        type=str,
        required=True,
        help='Path to CSV file with device data (gateway_data.csv or sent_events.csv)'
    )
    parser.add_argument(
        '--devices',
        nargs='+',
        default=None,
        help='Device IDs to plot (if not specified, plots first 2+ devices found)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='plots',
        help='Directory to save plots (default: plots)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output file path (default: auto-generated)'
    )
    
    args = parser.parse_args()
    
    try:
        plot_timeseries(
            csv_path=args.csv,
            device_ids=args.devices,
            output_path=args.output,
            output_dir=args.output_dir
        )
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())


