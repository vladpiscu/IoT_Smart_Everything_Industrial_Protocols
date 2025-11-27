"""
Latency histogram visualization script.
Compares latency distributions for Modbus vs MQTT/CoAP protocols.
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import re
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict


# Configure plotting style
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except OSError:
    try:
        plt.style.use('seaborn-darkgrid')
    except OSError:
        plt.style.use('default')
sns.set_palette("husl")


def parse_iso_datetime(iso_str: str) -> pd.Timestamp:
    """Parse ISO format datetime string."""
    return pd.to_datetime(iso_str, errors='coerce', utc=True)


def extract_loss_rate_from_filename(filename: str) -> Optional[float]:
    """
    Extract loss_rate from filename.
    
    Examples:
        sent_events_loss_0_1.csv -> 0.1
        gateway_data_loss_0_2.csv -> 0.2
        sent_events_loss_0_01.csv -> 0.01
    """
    # Pattern: loss_0_1, loss_0_2, etc.
    pattern = r'loss_(\d+)_(\d+)'
    match = re.search(pattern, filename)
    
    if match:
        integer_part = match.group(1)
        decimal_part = match.group(2)
        # Reconstruct the float: 0_1 -> 0.1, 0_01 -> 0.01
        loss_rate_str = f"{integer_part}.{decimal_part}"
        try:
            return float(loss_rate_str)
        except ValueError:
            return None
    
    return None


def load_latencies_by_loss_rate(
    experiment_dir: str,
    loss_rate_filter: Optional[float] = None
) -> Dict[float, Dict[str, List[float]]]:
    """
    Load latency data from all experiments, organized by loss_rate and protocol.
    
    Args:
        experiment_dir: Directory containing experiment CSV files
        loss_rate_filter: Optional loss_rate to filter by (if None, loads all)
    
    Returns:
        Dictionary mapping loss_rate to protocol to list of latencies in milliseconds
        Format: {loss_rate: {protocol: [latencies]}}
    """
    exp_dir = Path(experiment_dir)
    if not exp_dir.exists():
        raise ValueError(f"Experiment directory does not exist: {experiment_dir}")
    
    # Find all sent_events and gateway_data files
    sent_events_files = list(exp_dir.glob("sent_events_loss_*.csv"))
    gateway_data_files = list(exp_dir.glob("gateway_data_loss_*.csv"))
    
    if not sent_events_files:
        raise ValueError(f"No sent_events_loss_*.csv files found in {experiment_dir}")
    if not gateway_data_files:
        raise ValueError(f"No gateway_data_loss_*.csv files found in {experiment_dir}")
    
    # Group files by loss_rate
    sent_events_by_loss = {}
    gateway_data_by_loss = {}
    
    for sent_file in sent_events_files:
        loss_rate = extract_loss_rate_from_filename(sent_file.name)
        if loss_rate is not None:
            sent_events_by_loss[loss_rate] = sent_file
    
    for gateway_file in gateway_data_files:
        loss_rate = extract_loss_rate_from_filename(gateway_file.name)
        if loss_rate is not None:
            gateway_data_by_loss[loss_rate] = gateway_file
    
    # Match sent_events and gateway_data files by loss_rate
    matched_loss_rates = set(sent_events_by_loss.keys()) & set(gateway_data_by_loss.keys())
    
    if not matched_loss_rates:
        raise ValueError("No matching sent_events and gateway_data files found with same loss_rate")
    
    # Filter by loss_rate if specified
    if loss_rate_filter is not None:
        matched_loss_rates = {lr for lr in matched_loss_rates if abs(lr - loss_rate_filter) < 0.001}
        if not matched_loss_rates:
            raise ValueError(f"No experiments found with loss_rate={loss_rate_filter}")
    
    print(f"Found {len(matched_loss_rates)} experiment(s) with matching files:")
    for loss_rate in sorted(matched_loss_rates):
        print(f"  Loss rate {loss_rate:.3f}: {sent_events_by_loss[loss_rate].name} <-> {gateway_data_by_loss[loss_rate].name}")
    
    # Load latencies organized by loss_rate and protocol
    latencies_by_loss_rate = {}
    
    for loss_rate in sorted(matched_loss_rates):
        sent_file = sent_events_by_loss[loss_rate]
        gateway_file = gateway_data_by_loss[loss_rate]
        
        try:
            latencies_by_protocol = calculate_latencies(str(sent_file), str(gateway_file))
            latencies_by_loss_rate[loss_rate] = latencies_by_protocol
            
            print(f"  Calculated latencies for loss_rate {loss_rate:.3f}: "
                  f"{sum(len(l) for l in latencies_by_protocol.values())} total measurements")
        except Exception as e:
            print(f"  Warning: Error processing loss_rate {loss_rate:.3f}: {e}")
            continue
    
    return latencies_by_loss_rate


def calculate_latencies(sent_events_path: str, gateway_data_path: str) -> Dict[str, List[float]]:
    """
    Calculate latencies for each protocol.
    
    Returns:
        Dictionary mapping protocol name to list of latencies in milliseconds
    """
    # Load sent events
    sent_df = pd.read_csv(sent_events_path)
    sent_df['timestamp'] = parse_iso_datetime(sent_df['timestamp'])
    sent_df = sent_df.dropna(subset=['timestamp'])
    
    # Load gateway data
    gateway_df = pd.read_csv(gateway_data_path)
    gateway_df['timestamp'] = parse_iso_datetime(gateway_df['timestamp'])
    gateway_df['receive_time'] = parse_iso_datetime(gateway_df['receive_time'])
    gateway_df = gateway_df.dropna(subset=['timestamp', 'receive_time'])
    
    # Create a mapping from event_id to sent timestamp
    sent_dict = {}
    for _, row in sent_df.iterrows():
        event_id = row.get('event_id', '')
        if event_id:
            sent_dict[event_id] = {
                'timestamp': row['timestamp'],
                'protocol': row.get('protocol', '').lower()
            }
    
    # Calculate latencies by matching event_ids
    latencies_by_protocol = defaultdict(list)
    
    for _, row in gateway_df.iterrows():
        event_id = row.get('event_id', '')
        if event_id in sent_dict:
            sent_info = sent_dict[event_id]
            protocol = sent_info['protocol']
            
            if protocol:  # Only process if protocol is known
                # Calculate latency in milliseconds
                latency_ms = (row['receive_time'] - sent_info['timestamp']).total_seconds() * 1000
                
                # Filter out negative latencies (shouldn't happen, but handle edge cases)
                if latency_ms >= 0:
                    latencies_by_protocol[protocol].append(latency_ms)
    
    return dict(latencies_by_protocol)


def plot_latency_bar_chart(
    latencies_by_loss_rate: Dict[float, Dict[str, List[float]]],
    output_path: Optional[str] = None,
    output_dir: str = "plots"
) -> Path:
    """
    Create bar chart showing average latency for each protocol at each loss rate.
    
    Args:
        latencies_by_loss_rate: Dictionary mapping loss_rate to protocol to list of latencies (ms)
        output_path: Optional output file path
        output_dir: Directory to save plots
    """
    if not latencies_by_loss_rate:
        raise ValueError("No latency data available")
    
    # Get all unique protocols and loss rates
    all_protocols = set()
    for latencies_by_protocol in latencies_by_loss_rate.values():
        all_protocols.update(latencies_by_protocol.keys())
    
    # Filter to only protocols we want to compare
    protocols_to_plot = ['modbus', 'mqtt', 'coap']
    available_protocols = [p for p in protocols_to_plot if p in all_protocols]
    
    if not available_protocols:
        # If none of the target protocols are available, use all available
        available_protocols = sorted(list(all_protocols))
        print(f"Warning: None of the target protocols (Modbus, MQTT, CoAP) found. "
              f"Plotting available protocols: {available_protocols}")
    
    loss_rates = sorted(latencies_by_loss_rate.keys())
    
    # Calculate average latencies for each protocol at each loss rate
    data_for_plot = []
    for loss_rate in loss_rates:
        for protocol in available_protocols:
            latencies = latencies_by_loss_rate[loss_rate].get(protocol, [])
            if latencies:
                avg_latency = np.mean(latencies)
                data_for_plot.append({
                    'loss_rate': loss_rate,
                    'protocol': protocol.upper(),
                    'avg_latency': avg_latency,
                    'count': len(latencies)
                })
    
    if not data_for_plot:
        raise ValueError("No latency data available for plotting")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Color palette for protocols
    colors = {'MODBUS': '#2ecc71', 'MQTT': '#3498db', 'COAP': '#e74c3c'}
    
    # Prepare data for grouped bar chart
    # X-axis: groups for each loss_rate, with bars for each protocol
    x_positions = []
    bar_positions = {}
    bar_values = {}
    bar_colors = {}
    
    # Calculate bar width and positions
    num_protocols = len(available_protocols)
    bar_width = 0.8 / num_protocols
    group_width = 0.8
    
    for idx, loss_rate in enumerate(loss_rates):
        group_center = idx
        x_positions.append(group_center)
        
        for proto_idx, protocol in enumerate(available_protocols):
            protocol_upper = protocol.upper()
            # Position bars within each group
            offset = (proto_idx - (num_protocols - 1) / 2) * bar_width
            bar_pos = group_center + offset
            
            # Find data for this loss_rate and protocol
            data_point = next((d for d in data_for_plot 
                             if d['loss_rate'] == loss_rate and d['protocol'] == protocol_upper), None)
            
            if data_point:
                if protocol_upper not in bar_positions:
                    bar_positions[protocol_upper] = []
                    bar_values[protocol_upper] = []
                    bar_colors[protocol_upper] = colors.get(protocol_upper, '#95a5a6')
                
                bar_positions[protocol_upper].append(bar_pos)
                bar_values[protocol_upper].append(data_point['avg_latency'])
    
    # Plot bars for each protocol
    bars_plotted = False
    for protocol in available_protocols:
        protocol_upper = protocol.upper()
        if protocol_upper in bar_positions and bar_positions[protocol_upper]:
            ax.bar(bar_positions[protocol_upper], bar_values[protocol_upper],
                  width=bar_width, label=protocol_upper, 
                  color=bar_colors[protocol_upper], alpha=0.8, edgecolor='black', linewidth=0.5)
            bars_plotted = True
            print(f"  Plotted {len(bar_positions[protocol_upper])} bars for {protocol_upper}")
    
    if not bars_plotted:
        print("WARNING: No bars were plotted! Check that data exists for the protocols.")
        print(f"  Available protocols in data: {list(bar_positions.keys())}")
        print(f"  Protocols to plot: {[p.upper() for p in available_protocols]}")
        print(f"  Data points found: {len(data_for_plot)}")
    
    # Set x-axis labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels([f"Loss Rate\n{lr:.2f}" for lr in loss_rates], fontsize=10)
    
    # Set labels and title
    ax.set_ylabel('Average Latency (milliseconds)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Loss Rate Experiments', fontsize=12, fontweight='bold')
    ax.set_title('Average Latency by Protocol and Loss Rate', 
                fontsize=14, fontweight='bold')
    
    # Add legend
    ax.legend(loc='upper left', fontsize=11, title='Protocol')
    
    # Add grid
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    # Save plot
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(exist_ok=True)
    
    if output_path is None:
        output_path = output_dir_path / "latency_bar_chart.png"
    else:
        output_path = Path(output_path)
    
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f"Latency bar chart saved to: {output_path}")
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("AVERAGE LATENCY BY PROTOCOL AND LOSS RATE")
    print("=" * 80)
    print(f"{'Loss Rate':<12} {'Protocol':<15} {'Count':<10} {'Avg Latency (ms)':<18}")
    print("-" * 80)
    for loss_rate in loss_rates:
        for protocol in available_protocols:
            protocol_upper = protocol.upper()
            data_point = next((d for d in data_for_plot 
                             if d['loss_rate'] == loss_rate and d['protocol'] == protocol_upper), None)
            if data_point:
                print(f"{loss_rate:<12.3f} {protocol_upper:<15} {data_point['count']:<10} {data_point['avg_latency']:<18.2f}")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description='Create latency histogram comparing Modbus vs MQTT/CoAP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-detect and load from experiments/ directory (default behavior)
  python visualize_latency_histogram.py
  
  # Explicitly specify experiments directory
  python visualize_latency_histogram.py --experiment-dir experiments/
  
  # Filter by specific loss_rate
  python visualize_latency_histogram.py --experiment-dir experiments/ --loss-rate 0.1
  
  # Single experiment with specific files
  python visualize_latency_histogram.py --sent-events sent_events.csv --gateway-data gateway_data.csv
  python visualize_latency_histogram.py --sent-events sent_events.csv --gateway-data gateway_data.csv --bins 50
        """
    )
    parser.add_argument(
        '--sent-events',
        type=str,
        default=None,
        help='Path to sent events CSV file (for single experiment mode)'
    )
    parser.add_argument(
        '--gateway-data',
        type=str,
        default=None,
        help='Path to gateway data CSV file (for single experiment mode)'
    )
    parser.add_argument(
        '--experiment-dir',
        type=str,
        default=None,
        help='Directory containing multiple experiment results (default: auto-detect "experiments" directory)'
    )
    parser.add_argument(
        '--loss-rate',
        type=float,
        default=None,
        help='Filter experiments by specific loss_rate (optional)'
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
        # Calculate latencies
        print("Calculating latencies...")
        
        if args.experiment_dir:
            # Load from experiments directory
            latencies_by_loss_rate = load_latencies_by_loss_rate(
                experiment_dir=args.experiment_dir,
                loss_rate_filter=args.loss_rate
            )
        elif args.sent_events and args.gateway_data:
            # Single experiment mode - convert to the expected format
            latencies_by_protocol = calculate_latencies(args.sent_events, args.gateway_data)
            # For single experiment, we need a loss_rate - try to extract from filename or use 0.0
            loss_rate = 0.0
            if args.sent_events:
                loss_rate = extract_loss_rate_from_filename(Path(args.sent_events).name) or 0.0
            latencies_by_loss_rate = {loss_rate: latencies_by_protocol}
        elif Path("experiments").exists():
            # Auto-detect experiments directory
            print("Auto-detected 'experiments' directory. Loading data from there...")
            latencies_by_loss_rate = load_latencies_by_loss_rate(
                experiment_dir="experiments",
                loss_rate_filter=args.loss_rate
            )
        else:
            print("Error: Must provide either:")
            print("  --experiment-dir (for multiple experiments, default: auto-detect 'experiments')")
            print("  --sent-events and --gateway-data (for single experiment)")
            return 1
        
        if not latencies_by_loss_rate:
            print("Error: No latency data could be calculated. "
                  "Make sure both CSV files exist and contain matching event_ids.")
            return 1
        
        # Create bar chart
        plot_latency_bar_chart(
            latencies_by_loss_rate=latencies_by_loss_rate,
            output_path=args.output,
            output_dir=args.output_dir
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())


