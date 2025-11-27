"""
Packet Delivery Ratio (PDR) vs Fault Probability visualization script.
Plots how PDR changes with different fault probabilities for different protocols.
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
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


def calculate_pdr(sent_events_path: str, gateway_data_path: str) -> Dict[str, float]:
    """
    Calculate Packet Delivery Ratio (PDR) for each protocol.
    
    PDR = (received messages / sent messages) * 100
    
    Returns:
        Dictionary mapping protocol name to PDR percentage
    """
    # Load sent events
    sent_df = pd.read_csv(sent_events_path)
    
    # Load gateway data
    gateway_df = pd.read_csv(gateway_data_path)
    
    # Count sent messages by protocol
    sent_by_protocol = defaultdict(set)
    for _, row in sent_df.iterrows():
        event_id = row.get('event_id', '')
        protocol = row.get('protocol', '').lower()
        if event_id and protocol:
            sent_by_protocol[protocol].add(event_id)
    
    # Count received messages by protocol
    received_by_protocol = defaultdict(set)
    for _, row in gateway_df.iterrows():
        event_id = row.get('event_id', '')
        protocol = row.get('protocol', '').lower()
        if event_id and protocol:
            received_by_protocol[protocol].add(event_id)
    
    # Calculate PDR per protocol
    pdr_by_protocol = {}
    all_protocols = set(sent_by_protocol.keys()) | set(received_by_protocol.keys())
    
    for protocol in all_protocols:
        sent_count = len(sent_by_protocol.get(protocol, set()))
        received_count = len(sent_by_protocol.get(protocol, set()) & received_by_protocol.get(protocol, set()))
        
        if sent_count > 0:
            pdr = (received_count / sent_count) * 100
            pdr_by_protocol[protocol] = pdr
    
    return pdr_by_protocol


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


def load_fault_probability_from_config(config_path: str) -> Optional[float]:
    """Extract fault probability from gateway config file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Try to find fault probability in network settings
        network_config = config.get('network', {})
        if network_config:
            failure_prob = network_config.get('failure_probability', None)
            if failure_prob is not None:
                return failure_prob
        
        # Fallback: Check each protocol's proxy network settings
        for protocol in ['coap', 'mqtt', 'modbus']:
            if protocol in config:
                proxy = config[protocol].get('proxy', {})
                network = proxy.get('network', {})
                failure_prob = network.get('failure_probability', None)
                if failure_prob is not None:
                    return failure_prob
        
        return None
    except Exception as e:
        print(f"Warning: Could not load fault probability from config: {e}")
        return None


def load_experiment_data(
    experiment_dir: Optional[str] = None,
    sent_events_path: Optional[str] = None,
    gateway_data_path: Optional[str] = None,
    config_path: Optional[str] = None
) -> Dict[str, List[Tuple[float, float]]]:
    """
    Load experiment data with loss rates and PDR values.
    
    Returns:
        Dictionary mapping protocol to list of (loss_rate, pdr) tuples
    """
    data_by_protocol = defaultdict(list)
    
    if experiment_dir:
        # Load multiple experiment files from directory
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
        
        print(f"Found {len(matched_loss_rates)} experiment(s) with matching files:")
        for loss_rate in sorted(matched_loss_rates):
            print(f"  Loss rate {loss_rate:.3f}: {sent_events_by_loss[loss_rate].name} <-> {gateway_data_by_loss[loss_rate].name}")
        
        # Calculate PDR for each matched pair
        for loss_rate in sorted(matched_loss_rates):
            sent_file = sent_events_by_loss[loss_rate]
            gateway_file = gateway_data_by_loss[loss_rate]
            
            try:
                pdr_by_protocol = calculate_pdr(str(sent_file), str(gateway_file))
                
                # Add data point for each protocol
                for protocol, pdr in pdr_by_protocol.items():
                    data_by_protocol[protocol].append((loss_rate, pdr))
                
                print(f"  Calculated PDR for loss_rate {loss_rate:.3f}: {pdr_by_protocol}")
            except Exception as e:
                print(f"  Warning: Error processing loss_rate {loss_rate:.3f}: {e}")
                continue
    
    # Single experiment mode
    elif sent_events_path and gateway_data_path:
        # Calculate PDR
        pdr_by_protocol = calculate_pdr(sent_events_path, gateway_data_path)
        
        # Try to extract loss_rate from filenames first
        loss_rate = None
        if sent_events_path:
            loss_rate = extract_loss_rate_from_filename(Path(sent_events_path).name)
        if loss_rate is None and gateway_data_path:
            loss_rate = extract_loss_rate_from_filename(Path(gateway_data_path).name)
        
        # Fallback to config file if filename doesn't have loss_rate
        if loss_rate is None and config_path:
            # Try to get loss_rate from config
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                network_config = config.get('network', {})
                loss_rate = network_config.get('loss_rate', 0.0)
            except Exception:
                loss_rate = 0.0
        
        if loss_rate is None:
            loss_rate = 0.0
        
        # Add data point for each protocol
        for protocol, pdr in pdr_by_protocol.items():
            data_by_protocol[protocol].append((loss_rate, pdr))
    
    return dict(data_by_protocol)


def simulate_pdr_vs_fault(
    base_pdr: float = 90.0,
    fault_probabilities: Optional[List[float]] = None
) -> Dict[str, List[Tuple[float, float]]]:
    """
    Simulate PDR vs fault probability relationship.
    This is a theoretical model when actual experiment data is not available.
    
    Model: PDR = base_pdr * (1 - fault_probability) * (1 - loss_rate)
    """
    if fault_probabilities is None:
        fault_probabilities = np.linspace(0.0, 1.0, 21).tolist()  # 0 to 1 in steps of 0.05
    
    # Simulate for different protocols with different characteristics
    protocols_data = {
        'modbus': [],
        'mqtt': [],
        'coap': []
    }
    
    # Different base characteristics for each protocol
    protocol_params = {
        'modbus': {'base_pdr': 95.0, 'loss_rate': 0.1},
        'mqtt': {'base_pdr': 90.0, 'loss_rate': 0.1},
        'coap': {'base_pdr': 88.0, 'loss_rate': 0.1}
    }
    
    for protocol, params in protocol_params.items():
        for fault_prob in fault_probabilities:
            # Simplified model: PDR decreases with fault probability
            # PDR = base_pdr * (1 - fault_prob) * (1 - loss_rate)
            pdr = params['base_pdr'] * (1 - fault_prob) * (1 - params['loss_rate'])
            # Add some noise for realism
            pdr = max(0, min(100, pdr + np.random.normal(0, 2)))
            protocols_data[protocol].append((fault_prob, pdr))
    
    return protocols_data


def plot_pdr_vs_fault(
    data_by_protocol: Dict[str, List[Tuple[float, float]]],
    output_path: Optional[str] = None,
    output_dir: str = "plots",
    use_simulation: bool = False
) -> Path:
    """
    Create plot showing PDR vs fault probability for different protocols.
    
    Args:
        data_by_protocol: Dictionary mapping protocol to list of (fault_prob, pdr) tuples
        output_path: Optional output file path
        output_dir: Directory to save plots
        use_simulation: Whether to use simulated data if actual data is insufficient
    """
    # If we have insufficient data, use simulation
    if use_simulation or not data_by_protocol or all(len(v) < 2 for v in data_by_protocol.values()):
        print("Using simulated data for PDR vs fault probability plot.")
        print("For real data, run multiple experiments with different fault probabilities.")
        data_by_protocol = simulate_pdr_vs_fault()
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Color and marker mapping for protocols
    protocol_styles = {
        'modbus': {'color': '#2ecc71', 'marker': 'o', 'linestyle': '-', 'linewidth': 2},
        'mqtt': {'color': '#3498db', 'marker': 's', 'linestyle': '-', 'linewidth': 2},
        'coap': {'color': '#e74c3c', 'marker': '^', 'linestyle': '-', 'linewidth': 2}
    }
    
    # Plot each protocol
    for protocol, data_points in data_by_protocol.items():
        if not data_points:
            continue
        
        # Filter to only include loss rates up to 0.5
        filtered_data_points = [(lr, pdr) for lr, pdr in data_points if lr <= 0.5]
        
        if not filtered_data_points:
            print(f"Warning: No data points for {protocol} with loss_rate <= 0.5")
            continue
        
        # Sort by loss_rate (first element of tuple)
        filtered_data_points = sorted(filtered_data_points, key=lambda x: x[0])
        loss_rates = [x[0] for x in filtered_data_points]
        pdrs = [x[1] for x in filtered_data_points]
        
        # Get style for this protocol
        style = protocol_styles.get(protocol.lower(), {
            'color': None, 'marker': 'o', 'linestyle': '-', 'linewidth': 2
        })
        
        # Plot line with markers
        ax.plot(loss_rates, pdrs, 
               label=f'{protocol.upper()}', 
               marker=style['marker'],
               linestyle=style['linestyle'],
               linewidth=style['linewidth'],
               markersize=8,
               color=style['color'] if style['color'] else None,
               alpha=0.8)
    
    # Formatting
    # Determine x-axis label based on data (loss_rate or fault_probability)
    # Check if data points use loss_rate (from filenames) or fault_probability
    x_label = 'Loss Rate'
    title_suffix = 'Loss Rate'
    if data_by_protocol:
        # Check first data point to see if it's likely loss_rate or fault_probability
        first_protocol = list(data_by_protocol.keys())[0]
        if first_protocol and data_by_protocol[first_protocol]:
            # If max value is > 1, it's likely loss_rate (can be > 1.0)
            # If max value is <= 1, it could be either, but we'll assume loss_rate from filenames
            max_x = max(x[0] for x in data_by_protocol[first_protocol])
            if max_x > 1.0:
                x_label = 'Loss Rate'
                title_suffix = 'Loss Rate'
            else:
                # Could be either, but default to Loss Rate since we're using filenames
                x_label = 'Loss Rate / Fault Probability'
                title_suffix = 'Loss Rate / Fault Probability'
    
    ax.set_xlabel(x_label, fontsize=12, fontweight='bold')
    ax.set_ylabel('Packet Delivery Ratio (PDR) (%)', fontsize=12, fontweight='bold')
    ax.set_title(f'Packet Delivery Ratio vs {title_suffix}', 
                fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=11)
    
    # Set axis limits (x-axis limited to 0.5 as requested)
    ax.set_xlim(-0.02, 0.52)
    ax.set_ylim(-5, 105)
    
    # Add horizontal line at 100% for reference
    ax.axhline(100, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    
    plt.tight_layout()
    
    # Save plot
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(exist_ok=True)
    
    if output_path is None:
        output_path = output_dir_path / "pdr_vs_fault_probability.png"
    else:
        output_path = Path(output_path)
    
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f"PDR vs Fault Probability plot saved to: {output_path}")
    
    # Print summary (only showing data up to 0.5)
    print("\n" + "=" * 80)
    print("PDR vs LOSS RATE SUMMARY (Loss Rate <= 0.5)")
    print("=" * 80)
    for protocol, data_points in data_by_protocol.items():
        if data_points:
            # Filter to only show data up to 0.5
            filtered_points = [(lr, pdr) for lr, pdr in data_points if lr <= 0.5]
            if filtered_points:
                print(f"\n{protocol.upper()}:")
                print(f"  Data points (<= 0.5): {len(filtered_points)}")
                for loss_rate, pdr in sorted(filtered_points, key=lambda x: x[0]):
                    print(f"    Loss Rate: {loss_rate:.3f} -> PDR: {pdr:.2f}%")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description='Create PDR vs Fault Probability plot',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-detect and load from experiments/ directory (default behavior)
  python visualize_pdr_vs_fault.py
  
  # Explicitly specify experiments directory
  python visualize_pdr_vs_fault.py --experiment-dir experiments/
  
  # Single experiment with config file
  python visualize_pdr_vs_fault.py --sent-events sent_events.csv --gateway-data gateway_data.csv --config gateway_config.json
  
  # Use simulation (when multiple experiments not available)
  python visualize_pdr_vs_fault.py --simulate
        """
    )
    parser.add_argument(
        '--sent-events',
        type=str,
        default=None,
        help='Path to sent events CSV file'
    )
    parser.add_argument(
        '--gateway-data',
        type=str,
        default=None,
        help='Path to gateway data CSV file'
    )
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Path to gateway config JSON file (to extract fault probability)'
    )
    parser.add_argument(
        '--experiment-dir',
        type=str,
        default=None,
        help='Directory containing multiple experiment results (default: auto-detect "experiments" directory)'
    )
    parser.add_argument(
        '--simulate',
        action='store_true',
        help='Use simulated data (for demonstration when actual experiment data is limited)'
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
        data_by_protocol = {}
        
        if args.simulate:
            # Use simulation
            data_by_protocol = simulate_pdr_vs_fault()
        elif args.experiment_dir:
            # Load from experiment directory (default to 'experiments' if not specified)
            exp_dir = args.experiment_dir
            data_by_protocol = load_experiment_data(experiment_dir=exp_dir)
        elif Path("experiments").exists():
            # Auto-detect experiments directory if it exists
            print("Auto-detected 'experiments' directory. Loading data from there...")
            data_by_protocol = load_experiment_data(experiment_dir="experiments")
        elif args.sent_events and args.gateway_data:
            # Single experiment
            data_by_protocol = load_experiment_data(
                sent_events_path=args.sent_events,
                gateway_data_path=args.gateway_data,
                config_path=args.config
            )
        else:
            # Try to auto-detect experiments directory
            if Path("experiments").exists():
                print("Auto-detected 'experiments' directory. Loading data from there...")
                data_by_protocol = load_experiment_data(experiment_dir="experiments")
            else:
                print("Error: Must provide either:")
                print("  --simulate (for simulated data)")
                print("  --sent-events and --gateway-data (for single experiment)")
                print("  --experiment-dir (for multiple experiments, default: 'experiments')")
                return 1
        
        # Create plot
        plot_pdr_vs_fault(
            data_by_protocol=data_by_protocol,
            output_path=args.output,
            output_dir=args.output_dir,
            use_simulation=args.simulate
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

