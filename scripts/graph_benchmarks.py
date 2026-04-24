import json
import os
import argparse
import glob
import sys
from collections import defaultdict

# Add project root to sys.path to allow importing cephfs_perf_lib
# when running the script directly from the scripts directory or elsewhere
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

def load_json_results(json_files):
    results = []
    for file_path in json_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                test_params = data.get('test_parameters', {})
                runner = test_params.get('Workload Runner', 'fio')

                if runner == 'fio':
                    job = data.get('jobs', [{}])[0]
                    read_data = job.get('read', {})
                    write_data = job.get('write', {})
                    
                    read_bw = read_data.get('bw_bytes', 0)
                    write_bw = write_data.get('bw_bytes', 0)
                    read_iops = read_data.get('iops', 0)
                    write_iops = write_data.get('iops', 0)
                    
                    read_bytes = read_data.get('io_bytes', 0)
                    write_bytes = write_data.get('io_bytes', 0)
                    read_runtime = read_data.get('runtime', 0)
                    write_runtime = write_data.get('runtime', 0)
                    max_runtime_ms = max(read_runtime, write_runtime)
                    
                    agg_bw_mib = 0.0
                    agg_iops = 0.0
                    if max_runtime_ms > 0:
                        total_bytes = read_bytes + write_bytes
                        agg_bw_mib = (total_bytes / (max_runtime_ms / 1000.0)) / (1024 * 1024)
                        total_ios = read_data.get('total_ios', 0) + write_data.get('total_ios', 0)
                        agg_iops = total_ios / (max_runtime_ms / 1000.0)

                    result_entry = {**test_params}
                    result_entry['read_bw_bytes'] = read_bw
                    result_entry['write_bw_bytes'] = write_bw
                    result_entry['read_iops'] = read_iops
                    result_entry['write_iops'] = write_iops
                    result_entry['agg_bw_mib'] = agg_bw_mib
                    result_entry['agg_iops'] = agg_iops
                    result_entry['file_path'] = file_path
                    results.append(result_entry)

                elif runner == 'cephfs_tool':
                    summary = data.get('summary', {})
                    
                    # Read record
                    read_entry = {**test_params}
                    read_entry['Direction'] = 'Read'
                    read_entry['agg_bw_mib'] = summary.get('Read Throughput', {}).get('mean', 0)
                    read_entry['agg_iops'] = summary.get('File Reads (Opens)', {}).get('mean', 0)
                    read_entry['file_path'] = file_path
                    results.append(read_entry)
                    
                    # Write record
                    write_entry = {**test_params}
                    write_entry['Direction'] = 'Write'
                    write_entry['agg_bw_mib'] = summary.get('Write Throughput', {}).get('mean', 0)
                    write_entry['agg_iops'] = summary.get('File Creates', {}).get('mean', 0)
                    write_entry['file_path'] = file_path
                    results.append(write_entry)

                elif runner == 'sfs2020':
                    runs = data.get('runs', [])
                    if not runs:
                        continue
                    
                    # Usually we want the last run (highest load point or final result)
                    last_run = runs[-1]
                    metrics = last_run.get('metrics', {})
                    
                    # SFS2020 metrics names can vary, but throughput and iops are common
                    # Let's try to find them. 
                    # throughput is usually in MiB/s or KiB/s
                    # iops is usually 'ops/s'
                    throughput = metrics.get('throughput', {}).get('value', 0)
                    units = metrics.get('throughput', {}).get('units', '').lower()
                    if 'kib/s' in units:
                        throughput /= 1024.0
                    
                    iops = metrics.get('ops/s', {}).get('value', 0)
                    if iops == 0:
                        iops = metrics.get('iops', {}).get('value', 0)

                    result_entry = {**test_params}
                    result_entry['agg_bw_mib'] = throughput
                    result_entry['agg_iops'] = iops
                    result_entry['file_path'] = file_path
                    results.append(result_entry)

        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
    return results

def identify_swept_variables(results):
    if not results:
        return []
    
    all_keys = set()
    for entry in results:
        all_keys.update(entry.keys())
    
    ignore_cols = {
        'results_dir', 'file_path', 'extra_args', 'Filesystem Name', 
        'read_bw_bytes', 'write_bw_bytes', 'read_iops', 'write_iops',
        'agg_bw_mib', 'agg_iops', 'Workload Runner',
        'Duration', 'Ramp Time' # These might be constant but sometimes vary
    }
    
    swept_vars = []
    for key in all_keys:
        if key in ignore_cols:
            continue
        
        values = set()
        for entry in results:
            if key in entry:
                values.add(str(entry[key]))
        
        if len(values) > 1:
            swept_vars.append(key)
    
    return sorted(swept_vars)

def build_n_dimensional_representation(results, swept_vars, metric):
    # This will be a nested dictionary where each level corresponds to a swept variable
    def nested_dict():
        return defaultdict(nested_dict)
    
    representation = nested_dict()
    
    for entry in results:
        current_level = representation
        for var in swept_vars[:-1]:
            val = str(entry.get(var, 'N/A'))
            current_level = current_level[val]
        
        # Last variable points to the metric
        last_var = swept_vars[-1]
        last_val = str(entry.get(last_var, 'N/A'))
        current_level[last_val] = entry.get(metric, 0)
        
    return representation

def print_representation(rep, swept_vars, indent=0):
    if not isinstance(rep, dict):
        print("  " * indent + f": {rep}")
        return

    current_var = swept_vars[indent]
    for val in sorted(rep.keys(), key=get_sort_key):
        print("  " * indent + f"{current_var} = {val}")
        print_representation(rep[val], swept_vars, indent + 1)

import itertools
import re
from cephfs_perf_lib import CommonUtils

def parse_si_unit(val_str):
    """
    Parses a string with SI units (e.g., '1MiB', '16GiB', '256KiB') into an integer.
    Returns the integer value in bytes, or the original value if it can't be parsed.
    """
    if not isinstance(val_str, str):
        return val_str
    
    # Check for common SI units
    units = {
        'TiB': 1024**4, 'GiB': 1024**3, 'MiB': 1024**2, 'KiB': 1024,
        'TB': 1000**4, 'GB': 1000**3, 'MB': 1000**2, 'KB': 1000,
        'T': 1024**4, 'G': 1024**3, 'M': 1024**2, 'K': 1024
    }
    
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([a-zA-Z]+)$', val_str.strip())
    if match:
        number, unit = match.groups()
        if unit in units:
            return int(float(number) * units[unit])
            
    # If no unit, try to convert to float/int
    try:
        if '.' in val_str:
            return float(val_str)
        return int(val_str)
    except ValueError:
        return val_str

def get_sort_key(val):
    """
    Returns a key for sorting that handles SI units and numeric strings correctly.
    """
    if val is None:
        return (0, "")
    
    val_str = str(val)
    parsed = parse_si_unit(val_str)
    if isinstance(parsed, (int, float)):
        return (1, parsed)
    
    return (2, val_str)

def plot_results(results, swept_vars, metric, output_file):
    if not HAS_MATPLOTLIB:
        print("Matplotlib not found. Skipping plot generation.")
        return

    if len(swept_vars) == 0:
        print("No swept variables to plot.")
        return

    # Helper function to save plot and handle filename
    def save_plot(plt, base_output, pair_vars=None, other_vars_info=None):
        def format_val_for_filename(v):
            if v == "True": return "1"
            if v == "False": return "0"
            return str(v).replace(' ', '_').replace('/', '_')

        name, ext = os.path.splitext(base_output)
        if pair_vars:
            v_names = "_".join([CommonUtils.get_short_name(v) for v in pair_vars])
            name = f"{name}_{v_names}"
        
        if other_vars_info:
            other_info_str = "_".join([f"{CommonUtils.get_short_name(k)}-{format_val_for_filename(v)}" for k, v in sorted(other_vars_info.items())])
            name = f"{name}_{other_info_str}"

        filename = f"{name}{ext}"
        plt.savefig(filename)
        print(f"Plot saved to {filename}")

    if len(swept_vars) == 1:
        plt.figure(figsize=(10, 6))
        var = swept_vars[0]
        data = sorted([(str(r.get(var)), r.get(metric, 0)) for r in results], key=lambda x: get_sort_key(x[0]))
        x = [d[0] for d in data]
        y = [d[1] for d in data]
        plt.bar(x, y)
        plt.xlabel(var)
        plt.ylabel(metric)
        plt.title(f"Benchmark Results: {metric}\n{var} sweep")
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        save_plot(plt, output_file)
        plt.close()

    elif len(swept_vars) == 2:
        plt.figure(figsize=(10, 6))
        var1, var2 = swept_vars
        groups = defaultdict(list)
        for r in results:
            groups[str(r.get(var1))].append((str(r.get(var2)), r.get(metric, 0)))
        
        for group_label, values in sorted(groups.items(), key=lambda x: get_sort_key(x[0])):
            values.sort(key=lambda x: get_sort_key(x[0]))
            x = [v[0] for v in values]
            y = [v[1] for v in values]
            plt.plot(x, y, marker='o', label=f"{var1}={group_label}")
        
        plt.xlabel(var2)
        plt.ylabel(metric)
        plt.legend()
        plt.title(f"Benchmark Results: {metric}\n{var1} vs {var2}")
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        save_plot(plt, output_file)
        plt.close()

    else:
        # Generate plots for all unique pairs of swept variables
        pairs = list(itertools.combinations(swept_vars, 2))
        for var1, var2 in pairs:
            other_vars = [v for v in swept_vars if v != var1 and v != var2]
            
            # Group results by the "other" variables
            # Each unique combination of other variables gets its own plot
            subsets = defaultdict(list)
            for r in results:
                other_vals = tuple((v, str(r.get(v))) for v in other_vars)
                subsets[other_vals].append(r)
            
            for other_vals_tuple, subset_results in subsets.items():
                other_vars_dict = dict(other_vals_tuple)
                
                plt.figure(figsize=(10, 6))
                groups = defaultdict(list)
                for r in subset_results:
                    groups[str(r.get(var1))].append((str(r.get(var2)), r.get(metric, 0)))
                
                for group_label, values in sorted(groups.items(), key=lambda x: get_sort_key(x[0])):
                    values.sort(key=lambda x: get_sort_key(x[0]))
                    x = [v[0] for v in values]
                    y = [v[1] for v in values]
                    plt.plot(x, y, marker='o', label=f"{var1}={group_label}")
                
                plt.xlabel(var2)
                plt.ylabel(metric)
                plt.legend()
                
                title_lines = [f"{k}: {v}" for k, v in other_vars_dict.items()]
                title_suffix = "\n".join(title_lines)
                plt.title(f"Benchmark Results: {metric}\n{var1} vs {var2}\n{title_suffix}")
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.tight_layout()
                
                save_plot(plt, output_file, pair_vars=(var1, var2), other_vars_info=other_vars_dict)
                plt.close()

def main():
    parser = argparse.ArgumentParser(description='Graph benchmark results from FIO JSON output files.')
    parser.add_argument('files', nargs='+', help='JSON result files')
    parser.add_argument('--metric', help='Metric to use (e.g., write_bw_bytes, read_iops)')
    parser.add_argument('--output', default='benchmark_results.png', help='Output plot file name')
    args = parser.parse_args()

    # Expand wildcards in file list
    expanded_files = []
    for f in args.files:
        expanded_files.extend(glob.glob(f))
    
    if not expanded_files:
        print(f"No files found matching: {args.files}")
        return

    results = load_json_results(expanded_files)
    if not results:
        print("No results loaded.")
        return
    
    swept_vars = identify_swept_variables(results)
    print(f"Swept variables identified: {swept_vars}")
    
    metric = args.metric
    if not metric:
        # Default to the new aggregate bandwidth metric
        metric = 'agg_bw_mib'
        
    print(f"Using metric: {metric}")
    
    if not swept_vars:
        print("No variables were swept across the provided files.")
        for r in results:
             print(f"File: {os.path.basename(r['file_path'])} - {metric}: {r.get(metric, 0)}")
    else:
        # Create the n-dimensional representation
        # Sort variables to ensure consistent order, but put the one with most values last for better printing
        swept_vars = sorted(swept_vars, key=lambda v: len(set(str(r.get(v)) for r in results)))
        
        # Sort results by swept variables to ensure they are added in order if possible
        # though build_n_dimensional_representation doesn't strictly need it as print_representation sorts
        
        rep = build_n_dimensional_representation(results, swept_vars, metric)
        print("\nN-dimensional representation:")
        print_representation(rep, swept_vars)
        
        plot_results(results, swept_vars, metric, args.output)

if __name__ == "__main__":
    main()
