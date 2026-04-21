import json
import os
import argparse
import glob
from collections import defaultdict

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
                
                job = data.get('jobs', [{}])[0]
                read_bw = job.get('read', {}).get('bw_bytes', 0)
                write_bw = job.get('write', {}).get('bw_bytes', 0)
                read_iops = job.get('read', {}).get('iops', 0)
                write_iops = job.get('write', {}).get('iops', 0)
                
                result_entry = {**test_params}
                result_entry['read_bw_bytes'] = read_bw
                result_entry['write_bw_bytes'] = write_bw
                result_entry['read_iops'] = read_iops
                result_entry['write_iops'] = write_iops
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
    for val in sorted(rep.keys()):
        print("  " * indent + f"{current_var} = {val}")
        print_representation(rep[val], swept_vars, indent + 1)

def plot_results(results, swept_vars, metric, output_file):
    if not HAS_MATPLOTLIB:
        print("Matplotlib not found. Skipping plot generation.")
        return

    if len(swept_vars) == 0:
        print("No swept variables to plot.")
        return

    plt.figure(figsize=(10, 6))
    
    if len(swept_vars) == 1:
        # Simple bar chart or line chart
        var = swept_vars[0]
        data = sorted([(str(r.get(var)), r.get(metric, 0)) for r in results])
        x = [d[0] for d in data]
        y = [d[1] for d in data]
        plt.bar(x, y)
        plt.xlabel(var)
        plt.ylabel(metric)
    elif len(swept_vars) == 2:
        # Multi-line chart
        var1, var2 = swept_vars
        groups = defaultdict(list)
        for r in results:
            groups[str(r.get(var1))].append((str(r.get(var2)), r.get(metric, 0)))
        
        for group_label, values in sorted(groups.items()):
            values.sort()
            x = [v[0] for v in values]
            y = [v[1] for v in values]
            plt.plot(x, y, marker='o', label=f"{var1}={group_label}")
        
        plt.xlabel(var2)
        plt.ylabel(metric)
        plt.legend()
    else:
        # For > 2 variables, we'd need more complex visualization or subplots
        print(f"Plotting for {len(swept_vars)} variables not fully implemented. Plotting first two.")
        var1, var2 = swept_vars[:2]
        groups = defaultdict(list)
        for r in results:
            groups[str(r.get(var1))].append((str(r.get(var2)), r.get(metric, 0)))
        
        for group_label, values in sorted(groups.items()):
            values.sort()
            x = [v[0] for v in values]
            y = [v[1] for v in values]
            plt.plot(x, y, marker='o', label=f"{var1}={group_label}")
        plt.xlabel(var2)
        plt.ylabel(metric)
        plt.legend()

    plt.title(f"Benchmark Results: {metric}")
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"Plot saved to {output_file}")

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
        # Auto-detect metric
        total_read = sum(r.get('read_bw_bytes', 0) for r in results)
        total_write = sum(r.get('write_bw_bytes', 0) for r in results)
        metric = 'read_bw_bytes' if total_read > total_write else 'write_bw_bytes'
        
    print(f"Using metric: {metric}")
    
    if not swept_vars:
        print("No variables were swept across the provided files.")
        for r in results:
             print(f"File: {os.path.basename(r['file_path'])} - {metric}: {r.get(metric, 0)}")
    else:
        # Create the n-dimensional representation
        # Sort variables to ensure consistent order, but put the one with most values last for better printing
        swept_vars = sorted(swept_vars, key=lambda v: len(set(str(r.get(v)) for r in results)))
        
        rep = build_n_dimensional_representation(results, swept_vars, metric)
        print("\nN-dimensional representation:")
        print_representation(rep, swept_vars)
        
        plot_results(results, swept_vars, metric, args.output)

if __name__ == "__main__":
    main()
