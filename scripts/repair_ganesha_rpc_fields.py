#!/usr/bin/env python3
"""
Script to repair missing Ganesha RPC IOQ Thread Min/Max fields in fio_result JSON files.

Due to a bug in fio_runner.py, the ganesha_rpc_ioq_thrdmin and ganesha_rpc_ioq_thrdmax
fields may be missing from the test_parameters section of the JSON results.

This script parses the results_dir field to extract these values and adds them back
to the test_parameters if they are missing.

Example results_dir patterns:
  - _grpcmax20 -> "Ganesha RPC IOQ Thread Max": 20
  - _grpcmin5 -> "Ganesha RPC IOQ Thread Min": 5
  - _grpcmin5_grpcmax20 -> both fields

Usage:
  python3 scripts/repair_ganesha_rpc_fields.py <path_or_pattern>
  python3 scripts/repair_ganesha_rpc_fields.py results/20260516-130903-*/fio_result_*.json
  python3 scripts/repair_ganesha_rpc_fields.py results/
"""

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path


def parse_ganesha_rpc_from_path(results_dir):
    """
    Parse Ganesha RPC IOQ thread min/max values from the results_dir path.
    
    Args:
        results_dir: Path string containing encoded parameters
        
    Returns:
        dict: Dictionary with 'min' and/or 'max' keys if found, empty dict otherwise
    """
    rpc_values = {}
    
    # Pattern to match _grpcmin<number> or _grpcmax<number>
    min_pattern = r'_grpcmin(\d+)'
    max_pattern = r'_grpcmax(\d+)'
    
    min_match = re.search(min_pattern, results_dir)
    if min_match:
        rpc_values['min'] = int(min_match.group(1))
    
    max_match = re.search(max_pattern, results_dir)
    if max_match:
        rpc_values['max'] = int(max_match.group(1))
    
    return rpc_values


def repair_json_file(json_path, dry_run=False, verbose=False):
    """
    Repair a single JSON file by adding missing Ganesha RPC fields.
    
    Args:
        json_path: Path to the JSON file
        dry_run: If True, don't write changes
        verbose: If True, print detailed information
        
    Returns:
        tuple: (modified, error_message)
    """
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False, f"JSON decode error: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"
    
    # Check if test_parameters exists
    if 'test_parameters' not in data:
        return False, "No test_parameters section found"
    
    test_params = data['test_parameters']
    
    # Check if results_dir exists
    if 'results_dir' not in test_params:
        return False, "No results_dir in test_parameters"
    
    results_dir = test_params['results_dir']
    
    # Parse RPC values from results_dir
    rpc_values = parse_ganesha_rpc_from_path(results_dir)
    
    if not rpc_values:
        if verbose:
            print(f"  No Ganesha RPC values found in path: {results_dir}")
        return False, "No RPC values in path"
    
    # Check which fields are missing and need to be added
    modified = False
    changes = []
    
    if 'min' in rpc_values:
        field_name = "Ganesha RPC IOQ Thread Min"
        if field_name not in test_params:
            test_params[field_name] = rpc_values['min']
            modified = True
            changes.append(f"{field_name}: {rpc_values['min']}")
        elif verbose:
            print(f"  {field_name} already exists with value: {test_params[field_name]}")
    
    if 'max' in rpc_values:
        field_name = "Ganesha RPC IOQ Thread Max"
        if field_name not in test_params:
            test_params[field_name] = rpc_values['max']
            modified = True
            changes.append(f"{field_name}: {rpc_values['max']}")
        elif verbose:
            print(f"  {field_name} already exists with value: {test_params[field_name]}")
    
    if modified:
        if not dry_run:
            try:
                with open(json_path, 'w') as f:
                    json.dump(data, f, indent=4)
                return True, f"Added: {', '.join(changes)}"
            except Exception as e:
                return False, f"Error writing file: {e}"
        else:
            return True, f"Would add: {', '.join(changes)}"
    
    return False, "No changes needed"


def collect_json_files(path_pattern):
    """
    Collect JSON files from a path pattern (can be file, directory, or glob pattern).
    
    Args:
        path_pattern: Path pattern (file, directory, or glob with wildcards)
        
    Returns:
        list: List of Path objects for JSON files
    """
    json_files = []
    
    # Check if it's a glob pattern (contains wildcards)
    if '*' in path_pattern or '?' in path_pattern:
        # Use glob to expand the pattern
        matched_paths = glob.glob(path_pattern, recursive=True)
        for matched in matched_paths:
            p = Path(matched)
            if p.is_file() and p.suffix == '.json':
                json_files.append(p)
            elif p.is_dir():
                # If glob matched a directory, get all JSON files in it
                json_files.extend(p.glob('*.json'))
    else:
        # Not a glob pattern, treat as regular path
        p = Path(path_pattern)
        if p.is_file():
            if p.suffix == '.json':
                json_files.append(p)
        elif p.is_dir():
            json_files.extend(p.glob('*.json'))
    
    return sorted(set(json_files))  # Remove duplicates and sort


def process_files(json_files, dry_run=False, verbose=False):
    """
    Process a list of JSON files.
    
    Args:
        json_files: List of Path objects for JSON files
        dry_run: If True, don't write changes
        verbose: If True, print detailed information
        
    Returns:
        dict: Statistics about processed files
    """
    stats = {
        'total': 0,
        'modified': 0,
        'skipped': 0,
        'errors': 0
    }
    
    if not json_files:
        print("No JSON files found")
        return stats
    
    print(f"Found {len(json_files)} JSON file(s)")
    if dry_run:
        print("DRY RUN MODE - No files will be modified\n")
    
    for json_file in json_files:
        stats['total'] += 1
        
        if verbose or dry_run:
            print(f"\nProcessing: {json_file}")
        
        modified, message = repair_json_file(json_file, dry_run=dry_run, verbose=verbose)
        
        if modified:
            stats['modified'] += 1
            print(f"✓ {json_file.name}: {message}")
        elif "No changes needed" in message or "already exists" in message:
            stats['skipped'] += 1
            if verbose:
                print(f"○ {json_file.name}: {message}")
        else:
            stats['errors'] += 1
            if verbose:
                print(f"✗ {json_file.name}: {message}")
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Repair missing Ganesha RPC IOQ Thread Min/Max fields in fio result JSON files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Repair all JSON files in a specific results directory
  python3 scripts/repair_ganesha_rpc_fields.py results/20260516-130903-1778936943_perf_test_fs/
  
  # Use wildcards to match multiple directories and files
  python3 scripts/repair_ganesha_rpc_fields.py "results/20260516-*/fio_result_*.json"
  
  # Repair a single file
  python3 scripts/repair_ganesha_rpc_fields.py results/20260516-130903-*/fio_result_lp01.json
  
  # Dry run to see what would be changed
  python3 scripts/repair_ganesha_rpc_fields.py --dry-run "results/20260516-*/*.json"
  
  # Verbose output
  python3 scripts/repair_ganesha_rpc_fields.py --verbose results/
        """
    )
    
    parser.add_argument(
        'path',
        help='Path to JSON file(s), directory, or glob pattern (e.g., "results/*/fio_*.json")'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be changed without modifying files'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print detailed information about each file'
    )
    
    args = parser.parse_args()
    
    # Collect JSON files from the path pattern
    json_files = collect_json_files(args.path)
    
    if not json_files:
        print(f"Error: No JSON files found matching: {args.path}")
        sys.exit(1)
    
    # Process the files
    stats = process_files(
        json_files,
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total files processed: {stats['total']}")
    print(f"Files modified:        {stats['modified']}")
    print(f"Files skipped:         {stats['skipped']}")
    print(f"Files with errors:     {stats['errors']}")
    
    if args.dry_run and stats['modified'] > 0:
        print("\nRun without --dry-run to apply changes")
    
    # Exit with error code if there were errors
    sys.exit(1 if stats['errors'] > 0 else 0)


if __name__ == '__main__':
    main()

# Made with Bob
