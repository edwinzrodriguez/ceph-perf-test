# Repair Ganesha RPC Fields Script

## Overview

The `repair_ganesha_rpc_fields.py` script repairs missing Ganesha RPC IOQ Thread Min/Max fields in fio result JSON files.

## Problem

Due to a bug in `fio_runner.py`, the `ganesha_rpc_ioq_thrdmin` and `ganesha_rpc_ioq_thrdmax` fields may be missing from the `test_parameters` section of the JSON results, even though these values are encoded in the `results_dir` path.

## Solution

This script parses the `results_dir` field to extract the missing values and adds them back to the `test_parameters` section.

### Pattern Recognition

The script recognizes the following patterns in the `results_dir` path:

- `_grpcmin<number>` → "Ganesha RPC IOQ Thread Min": `<number>`
- `_grpcmax<number>` → "Ganesha RPC IOQ Thread Max": `<number>`

### Example

**Before:**
```json
{
    "test_parameters": {
        "results_dir": "/cephfs_perf/results/20260516-130903_perf_test_fs_grpcmin5_grpcmax20",
        "Ganesha Enabled": "True"
    }
}
```

**After:**
```json
{
    "test_parameters": {
        "results_dir": "/cephfs_perf/results/20260516-130903_perf_test_fs_grpcmin5_grpcmax20",
        "Ganesha Enabled": "True",
        "Ganesha RPC IOQ Thread Min": 5,
        "Ganesha RPC IOQ Thread Max": 20
    }
}
```

## Usage

### Basic Usage

```bash
# Repair all JSON files in a directory
python3 scripts/repair_ganesha_rpc_fields.py results/20260516-130903-1778936943_perf_test_fs/

# Repair a single file
python3 scripts/repair_ganesha_rpc_fields.py results/20260516-130903-*/fio_result_lp01.json
```

### Using Wildcards (Glob Patterns)

```bash
# Match multiple directories and files with wildcards
python3 scripts/repair_ganesha_rpc_fields.py "results/20260516-*/fio_result_*.json"

# Match all JSON files in subdirectories
python3 scripts/repair_ganesha_rpc_fields.py "results/*/fio_*.json"

# Process all JSON files recursively
python3 scripts/repair_ganesha_rpc_fields.py "results/**/*.json"
```

**Note:** When using wildcards, wrap the pattern in quotes to prevent shell expansion.

### Dry Run (Preview Changes)

```bash
# See what would be changed without modifying files
python3 scripts/repair_ganesha_rpc_fields.py --dry-run "results/20260516-*/*.json"
```

### Verbose Output

```bash
# Get detailed information about each file
python3 scripts/repair_ganesha_rpc_fields.py --verbose results/
```

### Combined Options

```bash
# Dry run with verbose output
python3 scripts/repair_ganesha_rpc_fields.py --dry-run --verbose "results/**/*.json"
```

## Command-Line Options

| Option | Short | Description |
|--------|-------|-------------|
| `--dry-run` | `-n` | Show what would be changed without modifying files |
| `--verbose` | `-v` | Print detailed information about each file |
| `--help` | `-h` | Show help message and exit |

## Supported Path Types

The script accepts three types of paths:

1. **Single File**: `/path/to/fio_result.json`
2. **Directory**: `/path/to/results/` (processes all `*.json` files in the directory)
3. **Glob Pattern**: `/path/to/results/*/fio_*.json` (supports wildcards `*` and `?`)

**Important:** When using glob patterns with wildcards, wrap the path in quotes to prevent shell expansion:
```bash
python3 scripts/repair_ganesha_rpc_fields.py "results/20260516-*/fio_result_*.json"
```

## Output

The script provides a summary of its operations:

```
Found 10 JSON file(s) in results/20260516-130903-*
✓ fio_result_lp01.json: Added: Ganesha RPC IOQ Thread Max: 20
✓ fio_result_lp02.json: Added: Ganesha RPC IOQ Thread Max: 20
○ fio_result_lp03.json: No changes needed

============================================================
SUMMARY
============================================================
Total files processed: 10
Files modified:        8
Files skipped:         2
Files with errors:     0
```

### Status Indicators

- `✓` - File was successfully modified
- `○` - File was skipped (no changes needed)
- `✗` - File had an error

## Exit Codes

- `0` - Success (no errors)
- `1` - One or more files had errors

## Safety Features

1. **Dry Run Mode**: Preview changes before applying them
2. **Non-Destructive**: Only adds missing fields, never removes or modifies existing ones
3. **JSON Validation**: Validates JSON structure before and after modifications
4. **Error Handling**: Continues processing other files if one fails

## Limitations

- Only processes files with a `test_parameters` section
- Only processes files with a `results_dir` field in `test_parameters`
- Only adds fields if they are missing (won't overwrite existing values)
- Requires the RPC values to be encoded in the `results_dir` path

## Related Files

- `lib/workload/fio_runner.py` - Contains the bug that causes missing fields
- `lib/workload/run_fio_workload.py` - Creates the JSON result files
- `cephfs_perf_lib.py` - Contains `get_human_readable_settings()` function

## Future Improvements

The proper fix would be to update `fio_runner.py` to ensure these fields are always included in the settings passed to `run_fio_workload.py`. This script serves as a workaround for existing result files.