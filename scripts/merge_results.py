#!/usr/bin/env python3
"""Merge workload result JSON files into a single output JSON.

Each input result is stored under a section name built from its
``test_parameters`` section. Each parameter contributes ``<short><value>``
where ``<short>`` is ``CommonUtils.get_short_name`` applied to the
human-readable parameter name.
"""

import argparse
import glob
import json
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils


_EXCLUDE_KEYS = {"results_dir", "extra_args"}


def _format_value(value):
    """Like ``format_config_value`` but also normalizes stringified booleans.

    test_parameters values are run through ``format_si_units`` upstream which
    turns Python booleans into ``"True"``/``"False"`` strings, so we need to
    catch both forms here.
    """
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, str) and value in ("True", "False"):
        return 1 if value == "True" else 0
    return CommonUtils.format_config_value(value)


def get_section_name(test_parameters):
    """Build a section name from test_parameters using short-name abbreviations."""
    ganesha_enabled = bool(_format_value(test_parameters.get("Ganesha Enabled", 0)))
    parts = []
    for key, value in test_parameters.items():
        if key in _EXCLUDE_KEYS:
            continue
        if key.startswith("Ganesha ") and not ganesha_enabled:
            continue
        short = CommonUtils.get_short_name(key)
        formatted = _format_value(value)
        parts.append(f"{short}{formatted}")
    return "_".join(parts)


def merge_results(input_files, output_file):
    merged = {}
    for fp in input_files:
        try:
            with open(fp, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {fp}: {e}", file=sys.stderr)
            continue

        test_params = data.get("test_parameters", {})
        if not test_params:
            print(
                f"Warning: {fp} has no test_parameters section; skipping",
                file=sys.stderr,
            )
            continue

        if "test_results_summary" not in data:
            data["test_results_summary"] = CommonUtils.get_summary(data)

        section = get_section_name(test_params)
        if section in merged:
            print(
                f"Warning: section '{section}' already present; overwriting "
                f"with {fp}",
                file=sys.stderr,
            )
        merged[section] = data

    with open(output_file, "w") as f:
        json.dump(merged, f, indent=4)

    print(f"Merged {len(merged)} result(s) into {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Merge workload result JSON files into a single output JSON."
    )
    parser.add_argument(
        "files", nargs="+", help="Input result JSON files (glob patterns supported)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="merged_results.json",
        help="Output JSON file (default: merged_results.json)",
    )
    args = parser.parse_args()

    expanded = []
    for pattern in args.files:
        matches = glob.glob(pattern)
        if matches:
            expanded.extend(sorted(matches))
        elif os.path.exists(pattern):
            expanded.append(pattern)

    if not expanded:
        print(f"No files found matching: {args.files}", file=sys.stderr)
        sys.exit(1)

    merge_results(expanded, args.output)


if __name__ == "__main__":
    main()
