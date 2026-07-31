#!/usr/bin/env python3
"""Convert an Ansible INI inventory to the nested inventory format used in
MDSConfigurationSettings.yml (and sibling settings files).

Variables such as ``{{ ssh_user }}`` are expanded using (in order):
  1. ``group_vars/all.yml`` and ``cluster.json`` next to the project root
  2. ``--vars-file`` YAML/JSON files
  3. ``-e`` / ``--extra-var`` KEY=VALUE pairs

Example:
  ./scripts/ansible_inventory_to_json.py ansible_inventory \\
      -e ssh_user=root -e ssh_user_home=/home/vpcuser

  ./scripts/ansible_inventory_to_json.py ansible_inventory --yaml \\
      -e ssh_user=root -e ssh_user_home=/home/vpcuser > inventory.yml
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import sys

import yaml

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import AnsibleInventoryProvider


def _parse_extra_var(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError(
            f"extra var must be KEY=VALUE, got: {raw!r}"
        )
    key, value = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise argparse.ArgumentTypeError(f"extra var key is empty: {raw!r}")
    return key, value


def _coerce_value(value):
    """Best-effort type coercion to match settings-file style."""
    if not isinstance(value, str):
        return value
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    return value


def _host_vars(meta: dict) -> dict:
    """Drop the internal ``name`` key; host name becomes the dict key."""
    return {
        key: _coerce_value(val)
        for key, val in meta.items()
        if key != "name"
    }


def to_nested_inventory(hosts_by_group: dict) -> dict:
    """
    Convert AnsibleInventoryProvider list format::

        {'mons': [{'name': 'host', 'ansible_ssh_host': '...'}, ...]}

    to DirectInventoryProvider / settings YAML format::

        {'mons': {'host': {'ansible_ssh_host': '...'}, ...}}
    """
    nested = {}
    for group, hosts in hosts_by_group.items():
        nested[group] = {}
        for meta in hosts:
            nested[group][meta["name"]] = _host_vars(meta)
    return nested


def load_vars_file(path: str) -> dict:
    with open(path, "r") as f:
        if path.endswith((".yml", ".yaml")):
            data = yaml.safe_load(f)
        else:
            data = json.load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise SystemExit(f"vars file must contain a mapping: {path}")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert an Ansible INI inventory to nested JSON/YAML inventory "
            "matching MDSConfigurationSettings.yml"
        )
    )
    parser.add_argument(
        "inventory",
        help="Path to the Ansible INI inventory file",
    )
    parser.add_argument(
        "-e",
        "--extra-var",
        action="append",
        default=[],
        type=_parse_extra_var,
        metavar="KEY=VALUE",
        help="Variable used to expand {{ KEY }} templates (repeatable)",
    )
    parser.add_argument(
        "--vars-file",
        action="append",
        default=[],
        metavar="PATH",
        help="YAML/JSON file of variables for template expansion (repeatable)",
    )
    parser.add_argument(
        "--yaml",
        action="store_true",
        help="Emit YAML instead of JSON (suitable for pasting into settings)",
    )
    parser.add_argument(
        "--wrap",
        action="store_true",
        help="Wrap output under a top-level 'inventory' key",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Write to PATH instead of stdout",
    )
    return parser


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)

    if not os.path.isfile(args.inventory):
        print(f"Error: inventory file not found: {args.inventory}", file=sys.stderr)
        return 1

    extra_vars: dict = {}
    for path in args.vars_file:
        extra_vars.update(load_vars_file(path))
    for key, value in args.extra_var:
        extra_vars[key] = value

    # AnsibleInventoryProvider logs load details to stdout; keep JSON/YAML clean.
    with contextlib.redirect_stdout(io.StringIO()):
        provider = AnsibleInventoryProvider(args.inventory, extra_vars=extra_vars)
    nested = to_nested_inventory(provider.get_hosts())

    unresolved = re.compile(r"\{\{\s*\w+\s*\}\}")
    blob = json.dumps(nested)
    leftover = sorted(set(unresolved.findall(blob)))
    if leftover:
        print(
            "Warning: unresolved template variables: "
            + ", ".join(leftover)
            + " (pass -e KEY=VALUE or --vars-file)",
            file=sys.stderr,
        )

    payload = {"inventory": nested} if args.wrap else nested

    if args.yaml:
        text = yaml.safe_dump(
            payload,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    else:
        text = json.dumps(payload, indent=2) + "\n"

    if args.output:
        with open(args.output, "w") as f:
            f.write(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
