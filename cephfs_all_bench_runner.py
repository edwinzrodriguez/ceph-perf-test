#!/usr/bin/env python3
"""Run the standard Ceph performance bench suite against one config.

Invokes, in order:
  - cephfs_rados_bench_runner.py
  - cephfs_rbd_runner.py
  - cephfs_tool_bench_runner.py
  - cephfs_fio_runner.py

Each runner receives the same config path and optional Ansible inventory
(and optional --ganesha flag) as this script.

Usage:
  ./cephfs_all_bench_runner.py MDSConfigurationSettings.yml
  ./cephfs_all_bench_runner.py MDSConfigurationSettings.yml trial196_ansible_inventory
  ./cephfs_all_bench_runner.py config.yml inventory --ganesha systemd
  ./cephfs_all_bench_runner.py config.yml --continue-on-error
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time


RUNNERS = [
    "cephfs_rados_bench_runner.py",
    "cephfs_rbd_runner.py",
    "cephfs_tool_bench_runner.py",
    "cephfs_fio_runner.py",
]

# Only these runners use NFS-Ganesha. rados/rbd force StubMount and ignore
# Ganesha; cephfs-tool talks via libcephfs, not NFS mounts. Passing --ganesha
# to those would at best be ignored and at worst expand a useless matrix.
RUNNERS_ACCEPTING_GANESHA = frozenset(
    {
        "cephfs_fio_runner.py",
    }
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run rados-bench, RBD, cephfs-tool, and fio performance runners "
            "sequentially against the same configuration."
        )
    )
    parser.add_argument("config", help="Path to the configuration YAML file")
    parser.add_argument(
        "inventory",
        nargs="?",
        help="Path to the Ansible inventory file (optional; may also be in config)",
    )
    parser.add_argument(
        "--ganesha",
        choices=["cephadm", "systemd"],
        help=(
            "Enable Ganesha and specify the type (passed only to runners that "
            "use NFS mounts, currently cephfs_fio_runner)"
        ),
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with remaining runners if one fails (default: stop on first failure)",
    )
    return parser.parse_args(argv)


def _runner_label(script_name: str) -> str:
    return os.path.splitext(script_name)[0]


def build_cmd(
    script_path: str,
    config: str,
    inventory: str | None,
    ganesha: str | None,
) -> list[str]:
    script_name = os.path.basename(script_path)
    cmd = [sys.executable, script_path, config]
    if inventory:
        cmd.append(inventory)
    if ganesha and script_name in RUNNERS_ACCEPTING_GANESHA:
        cmd.extend(["--ganesha", ganesha])
    return cmd


def run_one(script_path: str, cmd: list[str]) -> int:
    label = _runner_label(os.path.basename(script_path))
    print()
    print("=" * 72)
    print(f"Starting: {label}")
    print(f"Command:  {subprocess.list2cmdline(cmd)}")
    print("=" * 72)
    print(flush=True)

    start = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = time.monotonic() - start

    status = "OK" if result.returncode == 0 else f"FAILED (rc={result.returncode})"
    print()
    print("-" * 72)
    print(f"Finished: {label} — {status} in {elapsed:.1f}s")
    print("-" * 72, flush=True)
    return result.returncode


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = os.path.abspath(os.path.dirname(__file__))

    if not os.path.isfile(args.config):
        print(f"Error: config file not found: {args.config}", file=sys.stderr)
        return 2
    if args.inventory and not os.path.isfile(args.inventory):
        print(f"Error: inventory file not found: {args.inventory}", file=sys.stderr)
        return 2

    results: list[tuple[str, int]] = []

    for script_name in RUNNERS:
        script_path = os.path.join(project_root, script_name)
        if not os.path.isfile(script_path):
            print(f"Error: runner not found: {script_path}", file=sys.stderr)
            results.append((script_name, 127))
            if not args.continue_on_error:
                break
            continue

        cmd = build_cmd(script_path, args.config, args.inventory, args.ganesha)
        rc = run_one(script_path, cmd)
        results.append((script_name, rc))
        if rc != 0 and not args.continue_on_error:
            print(
                f"\nAborting remaining runners after failure of {script_name} "
                f"(use --continue-on-error to keep going).",
                file=sys.stderr,
            )
            break

    print()
    print("=" * 72)
    print("Summary")
    print("=" * 72)
    for name, rc in results:
        mark = "PASS" if rc == 0 else "FAIL"
        print(f"  [{mark}] {_runner_label(name)} (rc={rc})")

    skipped = [n for n in RUNNERS if n not in {r[0] for r in results}]
    for name in skipped:
        print(f"  [SKIP] {_runner_label(name)}")

    failed = [r for r in results if r[1] != 0]
    if failed:
        print(f"\n{len(failed)} of {len(results)} runner(s) failed.")
        return 1

    print(f"\nAll {len(results)} runner(s) completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
