#!/usr/bin/env python3
"""Local MDS admin-socket perf dump / histogram collector.

Runs on an MDS host as a background process. Periodically writes:
  <host>_<timestamp>_perf_dump.json
  <host>_<timestamp>_perf_histogram_dump_mds.json
into --output-dir. Stops on SIGTERM/SIGINT.

Example:
  python3 /cephfs_perf/mds_perf_dump_collector.py \\
    --host mon-000 \\
    --asok /var/run/ceph/ceph-mds.foo.asok \\
    --ceph-bin /usr/local/bin/ceph \\
    --output-dir /tmp/mds_perf_dump_lp01 \\
    --interval 5 \\
    --pid-file /tmp/mds_perf_dump_lp01.pid \\
    --env CEPH_INSTALL_PREFIX=/usr/local \\
    --env LD_LIBRARY_PATH=/usr/local/lib64
"""
from __future__ import annotations

import argparse
import datetime
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time


_stop = False


def _on_signal(signum, _frame):
    global _stop
    _stop = True


def _admin_daemon_cmd(ceph_bin: str, asok: str, args: str) -> list:
    """Build argv that cds into the asok dir (AF_UNIX sun_path limit)."""
    asok = (asok or "").rstrip("/")
    if "/" in asok:
        directory, name = asok.rsplit("/", 1)
    else:
        directory, name = ".", asok
    inner = (
        f"cd {shlex.quote(directory)} && "
        f"{shlex.quote(ceph_bin)} --admin-daemon {shlex.quote(name)} {args}"
    )
    return ["/bin/bash", "-c", inner]


_SYSTEM_PATH = "/usr/sbin:/usr/bin:/sbin:/bin"


def _ensure_system_path(env: dict) -> None:
    """Keep system dirs on PATH so bash/sudo remain findable after Ceph PATH overrides."""
    path = env.get("PATH", "") or ""
    parts = [p for p in path.split(":") if p]
    for d in _SYSTEM_PATH.split(":"):
        if d not in parts:
            parts.append(d)
    env["PATH"] = ":".join(parts)


def _run_cmd(cmd: list, dest_path: str, env: dict) -> subprocess.CompletedProcess:
    with open(dest_path, "w", encoding="utf-8") as out:
        return subprocess.run(
            cmd,
            stdout=out,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )


def _run_dump(ceph_bin: str, asok: str, args: str, dest_path: str, env: dict) -> None:
    cmd = _admin_daemon_cmd(ceph_bin, asok, args)
    # Controllers often SSH as root; Ceph env PATH may omit /usr/bin so bare
    # "sudo" fails with FileNotFoundError before we can fall back.
    use_sudo = os.geteuid() != 0 and bool(shutil.which("sudo", path=env.get("PATH")))
    if use_sudo:
        try:
            result = _run_cmd(["sudo", "-n"] + cmd, dest_path, env)
        except FileNotFoundError:
            result = None
        if result is not None and result.returncode == 0:
            return

    result = _run_cmd(cmd, dest_path, env)
    if result.returncode != 0:
        err = (result.stderr or "").strip()
        raise RuntimeError(
            f"admin-daemon '{args}' failed (rc={result.returncode}): {err}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Periodically dump MDS perf counters and histograms locally"
    )
    parser.add_argument("--host", required=True, help="Hostname used in output filenames")
    parser.add_argument("--asok", required=True, help="Path to MDS admin socket")
    parser.add_argument("--ceph-bin", required=True, help="Path to ceph CLI binary")
    parser.add_argument("--output-dir", required=True, help="Local directory for JSON dumps")
    parser.add_argument(
        "--interval", type=float, default=60.0, help="Seconds between dump cycles"
    )
    parser.add_argument(
        "--pid-file",
        default="",
        help="Write this process PID here (removed on exit)",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra environment variable for ceph CLI (repeatable)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    os.makedirs(args.output_dir, exist_ok=True)

    env = os.environ.copy()
    for item in args.env:
        if "=" not in item:
            print(f"Warning: ignoring malformed --env {item!r}", flush=True)
            continue
        key, value = item.split("=", 1)
        env[key] = value
    _ensure_system_path(env)

    if args.pid_file:
        with open(args.pid_file, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))

    interval = max(0.5, float(args.interval))
    print(
        f"mds_perf_dump_collector: host={args.host} asok={args.asok} "
        f"interval={interval}s output={args.output_dir}",
        flush=True,
    )

    try:
        while not _stop:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%d-%H%M%S-%f"
            )
            base = f"{args.host}_{ts}"
            try:
                if not os.path.exists(args.asok):
                    print(
                        f"Warning: admin socket missing ({args.asok}); skip cycle",
                        flush=True,
                    )
                else:
                    _run_dump(
                        args.ceph_bin,
                        args.asok,
                        "perf dump",
                        os.path.join(args.output_dir, f"{base}_perf_dump.json"),
                        env,
                    )
                    _run_dump(
                        args.ceph_bin,
                        args.asok,
                        "perf histogram dump mds",
                        os.path.join(
                            args.output_dir, f"{base}_perf_histogram_dump_mds.json"
                        ),
                        env,
                    )
            except Exception as exc:
                print(f"Warning: dump cycle failed: {exc}", flush=True)

            # Interruptible sleep
            deadline = time.monotonic() + interval
            while not _stop and time.monotonic() < deadline:
                time.sleep(min(0.5, deadline - time.monotonic()))
    finally:
        if args.pid_file:
            try:
                os.remove(args.pid_file)
            except OSError:
                pass
        print("mds_perf_dump_collector: stopped", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
