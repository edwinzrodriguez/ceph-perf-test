#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import subprocess
import sys
import time

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils


OP_FLAGS = {
    "write": ["--write"],
    "read": ["--read"],
    "writeread": ["--write", "--read"],
    # Match benchmark-s3.sh DELETE phase (-F / --delfiles only)
    "delete": ["--delfiles"],
    "mkdirs": ["--mkdirs"],
}


def load_json_arg(arg_value):
    if arg_value.startswith("@"):
        with open(arg_value[1:], "r") as f:
            return json.load(f)
    return json.loads(arg_value)


def parse_json_stream(text):
    """Parse one or more concatenated JSON objects (elbencho appends per phase)."""
    decoder = json.JSONDecoder()
    idx = 0
    objs = []
    text = text.lstrip()
    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        obj, end = decoder.raw_decode(text, idx)
        objs.append(obj)
        idx = end
    return objs


def start_elbencho_services(executable, clients, port, env_exports):
    for client in clients:
        print(
            f"[{client}] Starting elbencho service on port {port}...",
            flush=True,
        )
        cmd = (
            f"{env_exports}"
            f"pkill -f '[e]lbencho --service' 2>/dev/null || true; "
            f"sleep 0.5; "
            f"nohup {shlex.quote(executable)} --service --port {int(port)} "
            f"> /tmp/elbencho-service.log 2>&1 & "
            f"sleep 0.5; "
            # Confirm the listener came up; dump the log on failure
            f"for i in 1 2 3 4 5 6 7 8 9 10; do "
            f"  ss -ltn 2>/dev/null | grep -q ':{int(port)} ' && exit 0; "
            f"  sleep 0.5; "
            f"done; "
            f"echo 'elbencho service failed to listen on {int(port)}'; "
            f"tail -n 50 /tmp/elbencho-service.log 2>/dev/null || true; "
            f"exit 1"
        )
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", client, "bash -s"],
            input=cmd + "\n",
            text=True,
            capture_output=True,
            check=False,
        )
        if result.stdout:
            print(result.stdout, end="", flush=True)
        if result.stderr:
            print(result.stderr, end="", flush=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"elbencho --service failed to listen on {client}:{port} "
                f"(is the binary installed, and is TCP {port} allowed through "
                f"the client firewall?)"
            )
    time.sleep(1)


def stop_elbencho_services(clients):
    for client in clients:
        subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", client, "bash -s"],
            input="pkill -f '[e]lbencho --service' 2>/dev/null || true\n",
            text=True,
            check=False,
        )


def s3_auth_args(settings):
    args = []
    endpoints = settings.get("rgw_endpoints") or []
    access = settings.get("s3_access")
    secret = settings.get("s3_secret")
    if endpoints:
        args.extend(["--s3endpoints", ",".join(endpoints)])
    if access:
        args.extend(["--s3key", str(access)])
    if secret:
        args.extend(["--s3secret", str(secret)])
    return args


def ensure_buckets(settings, buckets, env_exports):
    executable = settings.get("executable_path", "/usr/local/bin/elbencho")
    for bucket in buckets:
        argv = [executable] + s3_auth_args(settings) + ["--mkdirs", str(bucket)]
        cmd = env_exports + " ".join(shlex.quote(str(a)) for a in argv)
        print(f"[elbencho] Ensuring bucket exists: {bucket}", flush=True)
        proc = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
        if proc.returncode != 0:
            # Bucket may already exist; log and continue
            print(
                f"[elbencho] mkdirs {bucket} rc={proc.returncode}: "
                f"{(proc.stdout or '') + (proc.stderr or '')}",
                flush=True,
            )


def build_elbencho_cmd(settings, lp_cfg, bucket, json_output, clients):
    executable = settings.get("executable_path", "/usr/local/bin/elbencho")
    port = settings.get("load_driver_port", 1611)
    distributed = settings.get("distributed", True)
    env_vars = dict(settings.get("env_vars") or {})

    readwrite = lp_cfg.get("readwrite", "write")
    op_flags = OP_FLAGS.get(readwrite)
    if op_flags is None:
        raise ValueError(
            f"Unknown readwrite '{readwrite}' "
            f"(expected one of {list(OP_FLAGS.keys())})"
        )

    threads = lp_cfg.get("threads", 16)
    size = lp_cfg.get("size", "4MiB")
    block_size = lp_cfg.get("block-size", size)
    files = lp_cfg.get("files", 1000)
    dirs = lp_cfg.get("dirs", 1)
    timelimit = lp_cfg.get("timelimit", settings.get("timelimit"))
    extra = lp_cfg.get("extra_args", "")

    size_bytes = CommonUtils.parse_si_unit(size)
    block_bytes = CommonUtils.parse_si_unit(block_size)

    env_exports = "".join(f'export {k}="{v}"; ' for k, v in env_vars.items())
    argv = [executable]
    if distributed and clients:
        argv.extend(["--hosts", ",".join(clients), "--port", str(port)])
    argv.extend(s3_auth_args(settings))
    argv.extend(op_flags)
    argv.extend(["--threads", str(threads)])
    argv.extend(["--size", str(size_bytes)])
    argv.extend(["--block", str(block_bytes)])
    argv.extend(["--dirs", str(dirs)])
    argv.extend(["--files", str(files)])
    argv.extend(["--jsonfile", json_output])
    if timelimit is not None:
        argv.extend(["--timelimit", str(timelimit)])

    # benchmark-s3.sh: --blockvarpct on PUT only, --s3fastget on GET only
    if readwrite in ("write", "writeread"):
        blockvarpct = lp_cfg.get("blockvarpct")
        if blockvarpct is not None:
            argv.extend(["--blockvarpct", str(blockvarpct)])
    if readwrite in ("read", "writeread") and lp_cfg.get("s3fastget"):
        argv.append("--s3fastget")

    if extra:
        # Allow raw extra flags from config
        argv.extend(shlex.split(str(extra)))
    argv.append(str(bucket))
    return env_exports + " ".join(shlex.quote(str(a)) for a in argv)


def summarize_phases(phases, readwrite):
    preferred = None
    for phase in phases:
        phase_type = str(phase.get("phase_type", "")).upper()
        if readwrite == "write" and phase_type == "WRITE":
            preferred = phase
            break
        if readwrite == "read" and phase_type == "READ":
            preferred = phase
            break
    if preferred is None and phases:
        preferred = phases[-1]

    last = (preferred or {}).get("last_done") or {}
    first = (preferred or {}).get("first_done") or {}
    bytes_per_s = float(last.get("bytes/s") or first.get("bytes/s") or 0)
    iops = float(last.get("iops") or first.get("iops") or 0)
    agg_bw_mib = bytes_per_s / (1024.0 * 1024.0)

    summary = {
        "agg_bw_mib": agg_bw_mib,
        "agg_iops": iops,
        "read": {"agg_bw_mib": 0.0, "agg_iops": 0.0},
        "write": {"agg_bw_mib": 0.0, "agg_iops": 0.0},
    }
    pattern = str(readwrite).lower()
    if "read" in pattern and "write" not in pattern:
        summary["read"] = {"agg_bw_mib": agg_bw_mib, "agg_iops": iops}
    elif "write" in pattern:
        summary["write"] = {"agg_bw_mib": agg_bw_mib, "agg_iops": iops}
        if "read" in pattern:
            for phase in phases:
                if str(phase.get("phase_type", "")).upper() == "READ":
                    rlast = phase.get("last_done") or {}
                    r_bps = float(rlast.get("bytes/s") or 0)
                    r_iops = float(rlast.get("iops") or 0)
                    summary["read"] = {
                        "agg_bw_mib": r_bps / (1024.0 * 1024.0),
                        "agg_iops": r_iops,
                    }
                    summary["agg_bw_mib"] = (
                        summary["write"]["agg_bw_mib"] + summary["read"]["agg_bw_mib"]
                    )
                    summary["agg_iops"] = (
                        summary["write"]["agg_iops"] + summary["read"]["agg_iops"]
                    )
                    break
    return summary


def main():
    parser = argparse.ArgumentParser(description="Elbencho S3 Workload Driver")
    parser.add_argument(
        "--settings",
        required=True,
        help="JSON settings or path to JSON file (prefix with @)",
    )
    parser.add_argument(
        "--loadpoints",
        required=True,
        help="JSON list of loadpoints or path to JSON file (prefix with @)",
    )
    parser.add_argument(
        "--clients",
        required=True,
        help="JSON list of elbencho client hosts or path to JSON file (prefix with @)",
    )
    parser.add_argument("--runner-name", help="Name of the workload runner")
    args = parser.parse_args()

    try:
        settings = load_json_arg(args.settings)
        loadpoints = load_json_arg(args.loadpoints)
        clients = load_json_arg(args.clients)
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)

    results_dir = settings.get("results_dir")
    if not results_dir:
        print("Error: results_dir must be set in settings")
        sys.exit(1)
    os.makedirs(results_dir, exist_ok=True)

    executable = settings.get("executable_path", "/usr/local/bin/elbencho")
    buckets = settings.get("buckets") or ["eot-classic"]
    distributed = settings.get("distributed", True)
    port = settings.get("load_driver_port", 1611)
    env_vars = dict(settings.get("env_vars") or {})
    env_exports = "".join(f'export {k}="{v}"; ' for k, v in env_vars.items())

    if distributed and clients:
        start_elbencho_services(executable, clients, port, env_exports)

    try:
        ensure_buckets(settings, buckets, env_exports)

        for i, lp_cfg in enumerate(loadpoints):
            lp = i + 1
            print(f"Starting tests... Load Point: {lp}", flush=True)
            time.sleep(1)
            print("Starting RUN phase", flush=True)

            readwrite = lp_cfg.get("readwrite", "write")
            loadpoint_results = []

            for bucket in buckets:
                json_name = (
                    f"{CommonUtils.get_workload_base_name('elbencho', 'result', bucket, lp, settings, lp_cfg)}.json"
                )
                remote_json = os.path.join("/tmp", json_name)
                if os.path.exists(remote_json):
                    os.remove(remote_json)

                cmd = build_elbencho_cmd(
                    settings, lp_cfg, bucket, remote_json, clients
                )
                print(f"[elbencho] Executing: {cmd}", flush=True)

                proc = subprocess.Popen(
                    ["bash", "-c", cmd],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                for line in proc.stdout:
                    print(f"[elbencho] {line}", end="", flush=True)
                proc.wait()
                if proc.returncode != 0:
                    print(
                        f"[elbencho] failed with return code {proc.returncode}",
                        flush=True,
                    )
                    stop_elbencho_services(clients)
                    sys.exit(proc.returncode)

                if not os.path.exists(remote_json):
                    print(
                        f"[elbencho] Warning: JSON result file missing: {remote_json}",
                        flush=True,
                    )
                    continue

                with open(remote_json, "r") as f:
                    raw = f.read()
                try:
                    phases = parse_json_stream(raw)
                except json.JSONDecodeError as e:
                    print(f"[elbencho] Failed to parse JSON results: {e}", flush=True)
                    phases = [{"raw": raw, "parse_error": str(e)}]

                result = {
                    "elbencho_phases": phases,
                    "bucket": bucket,
                    "phase_type": (phases[-1].get("phase_type") if phases else None),
                }
                if len(phases) == 1:
                    result.update(phases[0])

                result["test_parameters"] = CommonUtils.get_human_readable_settings(
                    settings, lp_cfg
                )
                if args.runner_name:
                    result["test_parameters"]["Workload Runner"] = args.runner_name
                result["test_parameters"]["Bucket"] = bucket
                result["test_results_summary"] = summarize_phases(phases, readwrite)
                # Keep raw first/last done alongside the numeric summary
                if phases:
                    result["elbencho_last_phase"] = phases[-1]

                dest = os.path.join(results_dir, json_name)
                with open(dest, "w") as f:
                    json.dump(result, f, indent=4)
                print(f"[elbencho] Wrote results to {dest}", flush=True)
                loadpoint_results.append(result)

                try:
                    os.remove(remote_json)
                except OSError:
                    pass

            CommonUtils.write_multi_client_results_summary(
                "elbencho",
                loadpoint_results,
                results_dir,
                lp,
                settings,
                lp_cfg,
                num_clients=len(clients) if clients else 1,
            )
            print(f"Finished Elbencho Load Point: {lp}", flush=True)
            time.sleep(1)
    finally:
        if distributed and clients:
            stop_elbencho_services(clients)


if __name__ == "__main__":
    main()
