#!/usr/bin/env python3
import json
import argparse
import re
import subprocess
import datetime
import os
import sys
import threading
import time

# Add project root to sys.path to allow importing cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils


def snake_to_pascal(snake_str):
    return "".join(x.capitalize() for x in snake_str.split("_"))


FIO_STATUS_RE = re.compile(
    r"Jobs: \d+ \(f=\d+\): \[.*?\]\[(?P<percent>[\d\.-]+)%\](?:\[.*\])?\[eta (?P<eta>.*)\]"
)


def build_fio_command(
    c,
    mp,
    loadpoint,
    lp_cfg,
    settings,
    fio_bin,
    base_env_vars,
    results_dir,
):
    fio_parts = []
    if base_env_vars:
        fio_parts.append(
            "".join(f'export {k}="{v}"; ' for k, v in base_env_vars.items())
        )
    fio_parts.append(fio_bin)
    fio_parts.append(f"--name=lp{loadpoint:02d}_{c}")
    fio_parts.append(f"--directory={mp}")

    if "size" in lp_cfg:
        fio_parts.append(f"--size={lp_cfg['size']}")
    if "block-size" in lp_cfg:
        fio_parts.append(f"--bs={lp_cfg['block-size']}")
    if "iodepth" in lp_cfg:
        fio_parts.append(f"--iodepth={lp_cfg['iodepth']}")
    if "readwrite" in lp_cfg:
        fio_parts.append(f"--rw={lp_cfg['readwrite']}")
    if "ioengine" in lp_cfg:
        fio_parts.append(f"--ioengine={lp_cfg['ioengine']}")
    if "direct" in lp_cfg:
        fio_parts.append(f"--direct={lp_cfg['direct']}")
    if "buffered" in lp_cfg:
        fio_parts.append(f"--buffered={lp_cfg['buffered']}")
    if "rwmixread" in lp_cfg:
        fio_parts.append(f"--rwmixread={lp_cfg['rwmixread']}")
    if "create_serialize" in lp_cfg:
        fio_parts.append(f"--create_serialize={lp_cfg['create_serialize']}")
    if "threads" in lp_cfg:
        fio_parts.append(f"--numjobs={lp_cfg['threads']}")
    if lp_cfg.get("threads_fio") is True or settings.get("threads_fio") is True:
        fio_parts.append("--thread")

    duration = lp_cfg.get("duration", settings.get("duration", 0))
    if duration:
        fio_parts.append("--time_based=1")
        fio_parts.append(f"--runtime={duration}")

    for key in ["gtod_reduce", "ramp_time", "randrepeat"]:
        if key in lp_cfg:
            fio_parts.append(f"--{key}={lp_cfg[key]}")
        elif key in settings:
            fio_parts.append(f"--{key}={settings[key]}")

    if "extra_args" in lp_cfg and lp_cfg["extra_args"]:
        fio_parts.append(lp_cfg["extra_args"])

    filename = (
        f"{CommonUtils.get_workload_base_name('fio', 'result', c, loadpoint, settings, lp_cfg)}.json"
    )
    remote_path = f"{results_dir}/{filename}"
    cmd = " ".join(fio_parts)
    cmd += (
        f" --group_reporting --output-format=json+ --output={remote_path} --eta=always"
    )
    return cmd, filename, remote_path


def format_si_units(value):
    try:
        val = int(value)
    except (ValueError, TypeError):
        return str(value)

    s_val = str(val)
    if len(s_val) <= 3:
        return s_val

    # Binary units (powers of 1024)
    if val > 0 and val % 1024 == 0:
        for unit in ["Ki", "Mi", "Gi", "Ti", "Pi"]:
            val //= 1024
            if val % 1024 != 0 or val < 1024:
                return f"{val}{unit}"

    # Decimal units (powers of 1000)
    if val > 0 and val % 1000 == 0:
        temp_val = int(s_val)
        for unit in ["k", "m", "g", "t", "p"]:
            temp_val //= 1000
            if temp_val % 1000 != 0 or temp_val < 1000:
                return f"{temp_val}{unit}"

    return s_val


def main():
    parser = argparse.ArgumentParser(description="Run Fio workload")
    parser.add_argument(
        "--settings", required=True, help="JSON string containing test settings or path to JSON file (prefix with @)"
    )
    parser.add_argument(
        "--mount-points", required=True, help="JSON string containing mount points or path to JSON file (prefix with @)"
    )
    parser.add_argument(
        "--clients", required=True, help="JSON string containing client list or path to JSON file (prefix with @)"
    )
    parser.add_argument(
        "--loadpoints", help="JSON string containing loadpoints configuration or path to JSON file (prefix with @)"
    )
    parser.add_argument("--runner-name", help="Name of the workload runner")
    parser.add_argument(
        "--first-loadpoint",
        type=int,
        default=1,
        help="Number assigned to the first loadpoint in --loadpoints (default: 1)",
    )

    args = parser.parse_args()

    def load_json_arg(arg_value):
        """Load JSON from string or file (if prefixed with @)"""
        if arg_value and arg_value.startswith('@'):
            file_path = arg_value[1:]
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            return json.loads(arg_value) if arg_value else None

    try:
        settings = load_json_arg(args.settings)
        mount_points = load_json_arg(args.mount_points)
        clients = load_json_arg(args.clients)
        loadpoints = load_json_arg(args.loadpoints) if args.loadpoints else []
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        print(f"Error loading JSON: {e}")
        return

    fs_name = settings.get("fs_name", "perf_test_fs")
    results_dir = settings.get("results_dir")
    timestamp_progress = settings.get("timestamp_progress", False)
    fio_bin = settings.get("executable_path", "/usr/local/bin/fio")
    base_env_vars = dict(settings.get("env_vars", {}))

    if not results_dir:
        print("Error: results_dir is required in settings")
        return

    print(f"Ensuring results directory exists: {results_dir}")
    os.makedirs(results_dir, exist_ok=True)

    # Use loadpoints
    if not loadpoints:
        print("Error: loadpoints is required")
        return

    workload_configs = loadpoints

    for idx, config in enumerate(workload_configs):
        loadpoint = args.first_loadpoint + idx
        print(f"Starting Fio Load Point: {loadpoint}", flush=True)

        # Signal that a new load point is starting for external monitoring
        print(f"Starting tests... Load Point: {loadpoint}", flush=True)

        loadpoint_results = []
        for c in clients:
            subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", c, f"mkdir -p {results_dir}"]
            )

        processes = []
        for c in clients:
            for mp in mount_points:
                cmd, filename, remote_path = build_fio_command(
                    c,
                    mp,
                    loadpoint,
                    config,
                    settings,
                    fio_bin,
                    base_env_vars,
                    results_dir,
                )
                print(f"[{c}] Executing Fio: {cmd}", flush=True)
                ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", c, "bash -s"]
                proc = subprocess.Popen(
                    ssh_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                proc.stdin.write(cmd + "\n")
                proc.stdin.close()
                processes.append((c, filename, remote_path, proc))

        run_phase_started = False
        run_phase_lock = threading.Lock()

        def monitor_fio_output(client_name, proc):
            nonlocal run_phase_started
            last_status_time = 0.0
            for line in proc.stdout:
                line = line.strip()
                if line.startswith("Jobs:"):
                    match = FIO_STATUS_RE.search(line)
                    if match:
                        percent = match.group("percent")
                        eta = match.group("eta")

                        try:
                            f_percent = float(percent)
                            if f_percent < 0:
                                percent = "0.0"
                        except ValueError:
                            pass

                        if percent != "-.-":
                            with run_phase_lock:
                                if not run_phase_started:
                                    print("Starting RUN phase", flush=True)
                                    run_phase_started = True

                        now = time.monotonic()
                        if now - last_status_time >= 1.0:
                            last_status_time = now
                            ts_prefix = ""
                            if timestamp_progress:
                                ts_prefix = (
                                    datetime.datetime.now(
                                        datetime.timezone.utc
                                    ).strftime("%Y-%m-%dT%H:%M:%S.%f+0000")
                                    + " "
                                )
                            print(
                                f"{ts_prefix}[{client_name}] Fio Status: {percent}% complete, ETA: {eta}",
                                flush=True,
                            )
                elif line:
                    print(f"[{client_name}] {line}", flush=True)

        monitor_threads = []
        for c, _filename, _remote_path, process in processes:
            t = threading.Thread(target=monitor_fio_output, args=(c, process))
            t.start()
            monitor_threads.append(t)

        failed_client = None
        failed_returncode = 0
        for c, _filename, _remote_path, process in processes:
            process.wait()
            if process.returncode != 0 and failed_client is None:
                failed_client = c
                failed_returncode = process.returncode

        for t in monitor_threads:
            t.join()

        if failed_client is not None:
            print(
                f"[{failed_client}] Fio failed with return code {failed_returncode}",
                flush=True,
            )

            class SimpleExecutor:
                def run_remote(self, host, cmd, check=False):
                    result = subprocess.run(
                        ["ssh", "-o", "StrictHostKeyChecking=no", host, "bash -s"],
                        input=cmd + "\n",
                        capture_output=True,
                        text=True,
                    )
                    return result.stdout

            CommonUtils.collect_journal_logs(SimpleExecutor(), clients, results_dir)
            sys.exit(failed_returncode)

        for c, filename, remote_path, _process in processes:
            print(f"[{c}] Copying results to {results_dir}...", flush=True)
            local_path = f"{results_dir}/{filename}"
            subprocess.run(
                [
                    "scp",
                    "-o",
                    "StrictHostKeyChecking=no",
                    f"{c}:{remote_path}",
                    local_path,
                ]
            )

            try:
                with open(local_path, "r") as f:
                    data = json.load(f)

                data["test_parameters"] = CommonUtils.get_human_readable_settings(
                    settings, config
                )
                if args.runner_name:
                    data["test_parameters"]["Workload Runner"] = args.runner_name

                data["test_results_summary"] = CommonUtils.get_summary(data)

                with open(local_path, "w") as f:
                    json.dump(data, f, indent=4)
                print(
                    f"[{c}] Injected test parameters into {local_path}", flush=True
                )
                loadpoint_results.append(data)
            except Exception as e:
                print(f"[{c}] Failed to inject test parameters: {e}", flush=True)

        CommonUtils.write_multi_client_results_summary(
            "fio",
            loadpoint_results,
            results_dir,
            loadpoint,
            settings,
            config,
            num_clients=len(clients),
        )

        print(f"Finished Fio Load Point: {loadpoint}", flush=True)


if __name__ == "__main__":
    main()
