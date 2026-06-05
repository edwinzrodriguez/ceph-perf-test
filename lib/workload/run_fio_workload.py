#!/usr/bin/env python3
import json
import argparse
import subprocess
import datetime
import os
import sys
import threading

# Add project root to sys.path to allow importing cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils


def snake_to_pascal(snake_str):
    return "".join(x.capitalize() for x in snake_str.split("_"))


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
        loadpoint = idx + 1
        print(f"Starting Fio Load Point: {loadpoint}", flush=True)

        # Signal that a new load point is starting for external monitoring
        print(f"Starting tests... Load Point: {loadpoint}", flush=True)

        for c in clients:
            # Ensure results directory exists on each client
            subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", c, f"mkdir -p {results_dir}"]
            )

            for mp in mount_points:
                variables = {
                    "mount_point": mp,
                    "client": c,
                    "results_dir": results_dir,
                    "fs_name": fs_name,
                }

                # Construct fio command from loadpoint configuration
                lp_cfg = config
                # fio command basic options
                fio_parts = ["fio"]
                fio_parts.append(f"--name=lp{loadpoint:02d}_{c}")
                fio_parts.append(f"--directory={mp}")

                # Map parameters
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

                # Duration to runtime mapping
                # First check loadpoint duration, then global settings
                duration = lp_cfg.get("duration", settings.get("duration", 0))
                if duration:
                    fio_parts.append(f"--time_based=1")
                    fio_parts.append(f"--runtime={duration}")

                # Other common settings from loadpoint or global settings
                for key in ["gtod_reduce", "ramp_time", "randrepeat"]:
                    if key in lp_cfg:
                        fio_parts.append(f"--{key}={lp_cfg[key]}")
                    elif key in settings:
                        fio_parts.append(f"--{key}={settings[key]}")

                if "extra_args" in lp_cfg and lp_cfg["extra_args"]:
                    fio_parts.append(lp_cfg["extra_args"])

                cmd = " ".join(fio_parts)
                filename = f"{CommonUtils.get_workload_base_name('fio', 'result', c, loadpoint, settings, lp_cfg)}.json"

                remote_path = f"{results_dir}/{filename}"
                cmd += f" --group_reporting --output-format=json+ --output={remote_path} --eta=always"

                print(f"[{c}] Executing Fio: {cmd}", flush=True)

                # Use Popen to read output in real-time
                # Pass command via stdin to avoid "Argument list too long" errors
                ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", c, "bash -s"]
                process = subprocess.Popen(
                    ssh_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                # Send the command to stdin and close it
                process.stdin.write(cmd + "\n")
                process.stdin.close()

                import re

                # Regex for Jobs: 8 (f=8): [w(8)][18.8%][w=454MiB/s][w=1858 IOPS][eta 02m:27s]
                # During ramp: Jobs: 8 (f=0): [/(8)][-.-%][eta 02m:57s]
                # Note: Fio can sometimes report negative percentages during certain phases or if clocks are skewed.
                # We use a non-greedy match for the first bracketed group to correctly find the percentage.
                status_re = re.compile(
                    r"Jobs: \d+ \(f=\d+\): \[.*?\]\[(?P<percent>[\d\.-]+)%\](?:\[.*\])?\[eta (?P<eta>.*)\]"
                )

                run_phase_started = False
                for line in process.stdout:
                    line = line.strip()
                    if line.startswith("Jobs:"):
                        # print(f"[{c}] Fio Status: {line}", flush=True)
                        match = status_re.search(line)
                        if match:
                            percent = match.group("percent")
                            eta = match.group("eta")

                            # Fio can sometimes report very large negative percentages if it gets confused
                            # about the timing (e.g. during ramp-down or if clocks are slightly out of sync).
                            # If we see a large negative number, treat it as 0.0 or just skip reporting it
                            # to avoid confusing the user.
                            try:
                                f_percent = float(percent)
                                if f_percent < 0:
                                    percent = "0.0"
                            except ValueError:
                                # This handles "-.-" case
                                pass

                            if not run_phase_started and percent != "-.-":
                                print("Starting RUN phase", flush=True)
                                run_phase_started = True

                            # Report percentage and status back to caller
                            print(
                                f"[{c}] Fio Status: {percent}% complete, ETA: {eta}",
                                flush=True,
                            )
                    else:
                        # Print other output as is
                        print(f"[{c}] {line}", flush=True)

                process.wait()

                if process.returncode != 0:
                    print(
                        f"[{c}] Fio failed with return code {process.returncode}",
                        flush=True,
                    )
                    # Use a dummy executor for log collection
                    class SimpleExecutor:
                        def run_remote(self, host, cmd, check=False):
                            # Pass command via stdin to avoid "Argument list too long" errors
                            result = subprocess.run(
                                ["ssh", "-o", "StrictHostKeyChecking=no", host, "bash -s"],
                                input=cmd + "\n",
                                capture_output=True,
                                text=True
                            )
                            return result.stdout

                    CommonUtils.collect_journal_logs(SimpleExecutor(), clients, results_dir)
                    sys.exit(process.returncode)

                # Copy results from client to admin (where this script is running)
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

                # Inject test parameters into the JSON file
                try:
                    with open(local_path, "r") as f:
                        data = json.load(f)

                    data["test_parameters"] = CommonUtils.get_human_readable_settings(
                        settings, lp_cfg
                    )
                    if args.runner_name:
                        data["test_parameters"]["Workload Runner"] = args.runner_name

                    data["test_results_summary"] = CommonUtils.get_summary(data)

                    with open(local_path, "w") as f:
                        json.dump(data, f, indent=4)
                    print(
                        f"[{c}] Injected test parameters into {local_path}", flush=True
                    )
                except Exception as e:
                    print(f"[{c}] Failed to inject test parameters: {e}", flush=True)

        print(f"Finished Fio Load Point: {loadpoint}", flush=True)


if __name__ == "__main__":
    main()
