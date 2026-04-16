#!/usr/bin/env python3
import json
import argparse
import subprocess
import datetime
import os
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
        "--settings", required=True, help="JSON string containing test settings"
    )
    parser.add_argument(
        "--mount-points", required=True, help="JSON string containing mount points"
    )
    parser.add_argument(
        "--clients", required=True, help="JSON string containing client list"
    )
    parser.add_argument(
        "--loadpoints", help="JSON string containing loadpoints configuration"
    )

    args = parser.parse_args()

    try:
        settings = json.loads(args.settings)
        mount_points = json.loads(args.mount_points)
        clients = json.loads(args.clients)
        loadpoints = json.loads(args.loadpoints) if args.loadpoints else []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
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
        print(f"Starting Fio Load Point: {loadpoint}")

        # Signal that a new load point is starting for external monitoring
        print(f"Starting tests... Load Point: {loadpoint}")
        print("Starting RUN phase")
        # Sleep for a few seconds to allow performance tools (perf, lockstat) to start
        import time
        time.sleep(5)

        for c in clients:
            # Ensure results directory exists on each client
            subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", c, f"mkdir -p {results_dir}"])

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
                if "create_serialize" in lp_cfg:
                    fio_parts.append(f"--create_serialize={lp_cfg['create_serialize']}")
                if "threads" in lp_cfg:
                    fio_parts.append(f"--numjobs={lp_cfg['threads']}")

                # Duration to runtime mapping
                # First check loadpoint duration, then global settings
                duration = lp_cfg.get("duration", settings.get("duration", 0))
                if duration:
                    fio_parts.append(f"--time_based=1")
                    fio_parts.append(f"--runtime={duration}")

                # Other common settings from global configuration if they exist
                for key in ["gtod_reduce", "ramp_time", "randrepeat"]:
                    if key in settings:
                        fio_parts.append(f"--{key}={settings[key]}")

                if "extra_args" in lp_cfg and lp_cfg["extra_args"]:
                    fio_parts.append(lp_cfg["extra_args"])

                cmd = " ".join(fio_parts)
                filename = f"{CommonUtils.get_workload_base_name('fio', 'result', c, loadpoint, settings, lp_cfg)}.json"

                remote_path = f"{results_dir}/{filename}"
                cmd += f" --group_reporting --output-format=json --output={remote_path}"

                print(f"[{c}] Executing Fio: {cmd}")
                subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", c, cmd])

                # Copy results from client to admin (where this script is running)
                print(f"[{c}] Copying results to {results_dir}...")
                subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", f"{c}:{remote_path}", f"{results_dir}/{filename}"])

        print(f"Finished Fio Load Point: {loadpoint}")


if __name__ == "__main__":
    main()
