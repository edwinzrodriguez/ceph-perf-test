#!/usr/bin/env python3
import json
import argparse
import subprocess
import datetime
import os


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
        "--commands", required=True, help="JSON string containing Fio command templates"
    )
    parser.add_argument(
        "--mount-points", required=True, help="JSON string containing mount points"
    )
    parser.add_argument(
        "--clients", required=True, help="JSON string containing client list"
    )

    args = parser.parse_args()

    try:
        settings = json.loads(args.settings)
        commands = json.loads(args.commands)
        mount_points = json.loads(args.mount_points)
        clients = json.loads(args.clients)
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

    for idx, cmd_template in enumerate(commands):
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
            # We assume this script runs on the admin host and can SSH to clients
            subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", c, f"mkdir -p {results_dir}"])

            for mp in mount_points:
                variables = {
                    "mount_point": mp,
                    "client": c,
                    "results_dir": results_dir,
                    "fs_name": fs_name,
                }
                cmd = cmd_template
                for k, v in variables.items():
                    cmd = cmd.replace(f"{{{k}}}", str(v))

                filename = f"fio_{c}_{fs_name}_lp{loadpoint}.json"
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
