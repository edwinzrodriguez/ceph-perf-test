#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
import time

# Add project root to sys.path to allow importing cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils


# Map user-facing readwrite values to rados-bench sub-command + flags.
# rados bench has three modes: write, seq, rand.
# We reuse 'write' for both seqwrite and randwrite; randwrite adds a
# variable object-size range and can produce non-sequential object writes
# when combined with --min-object-size/--max-object-size.
OP_MAP = {
    "seqwrite": "write",
    "randwrite": "write",
    "seqread": "seq",
    "randread": "rand",
}


def build_run_name(fs_name, client, lp_cfg):
    """Build a descriptive --run-name based on fs_name.

    The run-name is intentionally NOT per-loadpoint so that a seqread/randread
    loadpoint can find objects previously written by a seqwrite/randwrite
    loadpoint on the same client.
    """
    base = fs_name or "rados_bench"
    explicit = lp_cfg.get("run_name") if lp_cfg else None
    if explicit:
        return f"{explicit}_{client}"
    return f"{base}_{client}"


def main():
    parser = argparse.ArgumentParser(description="Rados Bench Workload Driver")
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
        help="JSON list of clients or path to JSON file (prefix with @)",
    )
    parser.add_argument("--runner-name", help="Name of the workload runner")
    args = parser.parse_args()

    def load_json_arg(arg_value):
        if arg_value.startswith("@"):
            with open(arg_value[1:], "r") as f:
                return json.load(f)
        return json.loads(arg_value)

    try:
        settings = load_json_arg(args.settings)
        loadpoints = load_json_arg(args.loadpoints)
        clients = load_json_arg(args.clients)
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)

    results_dir = settings.get("results_dir")
    fs_name = settings.get("fs_name")

    executable = settings.get("executable_path", "/usr/local/bin/rados")
    base_env_vars = dict(settings.get("env_vars", {}))
    config_path = settings.get("config_path")
    keyring = settings.get("keyring")
    client_id = settings.get("client_id")
    pool = settings.get("pool")
    global_duration = int(settings.get("duration", 30))
    global_no_cleanup = settings.get("no_cleanup", True)

    if not pool:
        print("Error: 'pool' must be set in rados_bench config")
        sys.exit(1)

    status_re = re.compile(
        r"^\s*(?P<sec>\d+)\s+(?P<cur>\d+)\s+(?P<started>\d+)\s+(?P<finished>\d+)"
    )

    for i, lp_cfg in enumerate(loadpoints):
        lp = i + 1
        print(f"Starting tests... Load Point: {lp}", flush=True)
        time.sleep(2)

        readwrite = lp_cfg.get("readwrite", "seqwrite")
        op = OP_MAP.get(readwrite)
        if op is None:
            print(
                f"Error: unknown readwrite value '{readwrite}' "
                f"(expected one of {list(OP_MAP.keys())})"
            )
            sys.exit(1)

        duration = int(lp_cfg.get("duration", global_duration))
        threads = lp_cfg.get("threads", 16)
        min_obj = lp_cfg.get("min-object-size")
        max_obj = lp_cfg.get("max-object-size")
        read_percent = lp_cfg.get("read-percent")
        no_cleanup = lp_cfg.get("no_cleanup", global_no_cleanup)
        extra = lp_cfg.get("extra_args", "")

        processes = []
        for client in clients:
            run_name = build_run_name(fs_name, client, lp_cfg)

            cmd_parts = []
            env_vars = dict(base_env_vars)
            if env_vars:
                env_str = " ".join(f'{k}="{v}"' for k, v in env_vars.items())
                cmd_parts.append(f"env {env_str}")

            cmd_parts.append(executable)
            if config_path:
                cmd_parts.extend(["-c", config_path])
            if keyring:
                cmd_parts.extend(["-k", keyring])
            if client_id:
                cmd_parts.extend(["-n", f"client.{client_id}"])
            cmd_parts.extend(["-p", pool])

            # Standard invocation: `rados -p POOL bench <SECONDS> <MODE> [opts]`.
            cmd_parts.extend(["bench", str(duration), op])

            cmd_parts.extend(["--concurrent-ios", str(threads)])
            cmd_parts.extend(["--run-name", run_name])

            if op == "write":
                # Variable object sizes apply to writes only.
                if min_obj is not None:
                    min_obj_bytes = str(CommonUtils.parse_si_unit(min_obj))
                    cmd_parts.extend(["--min-object-size", min_obj_bytes])
                if max_obj is not None:
                    max_obj_bytes = str(CommonUtils.parse_si_unit(max_obj))
                    cmd_parts.extend(["--max-object-size", max_obj_bytes])

            if read_percent is not None:
                cmd_parts.extend(["--read-percent", str(read_percent)])

            if no_cleanup:
                cmd_parts.append("--no-cleanup")

            json_output = f"/tmp/{CommonUtils.get_workload_base_name('rados_bench', 'result', client, lp, settings, lp_cfg)}.json"
            cmd_parts.extend(["--format", "json"])
            cmd_parts.extend(["--output", json_output])

            if extra:
                cmd_parts.append(str(extra))

            cmd = " ".join(cmd_parts)

            print(f"[{client}] Executing rados bench: {cmd}", flush=True)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", client, "bash -s"]
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
            processes.append((client, proc, json_output))

        for client, proc, remote_json in processes:
            run_phase_started = False
            last_status_time = 0.0
            for line in proc.stdout:
                line_clean = line.strip()
                m = status_re.match(line_clean)
                if m:
                    if not run_phase_started:
                        print("Starting RUN phase", flush=True)
                        run_phase_started = True
                    now = time.monotonic()
                    if now - last_status_time >= 1.0:
                        last_status_time = now
                        sec = m.group("sec")
                        cur = m.group("cur")
                        finished = m.group("finished")
                        print(
                            f"[{client}] rados bench: sec={sec} cur={cur} finished={finished}",
                            flush=True,
                        )
                else:
                    print(f"[{client}] {line_clean}", flush=True)

            proc.wait()
            if proc.returncode != 0:
                print(
                    f"[{client}] rados bench failed with return code {proc.returncode}"
                )

                class SimpleExecutor:
                    def run_remote(self, h, cmd, check=False):
                        result = subprocess.run(
                            ["ssh", "-o", "StrictHostKeyChecking=no", h, "bash -s"],
                            input=cmd + "\n",
                            capture_output=True,
                            text=True,
                        )
                        return result.stdout

                CommonUtils.collect_journal_logs(
                    SimpleExecutor(), clients, results_dir
                )
                sys.exit(proc.returncode)

            json_filename = os.path.basename(remote_json)
            local_json = os.path.join(results_dir, json_filename)

            print(f"[{client}] Copying results to {results_dir}...", flush=True)
            subprocess.run(
                [
                    "scp",
                    "-o",
                    "StrictHostKeyChecking=no",
                    f"{client}:{remote_json}",
                    local_json,
                ]
            )
            subprocess.run(
                [
                    "ssh",
                    "-o",
                    "StrictHostKeyChecking=no",
                    client,
                    f"rm -f {remote_json}",
                ],
                stderr=subprocess.DEVNULL,
            )

            try:
                with open(local_json, "r") as f:
                    data = json.load(f)

                data["test_parameters"] = CommonUtils.get_human_readable_settings(
                    settings, lp_cfg
                )
                if args.runner_name:
                    data["test_parameters"]["Workload Runner"] = args.runner_name

                data["test_results_summary"] = CommonUtils.get_summary(data)

                with open(local_json, "w") as f:
                    json.dump(data, f, indent=4)
                print(
                    f"[{client}] Injected test parameters into {local_json}",
                    flush=True,
                )
            except Exception as e:
                print(
                    f"[{client}] Failed to inject test parameters into {local_json}: {e}",
                    flush=True,
                )

        print(f"Finished Rados Bench Load Point: {lp}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
