#!/usr/bin/env python3
"""Remote driver for cephfs-mdsbench SpecStorage-style loadpoints.

Each loadpoint is ``{"clients": N}``. This script launches ``N`` processes
with ``--clients N --client-id K`` (barrier enabled) round-robin across the
inventory client hosts. Aggregate ``ops_per_sec`` is divided evenly so the
cluster-offered rate matches the configured target.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time

# Add project root to sys.path to allow importing cephfs_perf_lib
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils


def load_json_arg(arg_value):
    """Load JSON from string or file (if prefixed with @)."""
    if arg_value.startswith("@"):
        file_path = arg_value[1:]
        with open(file_path, "r") as f:
            return json.load(f)
    return json.loads(arg_value)


def build_mdsbench_cmd(settings, lp_cfg, total_clients, client_id, json_output):
    executable = settings.get("executable_path", "/usr/local/bin/cephfs-mdsbench")
    env_vars = dict(settings.get("env_vars", {}))
    config_path = settings.get("config_path")
    keyring = settings.get("keyring")
    ceph_user = settings.get("client_id")
    root_path = settings.get("root_path", "/")
    workdir = settings.get("workdir", "/mdsbench")
    mode = lp_cfg.get("mode", settings.get("mode", "balanced"))
    duration = lp_cfg.get("duration", settings.get("duration", 60))
    warmup = lp_cfg.get("warmup", settings.get("warmup", 5))
    seed = settings.get("seed", 1)
    write_bytes = settings.get("write_bytes")
    scratch_slots = settings.get("scratch_slots")
    init_file_bytes = settings.get("init_file_bytes")
    init_fill_percent = settings.get("init_fill_percent")
    init_threads = settings.get("init_threads")
    keep_tree = settings.get("keep_tree", False)
    skip_precreate = settings.get("skip_precreate", False)
    barrier_timeout = settings.get("barrier_timeout")
    progress = settings.get("progress", True)
    progress_interval = settings.get("progress_interval", 10)
    fs_name = settings.get("fs_name")
    extra = lp_cfg.get("extra_args", settings.get("extra_args"))

    aggregate_ops = float(
        lp_cfg.get("ops_per_sec", settings.get("ops_per_sec", 12000))
    )
    # Per-process share; sum across processes == aggregate target.
    per_proc_ops = aggregate_ops / total_clients if total_clients > 0 else aggregate_ops

    cmd_parts = []
    if env_vars:
        cmd_parts.append("".join(f'export {k}="{v}"; ' for k, v in env_vars.items()))
    cmd_parts.append(executable)

    if config_path:
        cmd_parts.extend(["-c", config_path])
    if keyring:
        cmd_parts.extend(["-k", keyring])
    if ceph_user:
        cmd_parts.extend(["--id", ceph_user])
    if fs_name:
        cmd_parts.extend(["--filesystem", fs_name])
    if root_path:
        cmd_parts.extend(["--root-path", root_path])
    if workdir:
        cmd_parts.extend(["--workdir", workdir])

    cmd_parts.extend(["--clients", str(total_clients)])
    cmd_parts.extend(["--client-id", str(client_id)])
    cmd_parts.extend(["--ops-per-sec", str(per_proc_ops)])
    cmd_parts.extend(["--duration", str(duration)])
    cmd_parts.extend(["--warmup", str(warmup)])
    cmd_parts.extend(["--seed", str(seed)])
    cmd_parts.extend(["--mode", str(mode)])

    if write_bytes is not None:
        cmd_parts.extend(["--write-bytes", str(write_bytes)])
    if scratch_slots is not None:
        cmd_parts.extend(["--scratch-slots", str(scratch_slots)])
    if init_file_bytes is not None:
        cmd_parts.extend(["--init-file-bytes", str(init_file_bytes)])
    if init_fill_percent is not None:
        cmd_parts.extend(["--init-fill-percent", str(init_fill_percent)])
    if init_threads is not None:
        cmd_parts.extend(["--init-threads", str(init_threads)])
    if keep_tree:
        cmd_parts.append("--keep-tree")
    if skip_precreate:
        cmd_parts.append("--skip-precreate")
    if barrier_timeout is not None:
        cmd_parts.extend(["--barrier-timeout", str(barrier_timeout)])

    if progress:
        cmd_parts.append("--progress")
        if progress_interval:
            cmd_parts.extend(["--progress-interval", str(progress_interval)])

    cmd_parts.extend(["--json", json_output])

    if extra:
        cmd_parts.append(str(extra))

    return " ".join(cmd_parts)


def main():
    parser = argparse.ArgumentParser(description="MDS-Bench SpecStorage-style Workload Driver")
    parser.add_argument(
        "--settings",
        type=str,
        required=True,
        help="JSON settings or path to JSON file (prefix with @)",
    )
    parser.add_argument(
        "--loadpoints",
        type=str,
        required=True,
        help="JSON list of loadpoints or path to JSON file (prefix with @)",
    )
    parser.add_argument(
        "--clients",
        type=str,
        required=True,
        help="JSON list of inventory client hosts or path to JSON file (prefix with @)",
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

    if not clients:
        print("Error: no inventory clients provided")
        sys.exit(1)

    results_dir = settings.get("results_dir")
    status_re = re.compile(
        r"\[(?P<percent>\d+(?:\.\d+)?)%\](?:\[.*\])*\[eta (?P<eta>.*)\]"
    )

    for i, lp_cfg in enumerate(loadpoints):
        lp = i + 1
        total_clients = int(lp_cfg.get("clients", 1))
        if total_clients < 1:
            print(f"Error: loadpoint {lp} clients must be >= 1, got {total_clients}")
            sys.exit(1)

        print(f"Starting tests... Load Point: {lp}", flush=True)
        time.sleep(2)  # Give time for runner to detect and reset perf

        processes = []
        # Round-robin client_id K across inventory hosts.
        for client_id in range(total_clients):
            host = clients[client_id % len(clients)]
            result_tag = f"{host}_c{client_id}"
            json_filename = (
                f"{CommonUtils.get_workload_base_name('mdsbench', 'result', result_tag, lp, settings, lp_cfg)}.json"
            )
            json_output = f"/tmp/{json_filename}"
            cmd = build_mdsbench_cmd(
                settings, lp_cfg, total_clients, client_id, json_output
            )

            print(
                f"[{host}] Executing MDS-Bench client-id={client_id}/{total_clients}: {cmd}",
                flush=True,
            )
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", host, "bash -s"]
            proc = subprocess.Popen(
                ssh_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=0,
            )
            proc.stdin.write(cmd + "\n")
            proc.stdin.close()
            processes.append((host, client_id, result_tag, json_filename, proc))

        run_phase_lock = threading.Lock()
        run_phase_emitted = {"value": False}
        print_lock = threading.Lock()

        def drain_proc(host, client_id, proc):
            buf = ""
            while True:
                chunk = proc.stdout.read(256)
                if not chunk:
                    break
                buf += chunk
                while True:
                    idx_r = buf.find("\r")
                    idx_n = buf.find("\n")
                    if idx_r < 0 and idx_n < 0:
                        break
                    if idx_r < 0:
                        idx = idx_n
                    elif idx_n < 0:
                        idx = idx_r
                    else:
                        idx = min(idx_r, idx_n)
                    line_clean = buf[:idx].strip()
                    buf = buf[idx + 1 :]
                    if not line_clean:
                        continue
                    match = status_re.search(line_clean)
                    with print_lock:
                        if match:
                            percent = float(match.group("percent"))
                            eta = match.group("eta")
                            if percent > 0:
                                with run_phase_lock:
                                    if not run_phase_emitted["value"]:
                                        print("Starting RUN phase", flush=True)
                                        run_phase_emitted["value"] = True
                            print(
                                f"[{host}/c{client_id}] MDS-Bench Status: "
                                f"{percent:.0f}% complete, ETA: {eta}",
                                flush=True,
                            )
                        else:
                            if (
                                not settings.get("progress", True)
                                and line_clean.startswith("Starting ")
                                and " clients" in line_clean
                                and "target=" in line_clean
                            ):
                                with run_phase_lock:
                                    if not run_phase_emitted["value"]:
                                        print("Starting RUN phase", flush=True)
                                        run_phase_emitted["value"] = True
                            print(
                                f"[{host}/c{client_id}] {line_clean}", flush=True
                            )
            leftover = buf.strip()
            if leftover:
                with print_lock:
                    print(f"[{host}/c{client_id}] {leftover}", flush=True)
            proc.wait()
            return proc.returncode

        drain_threads = []
        for host, client_id, result_tag, json_filename, proc in processes:
            proc._rc_box = []
            t = threading.Thread(
                target=lambda h=host, c=client_id, p=proc: p._rc_box.append(
                    drain_proc(h, c, p)
                )
            )
            t.start()
            drain_threads.append(t)

        for t in drain_threads:
            t.join()

        failed = None
        for host, client_id, result_tag, json_filename, proc in processes:
            rc = proc._rc_box[0] if proc._rc_box else proc.returncode
            if rc != 0 and failed is None:
                failed = (host, client_id, rc)

        if failed is not None:
            host, client_id, rc = failed
            print(f"[{host}/c{client_id}] MDS-Bench failed with return code {rc}")

            class SimpleExecutor:
                def run_remote(self, h, cmd, check=False):
                    result = subprocess.run(
                        ["ssh", "-o", "StrictHostKeyChecking=no", h, "bash -s"],
                        input=cmd + "\n",
                        capture_output=True,
                        text=True,
                    )
                    return result.stdout

            CommonUtils.collect_journal_logs(SimpleExecutor(), clients, results_dir)
            sys.exit(rc)

        loadpoint_results = []
        for host, client_id, result_tag, json_filename, proc in processes:
            remote_json = f"/tmp/{json_filename}"
            local_json = os.path.join(results_dir, json_filename)

            print(f"[{host}/c{client_id}] Copying results to {results_dir}...", flush=True)
            subprocess.run(
                [
                    "scp",
                    "-o",
                    "StrictHostKeyChecking=no",
                    f"{host}:{remote_json}",
                    local_json,
                ],
                check=False,
            )
            subprocess.run(
                [
                    "ssh",
                    "-o",
                    "StrictHostKeyChecking=no",
                    host,
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
                data["test_parameters"]["MDSBench Client Id"] = client_id
                data["test_parameters"]["MDSBench Clients"] = total_clients
                data["test_parameters"]["Inventory Host"] = host

                data["test_results_summary"] = CommonUtils.get_summary(data)

                with open(local_json, "w") as f:
                    json.dump(data, f, indent=4)
                print(
                    f"[{host}/c{client_id}] Injected test parameters into {local_json}",
                    flush=True,
                )
                loadpoint_results.append(data)
            except Exception as e:
                print(
                    f"[{host}/c{client_id}] Failed to inject test parameters into {local_json}: {e}",
                    flush=True,
                )

        CommonUtils.write_multi_client_results_summary(
            "mdsbench",
            loadpoint_results,
            results_dir,
            lp,
            settings,
            lp_cfg,
            num_clients=total_clients,
        )

        print(f"Finished MDS-Bench Load Point: {lp}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
