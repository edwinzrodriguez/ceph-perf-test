import argparse
import json
import subprocess
import time
import os
import sys
from cephfs_perf_lib import CommonUtils

def main():
    parser = argparse.ArgumentParser(description="CephFS-Tool Workload Driver")
    parser.add_argument("--settings", type=str, required=True, help="JSON settings")
    parser.add_argument("--loadpoints", type=str, required=True, help="JSON list of loadpoints")
    parser.add_argument("--clients", type=str, required=True, help="JSON list of clients")
    args = parser.parse_args()

    try:
        settings = json.loads(args.settings)
        loadpoints = json.loads(args.loadpoints)
        clients = json.loads(args.clients)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        sys.exit(1)
    results_dir = settings.get("results_dir")
    fs_name = settings.get("fs_name")
    
    executable = settings.get("executable_path", "/usr/local/bin/cephfs-tool")
    ceph_args = settings.get("ceph_args", "")
    config_path = settings.get("config_path")
    keyring = settings.get("keyring")
    client_id = settings.get("client_id")
    root_path = settings.get("root_path", "/")
    duration = settings.get("duration", 0)
    progress = settings.get("progress", True)
    progress_interval = settings.get("progress_interval", 10)

    for i, lp_cfg in enumerate(loadpoints):
        lp = i + 1
        print(f"Starting tests... Load Point: {lp}", flush=True)
        time.sleep(2)  # Give time for runner to detect and reset perf
        
        processes = []
        for client in clients:
            # Construct the cephfs-tool command
            # env CEPH_ARGS="..." /path/to/cephfs-tool bench -c ... -k ... -i ... --filesystem ... --root-path ... --files ... --size ... --threads ... --iterations ... [extra_args]
            
            cmd_parts = []
            if ceph_args:
                cmd_parts.append(f'env CEPH_ARGS="{ceph_args}"')
            
            cmd_parts.append(executable)
            cmd_parts.append("bench")
            
            if config_path:
                cmd_parts.extend(["-c", config_path])
            if keyring:
                cmd_parts.extend(["-k", keyring])
            if client_id:
                cmd_parts.extend(["-i", client_id])
            
            cmd_parts.extend(["--filesystem", fs_name])
            cmd_parts.extend(["--root-path", root_path])
            if duration:
                cmd_parts.extend(["--duration", str(duration)])
            
            if progress:
                cmd_parts.append("--progress")
                if progress_interval:
                    cmd_parts.extend(["--progress-interval", str(progress_interval)])
            
            json_output = f"/tmp/{CommonUtils.get_workload_base_name('cephfs_tool', 'result', client, lp, settings, lp_cfg)}.json"
            perf_dump_output = f"/tmp/{CommonUtils.get_workload_base_name('cephfs_tool', 'perf_dump', client, lp, settings, lp_cfg)}.json"
            cmd_parts.extend(["--json", json_output])
            cmd_parts.extend(["--perf-dump", perf_dump_output])
            
            cmd_parts.extend(["--files", str(lp_cfg.get("files", 1024))])
            
            size = lp_cfg.get("size", "$(( 128 * 2 ** 20 ))")
            size = str(CommonUtils.parse_si_unit(size))
            
            cmd_parts.extend(["--size", str(size)])
            cmd_parts.extend(["--threads", str(lp_cfg.get("threads", 32))])
            cmd_parts.extend(["--iterations", str(lp_cfg.get("iterations", 3))])
            
            client_oc = lp_cfg.get("client-oc")
            if client_oc is not None:
                cmd_parts.extend(["--client-oc", str(client_oc)])
            
            client_oc_size = lp_cfg.get("client-oc-size")
            if client_oc_size is not None:
                client_oc_size = str(CommonUtils.parse_si_unit(client_oc_size))
                cmd_parts.extend(["--client-oc-size", client_oc_size])
            
            block_size = lp_cfg.get("block-size")
            if block_size is not None:
                block_size = str(CommonUtils.parse_si_unit(block_size))
                cmd_parts.extend(["--block-size", block_size])
            
            extra = lp_cfg.get("extra_args")
            if extra:
                cmd_parts.append(str(extra))
            
            # Use shell=True if cmd contains environment variables or redirections
            # or if we want to support $(( ... )) in the command string.
            # However, subprocess.Popen(ssh_cmd, ...) where ssh_cmd is a list 
            # will result in 'ssh client "env CEPH_ARGS=... /path/to/cephfs-tool ..."'
            # which might need to be carefully handled.
            
            cmd = " ".join(cmd_parts)
            
            print(f"[{client}] Executing CephFS-Tool: {cmd}", flush=True)
            # We run it via ssh from the admin node where this script is running
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", client, cmd]
            proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            processes.append((client, proc))

        # Wait for all clients to finish this load point
        import re
        status_re = re.compile(r"\[(?P<percent>\d+)%\](?:\[.*\])*\[eta (?P<eta>.*)\]")
        
        for client, proc in processes:
            run_phase_started = False
            full_stdout = []
            for line in proc.stdout:
                line_clean = line.strip()
                match = status_re.search(line_clean)
                if match:
                    percent = int(match.group("percent"))
                    eta = match.group("eta")
                    
                    if not run_phase_started and percent > 0:
                        print("Starting RUN phase", flush=True)
                        run_phase_started = True
                        
                    print(f"[{client}] CephFS-Tool Status: {percent}% complete, ETA: {eta}", flush=True)
                else:
                    print(f"[{client}] {line_clean}", flush=True)
                
                full_stdout.append(line)
            
            proc.wait()
            stdout = "".join(full_stdout)
            if proc.returncode != 0:
                print(f"[{client}] CephFS-Tool failed with return code {proc.returncode}")
            
            # Copy result JSON from client to results_dir and inject parameters
            json_filename = f"{CommonUtils.get_workload_base_name('cephfs_tool', 'result', client, lp, settings, lp_cfg)}.json"
            remote_json = f"/tmp/{json_filename}"
            local_json = os.path.join(results_dir, json_filename)
            
            print(f"[{client}] Copying results to {results_dir}...", flush=True)
            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", f"{client}:{remote_json}", local_json])
            
            # Inject test parameters
            try:
                with open(local_json, "r") as f:
                    data = json.load(f)
                
                data["test_parameters"] = CommonUtils.get_human_readable_settings(settings, lp_cfg)
                
                with open(local_json, "w") as f:
                    json.dump(data, f, indent=4)
                print(f"[{client}] Injected test parameters into {local_json}", flush=True)
            except Exception as e:
                print(f"[{client}] Failed to inject test parameters into {local_json}: {e}", flush=True)

        print(f"Finished CephFS-Tool Load Point: {lp}", flush=True)
        time.sleep(2)

if __name__ == "__main__":
    main()
