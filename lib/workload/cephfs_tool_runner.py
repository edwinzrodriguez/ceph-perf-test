import datetime
import json
import os
import subprocess
import threading
from lib.workload.workload_runner import WorkloadRunner
from cephfs_perf_lib import CommonUtils


class CephFSToolWorkloadRunner(WorkloadRunner):
    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
        results_dir=None,
    ):
        cfg = self.config.cephfs_tool
        loadpoints = cfg.get("loadpoints", [])
        loadpoints = CommonUtils.expand_loadpoints(loadpoints)
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/cephfs_tool/run_cephfs_workload.py"
        )
        perf_record_enabled = cfg.get("perf_record", False)
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = results_dir or self.get_results_dir(settings, ts)

        # Create results directory on admin host
        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir

        # Add global ceph config to payload
        for key, config_val in [
            ("config_path", self.config.ceph_conf_path),
            ("keyring", self.config.ceph_keyring_path),
            ("client_id", self.config.ceph_user_id),
        ]:
            if config_val:
                payload[key] = config_val

        # Add global cephfs_tool config options to payload, overriding with tool-specific if present
        for key in [
            "executable_path",
            "ceph_args",
            "config_path",
            "keyring",
            "client_id",
            "root_path",
            "duration",
            "msgr_workers",
        ]:
            if key in cfg:
                payload[key] = cfg[key]

        lockstat_cfg = cfg.get("lockstat", {})
        if lockstat_cfg.get("enabled", False):
            payload["cephfs_tool_lockstat_enabled"] = True
            payload["cephfs_tool_lockstat_asok"] = lockstat_cfg.get(
                "asok", "/var/run/ceph/cephfs-tool.asok"
            )
            payload["cephfs_tool_lockstat_path"] = lockstat_cfg.get(
                "path", "/usr/local/bin/ceph-lockstat"
            )

        settings_json = json.dumps(payload)
        loadpoints_json = json.dumps(loadpoints)
        clients_json = json.dumps(self.config.clients)

        print(f"Running CephFS-Tool Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        
        # To avoid "Argument list too long" errors when loadpoints JSON is very large,
        # we write JSON data to temporary files and pass file paths with @ prefix
        import base64
        
        # Create temporary file paths on remote host
        tmp_settings = f"/tmp/cephfs_settings_{os.getpid()}.json"
        tmp_loadpoints = f"/tmp/cephfs_loadpoints_{os.getpid()}.json"
        tmp_clients = f"/tmp/cephfs_clients_{os.getpid()}.json"
        
        # Write JSON files to remote host using base64 encoding to avoid shell escaping issues
        settings_b64 = base64.b64encode(settings_json.encode()).decode()
        loadpoints_b64 = base64.b64encode(loadpoints_json.encode()).decode()
        clients_b64 = base64.b64encode(clients_json.encode()).decode()
        
        setup_cmd = (
            f"echo '{settings_b64}' | base64 -d > {tmp_settings} && "
            f"echo '{loadpoints_b64}' | base64 -d > {tmp_loadpoints} && "
            f"echo '{clients_b64}' | base64 -d > {tmp_clients}"
        )
        
        # Execute setup command to create temp files
        self.executor.run_remote(self.admin, setup_cmd)
        
        # Now run the workload with file paths (@ prefix tells script to read from file)
        full_cmd = (
            f"python3 {run_cmd} "
            f"--settings '@{tmp_settings}' "
            f"--loadpoints '@{tmp_loadpoints}' "
            f"--clients '@{tmp_clients}' "
            f"--runner-name '{self.get_name()}'; "
            f"rm -f {tmp_settings} {tmp_loadpoints} {tmp_clients}"
        )
        
        print(f"[{self.admin}] Executing workload with temp files...")
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-p",
            port,
            f"{user}@{host}",
            "bash -s",
        ]

        current_lp, run_phase_started = 0, False
        perf_triggered = False
        lockstat_cfg = self.config.get("cephfs_tool", {}).get("lockstat", {})
        cephfs_lockstat_enabled = lockstat_cfg.get("enabled", False)
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        lockstat_asok = lockstat_cfg.get("asok", "/var/run/ceph/cephfs-tool.asok")
        lockstat_started = False
        write_lockstat_dumped = False
        read_lockstat_dumped = False
        current_phase = None
        perf_threads = []
        lockstat_data = {}

        process = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        # Send the command to stdin and close it
        process.stdin.write(full_cmd + "\n")
        process.stdin.close()

        output = []
        for line in process.stdout:
            print(f"[{self.admin}] {line}", end="")
            output.append(line)
            if "Starting tests..." in line:
                current_lp += 1
                run_phase_started, perf_triggered = False, False
                current_phase = None
                write_lockstat_dumped = False
                read_lockstat_dumped = False
                lockstat_data = {}
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True
                if cephfs_lockstat_enabled and not lockstat_started:
                    print(f"Starting cephfs-tool lockstat for Load Point {current_lp}...")
                    self._start_client_lockstat(self.config.clients, lockstat_path, lockstat_asok)
                    lockstat_started = True

            if run_phase_started and not perf_triggered:
                if perf_record_enabled:
                    print(f"Triggering perf recording for Load Point {current_lp}...")
                    lp_cfg = loadpoints[current_lp - 1]
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(
                            "cephfs_tool",
                            self.config.clients,
                            current_lp,
                            results_dir,
                            payload,
                            lp_cfg,
                        ),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True

            if "Starting WRITE phase" in line:
                if cephfs_lockstat_enabled:
                    if not lockstat_started:
                        print(
                            f"Starting cephfs-tool lockstat for Load Point {current_lp}..."
                        )
                        self._start_client_lockstat(
                            self.config.clients, lockstat_path, lockstat_asok
                        )
                        lockstat_started = True
                    else:
                        print(
                            f"Resetting lockstat for Write Phase, Load Point {current_lp}..."
                        )
                        self._reset_client_lockstat(self.config.clients, lockstat_path, lockstat_asok)
                current_phase = "write"
            if "Starting READ phase" in line:
                if cephfs_lockstat_enabled:
                    if not lockstat_started:
                        print(
                            f"Starting cephfs-tool lockstat for Load Point {current_lp}..."
                        )
                        self._start_client_lockstat(
                            self.config.clients, lockstat_path, lockstat_asok
                        )
                        lockstat_started = True
                    else:
                        print(f"Resetting lockstat for Read Phase, Load Point {current_lp}...")
                        self._reset_client_lockstat(self.config.clients, lockstat_path, lockstat_asok)
                current_phase = "read"
            if cephfs_lockstat_enabled and not write_lockstat_dumped and "Write:" in line and "MiB/s" in line:
                print(f"Dumping lockstat for Write Phase, Load Point {current_lp}...")
                phase_data = self._dump_client_lockstat(
                    self.config.clients, lockstat_asok, "write",
                )
                for client, data in phase_data.items():
                    lockstat_data.setdefault(client, {})["write"] = data
                write_lockstat_dumped = True
            if cephfs_lockstat_enabled and not read_lockstat_dumped and "Read:" in line and "MiB/s" in line:
                print(f"Dumping lockstat for Read Phase, Load Point {current_lp}...")
                phase_data = self._dump_client_lockstat(
                    self.config.clients, lockstat_asok, "read",
                )
                for client, data in phase_data.items():
                    lockstat_data.setdefault(client, {})["read"] = data
                read_lockstat_dumped = True
            if "Finished CephFS-Tool Load Point:" in line:
                if cephfs_lockstat_enabled and lockstat_data:
                    self._inject_lockstat_into_results(
                        results_dir, lockstat_data, current_lp,
                        payload, loadpoints[current_lp - 1],
                    )
                run_phase_started = False
                lockstat_started = False

        process.wait()
        for t in perf_threads:
            t.join()

        if process.returncode != 0:
            raise RuntimeError(f"CephFS-Tool failed on {self.admin} with return code {process.returncode}")

        return "".join(output)

    def _start_client_lockstat(self, clients, lockstat_path, asok_path):
        for client in clients:
            print(f"[{client}] Starting cephfs-tool lockstat via {asok_path}...")
            self.executor.run_remote(
                client, f"{lockstat_path} {asok_path} start"
            )

    def _reset_client_lockstat(self, clients, lockstat_path, asok_path):
        for client in clients:
            print(f"[{client}] Resetting cephfs-tool lockstat via {asok_path}...")
            self.executor.run_remote(
                client, f"{lockstat_path} {asok_path} reset"
            )

    def _dump_client_lockstat(self, clients, asok_path, phase):
        results = {}
        for client in clients:
            print(f"[{client}] Dumping cephfs-tool lockstat ({phase} phase) via {asok_path}...")
            dump_cmd = f"ceph --admin-daemon {asok_path} lockstat dump 2>&1"
            output = self.executor.run_remote(client, dump_cmd)
            try:
                results[client] = json.loads(output)
            except json.JSONDecodeError as e:
                print(f"[{client}] Failed to parse lockstat JSON: {e}")
                print(f"[{client}] lockstat dump output: {repr(output)}")
                results[client] = {"raw": output, "parse_error": str(e)}
        return results

    def _inject_lockstat_into_results(self, results_dir, lockstat_data, current_lp, payload, lp_cfg):
        import base64
        for client, phase_data in lockstat_data.items():
            json_filename = f"{CommonUtils.get_workload_base_name('cephfs_tool', 'result', client, current_lp, payload, lp_cfg)}.json"
            results_path = f"{results_dir}/{json_filename}"
            lockstat_b64 = base64.b64encode(json.dumps(phase_data).encode()).decode()
            inject_cmd = (
                f"python3 -c \""
                f"import json, base64; "
                f"f=open('{results_path}'); data=json.load(f); f.close(); "
                f"data['lockstat_results']=json.loads(base64.b64decode('{lockstat_b64}')); "
                f"f=open('{results_path}','w'); json.dump(data,f,indent=4); f.close()\""
            )
            print(f"[{client}] Injecting lockstat results into {results_path}...")
            self.executor.run_remote(self.admin, inject_cmd)

    def get_results_dir(self, settings, shared_ts=None):
        cfg = self.config.cephfs_tool
        base = cfg.get("results_base_dir", "/tmp/cephfs_tool_results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = (
            f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}"
        )
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        g_p = ""

        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}{g_p}")

    def get_name(self):
        return "cephfs_tool"

    def prepare_storage(self):
        cfg = self.config.get("cephfs_tool", {})
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/cephfs_tool/run_cephfs_workload.py"
        )
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")
        stap_script = cfg.get("stap_script")

        # Collect all targets to copy scripts to: admin, clients, ganeshas, mons, mdss
        targets = set(
            [self.admin]
            + self.config.clients
            + self.config.ganeshas
            + self.config.mons
            + self.config.mdss
        )

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            # Ensure the directory exists on each target
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(
                target, f"sudo mkdir -p {remote_dir} && sudo chown {u}:{u} {remote_dir}"
            )

            # Copy local files to each target
            files_to_copy = [
                ("lib/workload/run_cephfs_workload.py", run_cmd),
                ("perf_record.py", perf_script),
                ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
            ]

            if stap_script and os.path.exists(os.path.basename(stap_script)):
                files_to_copy.append((os.path.basename(stap_script), stap_script))

            for local_file, remote_path in files_to_copy:
                if os.path.exists(local_file):
                    print(f"Copying local {local_file} to {remote_path} on {target}...")
                    subprocess.run(
                        [
                            "scp",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-P",
                            str(p),
                            local_file,
                            f"{u}@{h}:{remote_path}",
                        ]
                    )
