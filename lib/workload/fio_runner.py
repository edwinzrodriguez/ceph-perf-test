import base64
import datetime
import json
import os
import subprocess
import threading
from lib.workload.workload_runner import WorkloadRunner
from cephfs_perf_lib import CommonUtils


class FioWorkloadRunner(WorkloadRunner):
    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
        results_dir=None,
    ):
        fio_cfg = self.config.fio
        run_cmd = fio_cfg.get("run_command", "/cephfs_perf/fio/run_fio_workload.py")
        perf_record_enabled = fio_cfg.get("perf_record", False)
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = results_dir or self.get_results_dir(settings, ts)

        # Create results directory on admin host
        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.provision_ganesha(results_dir=results_dir)

        mpfs = self.config.get("fio", {}).get("mounts_per_fs", 1)
        mount_points = []
        for fs in self.fs_names:
            for i in range(mpfs):
                mount_points.append(
                    f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                )

        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir

        # Add global fio settings to payload
        for key in ["gtod_reduce", "ramp_time", "threads_fio"]:
            if key in fio_cfg:
                payload[key] = fio_cfg[key]

        # Add Ganesha settings to payload
        ganesha_keys = [
            "ganesha_enabled",
            "ganesha_worker_threads",
            "ganesha_umask",
            "ganesha_client_oc",
            "ganesha_async",
            "ganesha_zerocopy",
            "ganesha_client_oc_size",
            "ganesha_msgr_workers",
            "ganesha_rpc_ioq_thrdmin",
            "ganesha_rpc_ioq_thrdmax",
        ]
        for k in ganesha_keys:
            val = getattr(self.config, k, None)
            if val is not None:
                payload[k] = val

        loadpoints = fio_cfg.get("loadpoints", [])
        if isinstance(loadpoints, dict):
            loadpoints = [loadpoints]
        expanded_loadpoints = CommonUtils.expand_loadpoints(loadpoints)

        settings_json = json.dumps(payload)
        mount_points_json = json.dumps(mount_points)
        clients_json = json.dumps(self.config.clients)
        loadpoints_json = json.dumps(expanded_loadpoints)

        print(f"Running Fio Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        
        # To avoid "Argument list too long" errors when loadpoints JSON is very large,
        # we write JSON data to temporary files and pass file paths with @ prefix
        
        # Create temporary file paths on remote host
        tmp_settings = f"/tmp/fio_settings_{os.getpid()}.json"
        tmp_mount_points = f"/tmp/fio_mount_points_{os.getpid()}.json"
        tmp_clients = f"/tmp/fio_clients_{os.getpid()}.json"
        tmp_loadpoints = f"/tmp/fio_loadpoints_{os.getpid()}.json"
        
        # Write JSON files to remote host using base64 encoding to avoid shell escaping issues
        settings_b64 = base64.b64encode(settings_json.encode()).decode()
        mount_points_b64 = base64.b64encode(mount_points_json.encode()).decode()
        clients_b64 = base64.b64encode(clients_json.encode()).decode()
        loadpoints_b64 = base64.b64encode(loadpoints_json.encode()).decode()
        
        setup_cmd = (
            f"echo '{settings_b64}' | base64 -d > {tmp_settings} && "
            f"echo '{mount_points_b64}' | base64 -d > {tmp_mount_points} && "
            f"echo '{clients_b64}' | base64 -d > {tmp_clients} && "
            f"echo '{loadpoints_b64}' | base64 -d > {tmp_loadpoints}"
        )
        
        # Execute setup command to create temp files
        self.executor.run_remote(self.admin, setup_cmd)
        
        # Now run the workload with file paths (@ prefix tells script to read from file)
        full_cmd = (
            f"python3 {run_cmd} "
            f"--settings '@{tmp_settings}' "
            f"--mount-points '@{tmp_mount_points}' "
            f"--clients '@{tmp_clients}' "
            f"--loadpoints '@{tmp_loadpoints}' "
            f"--runner-name '{self.get_name()}'; "
            f"rm -f {tmp_settings} {tmp_mount_points} {tmp_clients} {tmp_loadpoints}"
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

        processes = []
        ganeshas = self.config.ganeshas

        current_lp, run_phase_started = 0, False
        perf_triggered = False
        ganesha_perf_enabled = self.config.ganesha_enabled and ganesha_manager
        perf_threads = []

        process = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
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
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True
                if ganesha_perf_enabled:
                    print(
                        f"Resetting Ganesha perf counters for Load Point {current_lp}..."
                    )
                    for g_host in ganesha_manager.ganeshas:
                        ganesha_manager.reset_ganesha_perf(g_host)
                        if self.config.get("ganesha", {}).get("lockstat", {}).get("enabled", False):
                            ganesha_manager.reset_lockstat(g_host)
            if run_phase_started and not perf_triggered:
                if perf_record_enabled:
                    print(f"Triggering perf recording for Load Point {current_lp}...")
                    lp_cfg = expanded_loadpoints[current_lp - 1]
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(
                            "fio",
                            self.config.mdss,
                            current_lp,
                            results_dir,
                            settings,
                            lp_cfg,
                        ),
                    )
                    t.start()
                    perf_threads.append(t)

                if self.config.ganesha_enabled and self.config.ganesha_perf_record:
                    print(
                        f"Triggering Ganesha perf recording for Load Point {current_lp}..."
                    )
                    lp_cfg = expanded_loadpoints[current_lp - 1]
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(
                            "ganesha",
                            self.config.ganeshas,
                            current_lp,
                            results_dir,
                            settings,
                            lp_cfg,
                        ),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True
            if "Finished Fio Load Point:" in line:
                if ganesha_perf_enabled:
                    print(
                        f"Dumping Ganesha perf counters for Load Point {current_lp}..."
                    )
                    ganesha_lockstat_enabled = self.config.get("ganesha", {}).get("lockstat", {}).get("enabled", False)
                    lockstat_results = {}
                    for g_host in ganesha_manager.ganeshas:
                        perf_dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                        if perf_dump:
                            filename = (
                                f"ganesha_perf_dump_{g_host}_lp{current_lp:02d}.json"
                            )
                            local_temp = f"/tmp/{filename}"
                            with open(local_temp, "w") as f:
                                json.dump(perf_dump, f)
                            u, h, p = self.executor.get_ssh_details(self.admin)
                            p = str(p)
                            remote_path = f"{results_dir}/{filename}"
                            subprocess.run(
                                [
                                    "scp",
                                    "-o",
                                    "StrictHostKeyChecking=no",
                                    "-P",
                                    p,
                                    local_temp,
                                    f"{u}@{h}:{remote_path}",
                                ]
                            )
                            os.remove(local_temp)
                        if ganesha_lockstat_enabled:
                            lockstat_json = ganesha_manager.dump_lockstat(g_host)
                            if lockstat_json:
                                lockstat_results[g_host] = lockstat_json
                    if lockstat_results:
                        lp_cfg = expanded_loadpoints[current_lp - 1]
                        lockstat_b64 = base64.b64encode(json.dumps(lockstat_results).encode()).decode()
                        for client in self.config.clients:
                            json_filename = f"{CommonUtils.get_workload_base_name('fio', 'result', client, current_lp, payload, lp_cfg)}.json"
                            results_path = f"{results_dir}/{json_filename}"
                            inject_cmd = (
                                f"python3 -c \""
                                f"import json, base64; "
                                f"f=open('{results_path}'); data=json.load(f); f.close(); "
                                f"data['lockstat_results']=json.loads(base64.b64decode('{lockstat_b64}')); "
                                f"f=open('{results_path}','w'); json.dump(data,f,indent=4); f.close()\""
                            )
                            print(f"[{client}] Injecting lockstat results into {results_path}...")
                            self.executor.run_remote(self.admin, inject_cmd)

        process.wait()
        for t in perf_threads:
            t.join()

        if process.returncode != 0:
            raise RuntimeError(f"Fio failed on {self.admin} with return code {process.returncode}")

        return "".join(output)

    def get_results_dir(self, settings, shared_ts=None):
        fio_cfg = self.config.fio
        base = fio_cfg.get("results_base_dir", "/tmp/fio_results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}-m{fio_cfg.get('mounts_per_fs', 1)}"
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        g_p = ""
        if self.config.ganesha_enabled:
            from lib.ganesha.ganesha_manager import GaneshaManager

            g_str = GaneshaManager.get_ganesha_config_str(
                self.config.get("ganesha", {})
            )
            if g_str:
                g_p = "_" + g_str

        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}{g_p}")

    def prepare_storage(self):
        fio_cfg = self.config.get("fio", {})
        run_cmd = fio_cfg.get("run_command", "/cephfs_perf/fio/run_fio_workload.py")
        perf_script = fio_cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")
        stap_script = fio_cfg.get("stap_script")

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
                ("lib/workload/run_fio_workload.py", run_cmd),
                ("perf_record.py", perf_script),
                ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
            ]

            # Also copy ganesha perf record script if different
            g_cfg = self.config.get("ganesha", {})
            g_perf_script = g_cfg.get("perf_record_script")
            if g_perf_script and g_perf_script != perf_script:
                # Ensure the directory exists on each target
                g_remote_dir = os.path.dirname(g_perf_script)
                self.executor.run_remote(
                    target,
                    f"sudo mkdir -p {g_remote_dir} && sudo chown {u}:{u} {g_remote_dir}",
                )
                files_to_copy.append(("perf_record.py", g_perf_script))

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

    def get_name(self):
        return "fio"
