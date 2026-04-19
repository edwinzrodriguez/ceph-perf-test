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
        run_cmd = fio_cfg.get("run_command", "/cephfs_perf/sfs2020/run_fio_workload.py")
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
                mount_points.append(f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else ""))

        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir

        # Add global fio settings to payload
        for key in ["gtod_reduce", "ramp_time"]:
            if key in fio_cfg:
                payload[key] = fio_cfg[key]

        # Add Ganesha settings to payload if enabled
        if self.config.ganesha_enabled:
            ganesha_keys = [
                "ganesha_worker_threads", "ganesha_umask", "ganesha_client_oc",
                "ganesha_async", "ganesha_zerocopy", "ganesha_client_oc_size"
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
        full_cmd = (
            f"python3 {run_cmd} "
            f"--settings '{settings_json}' "
            f"--mount-points '{mount_points_json}' "
            f"--clients '{clients_json}' "
            f"--loadpoints '{loadpoints_json}'"
        )
        print(f"[{self.admin}] Executing: {full_cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, f"{user}@{host}", full_cmd]

        current_lp, run_phase_started = 0, False
        perf_triggered = False
        ganesha_perf_enabled = self.config.ganesha_enabled and ganesha_manager
        perf_threads = []

        process = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            stdin=subprocess.DEVNULL,
        )

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
                    print(f"Resetting Ganesha perf counters for Load Point {current_lp}...")
                    for g_host in ganesha_manager.ganeshas:
                        ganesha_manager.reset_ganesha_perf(g_host)
            if run_phase_started and not perf_triggered:
                if perf_record_enabled and ganesha_manager:
                    print(f"Triggering Ganesha perf recording for Load Point {current_lp}...")
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(current_lp, results_dir, settings, expanded_loadpoints[current_lp-1]),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True
            if "Finished Fio Load Point:" in line:
                if ganesha_perf_enabled:
                    print(f"Dumping Ganesha perf counters for Load Point {current_lp}...")
                    for g_host in ganesha_manager.ganeshas:
                        perf_dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                        if perf_dump:
                            filename = f"ganesha_perf_dump_{g_host}_lp{current_lp:02d}.json"
                            local_temp = f"/tmp/{filename}"
                            with open(local_temp, "w") as f:
                                json.dump(perf_dump, f)
                            u, h, p = self.executor.get_ssh_details(self.admin)
                            p = str(p)
                            remote_path = f"{results_dir}/{filename}"
                            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", p, local_temp,
                                            f"{u}@{h}:{remote_path}"])
                            os.remove(local_temp)

        process.wait()
        for t in perf_threads:
            t.join()

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
            # Dynamically determine ganesha manager to call get_ganesha_config_str
            from lib.ganesha.ganesha_systemd_manager import GaneshaSystemdManager
            from lib.ganesha.ganesha_cephadm_manager import GaneshaCephadmManager
            if self.config.ganesha_type == "systemd":
                gm = GaneshaSystemdManager(self.executor, self.config)
            else:
                gm = GaneshaCephadmManager(self.executor, self.config)
            g_str = gm.get_ganesha_config_str(self.config.get("ganesha", {}))
            if g_str:
                g_p = "_" + g_str

        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}{g_p}")

    def prepare_storage(self):
        fio_cfg = self.config.get("fio", {})
        run_cmd = fio_cfg.get("run_command", "/cephfs_perf/sfs2020/run_fio_workload.py")
        perf_script = fio_cfg.get("perf_record_script", "/cephfs_perf/sfs2020/perf_record.py")
        stap_script = fio_cfg.get("stap_script")

        # Collect all targets to copy scripts to: admin, clients, ganeshas, mons, mdss
        targets = set([self.admin] + self.config.clients + self.config.ganeshas + self.config.mons + self.config.mdss)

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            # Ensure the directory exists on each target
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(target, f"sudo mkdir -p {remote_dir} && sudo chown {u}:{u} {remote_dir}")

            # Copy local files to each target
            files_to_copy = [
                ("run_fio_workload.py", run_cmd),
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

    def execute_perf_record(self, loadpoint, results_dir=None, settings=None, lp_cfg=None):
        fio_cfg = self.config.get("fio", {})
        perf_script = fio_cfg.get("perf_record_script", "/cephfs_perf/sfs2020/perf_record.py")
        perf_exe = fio_cfg.get("perf_record_executable", "ganesha.nfsd")
        perf_dur = fio_cfg.get("perf_record_duration", 5)
        fg_path = fio_cfg.get("perf_record_flamegraph_path", "/cephfs_perf/FlameGraph")
        stap_script = fio_cfg.get("stap_script", "")
        processes = []
        ganeshas = self.config.ganeshas

        options_str = ""
        if settings:
            full_base = CommonUtils.get_workload_base_name('fio', 'perf_record', 'server', loadpoint, settings, lp_cfg=lp_cfg, config=self.config)
            lp_tag = f"lp{int(loadpoint):02d}_"
            idx = full_base.find(lp_tag)
            if idx != -1:
                options_str = full_base[idx + len(lp_tag):]

        for server_name in ganeshas:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            stap_arg = f" --stap-script {stap_script}" if stap_script else ""
            opt_arg = f" --options {options_str}" if options_str else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            p = str(p)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", p, f"{u}@{h}",
                       f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_exe} --duration {perf_dur} --workload fio{opt_arg}{fg_arg}{stap_arg}"]
            print(f"[{server_name}] Executing perf record for Load Point {loadpoint}: {ssh_cmd}")
            proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False,
                                    stdin=subprocess.DEVNULL)
            processes.append((server_name, proc))

        def collect_output(s_name, p):
            out_bytes, _ = p.communicate()
            out = out_bytes.decode("utf-8", errors="replace")
            if p.returncode != 0:
                print(f"Error on {s_name} during perf record: {out}")
            else:
                if out_bytes:
                    print(f"[{s_name}] Output:\n{out}")
                print(f"[{s_name}] Finished perf record for Load Point {loadpoint}.")

        threads = []
        for s_name, proc in processes:
            t = threading.Thread(target=collect_output, args=(s_name, proc))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if results_dir:
            print(f"Copying perf reports and stap traces to {results_dir} on {self.admin}...")
            au, ah, ap = self.executor.get_ssh_details(self.admin)
            for s_name in ganeshas:
                check_cmd = f"ls /tmp/fio_perf_record_{s_name}_lp{int(loadpoint):02d}_* 2>/dev/null"
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except:
                    print(f"[{s_name}] No trace/report files found for Load Point {loadpoint}, skipping copy.")
