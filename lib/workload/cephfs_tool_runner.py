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
    ):
        cfg = self.config.cephfs_tool
        loadpoints = cfg.get("loadpoints", [])
        loadpoints = CommonUtils.expand_loadpoints(loadpoints)
        run_cmd = cfg.get("run_command", "/cephfs_perf/cephfs_tool/run_cephfs_workload.py")
        perf_record_enabled = cfg.get("perf_record", False)
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = self.get_results_dir(settings, ts)

        # Create results directory on admin host
        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.provision_ganesha(results_dir=results_dir)

        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir

        # Add global cephfs_tool config options to payload
        for key in ["executable_path", "ceph_args", "config_path", "keyring", "client_id", "root_path", "duration"]:
            if key in cfg:
                payload[key] = cfg[key]

        settings_json = json.dumps(payload)
        loadpoints_json = json.dumps(loadpoints)
        clients_json = json.dumps(self.config.clients)

        print(f"Running CephFS-Tool Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        full_cmd = (
            f"python3 {run_cmd} "
            f"--settings '{settings_json}' "
            f"--loadpoints '{loadpoints_json}' "
            f"--clients '{clients_json}'"
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
            bufsize=1,
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
            if run_phase_started and not perf_triggered:
                if perf_record_enabled:
                    print(f"Triggering perf recording for Load Point {current_lp}...")
                    lp_cfg = loadpoints[current_lp - 1]
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(current_lp, results_dir, payload, lp_cfg),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True
            if "Finished CephFS-Tool Load Point:" in line:
                lp_cfg = loadpoints[current_lp - 1]
                for client in self.config.clients:
                    # Collect the cephfs-tool performance dump file
                    perf_dump_name = f"{CommonUtils.get_workload_base_name('cephfs_tool', 'perf_dump', client, current_lp, payload, lp_cfg)}.json"
                    perf_dump_json = f"/tmp/{perf_dump_name}"
                    try:
                        # Check if file exists first
                        check = self.executor.run_remote(client, f"test -f {perf_dump_json} && echo EXISTS || echo MISSING").strip()
                        if "EXISTS" in check:
                            au, ah, ap = self.executor.get_ssh_details(self.admin)
                            remote_path = f"{results_dir}/{perf_dump_name}"
                            self.executor.run_remote(client, f"sudo -n chmod 0644 {perf_dump_json}")
                            copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {perf_dump_json} {au}@{ah}:{remote_path}"
                            self.executor.run_remote(client, copy_cmd)
                            self.executor.run_remote(client, f"rm -f {perf_dump_json}")
                    except Exception as e:
                        print(f"[{client}] Warning: failed to collect {perf_dump_json}: {e}")

                    # Also collect the cephfs-tool JSON output file
                    tool_result_name = f"{CommonUtils.get_workload_base_name('cephfs_tool', 'result', client, current_lp, payload, lp_cfg)}.json"
                    tool_json = f"/tmp/{tool_result_name}"
                    try:
                        # Check if file exists first
                        check = self.executor.run_remote(client, f"test -f {tool_json} && echo EXISTS || echo MISSING").strip()
                        if "EXISTS" in check:
                            au, ah, ap = self.executor.get_ssh_details(self.admin)
                            remote_path = f"{results_dir}/{tool_result_name}"
                            self.executor.run_remote(client, f"sudo -n chmod 0644 {tool_json}")
                            copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {tool_json} {au}@{ah}:{remote_path}"
                            self.executor.run_remote(client, copy_cmd)
                            self.executor.run_remote(client, f"rm -f {tool_json}")
                    except Exception as e:
                        print(f"[{client}] Warning: failed to collect {tool_json}: {e}")

        process.wait()
        for t in perf_threads:
            t.join()

        return "".join(output)

    def get_results_dir(self, settings, shared_ts=None):
        cfg = self.config.cephfs_tool
        base = cfg.get("results_base_dir", "/tmp/cephfs_tool_results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}"
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}")

    def prepare_storage(self):
        cfg = self.config.get("cephfs_tool", {})
        run_cmd = cfg.get("run_command", "/cephfs_perf/cephfs_tool/run_cephfs_workload.py")
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/sfs2020/perf_record.py")
        stap_script = cfg.get("stap_script")

        # Collect all targets to copy scripts to: admin, clients, ganeshas, mons, mdss
        targets = set([self.admin] + self.config.clients + self.config.ganeshas + self.config.mons + self.config.mdss)

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            # Ensure the directory exists on each target
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(target, f"sudo mkdir -p {remote_dir} && sudo chown {u}:{u} {remote_dir}")

            # Copy local files to each target
            files_to_copy = [
                ("run_cephfs_workload.py", run_cmd),
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
                            p,
                            local_file,
                            f"{u}@{h}:{remote_path}",
                        ]
                    )

    def execute_perf_record(self, loadpoint, results_dir=None, settings=None, lp_cfg=None):
        cfg = self.config.get("cephfs_tool", {})
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/sfs2020/perf_record.py")
        perf_exe = cfg.get("perf_record_executable", "ceph-mds")
        perf_dur = cfg.get("perf_record_duration", 5)
        fg_path = cfg.get("perf_record_flamegraph_path", "/cephfs_perf/FlameGraph")
        stap_script = cfg.get("stap_script", "")
        processes = []

        client_perf_exe = cfg.get("perf_record_executable", "cephfs-tool")
        client_record_nodes = self.config.clients

        # Construct options string for filename
        options_str = ""
        if settings and lp_cfg:
            # We want just the options part, not the whole workload_result_...
            # get_workload_base_name returns workload_type_client_lp_options
            # We can extract options by splitting and taking everything after lpXX
            full_base = CommonUtils.get_workload_base_name('cephfs_tool', 'perf_record', 'client', loadpoint, settings, lp_cfg)
            # Find the index of lpXX_ and take what's after it
            lp_tag = f"lp{int(loadpoint):02d}_"
            idx = full_base.find(lp_tag)
            if idx != -1:
                options_str = full_base[idx + len(lp_tag):]

        for server_name in client_record_nodes:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            stap_arg = f" --stap-script {stap_script}" if stap_script else ""
            opt_arg = f" --options {options_str}" if options_str else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", p, f"{u}@{h}",
                       f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {client_perf_exe} --duration {perf_dur} --workload cephfs_tool{opt_arg}{fg_arg}{stap_arg}"]
            print(f"[{server_name}] Executing perf record for Load Point {loadpoint}: {subprocess.list2cmdline(ssh_cmd)}")
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
            for s_name in list(client_record_nodes):
                check_cmd = f"ls /tmp/cephfs_tool_perf_record_{s_name}_lp{int(loadpoint):02d}_* 2>/dev/null"
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except:
                    print(f"[{s_name}] No trace/report files found for Load Point {loadpoint}, skipping copy.")
