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
        commands = cfg.get("commands", [])
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

        settings_json = json.dumps(payload)
        commands_json = json.dumps(commands)
        clients_json = json.dumps(self.config.clients)

        print(f"Running CephFS-Tool Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        full_cmd = (
            f"python3 {run_cmd} "
            f"--settings '{settings_json}' "
            f"--commands '{commands_json}' "
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
                if perf_record_enabled:
                    print(f"Triggering perf recording for Load Point {current_lp}...")
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(current_lp, results_dir),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True
            if "Finished CephFS-Tool Load Point:" in line:
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
                            remote_path = f"{results_dir}/{filename}"
                            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", p, local_temp,
                                            f"{u}@{h}:{remote_path}"])
                            os.remove(local_temp)

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

    def execute_perf_record(self, loadpoint, results_dir=None):
        cfg = self.config.get("cephfs_tool", {})
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/sfs2020/perf_record.py")
        perf_exe = cfg.get("perf_record_executable", "ceph-mds")
        perf_dur = cfg.get("perf_record_duration", 5)
        fg_path = cfg.get("perf_record_flamegraph_path", "/cephfs_perf/FlameGraph")
        stap_script = cfg.get("stap_script", "")
        processes = []

        # Determine which nodes to record on. If Ganesha is enabled, record on Ganeshas. Otherwise, record on MDSs.
        if self.config.ganesha_enabled:
            record_nodes = self.config.ganeshas
            if "ganesha" not in perf_exe.lower():
                 print(f"Warning: Ganesha is enabled but perf_record_executable is '{perf_exe}'. Recording on Ganesha nodes anyway.")
        else:
            record_nodes = self.config.mdss

        for server_name in record_nodes:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            stap_arg = f" --stap-script {stap_script}" if stap_script else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", p, f"{u}@{h}",
                       f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_exe} --duration {perf_dur}{fg_arg}{stap_arg}"]
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
            for s_name in record_nodes:
                check_cmd = f"ls /tmp/perf_report_{s_name}_lp{int(loadpoint):02d}.* /tmp/*_lp{int(loadpoint):02d}_*_stap_trace.txt 2>/dev/null"
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except:
                    print(f"[{s_name}] No trace/report files found for Load Point {loadpoint}, skipping copy.")
