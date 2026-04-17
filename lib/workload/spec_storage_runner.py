import datetime
import json
import os
import subprocess
import threading
import yaml
from lib.workload.workload_runner import WorkloadRunner
from cephfs_perf_lib import CommonUtils


class SpecStorageWorkloadRunner(WorkloadRunner):
    def run_workload(
            self,
            settings,
            shared_ts=None,
            cephfs_manager=None,
            ganesha_manager=None,
            results_dir=None,
    ):
        cmd = self.config["specstorage"]["run_command"]
        cfg = self.config["specstorage"]["output_path"]
        workload_dir = self.config["specstorage"].get("workload_dir")
        perf_record_enabled = self.config["specstorage"].get("perf_record", False)
        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["num_filesystems"] = self.config.num_filesystems
        if workload_dir:
            payload["workload_dir"] = workload_dir
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_part = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}-m{self.config.get('specstorage', {}).get('mounts_per_fs', 1)}"
        payload["run_name"] = f"{ts}_{fs_part}"
        results_dir = results_dir or self.get_results_dir(settings, ts)
        if results_dir:
            payload["results_dir"] = results_dir
        settings_json = json.dumps(payload)
        print(f"Running SPECSTORAGE on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        full_cmd = f"{cmd} -f {cfg} --settings '{settings_json}'"
        print(f"[{self.admin}] Executing: {full_cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, f"{user}@{host}", full_cmd]
        current_lp, run_phase_started = 0, False
        perf_triggered, logging_triggered = False, False
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
                run_phase_started, perf_triggered, logging_triggered = False, False, False
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True
                if self.config.get("specstorage", {}).get("lockstat", {}).get("enabled") and cephfs_manager:
                    print(f"Resetting lockstat for Load Point {current_lp}...")
                    cephfs_manager.reset_lockstat()
                if self.config.get("logging", {}).get("enabled") and not logging_triggered and cephfs_manager:
                    print(f"Triggering MDS logging for Load Point {current_lp}...")
                    cephfs_manager.start_mds_logging(current_lp)
                    logging_triggered = True
                if ganesha_perf_enabled:
                    print(f"Resetting Ganesha perf counters for Load Point {current_lp}...")
                    for g_host in self.config.ganeshas:
                        ganesha_manager.reset_ganesha_perf(g_host)
            if "Tests finished" in line:
                r_dir = payload.get("results_dir")
                if self.config.get("specstorage", {}).get("lockstat", {}).get("enabled") and cephfs_manager:
                    print(f"Dumping lockstat for Load Point {current_lp}...")
                    cephfs_manager.dump_lockstat(current_lp, r_dir)
                if logging_triggered and cephfs_manager:
                    print(f"Stopping MDS logging for Load Point {current_lp}...")
                    cephfs_manager.stop_mds_logging(current_lp, r_dir)
                if ganesha_perf_enabled and r_dir:
                    print(f"Collecting Ganesha perf dumps for Load Point {current_lp}...")
                    lp_tag = f"{int(current_lp):02d}"
                    for g_host in self.config.ganeshas:
                        dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                        if dump:
                            self.save_json_to_results(f"{g_host}_lp{lp_tag}_ganesha_perf.json", dump, r_dir)
            if perf_record_enabled and run_phase_started and not perf_triggered:
                if "Run " in line and " percent complete" in line:
                    print(f"Triggering perf record for Load Point {current_lp}...")
                    r_dir = payload.get("results_dir")
                    t = threading.Thread(target=self.execute_perf_record, args=(current_lp, r_dir, settings, None))
                    t.start()
                    perf_threads.append(t)
                    perf_triggered = True
        process.wait()
        for t in perf_threads:
            t.join()
        if process.returncode != 0:
            print(f"Error on {self.admin}: process exited with {process.returncode}")
        return "".join(output)

    def execute_perf_record(self, loadpoint, results_dir=None, settings=None, lp_cfg=None):
        perf_script = self.config.get("specstorage", {}).get("perf_record_script",
                                                             "/cephfs_perf/sfs2020/perf_record.py")
        perf_exe = self.config.get("specstorage", {}).get("perf_record_executable", "ceph-mds")
        perf_dur = self.config.get("specstorage", {}).get("perf_record_duration", 5)
        fg_path = self.config.get("specstorage", {}).get("perf_record_flamegraph_path", "")
        stap_script = self.config.get("specstorage", {}).get("stap_script", "")
        processes = []

        options_str = ""
        if settings:
            full_base = CommonUtils.get_workload_base_name('sfs2020', 'perf_record', 'server', loadpoint, settings, lp_cfg, self.config)
            lp_tag = f"lp{int(loadpoint):02d}_"
            idx = full_base.find(lp_tag)
            if idx != -1:
                options_str = full_base[idx + len(lp_tag):]

        for server_name in self.config.mdss:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            stap_arg = f" --stap-script {stap_script}" if stap_script else ""
            opt_arg = f" --options {options_str}" if options_str else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", p, f"{u}@{h}",
                       f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_exe} --duration {perf_dur} --workload sfs2020{opt_arg}{fg_arg}{stap_arg}"]
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
            for s_name in self.config.mdss:
                check_cmd = f"ls /tmp/sfs2020_perf_record_{s_name}_lp{int(loadpoint):02d}_* 2>/dev/null"
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except:
                    print(f"[{s_name}] No trace/report files found for Load Point {loadpoint}, skipping copy.")

    def save_json_to_results(self, filename, data, results_dir):
        local_temp = f"/tmp/{filename}"
        with open(local_temp, "w") as f:
            json.dump(data, f, indent=4)
        u, h, p = self.executor.get_ssh_details(self.admin)
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", p, local_temp, f"{u}@{h}:{results_dir}/"])
        os.remove(local_temp)

    def prepare_storage(self):
        spec_cfg = self.config.get("specstorage", {})
        proto = spec_cfg["prototype"]
        out = spec_cfg["output_path"]
        run_cmd = spec_cfg.get("run_command", "/cephfs_perf/sfs2020/run_sfs2020_workload.py")
        perf_script = spec_cfg.get("perf_record_script", "/cephfs_perf/sfs2020/perf_record.py")

        u, h, p = self.executor.get_ssh_details(self.admin)

        # Copy local files to the remote machine
        remote_dir = os.path.dirname(run_cmd)
        files_to_copy = [
            ("sfs_rc", proto),
            ("run_workload.py", run_cmd),
            ("perf_record.py", perf_script),
            ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
        ]
        stap_script = spec_cfg.get("stap_script")
        if stap_script and os.path.exists(os.path.basename(stap_script)):
            files_to_copy.append((os.path.basename(stap_script), stap_script))

        for local_file, remote_path in files_to_copy:
            if os.path.exists(local_file):
                print(f"Copying local {local_file} to {remote_path} on {self.admin}...")
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

        mpfs = spec_cfg.get("mounts_per_fs", 1)
        mps = []
        for fs in self.fs_names:
            for i in range(mpfs):
                for c in self.config.clients:
                    mps.append(
                        f"{c}:/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    )
        content = (
                self.executor.run_remote(self.admin, f"cat {proto}")
                + f"\nCLIENT_MOUNTPOINTS={' '.join(mps)}\n"
        )
        with open("/tmp/spec_cfg", "w") as f:
            f.write(content)

        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                p,
                "/tmp/spec_cfg",
                f"{u}@{h}:{out}",
            ]
        )

    def get_results_dir(self, settings, shared_ts=None):
        spec_cfg = self.config.get("specstorage", {})
        base = spec_cfg.get("results_base_dir", "/cephfs_perf/sfs2020/results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}-m{spec_cfg.get('mounts_per_fs', 1)}"
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
