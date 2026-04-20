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
        # Use CommonUtils.get_workload_base_name to form the run_name
        options = CommonUtils.get_workload_base_name('sfs2020', 'result', self.admin, 0, settings, config=self.config)
        # Remove the prefix part (workload_result_client_lp00_) to get just the options
        prefix = f"sfs2020_result_{self.admin}_lp00_"
        if options.startswith(prefix):
            options = options[len(prefix):]

        payload["run_name"] = f"{ts}_{options}"
        results_dir = results_dir or self.get_results_dir(settings, ts)
        if results_dir:
            payload["results_dir"] = results_dir

        # Add Ganesha settings to payload
        ganesha_keys = [
            "ganesha_enabled", "ganesha_worker_threads", "ganesha_umask", "ganesha_client_oc",
            "ganesha_async", "ganesha_zerocopy", "ganesha_client_oc_size",
            "ganesha_user_id", "ganesha_keyring_path", "ganesha_ceph_binary_path"
        ]
        for k in ganesha_keys:
            val = getattr(self.config, k, None)
            if val is not None:
                payload[k] = val

        settings_json = json.dumps(payload)
        print(f"Running SPECSTORAGE on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        full_cmd = f"{cmd} -f {cfg} --settings '{settings_json}'"
        print(f"[{self.admin}] Executing: {full_cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, f"{user}@{host}", full_cmd]
        current_lp, run_phase_started = 0, False
        perf_triggered, ganesha_perf_triggered, logging_triggered = False, False, False
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
                run_phase_started, perf_triggered, ganesha_perf_triggered, logging_triggered = False, False, False, False
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True
                if self.config.get("specstorage", {}).get("lockstat", {}).get("enabled") and cephfs_manager:
                    print(f"Resetting lockstat for Load Point {current_lp}...")
                    cephfs_manager.reset_lockstat()
                if self.config.get("logging", {}).get("enabled") and not logging_triggered and cephfs_manager:
                    print(f"Triggering MDS logging for Load Point {current_lp}...")
                    cephfs_manager.start_fs_logging(current_lp)
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
                    cephfs_manager.stop_fs_logging(current_lp, r_dir)
                if ganesha_perf_enabled and r_dir:
                    print(f"Collecting Ganesha perf dumps for Load Point {current_lp}...")
                    lp_tag = f"{int(current_lp):02d}"
                    for g_host in self.config.ganeshas:
                        dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                        if dump:
                            self.save_json_to_results(f"{g_host}_lp{lp_tag}_ganesha_perf.json", dump, r_dir)
            if run_phase_started:
                if perf_record_enabled and not perf_triggered:
                    if "Run " in line and " percent complete" in line:
                        print(f"Triggering perf record for Load Point {current_lp}...")
                        r_dir = payload.get("results_dir")
                        t = threading.Thread(target=self.execute_perf_record, args=("sfs2020", self.config.mdss, current_lp, r_dir, settings, None))
                        t.start()
                        perf_threads.append(t)
                        perf_triggered = True

                if self.config.ganesha_enabled and self.config.ganesha_perf_record and not ganesha_perf_triggered:
                    print(f"Triggering Ganesha perf recording for Load Point {current_lp}...")
                    r_dir = payload.get("results_dir")
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=("ganesha", self.config.ganeshas, current_lp, r_dir, settings, None),
                    )
                    t.start()
                    perf_threads.append(t)
                    ganesha_perf_triggered = True
        process.wait()
        for t in perf_threads:
            t.join()
        if process.returncode != 0:
            print(f"Error on {self.admin}: process exited with {process.returncode}")
        return "".join(output)


    def save_json_to_results(self, filename, data, results_dir):
        local_temp = f"/tmp/{filename}"
        with open(local_temp, "w") as f:
            json.dump(data, f, indent=4)
        u, h, p = self.executor.get_ssh_details(self.admin)
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", str(p), local_temp, f"{u}@{h}:{results_dir}/"])
        os.remove(local_temp)

    def prepare_storage(self):
        spec_cfg = self.config.get("specstorage", {})
        proto = spec_cfg["prototype"]
        out = spec_cfg["output_path"]
        run_cmd = spec_cfg.get("run_command", "/cephfs_perf/sfs2020/run_sfs2020_workload.py")
        perf_script = spec_cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")

        # Collect all targets to copy scripts to: admin, clients, ganeshas, mons, mdss
        targets = set([self.admin] + self.config.clients + self.config.ganeshas + self.config.mons + self.config.mdss)

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)

            # Copy local files to the remote machine
            remote_dir = os.path.dirname(run_cmd)
            files_to_copy = [
                ("sfs_rc", proto),
                ("lib/workload/run_sfs2020_workload.py", run_cmd),
                ("perf_record.py", perf_script),
                ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
            ]

            # Also copy ganesha perf record script if different
            g_cfg = self.config.get("ganesha", {})
            g_perf_script = g_cfg.get("perf_record_script")
            if g_perf_script and g_perf_script != perf_script:
                # Ensure the directory exists on each target
                g_remote_dir = os.path.dirname(g_perf_script)
                self.executor.run_remote(self.admin, f"sudo mkdir -p {g_remote_dir} && sudo chown {u}:{u} {g_remote_dir}")
                files_to_copy.append(("perf_record.py", g_perf_script))

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
                            str(p),
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
            from lib.ganesha.ganesha_manager import GaneshaManager
            g_str = GaneshaManager.get_ganesha_config_str(self.config.get("ganesha", {}))
            if g_str:
                g_p = "_" + g_str

        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}{g_p}")
