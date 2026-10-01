import base64
import datetime
import json
import os
import subprocess
import threading
from lib.workload.workload_runner import WorkloadRunner
from cephfs_perf_lib import CommonUtils


class MdsBenchWorkloadRunner(WorkloadRunner):
    """Drive ``cephfs-mdsbench`` with SpecStorage-like loadpoint scaling.

    Each loadpoint is an integer business metric ``N``: launch ``N`` processes
    with ``--clients N --client-id K`` (barrier on) distributed across inventory
    client hosts. Aggregate ``--ops-per-sec`` is split evenly across processes.
    """

    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
        results_dir=None,
        mount_manager=None,
        rgw_manager=None,
    ):
        cfg = self.config.mdsbench
        loadpoints = self._normalize_loadpoints(cfg.get("loadpoints", []))
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/mdsbench/run_mdsbench_workload.py"
        )
        perf_record_enabled = cfg.get("perf_record", False)
        mds_perf_record_enabled = bool(
            cephfs_manager and cephfs_manager.is_mds_perf_record_enabled()
        )
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = results_dir or self.get_results_dir(settings, ts)

        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir

        for key, config_val in [
            ("config_path", self.config.ceph_conf_path),
            ("keyring", self.config.ceph_keyring_path),
            ("client_id", self.config.ceph_user_id),
        ]:
            if config_val:
                payload[key] = config_val

        for key in [
            "executable_path",
            "config_path",
            "keyring",
            "client_id",
            "root_path",
            "workdir",
            "mode",
            "ops_per_sec",
            "duration",
            "warmup",
            "seed",
            "write_bytes",
            "scratch_slots",
            "init_file_bytes",
            "init_fill_percent",
            "init_threads",
            "keep_tree",
            "skip_precreate",
            "barrier_timeout",
            "progress",
            "progress_interval",
            "extra_args",
        ]:
            if key in cfg:
                payload[key] = cfg[key]
        payload["env_vars"] = self.config.get_merged_env_vars(cfg.get("env_vars"))

        settings_json = json.dumps(payload)
        loadpoints_json = json.dumps(loadpoints)
        clients_json = json.dumps(self.config.clients)

        print(f"Running MDS-Bench Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)

        tmp_settings = f"/tmp/mdsbench_settings_{os.getpid()}.json"
        tmp_loadpoints = f"/tmp/mdsbench_loadpoints_{os.getpid()}.json"
        tmp_clients = f"/tmp/mdsbench_clients_{os.getpid()}.json"

        settings_b64 = base64.b64encode(settings_json.encode()).decode()
        loadpoints_b64 = base64.b64encode(loadpoints_json.encode()).decode()
        clients_b64 = base64.b64encode(clients_json.encode()).decode()

        setup_cmd = (
            f"echo '{settings_b64}' | base64 -d > {tmp_settings} && "
            f"echo '{loadpoints_b64}' | base64 -d > {tmp_loadpoints} && "
            f"echo '{clients_b64}' | base64 -d > {tmp_clients}"
        )
        self.executor.run_remote(self.admin, setup_cmd)

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
        perf_triggered, logging_triggered = False, False
        perf_threads = []

        process = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        process.stdin.write(full_cmd + "\n")
        process.stdin.close()

        output = []
        for line in process.stdout:
            print(f"[{self.admin}] {line}", end="")
            output.append(line)
            if "Starting tests..." in line:
                current_lp += 1
                run_phase_started, perf_triggered, logging_triggered = (
                    False,
                    False,
                    False,
                )
                total_lps = len(loadpoints)
                pct = int(100 * current_lp / total_lps) if total_lps else 0
                print(
                    f"Detected Starting tests... Load Point: "
                    f"{current_lp}/{total_lps} ({pct}% done)"
                )
            if "Starting RUN phase" in line:
                run_phase_started = True
                if cephfs_manager and cephfs_manager.is_mds_lockstat_enabled():
                    print(f"Resetting MDS lockstat for Load Point {current_lp}...")
                    cephfs_manager.reset_lockstat()
                if cephfs_manager:
                    print(
                        f"Resetting MDS perf counters for Load Point {current_lp}..."
                    )
                    cephfs_manager.reset_perf_counters()
                if cephfs_manager:
                    cephfs_manager.start_periodic_perf_dump(
                        current_lp, results_dir
                    )
                if (
                    cephfs_manager
                    and cephfs_manager.is_mds_logging_enabled()
                    and not logging_triggered
                ):
                    print(f"Triggering MDS logging for Load Point {current_lp}...")
                    cephfs_manager.start_fs_logging(current_lp)
                    logging_triggered = True

            if run_phase_started and not perf_triggered:
                lp_cfg = loadpoints[current_lp - 1]
                if perf_record_enabled:
                    print(
                        f"Triggering client-side perf recording for Load Point {current_lp}..."
                    )
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(
                            "mdsbench",
                            self.config.clients,
                            current_lp,
                            results_dir,
                            payload,
                            lp_cfg,
                        ),
                    )
                    t.start()
                    perf_threads.append(t)
                if mds_perf_record_enabled:
                    print(
                        f"Triggering MDS perf recording for Load Point {current_lp}..."
                    )
                    t = threading.Thread(
                        target=self.execute_mds_perf_record,
                        args=(current_lp, results_dir, payload, lp_cfg),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True

            if "Finished MDS-Bench Load Point:" in line:
                if cephfs_manager:
                    cephfs_manager.stop_periodic_perf_dump()
                if cephfs_manager and cephfs_manager.is_mds_lockstat_enabled():
                    print(f"Dumping MDS lockstat for Load Point {current_lp}...")
                    cephfs_manager.dump_lockstat(
                        current_lp,
                        results_dir,
                        settings=payload,
                        lp_cfg=loadpoints[current_lp - 1],
                    )
                if cephfs_manager and results_dir:
                    print(
                        f"Dumping MDS perf counters for Load Point {current_lp}..."
                    )
                    cephfs_manager.dump_perf_counters(
                        current_lp,
                        results_dir,
                        settings=payload,
                        lp_cfg=loadpoints[current_lp - 1],
                    )
                if logging_triggered and cephfs_manager:
                    print(f"Stopping MDS logging for Load Point {current_lp}...")
                    cephfs_manager.stop_fs_logging(current_lp, results_dir)
                run_phase_started = False

        process.wait()
        if cephfs_manager:
            cephfs_manager.stop_periodic_perf_dump()
        for t in perf_threads:
            t.join()

        if process.returncode != 0:
            raise RuntimeError(
                f"MDS-Bench failed on {self.admin} with return code {process.returncode}"
            )

        return "".join(output)

    @staticmethod
    def _normalize_loadpoints(loadpoints):
        """Accept SpecStorage-style ints or dicts with a ``clients`` key."""
        if loadpoints is None:
            return []
        if isinstance(loadpoints, (int, float)):
            return [{"clients": int(loadpoints)}]
        normalized = []
        for lp in loadpoints:
            if isinstance(lp, (int, float)):
                normalized.append({"clients": int(lp)})
            elif isinstance(lp, dict):
                if "clients" not in lp:
                    raise ValueError(
                        f"mdsbench loadpoint dict must include 'clients': {lp}"
                    )
                normalized.append(dict(lp))
            else:
                raise ValueError(
                    f"mdsbench loadpoint must be int or dict, got {type(lp)}: {lp}"
                )
        return normalized

    def get_results_dir(self, settings, shared_ts=None):
        cfg = self.config.mdsbench
        base = cfg.get("results_base_dir", "/tmp/mdsbench_results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = (
            f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}"
        )
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        return os.path.join(
            base,
            f"{ts}_{self.get_name()}_{fs_p}_{mds_p}{CommonUtils.mount_name_suffix(self.config)}",
        )

    def get_name(self):
        return "mdsbench"

    def prepare_storage(self):
        cfg = self.config.get("mdsbench", {})
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/mdsbench/run_mdsbench_workload.py"
        )
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")
        stap_script = cfg.get("stap_script")

        targets = set(
            [self.admin]
            + self.config.clients
            + self.config.ganeshas
            + self.config.mons
            + self.config.mdss
        )

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(
                target, f"sudo mkdir -p {remote_dir} && sudo chown {u}:{u} {remote_dir}"
            )

            files_to_copy = [
                ("lib/workload/run_mdsbench_workload.py", run_cmd),
                ("perf_record.py", perf_script),
                ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
                (
                    "mds_perf_dump_collector.py",
                    "/cephfs_perf/mds_perf_dump_collector.py",
                ),
            ]

            if stap_script and os.path.exists(os.path.basename(stap_script)):
                files_to_copy.append((os.path.basename(stap_script), stap_script))

            self.executor.run_remote(
                target, f"sudo mkdir -p /cephfs_perf && sudo chown {u}:{u} /cephfs_perf"
            )
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
