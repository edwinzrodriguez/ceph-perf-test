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
        options = CommonUtils.get_workload_base_name(
            "sfs2020", "result", self.admin, 0, settings, config=self.config
        )
        # Remove the prefix part (workload_result_client_lp00_) to get just the options
        prefix = f"sfs2020_result_{self.admin}_lp00_"
        if options.startswith(prefix):
            options = options[len(prefix) :]

        payload["run_name"] = f"{ts}_{options}"
        results_dir = results_dir or self.get_results_dir(settings, ts)
        if results_dir:
            payload["results_dir"] = results_dir

        # Add Ganesha settings to payload
        ganesha_keys = [
            "ganesha_enabled",
            "ganesha_worker_threads",
            "ganesha_umask",
            "ganesha_client_oc",
            "ganesha_async",
            "ganesha_zerocopy",
            "ganesha_client_oc_size",
            "ganesha_user_id",
            "ganesha_keyring_path",
            "ganesha_ceph_binary_path",
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
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-p",
            port,
            f"{user}@{host}",
            full_cmd,
        ]
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
                (
                    run_phase_started,
                    perf_triggered,
                    ganesha_perf_triggered,
                    logging_triggered,
                ) = (False, False, False, False)
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True
                if (
                    self.config.get("specstorage", {})
                    .get("lockstat", {})
                    .get("enabled")
                    and cephfs_manager
                ):
                    print(f"Resetting lockstat for Load Point {current_lp}...")
                    cephfs_manager.reset_lockstat()
                if (
                    self.config.get("logging", {}).get("enabled")
                    and not logging_triggered
                    and cephfs_manager
                ):
                    print(f"Triggering MDS logging for Load Point {current_lp}...")
                    cephfs_manager.start_fs_logging(current_lp)
                    logging_triggered = True
                if ganesha_perf_enabled:
                    print(
                        f"Resetting Ganesha perf counters for Load Point {current_lp}..."
                    )
                    for g_host in self.config.ganeshas:
                        ganesha_manager.reset_ganesha_perf(g_host)
            if "Tests finished" in line:
                r_dir = payload.get("results_dir")
                if (
                    self.config.get("specstorage", {})
                    .get("lockstat", {})
                    .get("enabled")
                    and cephfs_manager
                ):
                    print(f"Dumping lockstat for Load Point {current_lp}...")
                    cephfs_manager.dump_lockstat(current_lp, r_dir)
                if logging_triggered and cephfs_manager:
                    print(f"Stopping MDS logging for Load Point {current_lp}...")
                    cephfs_manager.stop_fs_logging(current_lp, r_dir)
                if ganesha_perf_enabled and r_dir:
                    print(
                        f"Collecting Ganesha perf dumps for Load Point {current_lp}..."
                    )
                    lp_tag = f"{int(current_lp):02d}"
                    for g_host in self.config.ganeshas:
                        dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                        if dump:
                            self.save_json_to_results(
                                f"{g_host}_lp{lp_tag}_ganesha_perf.json", dump, r_dir
                            )
            if run_phase_started:
                if perf_record_enabled and not perf_triggered:
                    if "Run " in line and " percent complete" in line:
                        print(f"Triggering perf record for Load Point {current_lp}...")
                        r_dir = payload.get("results_dir")
                        t = threading.Thread(
                            target=self.execute_perf_record,
                            args=(
                                "sfs2020",
                                self.config.mdss,
                                current_lp,
                                r_dir,
                                settings,
                                None,
                            ),
                        )
                        t.start()
                        perf_threads.append(t)
                        perf_triggered = True

                if (
                    self.config.ganesha_enabled
                    and self.config.ganesha_perf_record
                    and not ganesha_perf_triggered
                ):
                    print(
                        f"Triggering Ganesha perf recording for Load Point {current_lp}..."
                    )
                    r_dir = payload.get("results_dir")
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(
                            "ganesha",
                            self.config.ganeshas,
                            current_lp,
                            r_dir,
                            settings,
                            None,
                        ),
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
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(p),
                local_temp,
                f"{u}@{h}:{results_dir}/",
            ]
        )
        os.remove(local_temp)

    def setup_sfs2020_on_target(
        self, target, remote_dir, sfs2020_archive, remote_archive_path
    ):
        """
        Copies the SPECstorage archive to the target, untars it, and runs SM2020 installation.
        Only performs setup if /SM2020 does not exist.
        """
        # Check if SPECstorage is already installed
        check_cmd = "test -d /SM2020"
        try:
            check_result = self.executor.run_remote(
                target, "[ -d /SM2020 ] && echo 'EXISTS' || echo 'MISSING'"
            )
            if "EXISTS" in check_result:
                print(
                    f"SPECStorage already installed on {target} (/SM2020 exists). Skipping setup."
                )
                return
        except Exception as e:
            print(
                f"Warning: Could not check for /SM2020 on {target}: {e}. Proceeding with setup."
            )

        u, h, p = self.executor.get_ssh_details(target)

        # Copy the archive
        print(f"Copying {sfs2020_archive} to {remote_archive_path} on {target}...")
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(p),
                sfs2020_archive,
                f"{u}@{h}:{remote_archive_path}",
            ],
            check=True,
        )

        # Untar the archive
        untar_cmd = f"tar -C {remote_dir} -xf {remote_archive_path}"
        if sfs2020_archive.endswith(".tgz") or sfs2020_archive.endswith(".tar.gz"):
            untar_cmd = f"tar -C {remote_dir} -zxf {remote_archive_path}"

        print(f"Untarring {remote_archive_path} on {target}...")
        self.executor.run_remote(target, untar_cmd)

        # Run pyupgrade on SM2020
        spec_dir = os.path.join(remote_dir, "SPECstorage2020")
        pyupgrade_cmd = f"pyupgrade --py3-plus {spec_dir}/SM2020"
        print(f"Running pyupgrade on {spec_dir}/SM2020 on {target}...")
        self.executor.run_remote(target, pyupgrade_cmd)

        # Run SM2020 installation
        # cd remote_dir/SPECstorage2020 && python3 SM2020 --install-dir=/SPEC2020
        install_cmd = f"cd {spec_dir} && python3 SM2020 --install-dir=/SM2020"
        print(f"Running SM2020 install on {target}...")
        self.executor.run_remote(target, install_cmd)

    def _parse_netmist_env(self):
        spec_cfg = self.config.get("specstorage", {})
        netmist_env_path = spec_cfg.get("netmist_env", "netmist.env")
        env_data = {}

        if not os.path.exists(netmist_env_path):
            return env_data

        try:
            with open(netmist_env_path, "r") as f:
                content = f.read()
            for line in content.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    env_data[k.strip()] = v.strip().strip('"').strip("'")
        except Exception as e:
            print(f"Warning: Could not read netmist_env from {netmist_env_path}: {e}")
        return env_data

    def _generate_spec_file(self):
        spec_cfg = self.config.get("specstorage", {})
        workload_dir = spec_cfg.get(
            "workload_dir", "/cephfs_perf/sfs2020/SPECstorage2020"
        )

        # Parse netmist_env
        env_data = self._parse_netmist_env()
        license_key = env_data.get("NETMIST_LICENSE_KEY", "")
        license_path = env_data.get("NETMIST_LICENSE_KEY_PATH", "")

        if not license_key or not license_path:
            netmist_env_path = spec_cfg.get("netmist_env", "netmist.env")
            raise RuntimeError(
                f"Error: NETMIST_LICENSE_KEY or NETMIST_LICENSE_KEY_PATH is not defined or empty in {netmist_env_path}"
            )

        # Construct LOAD entry from loadpoints
        loadpoints = spec_cfg.get("loadpoints", [])
        load_str = " ".join(map(str, loadpoints))

        # Construct CLIENT_MOUNTPOINTS
        mpfs = spec_cfg.get("mounts_per_fs", 1)
        mps = []
        for fs in self.fs_names:
            for i in range(mpfs):
                for c in self.config.clients:
                    mps.append(
                        f"{c}:/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    )
        mps_str = " ".join(mps)

        # Build final content from scratch
        lines = [
            "USER=root",
            "PASSWORD=",
            f"EXEC_PATH={workload_dir}/binaries/linux/x86_64/netmist",
            # f"NETMIST_LOGS={workload_dir}/test1",
            f"BENCHMARK={spec_cfg.get('benchmark', 'SWBUILD')}",
            f"LOAD={load_str}",
            f"INCR_LOAD={spec_cfg.get('increment', 1)}",
            f"NUM_RUNS={spec_cfg.get('num_runs', 1)}",
            f"CLIENT_MOUNTPOINTS={mps_str}",
            "IPV6_ENABLE=0",
            # "PRIME_MON_SCRIPT=",
            # "PRIME_MON_ARGS=",
            "NETMIST_LICENSE_KEY=" + license_key,
            "NETMIST_LICENSE_KEY_PATH=" + license_path,
            # "MAX_FD=",
            # "LOCAL_ONLY=0",
            # "FILE_ACCESS_LIST=0",
            # "PDSM_MODE=0",
            # "PDSM_INTERVAL=",
            # "UNIX_PDSM_LOG=",
            # "WINDOWS_PDSM_LOG=",
            # "UNIX_PDSM_CONTROL=",
            # "WINDOWS_PDSM_CONTROL=",
            "",
        ]

        return "\n".join(lines)

    def _setup_target(
        self,
        target,
        remote_dir,
        base_files_to_copy,
        g_perf_script,
        perf_script,
        skip_setup,
        sfs2020_archive,
        remote_archive_path,
    ):
        u, h, p = self.executor.get_ssh_details(target)

        # Ensure the remote directory exists
        self.executor.run_remote(
            target,
            f"sudo mkdir -p {remote_dir} && sudo chown {u}:{u} {remote_dir}",
        )

        # Ensure g_remote_dir exists if applicable
        if g_perf_script and g_perf_script != perf_script:
            g_remote_dir = os.path.dirname(g_perf_script)
            self.executor.run_remote(
                target,
                f"sudo mkdir -p {g_remote_dir} && sudo chown {u}:{u} {g_remote_dir}",
            )

        files_to_copy = list(base_files_to_copy)

        # If sfs2020_archive is valid and target is admin, setup SPECstorage
        if not skip_setup and sfs2020_archive:
            self.setup_sfs2020_on_target(
                target, remote_dir, sfs2020_archive, remote_archive_path
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

    def prepare_storage(self):
        spec_cfg = self.config.get("specstorage", {})
        out = spec_cfg["output_path"]
        run_cmd = spec_cfg.get(
            "run_command", "/cephfs_perf/sfs2020/run_sfs2020_workload.py"
        )
        perf_script = spec_cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")

        env_data = self._parse_netmist_env()
        sfs2020_archive = env_data.get("sfs2020_archive")
        skip_setup = False

        if not sfs2020_archive:
            print(
                "Warning: sfs2020_archive not found in netmist.env. Skipping SPECstorage setup."
            )
            skip_setup = True
        elif not os.path.exists(sfs2020_archive):
            print(
                f"Warning: SPECstorage archive {sfs2020_archive} not found. Skipping SPECstorage setup."
            )
            skip_setup = True
        elif not (
            sfs2020_archive.endswith(".tar")
            or sfs2020_archive.endswith(".tgz")
            or sfs2020_archive.endswith(".tar.gz")
        ):
            print(
                f"Warning: SPECstorage archive {sfs2020_archive} has unsupported extension. Skipping SPECstorage setup."
            )
            skip_setup = True

        # Collect all targets to copy scripts to: admin, clients, ganeshas, mons, mdss
        targets = set(
            [self.admin]
            + self.config.clients
            + self.config.ganeshas
            + self.config.mons
            + self.config.mdss
        )

        remote_dir = os.path.dirname(run_cmd)
        base_files_to_copy = [
            ("lib/workload/run_sfs2020_workload.py", run_cmd),
            ("perf_record.py", perf_script),
            ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
        ]

        # Also copy ganesha perf record script if different
        g_cfg = self.config.get("ganesha", {})
        g_perf_script = g_cfg.get("perf_record_script")
        if g_perf_script and g_perf_script != perf_script:
            base_files_to_copy.append(("perf_record.py", g_perf_script))

        stap_script = spec_cfg.get("stap_script")
        if stap_script and os.path.exists(os.path.basename(stap_script)):
            base_files_to_copy.append((os.path.basename(stap_script), stap_script))

        archive_basename = (
            os.path.basename(sfs2020_archive) if sfs2020_archive else None
        )
        remote_archive_path = (
            os.path.join(remote_dir, archive_basename) if archive_basename else None
        )

        threads = []
        for target in targets:
            t = threading.Thread(
                target=self._setup_target,
                args=(
                    target,
                    remote_dir,
                    base_files_to_copy,
                    g_perf_script,
                    perf_script,
                    skip_setup,
                    sfs2020_archive,
                    remote_archive_path,
                ),
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        content = self._generate_spec_file()
        with open("/tmp/spec_cfg", "w") as f:
            f.write(content)

        u_admin, h_admin, p_admin = self.executor.get_ssh_details(self.admin)
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(p_admin),
                "/tmp/spec_cfg",
                f"{u_admin}@{h_admin}:{out}",
            ]
        )
        os.remove("/tmp/spec_cfg")

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

            g_str = GaneshaManager.get_ganesha_config_str(
                self.config.get("ganesha", {})
            )
            if g_str:
                g_p = "_" + g_str

        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}{g_p}")
