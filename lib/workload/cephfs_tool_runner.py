import base64
import datetime
import json
import os
import subprocess
from lib.workload.workload_runner import WorkloadRunner
from cephfs_perf_lib import CommonUtils
from lib.run_logging import get_logger

log = get_logger(__name__)



class CephFSToolWorkloadRunner(WorkloadRunner):
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
        cfg = self.config.cephfs_tool
        loadpoints = cfg.get("loadpoints", [])
        loadpoints = CommonUtils.expand_loadpoints(loadpoints)
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/cephfs_tool/run_cephfs_workload.py"
        )
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
            "config_path",
            "keyring",
            "client_id",
            "root_path",
            "duration",
            "msgr_workers",
            "measure_latencies",
        ]:
            if key in cfg:
                payload[key] = cfg[key]
        payload["env_vars"] = self.config.get_merged_env_vars(cfg.get("env_vars"))

        # Built-in cephfs-tool --lockstat-dump / --lockstat-threshold (no asok).
        lockstat_cfg = self.config.client_lockstat("cephfs_tool")
        if lockstat_cfg.get("enabled", False):
            payload["cephfs_tool_lockstat_enabled"] = True
            payload["cephfs_tool_lockstat_threshold"] = lockstat_cfg.get(
                "threshold", 0
            )

        settings_json = json.dumps(payload)
        loadpoints_json = json.dumps(loadpoints)
        clients_json = json.dumps(self.config.clients)

        log.info(f"Running CephFS-Tool Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)

        # To avoid "Argument list too long" errors when loadpoints JSON is very large,
        # we write JSON data to temporary files and pass file paths with @ prefix.

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
        
        log.info(f"[{self.admin}] Executing workload with temp files...")
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-p",
            port,
            f"{user}@{host}",
            "bash -s",
        ]

        monitor = self.make_loadpoint_monitor(
            workload_name="cephfs_tool",
            config_section="cephfs_tool",
            cephfs_manager=cephfs_manager,
            ganesha_manager=ganesha_manager,
            mount_manager=mount_manager,
            results_dir=results_dir,
            settings=settings,
            payload=payload,
            loadpoints=loadpoints,
            finish_markers=("Finished CephFS-Tool Load Point:",),
        )

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

        output, returncode = self.consume_remote_stdout(process, monitor)
        if returncode != 0:
            raise RuntimeError(
                f"CephFS-Tool failed on {self.admin} with return code {returncode}"
            )
        return output

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

        return os.path.join(
            base,
            f"{ts}_{self.get_name()}_{fs_p}_{mds_p}{CommonUtils.mount_name_suffix(self.config)}",
        )

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
            + self.config.osds
        )

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            # Ensure the directory exists on each target
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(
                target,
                f"sudo mkdir -p {remote_dir}/lib && sudo chown {u}:{u} {remote_dir}",
            )

            # Copy local files to each target
            files_to_copy = [
                ("lib/workload/run_cephfs_workload.py", run_cmd),
                ("perf_record.py", perf_script),
                (
                    "mds_perf_dump_collector.py",
                    "/cephfs_perf/mds_perf_dump_collector.py",
                ),
            ]
            files_to_copy.extend(self.cephfs_perf_lib_remote_deps(remote_dir))

            if stap_script and os.path.exists(os.path.basename(stap_script)):
                files_to_copy.append((os.path.basename(stap_script), stap_script))

            self.executor.run_remote(
                target, f"sudo mkdir -p /cephfs_perf && sudo chown {u}:{u} /cephfs_perf"
            )
            for local_file, remote_path in files_to_copy:
                if os.path.exists(local_file):
                    log.info(f"Copying local {local_file} to {remote_path} on {target}...")
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
