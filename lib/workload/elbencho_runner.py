import base64
import datetime
import json
import os
import subprocess

from cephfs_perf_lib import CommonUtils
from lib.workload.workload_runner import WorkloadRunner
from lib.run_logging import get_logger

log = get_logger(__name__)



class ElbenchoWorkloadRunner(WorkloadRunner):
    """Drive elbencho S3 benchmarks against provisioned RGW endpoints."""

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
        cfg = self.config.elbencho or {}
        loadpoints = cfg.get("loadpoints", [])
        if isinstance(loadpoints, dict):
            loadpoints = [loadpoints]
        loadpoints = CommonUtils.expand_loadpoints(loadpoints)
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/elbencho/run_elbencho_workload.py"
        )
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = results_dir or self.get_results_dir(settings, ts)

        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        credentials = self._load_s3_credentials(rgw_manager)
        conf_text = self._build_conf_text(credentials, results_dir)
        conf_path = cfg.get("conf_path", "/cephfs_perf/rgw/s3-classic.conf")
        self._write_conf(conf_path, conf_text)
        # Also keep a copy beside results for the run
        self._write_conf(f"{results_dir}/s3-classic.conf", conf_text)

        # Elbencho --service listens on load_driver_port on each CLIENT;
        # open that port when firewalld/iptables is active (same as RGW ports).
        self._open_client_load_driver_firewall(cfg)

        payload = settings.copy()
        # Keep result filenames short: only matrix settings + a few RGW dims
        payload["name_settings"] = dict(settings)
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir
        payload["conf_path"] = conf_path
        payload["credentials_path"] = self.config.rgw_credentials_path
        payload["s3_access"] = credentials.get("access_key")
        payload["s3_secret"] = credentials.get("secret_key")
        payload["rgw_endpoints"] = credentials.get("endpoints") or []
        payload["buckets"] = cfg.get("buckets") or ["eot-classic"]
        payload["size_limit"] = cfg.get("size_limit", "4T")
        payload["object_limit"] = cfg.get("object_limit", 1000000)
        payload["load_driver_port"] = cfg.get("load_driver_port", 1611)
        payload["distributed"] = cfg.get("distributed", True)

        for key in ["executable_path", "timelimit"]:
            if key in cfg:
                payload[key] = cfg[key]
        payload["env_vars"] = self.config.get_merged_env_vars(cfg.get("env_vars"))

        for k in [
            "rgw_enabled",
            "rgw_type",
            "rgw_service_id",
            "rgw_count_per_host",
            "rgw_frontend_port",
            "rgw_uid",
        ]:
            val = getattr(self.config, k, None)
            if val is not None:
                payload[k] = val

        clients = self._client_endpoints()
        payload["elbencho_clients"] = clients

        settings_json = json.dumps(payload)
        loadpoints_json = json.dumps(loadpoints)
        clients_json = json.dumps(clients)

        log.info(f"Running Elbencho S3 Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)

        tmp_settings = f"/tmp/elbencho_settings_{os.getpid()}.json"
        tmp_loadpoints = f"/tmp/elbencho_loadpoints_{os.getpid()}.json"
        tmp_clients = f"/tmp/elbencho_clients_{os.getpid()}.json"

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
            workload_name="elbencho",
            config_section="elbencho",
            cephfs_manager=cephfs_manager,
            ganesha_manager=ganesha_manager,
            results_dir=results_dir,
            settings=settings,
            payload=payload,
            loadpoints=loadpoints,
            finish_markers=("Finished Elbencho Load Point:",),
        )

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

        output, returncode = self.consume_remote_stdout(process, monitor)
        if returncode != 0:
            raise RuntimeError(
                f"Elbencho failed on {self.admin} with return code {returncode}"
            )
        return output

    def _open_client_load_driver_firewall(self, cfg):
        """Open elbencho --service port on inventory clients (and admin)."""
        manage = cfg.get("manage_firewall")
        if manage is None:
            manage = self.config.rgw_manage_firewall
        if not manage:
            return
        port = int(cfg.get("load_driver_port", 1611))
        # Inventory host names (not just IPs) so executor can SSH
        hosts = list(dict.fromkeys(self.config.clients + [self.admin]))
        log.info(
            f"Opening firewall TCP port {port} on elbencho clients "
            f"({', '.join(hosts)})..."
        )
        CommonUtils.open_firewall_tcp_ports(self.executor, hosts, [port])

    def _load_s3_credentials(self, rgw_manager):
        if rgw_manager is not None and getattr(rgw_manager, "_credentials", None):
            return dict(rgw_manager._credentials)

        path = self.config.rgw_credentials_path
        raw = self.executor.run_remote(
            self.admin, f"sudo cat {path} 2>/dev/null || true"
        )
        try:
            data = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Failed to parse S3 credentials at {self.admin}:{path}: {e}"
            ) from e
        if not data.get("access_key") or not data.get("secret_key"):
            raise RuntimeError(
                f"S3 credentials missing access/secret key at {self.admin}:{path}. "
                "Ensure rgw is enabled and provisioned before the elbencho workload."
            )
        if not data.get("endpoints"):
            data["endpoints"] = self._fallback_rgw_endpoints()
        return data

    def _fallback_rgw_endpoints(self):
        endpoints = []
        port = int(self.config.rgw_frontend_port)
        count = int(self.config.rgw_count_per_host)
        for host_name in self.config.rgws:
            meta = self.config.all_hosts_meta.get(host_name, {})
            addr = meta.get("private_ip") or meta.get("ansible_ssh_host") or host_name
            for i in range(count):
                endpoints.append(f"http://{addr}:{port + i}")
        return endpoints

    def _client_endpoints(self):
        clients = []
        for host_name in self.config.clients:
            meta = self.config.all_hosts_meta.get(host_name, {})
            addr = meta.get("private_ip") or meta.get("ansible_ssh_host") or host_name
            clients.append(addr)
        return clients

    def _admin_ssh_user(self):
        meta = self.config.all_hosts_meta.get(self.admin, {})
        return meta.get("ansible_ssh_user") or "root"

    def _build_conf_text(self, credentials, results_dir):
        cfg = self.config.elbencho or {}
        size_limit = cfg.get("size_limit", "4T")
        object_limit = cfg.get("object_limit", 1000000)
        load_driver_port = cfg.get("load_driver_port", 1611)
        buckets = cfg.get("buckets") or ["eot-classic"]
        access = credentials.get("access_key", "")
        secret = credentials.get("secret_key", "")
        endpoints = credentials.get("endpoints") or self._fallback_rgw_endpoints()
        clients = self._client_endpoints()
        admin_addr = (
            self.config.all_hosts_meta.get(self.admin, {}).get("private_ip")
            or self.config.all_hosts_meta.get(self.admin, {}).get("ansible_ssh_host")
            or self.admin
        )
        admin_user = self._admin_ssh_user()
        output_dir = results_dir

        lines = [
            f"SIZE_LIMIT='{size_limit}'        # default is 4T",
            f"OBJECT_LIMIT={object_limit}   # default 1000000 (1M)",
            f"OUTPUT_DIR='{output_dir}'",
            "",
            "BUCKET_LIST=(",
        ]
        for bucket in buckets:
            lines.append(f'    "{bucket}"')
        lines.extend(
            [
                ")",
                "",
                f"LOAD_DRIVER_PORT={load_driver_port}",
                f"S3_ACCESS='{access}'",
                f"S3_SECRET='{secret}'",
                "",
                f"RADOSGW_ADMIN_HOST='{admin_addr}'",
                f"RADOSGW_ADMIN_HOST_USER='{admin_user}'",
                "",
                "CLIENTS=(",
            ]
        )
        for client in clients:
            lines.append(f'    "{client}"')
        lines.extend(["", ")", "", "RGW_HOSTS=("])
        for ep in endpoints:
            lines.append(f'    "{ep}"')
        lines.extend([")", ""])
        return "\n".join(lines)

    def _write_conf(self, remote_path, conf_text):
        remote_dir = os.path.dirname(remote_path)
        if remote_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {remote_dir}")
        escaped = conf_text.replace("'", "'\\''")
        self.executor.run_remote(
            self.admin,
            f"printf '%s\\n' '{escaped}' | sudo tee {remote_path} > /dev/null",
        )
        self.executor.run_remote(self.admin, f"sudo chmod 0644 {remote_path}")
        log.info(f"Wrote elbencho conf to {self.admin}:{remote_path}")

    def get_results_dir(self, settings, shared_ts=None):
        cfg = self.config.elbencho or {}
        base = cfg.get("results_base_dir", "/cephfs_perf/results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = (
            f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}"
        )
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        rgw_parts = []
        if self.config.rgw_count_per_host:
            rgw_parts.append(
                f"{CommonUtils.get_short_name('RGW Count Per Host')}"
                f"{self.config.rgw_count_per_host}"
            )
        if self.config.rgw_frontend_port:
            rgw_parts.append(
                f"{CommonUtils.get_short_name('RGW Frontend Port')}"
                f"{self.config.rgw_frontend_port}"
            )
        rgw_p = ("_" + "_".join(rgw_parts)) if rgw_parts else ""
        return os.path.join(
            base,
            f"{ts}_{self.get_name()}_{fs_p}_{mds_p}{rgw_p}"
            f"{CommonUtils.mount_name_suffix(self.config)}",
        )

    def get_name(self):
        return "elbencho"

    def prepare_storage(self):
        cfg = self.config.get("elbencho", {}) or {}
        run_cmd = cfg.get(
            "run_command", "/cephfs_perf/elbencho/run_elbencho_workload.py"
        )
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")

        targets = set(
            [self.admin]
            + self.config.clients
            + self.config.rgws
            + self.config.mons
            + self.config.osds
        )

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(
                target,
                f"sudo mkdir -p {remote_dir}/lib && sudo chown {u}:{u} {remote_dir}",
            )

            files_to_copy = [
                ("lib/workload/run_elbencho_workload.py", run_cmd),
                ("perf_record.py", perf_script),
            ]
            files_to_copy.extend(self.cephfs_perf_lib_remote_deps(remote_dir))

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
