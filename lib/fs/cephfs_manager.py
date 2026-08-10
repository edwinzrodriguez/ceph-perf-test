import time
from cephfs_perf_lib import CommonUtils, FSManager


class CephFSManager(FSManager):
    """Base CephFS manager with common lifecycle, logging, and lockstat logic.

    Deployment-specific behavior (cephadm orch vs local ceph-mds process) is
    implemented by subclasses via ``_remove_mds_service``, ``_deploy_mds``,
    and ``_collect_mds_logs``.
    """

    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        self.admin = config.admin_host
        self.fs_name = config.fs_name
        self.num_filesystems = config.num_filesystems
        self.fs_names = (
            [self.fs_name]
            + [f"{self.fs_name}_{i:02d}" for i in range(2, self.num_filesystems + 1)]
            if self.num_filesystems > 1
            else [self.fs_name]
        )
        self.mdss = config.mdss
        self.lockstat_exists = {}
        # Track deployed MDS instances: {fs_name: [(host, mds_id), ...]}
        self._mds_instances = {}

    def start_fs_logging(self, loadpoint):
        debug_mds = self.config.get("logging", {}).get("debug_mds", 20)
        debug_ms = self.config.get("logging", {}).get("debug_ms", 1)
        for server_name in self.mdss:
            print(
                f"[{server_name}] Starting MDS debug logging for Load Point {loadpoint}"
            )
            self.executor.run_remote(
                server_name, f"sudo ceph config set mds debug_mds {debug_mds}"
            )
            self.executor.run_remote(
                server_name, f"sudo ceph config set mds debug_ms {debug_ms}"
            )

    def stop_fs_logging(self, loadpoint, results_dir=None):
        for server_name in self.mdss:
            print(
                f"[{server_name}] Stopping MDS debug logging for Load Point {loadpoint}"
            )
            self.executor.run_remote(
                server_name, "sudo ceph config set mds debug_mds 1"
            )
            self.executor.run_remote(server_name, "sudo ceph config set mds debug_ms 1")
        if results_dir:
            self._collect_mds_logs(loadpoint, results_dir)

    def start_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        threshold = lockstat_cfg.get("threshold", 0)
        env_vars = self.config.env_vars
        for server_name in self.mdss:
            if server_name not in self.lockstat_exists:
                check = self.executor.run_remote(
                    server_name,
                    f"test -f {lockstat_path} && echo EXISTS || echo MISSING",
                ).strip()
                self.lockstat_exists[server_name] = "EXISTS" in check
            if self.lockstat_exists[server_name]:
                print(
                    f"[{server_name}] Starting lockstat for mds.{fs} via {lockstat_path} with threshold {threshold}"
                )
                self.executor.run_remote(
                    server_name,
                    CommonUtils.with_env_exports(
                        f"python3 {lockstat_path} mds.{fs} start --threshold {threshold}",
                        env_vars,
                        sudo=True,
                    ),
                )

    def stop_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        env_vars = self.config.env_vars
        for server_name in self.mdss:
            if self.lockstat_exists.get(server_name):
                print(f"[{server_name}] Stopping lockstat for mds.{fs} via {lockstat_path}")
                self.executor.run_remote(
                    server_name,
                    CommonUtils.with_env_exports(
                        f"python3 {lockstat_path} mds.{fs} stop",
                        env_vars,
                        sudo=True,
                    ),
                )

    def _check_lockstat_exists(self, server_name, lockstat_path):
        """Populate lockstat_exists for server_name if not yet checked."""
        if server_name not in self.lockstat_exists:
            check = self.executor.run_remote(
                server_name,
                f"test -f {lockstat_path} && echo EXISTS || echo MISSING",
            ).strip()
            self.lockstat_exists[server_name] = "EXISTS" in check

    def reset_lockstat(self, config_section="specstorage"):
        lockstat_cfg = self.config.get(config_section, {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        env_vars = self.config.env_vars
        for fs in self.get_fs_names():
            for server_name in self.mdss:
                self._check_lockstat_exists(server_name, lockstat_path)
                if self.lockstat_exists.get(server_name):
                    print(f"[{server_name}] Resetting lockstat for mds.{fs}")
                    self.executor.run_remote(
                        server_name,
                        CommonUtils.with_env_exports(
                            f"python3 {lockstat_path} mds.{fs} reset",
                            env_vars,
                            sudo=True,
                        ),
                    )

    def dump_lockstat(self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None, config_section="specstorage"):
        lockstat_cfg = self.config.get(config_section, {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        env_vars = self.config.env_vars
        for fs in self.get_fs_names():
            for server_name in self.mdss:
                self._check_lockstat_exists(server_name, lockstat_path)
                if self.lockstat_exists.get(server_name):
                    phase_label = f" ({phase} phase)" if phase else ""
                    print(
                        f"[{server_name}] Dumping lockstat for mds.{fs} (Load Point {loadpoint}{phase_label})"
                    )
                    if results_dir:
                        dump_cmd = CommonUtils.with_env_exports(
                            f"python3 {lockstat_path} mds.{fs} dump --detail",
                            env_vars,
                        )
                        CommonUtils.dump_lockstat_common(
                            self.executor,
                            server_name,
                            loadpoint,
                            results_dir,
                            f"mds.{fs}",
                            dump_cmd,
                            self.admin,
                            settings=settings,
                            lp_cfg=lp_cfg,
                            phase=phase,
                        )

    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        self.executor.run_remote(
            self.admin, "sudo ceph config set mon mon_allow_pool_delete true"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph config set global mon_max_pg_per_osd 1000"
        )
        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.cleanup_ganesha()
        for fs in self.get_fs_names():
            self._remove_mds_service(fs)
            self.executor.run_remote(
                self.admin, f"sudo ceph fs fail {fs} --yes-i-really-mean-it || true"
            )
            self.executor.run_remote(
                self.admin, f"sudo ceph fs rm {fs} --yes-i-really-mean-it || true"
            )
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool delete {fs}_metadata {fs}_metadata --yes-i-really-really-mean-it || true",
            )
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool delete {fs}_data {fs}_data --yes-i-really-really-mean-it || true",
            )

            osd_hosts_count = 0
            try:
                osd_tree_raw = self.executor.run_remote(self.admin, "sudo ceph osd tree --format json")
                osd_tree = self.safe_json_load(osd_tree_raw, {})
                nodes = osd_tree.get("nodes", [])
                osd_hosts_count = sum(1 for node in nodes if node.get("type") == "host")
            except Exception:
                pass

            self.executor.run_remote(
                self.admin, f"sudo ceph osd pool create {fs}_metadata"
            )
            self.executor.run_remote(
                self.admin, f"sudo ceph osd pool create {fs}_data"
            )

            if 0 < osd_hosts_count < 3:
                for pool in [f"{fs}_metadata", f"{fs}_data"]:
                    self.executor.run_remote(
                        self.admin, f"sudo ceph osd pool set {pool} size 2"
                    )
                    self.executor.run_remote(
                        self.admin, f"sudo ceph osd pool set {pool} min_size 1"
                    )
            self.executor.run_remote(
                self.admin, f"sudo ceph fs new {fs} {fs}_metadata {fs}_data"
            )
            self._deploy_mds(fs, settings)
            self._wait_for_mds_active(fs)
            self.setup_client_auth(fs)
        self.distribute_keys_and_config()

    def _remove_mds_service(self, fs):
        """Tear down MDS daemons for the given filesystem. Subclasses implement this."""
        raise NotImplementedError

    def _deploy_mds(self, fs, settings):
        """Deploy MDS daemons for the given filesystem. Subclasses implement this."""
        raise NotImplementedError

    def _select_mds_hosts(self, fs, count):
        """Select host placement for MDS daemons (active + standbys)."""
        num_mdss = len(self.mdss)
        if num_mdss == 0:
            raise RuntimeError("No MDS hosts available in inventory")
        num_hosts = min(count + 2, num_mdss)
        start_idx = (
            self.get_fs_names().index(fs) if fs in self.get_fs_names() else 0
        ) % num_mdss
        return [self.mdss[(start_idx + i) % num_mdss] for i in range(num_hosts)]

    def _wait_for_mds_active(self, fs, timeout_iters=60, sleep_secs=5):
        for _ in range(timeout_iters):
            status_raw = self.executor.run_remote(
                self.admin, f"sudo ceph fs status {fs} --format json"
            )
            status = self.safe_json_load(status_raw, {})
            if isinstance(status, list):
                status = status[0] if status else {}
            mdsmap = status.get("mdsmap", {})
            if isinstance(mdsmap, dict):
                if (
                    mdsmap.get("up")
                    or mdsmap.get("up:active")
                    or mdsmap.get("active")
                ):
                    return
            elif isinstance(mdsmap, list):
                if any(
                    str(e.get("state", "")).lower() in ["active", "up:active"]
                    or "active" in str(e.get("state", "")).lower()
                    for e in mdsmap
                ):
                    return
            time.sleep(sleep_secs)

    def get_fs_names(self):
        return self.fs_names

    def apply_fs_settings(self, settings):
        for k, v in settings.items():
            if k in ["max_mds", "cpus"]:
                continue
            val = CommonUtils.format_si_units(v)
            for fs in self.get_fs_names():
                self.executor.run_remote(
                    self.admin, f"sudo ceph fs set {fs} mds_{k} {val}"
                )
        if "max_mds" in settings:
            for fs in self.get_fs_names():
                self.executor.run_remote(
                    self.admin, f"sudo ceph fs set {fs} max_mds {settings['max_mds']}"
                )

    def setup_client_auth(self, fs):
        self.executor.run_remote(
            self.admin, f"sudo ceph fs authorize {fs} client.0 / rwps"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph auth get client.0 -o /etc/ceph/ceph.client.0.keyring"
        )

    def distribute_keys_and_config(self):
        targets = self.config.clients + self.config.ganeshas
        for t in targets:
            self.executor.run_remote(t, "sudo mkdir -p /etc/ceph")
            u, h, p = self.executor.get_ssh_details(t)
            files = "/etc/ceph/ceph.conf /etc/ceph/ceph.client.0.keyring /etc/ceph/ceph.client.admin.keyring"
            self.executor.run_remote(
                self.admin,
                f"sudo scp -o StrictHostKeyChecking=no -P {p} {files} {u}@{h}:/tmp/",
            )
            self.executor.run_remote(
                t,
                "sudo mv /tmp/ceph.conf /tmp/ceph.client.0.keyring /tmp/ceph.client.admin.keyring /etc/ceph/ && sudo chmod 0600 /etc/ceph/*.keyring",
            )

    def _copy_log_to_results(self, server_name, src_log, dest_log, results_dir):
        """Copy a remote log file to the admin host results directory."""
        self.executor.run_remote(server_name, f"sudo cp {src_log} /tmp/{dest_log}")
        user, _, _ = self.executor.get_ssh_details(server_name)
        self.executor.run_remote(
            server_name, f"sudo chown {user}:{user} /tmp/{dest_log}"
        )
        admin_user, admin_host, admin_port = self.executor.get_ssh_details(self.admin)
        copy_cmd = (
            f"scp -o StrictHostKeyChecking=no -P {admin_port} "
            f"/tmp/{dest_log} {admin_user}@{admin_host}:{results_dir}/"
        )
        self.executor.run_remote(server_name, copy_cmd)
        self.executor.run_remote(server_name, f"rm -f /tmp/{dest_log}")
        self.executor.run_remote(server_name, f"sudo truncate -s 0 {src_log}")
