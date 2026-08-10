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

    # ------------------------------------------------------------------
    # ceph CLI helpers (always honor env_vars + sudo library path)
    # ------------------------------------------------------------------

    def _ceph_env(self):
        """Env for ceph CLI / MDS tools (top-level env_vars)."""
        return self.config.env_vars

    def _ceph_bin(self):
        """Resolved path to the ceph CLI.

        Preference order:
          1. mds.ceph_binary_path
          2. ganesha.ceph_binary_path
          3. ${CEPH_INSTALL_PREFIX}/bin/ceph  (from env_vars)
          4. /usr/local/bin/ceph
        """
        mds_cfg = self.config.get("mds", {}) or {}
        if mds_cfg.get("ceph_binary_path"):
            return self.config.expand_env(mds_cfg["ceph_binary_path"])
        ganesha_cfg = self.config.get("ganesha", {}) or {}
        if ganesha_cfg.get("ceph_binary_path"):
            return self.config.expand_env(ganesha_cfg["ceph_binary_path"])
        known = CommonUtils.expand_env_vars_map(self._ceph_env())
        prefix = known.get("CEPH_INSTALL_PREFIX") or "/usr/local"
        return f"{prefix}/bin/ceph"

    def _ceph_cmd(self, args, sudo=True):
        """Build a shell command that runs the ceph CLI with env_vars applied.

        ``args`` is the remainder of the command (e.g. ``"fs ls --format json"``).
        Paths and env references in env_vars are expanded; LD_LIBRARY_PATH is
        exported so a prefix-installed ``ceph`` can load its shared libs.
        """
        cmd = f"{self._ceph_bin()} {args}".strip()
        return CommonUtils.with_env_exports(cmd, self._ceph_env(), sudo=sudo)

    def _run_ceph(self, host, args, check=False, sudo=True):
        """Run the ceph CLI on *host* with env_vars + optional sudo."""
        return self.executor.run_remote(
            host, self._ceph_cmd(args, sudo=sudo), check=check
        )

    def start_fs_logging(self, loadpoint):
        debug_mds = self.config.get("logging", {}).get("debug_mds", 20)
        debug_ms = self.config.get("logging", {}).get("debug_ms", 1)
        for server_name in self.mdss:
            print(
                f"[{server_name}] Starting MDS debug logging for Load Point {loadpoint}"
            )
            self._run_ceph(server_name, f"config set mds debug_mds {debug_mds}")
            self._run_ceph(server_name, f"config set mds debug_ms {debug_ms}")

    def stop_fs_logging(self, loadpoint, results_dir=None):
        for server_name in self.mdss:
            print(
                f"[{server_name}] Stopping MDS debug logging for Load Point {loadpoint}"
            )
            self._run_ceph(server_name, "config set mds debug_mds 1")
            self._run_ceph(server_name, "config set mds debug_ms 1")
        if results_dir:
            self._collect_mds_logs(loadpoint, results_dir)

    def start_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        threshold = lockstat_cfg.get("threshold", 0)
        env_vars = self._ceph_env()
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
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        env_vars = self._ceph_env()
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
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        env_vars = self._ceph_env()
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
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        env_vars = self._ceph_env()
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
        self._run_ceph(self.admin, "config set mon mon_allow_pool_delete true")
        self._run_ceph(self.admin, "config set global mon_max_pg_per_osd 1000")
        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.cleanup_ganesha()
        for fs in self.get_fs_names():
            self._remove_mds_service(fs)
            # Give MDS processes time to drop mon sessions before fail/rm.
            # Otherwise: fs rm fails, pools get deleted under a leftover FS,
            # fs new fails ("already exists"), and MDS stay forever in standby
            # against a non-joinable / pool-less filesystem.
            time.sleep(2)
            self._run_ceph(
                self.admin, f"fs fail {fs} --yes-i-really-mean-it || true"
            )
            self._run_ceph(
                self.admin, f"fs rm {fs} --yes-i-really-mean-it || true"
            )
            # Confirm the FS is gone before deleting pools (avoids orphan FS map).
            ls_raw = "[]"
            for _ in range(30):
                ls_raw = self._run_ceph(
                    self.admin, "fs ls --format json || echo []"
                )
                fs_list = self.safe_json_load(ls_raw, [])
                names = []
                if isinstance(fs_list, list):
                    for entry in fs_list:
                        if isinstance(entry, dict) and entry.get("name"):
                            names.append(entry["name"])
                if fs not in names:
                    break
                # Still present — retry rm
                self._run_ceph(
                    self.admin, f"fs rm {fs} --yes-i-really-mean-it || true"
                )
                time.sleep(1)
            else:
                raise RuntimeError(
                    f"Failed to remove filesystem '{fs}' before recreate; "
                    f"refusing to delete pools (would leave a pool-less FS). "
                    f"fs ls={ls_raw!r}"
                )

            self._run_ceph(
                self.admin,
                f"osd pool delete {fs}_metadata {fs}_metadata "
                f"--yes-i-really-really-mean-it || true",
            )
            self._run_ceph(
                self.admin,
                f"osd pool delete {fs}_data {fs}_data "
                f"--yes-i-really-really-mean-it || true",
            )

            osd_hosts_count = 0
            try:
                osd_tree_raw = self._run_ceph(
                    self.admin, "osd tree --format json"
                )
                osd_tree = self.safe_json_load(osd_tree_raw, {})
                nodes = osd_tree.get("nodes", [])
                osd_hosts_count = sum(1 for node in nodes if node.get("type") == "host")
            except Exception:
                pass

            self._run_ceph(
                self.admin, f"osd pool create {fs}_metadata", check=True
            )
            self._run_ceph(
                self.admin, f"osd pool create {fs}_data", check=True
            )

            if 0 < osd_hosts_count < 3:
                for pool in [f"{fs}_metadata", f"{fs}_data"]:
                    self._run_ceph(self.admin, f"osd pool set {pool} size 2")
                    self._run_ceph(self.admin, f"osd pool set {pool} min_size 1")
            self._run_ceph(
                self.admin,
                f"fs new {fs} {fs}_metadata {fs}_data",
                check=True,
            )
            # Ensure ranks are eligible (fs fail leaves joinable=false; new FS
            # should already be joinable, but force it for recreate safety).
            self._run_ceph(self.admin, f"fs set {fs} joinable true || true")
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
        last_status = ""
        for i in range(timeout_iters):
            status_raw = self._run_ceph(
                self.admin, f"fs status {fs} --format json"
            )
            last_status = status_raw
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
            if i % 6 == 0:
                print(
                    f"[wait] {fs}: no active MDS yet "
                    f"(iter {i + 1}/{timeout_iters}); status={status_raw[:500]}"
                )
            time.sleep(sleep_secs)

        # Dump extra diagnostics so a hang is actionable instead of silent.
        extras = []
        for args in (
            "fs ls",
            f"fs get {fs} || true",
            "fs dump || true",
            "osd lspools || true",
        ):
            try:
                extras.append(
                    f"$ ceph {args}\n{self._run_ceph(self.admin, args)}"
                )
            except Exception as e:
                extras.append(f"$ ceph {args}\n<error: {e}>")
        raise RuntimeError(
            f"Timed out waiting for an active MDS on filesystem '{fs}'.\n"
            f"Last fs status:\n{last_status}\n" + "\n".join(extras)
        )

    def get_fs_names(self):
        return self.fs_names

    def apply_fs_settings(self, settings):
        for k, v in settings.items():
            if k in ["max_mds", "cpus"]:
                continue
            # Settings keys are already mds_* (e.g. mds_cache_memory_limit);
            # do not double-prefix with mds_.
            fs_key = k if k.startswith("mds_") else f"mds_{k}"
            val = CommonUtils.format_si_units(v)
            for fs in self.get_fs_names():
                self._run_ceph(self.admin, f"fs set {fs} {fs_key} {val}")
        if "max_mds" in settings:
            for fs in self.get_fs_names():
                self._run_ceph(
                    self.admin, f"fs set {fs} max_mds {settings['max_mds']}"
                )

    def setup_client_auth(self, fs):
        self._run_ceph(self.admin, f"fs authorize {fs} client.0 / rwps")
        self._run_ceph(
            self.admin,
            "auth get client.0 -o /etc/ceph/ceph.client.0.keyring",
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

    def _collect_mds_logs(self, loadpoint, results_dir):
        """Collect MDS logs. Subclasses override for their deployment model."""
        pass

    def is_mds_lockstat_enabled(self):
        # Prefer mds.lockstat; fall back to legacy specstorage.lockstat
        mds_ls = (self.config.get("mds", {}) or {}).get("lockstat", {}) or {}
        if "enabled" in mds_ls:
            return bool(mds_ls.get("enabled"))
        return bool(
            self.config.get("specstorage", {}).get("lockstat", {}).get("enabled")
        )

    def is_mds_perf_record_enabled(self):
        return bool(self.config.get("mds", {}).get("perf_record", False))
