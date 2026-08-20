import time
from cephfs_perf_lib import CommonUtils, FSManager


class CephPoolManager(FSManager):
    """Manages a single Rados pool for benchmarks that don't need a CephFS
    (e.g. `rados bench`, `fio --ioengine=rbd`).

    The config section that carries `pool` / `pool_pg_num` / etc. is passed
    via ``section`` (defaults to auto-detect: "rbd" if present, else
    "rados_bench"). All MDS-specific hooks are no-ops.
    """

    def __init__(self, executor, config, section=None):
        self.executor = executor
        self.config = config
        self.admin = config.admin_host

        if section is None:
            if config.get("rbd"):
                section = "rbd"
            else:
                section = "rados_bench"
        self.section = section

        cfg = config.get(section) or {}
        self.pool_name = cfg.get("pool")
        if not self.pool_name:
            raise ValueError(
                f"{section}.pool must be set when using CephPoolManager"
            )
        self.pg_num = cfg.get("pool_pg_num")
        self.pool_size = cfg.get("pool_size")
        self.pool_min_size = cfg.get("pool_min_size")
        self.recreate = cfg.get("pool_recreate", False)
        # Default application: 'rbd' for RBD pools (triggers `rbd pool init`),
        # 'rados' for object-store benchmarks like `rados bench`.
        default_app = "rbd" if section == "rbd" else "rados"
        self.application = cfg.get("pool_application", default_app)

    def get_fs_names(self):
        return [self.pool_name]

    def start_fs_logging(self, loadpoint):
        pass

    def stop_fs_logging(self, loadpoint, results_dir=None):
        pass

    def start_lockstat(self, fs):
        pass

    def stop_lockstat(self, fs):
        pass

    def reset_lockstat(self):
        pass

    def dump_lockstat(self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None):
        pass

    def reset_perf_counters(self):
        pass

    def dump_perf_counters(
        self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None
    ):
        pass

    def apply_fs_settings(self, settings):
        # mds_* settings from the benchmark matrix don't apply to a bare pool.
        # Silently ignore so the matrix expansion in BenchRunner still drives
        # multiple iterations if desired.
        pass

    def _section_cfg(self):
        return self.config.get(self.section) or {}

    def _ceph_env(self):
        """Env for ceph/rbd CLI (top-level env_vars + section env_vars)."""
        return self.config.get_merged_env_vars(self._section_cfg().get("env_vars"))

    def _ceph_bin(self):
        cfg = self._section_cfg()
        if cfg.get("ceph_binary_path"):
            return self.config.expand_env(cfg["ceph_binary_path"])
        mds_cfg = self.config.get("mds", {}) or {}
        if mds_cfg.get("ceph_binary_path"):
            return self.config.expand_env(mds_cfg["ceph_binary_path"])
        ganesha_cfg = self.config.get("ganesha", {}) or {}
        if ganesha_cfg.get("ceph_binary_path"):
            return self.config.expand_env(ganesha_cfg["ceph_binary_path"])
        known = CommonUtils.expand_env_vars_map(self._ceph_env())
        prefix = known.get("CEPH_INSTALL_PREFIX") or "/usr/local"
        return f"{prefix}/bin/ceph"

    def _rbd_bin(self):
        cfg = self._section_cfg()
        if cfg.get("rbd_executable_path"):
            return self.config.expand_env(cfg["rbd_executable_path"])
        known = CommonUtils.expand_env_vars_map(self._ceph_env())
        prefix = known.get("CEPH_INSTALL_PREFIX") or "/usr/local"
        return f"{prefix}/bin/rbd"

    def _ceph_cmd(self, args, sudo=True):
        cmd = f"{self._ceph_bin()} {args}".strip()
        return CommonUtils.with_env_exports(cmd, self._ceph_env(), sudo=sudo)

    def _rbd_cmd(self, args, sudo=True):
        cmd = f"{self._rbd_bin()} {args}".strip()
        return CommonUtils.with_env_exports(cmd, self._ceph_env(), sudo=sudo)

    def _run_ceph(self, host, args, sudo=True):
        return self.executor.run_remote(host, self._ceph_cmd(args, sudo=sudo))

    def _run_rbd(self, host, args, sudo=True):
        return self.executor.run_remote(host, self._rbd_cmd(args, sudo=sudo))

    def _pool_exists(self, pool):
        out = self._run_ceph(self.admin, "osd pool ls --format json")
        pools = self.safe_json_load(out, [])
        return pool in pools

    def _wait_for_pool(self, pool, exists=True, attempts=24, interval=2, raise_on_timeout=True):
        for _ in range(attempts):
            if self._pool_exists(pool) == exists:
                return True
            time.sleep(interval)
        if raise_on_timeout:
            verb = "appear" if exists else "be deleted"
            raise RuntimeError(f"Timed out waiting for pool {pool} to {verb}")
        return False

    def _osd_hosts_count(self):
        try:
            raw = self._run_ceph(self.admin, "osd tree --format json")
            tree = self.safe_json_load(raw, {})
            return sum(1 for n in tree.get("nodes", []) if n.get("type") == "host")
        except Exception:
            return 0

    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        self._run_ceph(self.admin, "config set mon mon_allow_pool_delete true")
        self._run_ceph(self.admin, "config set global mon_max_pg_per_osd 1000")

        if self.recreate and self._pool_exists(self.pool_name):
            print(f"Deleting existing pool {self.pool_name}...")
            self._run_ceph(
                self.admin,
                f"osd pool delete {self.pool_name} {self.pool_name} "
                f"--yes-i-really-really-mean-it || true",
            )
            self._wait_for_pool(self.pool_name, exists=False, raise_on_timeout=False)

        if not self._pool_exists(self.pool_name):
            print(f"Creating pool {self.pool_name}...")
            create_args = f"osd pool create {self.pool_name}"
            if self.pg_num is not None:
                create_args += f" {self.pg_num}"
            self._run_ceph(self.admin, create_args)
            self._wait_for_pool(self.pool_name, exists=True)
            if self.application == "rbd":
                # `rbd pool init` both enables the rbd application and does
                # the RBD-specific pool init (writes the rbd_directory object).
                self._run_rbd(self.admin, f"pool init {self.pool_name} || true")
            elif self.application:
                self._run_ceph(
                    self.admin,
                    f"osd pool application enable {self.pool_name} "
                    f"{self.application} || true",
                )

            osd_hosts = self._osd_hosts_count()
            if 0 < osd_hosts < 3 and self.pool_size is None:
                self._run_ceph(self.admin, f"osd pool set {self.pool_name} size 2")
                self._run_ceph(
                    self.admin, f"osd pool set {self.pool_name} min_size 1"
                )

        if self.pool_size is not None:
            self._run_ceph(
                self.admin, f"osd pool set {self.pool_name} size {self.pool_size}"
            )
        if self.pool_min_size is not None:
            self._run_ceph(
                self.admin,
                f"osd pool set {self.pool_name} min_size {self.pool_min_size}",
            )

        self._distribute_keys_and_config()

    def _distribute_keys_and_config(self):
        for t in self.config.clients:
            self.executor.run_remote(t, "sudo mkdir -p /etc/ceph")
            u, h, p = self.executor.get_ssh_details(t)
            files = "/etc/ceph/ceph.conf /etc/ceph/ceph.client.admin.keyring"
            self.executor.run_remote(
                self.admin,
                f"sudo scp -o StrictHostKeyChecking=no -P {p} {files} {u}@{h}:/tmp/",
            )
            self.executor.run_remote(
                t,
                "sudo mv /tmp/ceph.conf /tmp/ceph.client.admin.keyring /etc/ceph/ && "
                "sudo chmod 0600 /etc/ceph/*.keyring",
            )
