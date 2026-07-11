import time
from cephfs_perf_lib import FSManager


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

    def dump_lockstat(self, loadpoint, results_dir=None):
        pass

    def apply_fs_settings(self, settings):
        # mds_* settings from the benchmark matrix don't apply to a bare pool.
        # Silently ignore so the matrix expansion in BenchRunner still drives
        # multiple iterations if desired.
        pass

    def _pool_exists(self, pool):
        out = self.executor.run_remote(
            self.admin, "sudo ceph osd pool ls --format json"
        )
        pools = self.safe_json_load(out, [])
        return pool in pools

    def _osd_hosts_count(self):
        try:
            raw = self.executor.run_remote(
                self.admin, "sudo ceph osd tree --format json"
            )
            tree = self.safe_json_load(raw, {})
            return sum(1 for n in tree.get("nodes", []) if n.get("type") == "host")
        except Exception:
            return 0

    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        self.executor.run_remote(
            self.admin, "sudo ceph config set mon mon_allow_pool_delete true"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph config set global mon_max_pg_per_osd 1000"
        )

        if self.recreate and self._pool_exists(self.pool_name):
            print(f"Deleting existing pool {self.pool_name}...")
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool delete {self.pool_name} {self.pool_name} "
                f"--yes-i-really-really-mean-it || true",
            )
            for _ in range(24):
                if not self._pool_exists(self.pool_name):
                    break
                time.sleep(2)

        if not self._pool_exists(self.pool_name):
            print(f"Creating pool {self.pool_name}...")
            create_cmd = f"sudo ceph osd pool create {self.pool_name}"
            if self.pg_num is not None:
                create_cmd += f" {self.pg_num}"
            self.executor.run_remote(self.admin, create_cmd)
            if self.application == "rbd":
                # `rbd pool init` both enables the rbd application and does
                # the RBD-specific pool init (writes the rbd_directory object).
                self.executor.run_remote(
                    self.admin, f"sudo rbd pool init {self.pool_name} || true"
                )
            elif self.application:
                self.executor.run_remote(
                    self.admin,
                    f"sudo ceph osd pool application enable {self.pool_name} "
                    f"{self.application} || true",
                )

            osd_hosts = self._osd_hosts_count()
            if 0 < osd_hosts < 3 and self.pool_size is None:
                self.executor.run_remote(
                    self.admin,
                    f"sudo ceph osd pool set {self.pool_name} size 2",
                )
                self.executor.run_remote(
                    self.admin,
                    f"sudo ceph osd pool set {self.pool_name} min_size 1",
                )

        if self.pool_size is not None:
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool set {self.pool_name} size {self.pool_size}",
            )
        if self.pool_min_size is not None:
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool set {self.pool_name} min_size {self.pool_min_size}",
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
