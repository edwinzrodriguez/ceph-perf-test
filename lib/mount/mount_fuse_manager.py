import re
import shlex

from cephfs_perf_lib import CommonUtils
from lib.mount.mount_manager import MountManager


class MountFuseManager(MountManager):
    """Mount CephFS via ``ceph-fuse`` on client hosts.

    Used when ``mount_manager_type`` is ``MountFuseManager`` and Ganesha is
    disabled. Monitor addresses are resolved on the admin host (same as the
    kernel client). Auth and config paths default to the top-level ``ceph:``
    section; override via ``mount_fuse`` in the settings YAML.
    """

    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)
        self.fs_manager = fs_manager

    def _fuse_cfg(self):
        return self.config.get("mount_fuse", {}) or {}

    def _fuse_bin(self):
        fuse_cfg = self._fuse_cfg()
        if fuse_cfg.get("binary_path"):
            return self.config.expand_env(fuse_cfg["binary_path"])
        known = CommonUtils.expand_env_vars_map(self.config.env_vars)
        prefix = known.get("CEPH_INSTALL_PREFIX") or "/usr/local"
        return f"{prefix}/bin/ceph-fuse"

    def _fuse_env(self):
        return self.config.get_merged_env_vars(self._fuse_cfg().get("env_vars"))

    def mount(self):
        admin_host = self.config.admin_host
        mon_dump = self.fs_manager._run_ceph(admin_host, "mon dump")
        mon_addrs = re.findall(
            r"v1:([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+)", mon_dump
        )
        if not mon_addrs:
            raise RuntimeError(
                f"Could not parse mon v1 address from `ceph mon dump` on {admin_host}"
            )
        maddrs = ",".join(dict.fromkeys(mon_addrs))

        fuse_cfg = self._fuse_cfg()
        fuse_bin = self._fuse_bin()
        conf = fuse_cfg.get("conf") or self.config.ceph_conf_path
        keyring = fuse_cfg.get("keyring") or self.config.ceph_keyring_path
        client_id = fuse_cfg.get("client_id") or self.config.ceph_user_id
        extra_opts = (fuse_cfg.get("mount_options") or "").strip()
        env_vars = self._fuse_env()

        mpfs = self._mounts_per_fs()
        for fs in self.fs_names:
            for c in self.clients:
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    fuse_cmd = (
                        f"{shlex.quote(fuse_bin)} {shlex.quote(p)} "
                        f"-m {shlex.quote(maddrs)} "
                        f"-c {shlex.quote(conf)} "
                        f"--id {shlex.quote(client_id)} "
                        f"-k {shlex.quote(keyring)} "
                        f"--client_mds_namespace={shlex.quote(fs)}"
                    )
                    if extra_opts:
                        fuse_cmd = f"{fuse_cmd} {extra_opts}"
                    inner = (
                        f"mkdir -p {shlex.quote(p)} && "
                        f"{fuse_cmd} && "
                        f"mountpoint -q {shlex.quote(p)}"
                    )
                    self.executor.run_remote(
                        c,
                        CommonUtils.with_env_exports(inner, env_vars, sudo=True),
                        check=True,
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(
                        c, f"sudo chown {u}:{u} {p}", check=True
                    )
                    print(f"[{c}] ceph-fuse mounted {fs} at {p} via {maddrs}")

    def display_name(self):
        return "fuse"

    def fuse_mount(self):
        self.mount()
