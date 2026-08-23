import re

from lib.mount.mount_manager import MountManager


class MountKernelManager(MountManager):
    """Mount CephFS via the kernel client (``mount -t ceph``).

    Used when ``ganesha.enabled`` is false. Ceph CLI calls go through the
    filesystem manager helpers so ``CEPH_INSTALL_PREFIX`` / ``mds.ceph_binary_path``
    are honored (bare ``sudo ceph`` fails when ceph lives under a custom prefix).

    Mount options use ``mds_namespace=`` (not ``fs=``): older kernels reject
    ``fs=`` with ``Unknown parameter 'fs'``. ``mds_namespace`` remains accepted
    as a synonym on newer kernels.
    """

    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)
        self.fs_manager = fs_manager

    def mount(self):
        admin_host = self.config.admin_host
        # Resolve mon addr + client key with the same env-aware ceph binary as
        # CephFSManager (prefix installs are not on sudo's secure_path).
        mon_dump = self.fs_manager._run_ceph(admin_host, "mon dump")
        mon_addrs = re.findall(
            r"v1:([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+)", mon_dump
        )
        if not mon_addrs:
            raise RuntimeError(
                f"Could not parse mon v1 address from `ceph mon dump` on {admin_host}"
            )
        # Device syntax: mon1:port[,mon2:port,...]:/path
        maddrs = ",".join(dict.fromkeys(mon_addrs))
        key = self.fs_manager._run_ceph(
            admin_host, "auth get-key client.0"
        ).strip()
        if not key:
            raise RuntimeError(
                f"Empty key from `ceph auth get-key client.0` on {admin_host}"
            )
        mpfs = self.config.get("specstorage", {}).get("mounts_per_fs", 1)
        for fs in self.fs_names:
            for c in self.clients:
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    # mds_namespace selects the CephFS; quote secret (base64 may
                    # contain + / =). check=True so a failed mount aborts the run.
                    opts = f"name=0,secret={key},mds_namespace={fs}"
                    self.executor.run_remote(
                        c,
                        f"sudo mkdir -p {p} && "
                        f"sudo mount -t ceph '{maddrs}:/' '{p}' -o '{opts}' && "
                        f"mountpoint -q '{p}'",
                        check=True,
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(
                        c, f"sudo chown {u}:{u} {p}", check=True
                    )
                    print(f"[{c}] Kernel-mounted cephfs {fs} at {p} via {maddrs}")

    def display_name(self):
        return "kernel_cephfs"

    def kernel_mount(self):
        self.mount()
