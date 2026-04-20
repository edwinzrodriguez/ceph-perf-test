from lib.mount.mount_manager import MountManager


class MountKernelManager(MountManager):
    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)

    def mount(self):
        admin_host = self.config.admin_host
        maddrs = self.executor.run_remote(
            admin_host,
            "sudo ceph mon dump | grep -oE 'v1:[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:[0-9]+' | head -n 1 | sed 's/v1://'",
        ).strip()
        key = self.executor.run_remote(
            admin_host, "sudo ceph auth get-key client.0"
        ).strip()
        mpfs = self.config.get("specstorage", {}).get("mounts_per_fs", 1)
        for fs in self.fs_names:
            for c in self.clients:
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    self.executor.run_remote(
                        c,
                        f"sudo mkdir -p {p} && sudo mount -t ceph {maddrs}:/ {p} -o name=0,secret={key},fs={fs}",
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(c, f"sudo chown {u}:{u} {p}")

    def kernel_mount(self):
        self.mount()
