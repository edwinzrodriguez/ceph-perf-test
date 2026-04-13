from mount_manager import MountManager

class MountNfsManager(MountManager):
    def mount(self):
        gs = self.config.ganeshas
        mpfs = self.config.get("specstorage", {}).get("mounts_per_fs", 1)
        for fs in self.fs_names:
            for idx, c in enumerate(self.clients):
                gh = gs[idx % len(gs)]
                gt = (
                    self.config.all_hosts_meta.get(gh, {}).get("private_ip")
                    or self.executor.get_ssh_details(gh)[1]
                )
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    self.executor.run_remote(
                        c,
                        f"sudo mkdir -p {p} && sudo mount -t nfs -o nfsvers=4.1,proto=tcp {gt}:/{fs}-export {p}",
                        check=True,
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(c, f"sudo chown {u}:{u} {p}")

    def nfs_mount(self):
        self.mount()
