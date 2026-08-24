from lib.mount.mount_manager import MountManager


class MountSmbManager(MountManager):
    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)

    def _mounts_per_fs(self):
        if self.config.fio:
            return self.config.fio.get("mounts_per_fs", 1)
        return self.config.get("specstorage", {}).get("mounts_per_fs", 1)

    def _server_ip(self, host_name):
        return (
            self.config.all_hosts_meta.get(host_name, {}).get("private_ip")
            or self.executor.get_ssh_details(host_name)[1]
        )

    def mount(self):
        servers = self.config.sambas
        if not servers:
            raise RuntimeError("No Samba hosts configured in inventory group 'sambas'")

        mpfs = self._mounts_per_fs()
        mount_opts = self.config.get("mount_smb", {}).get(
            "mount_options",
            "vers=3.0,cache=strict,file_mode=0664,dir_mode=0775,noperm",
        )
        username = self.config.samba_username
        password = self.config.samba_password
        cred_opts = f"username={username},password={password}"
        opts = f"{mount_opts},{cred_opts}" if mount_opts else cred_opts

        for fs in self.fs_names:
            share_name = (
                f"{self.config.samba_share_prefix}{fs}"
                if self.config.samba_share_prefix
                else fs
            )
            for idx, client in enumerate(self.clients):
                server_host = servers[idx % len(servers)]
                server_ip = self._server_ip(server_host)
                for i in range(mpfs):
                    mount_path = (
                        f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    )
                    self.executor.run_remote(
                        client,
                        f"sudo mkdir -p {mount_path} && "
                        f"sudo mount -t cifs //{server_ip}/{share_name} {mount_path} "
                        f"-o '{opts}' && mountpoint -q {mount_path}",
                        check=True,
                    )
                    self.executor.run_remote(
                        client,
                        f"sudo touch {mount_path}/.mount_smb_test && "
                        f"sudo rm -f {mount_path}/.mount_smb_test",
                        check=True,
                    )
                    print(
                        f"[{client}] CIFS-mounted //{server_ip}/{share_name} at {mount_path}"
                    )

    def display_name(self):
        return "smb"

    def smb_mount(self):
        self.mount()
