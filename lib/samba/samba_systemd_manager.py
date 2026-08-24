import re

from lib.samba.samba_manager import SambaManager


class SambaSystemdManager(SambaManager):
    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)

    def provision_samba(self, results_dir=None):
        if self._provisioned:
            print("Samba already provisioned. Skipping.")
            return
        if not self.sambas:
            raise RuntimeError("No Samba hosts configured in inventory group 'sambas'")

        self._ensure_smb_subvolumes()
        for host_name in self.sambas:
            self._ensure_samba_user(host_name)
            self._mount_filesystems(host_name)
            self._fix_backend_permissions(host_name)
            self._write_smb_conf(host_name)
            self._restart_smbd(host_name)

        self._provisioned = True

    def cleanup_samba(self):
        print("Cleaning up SMB shares and stopping Samba service...")
        for host_name in self.sambas:
            self._write_empty_smb_conf(host_name)
            self.executor.run_remote(
                host_name,
                "sudo systemctl stop smbd nmbd 2>/dev/null || "
                "sudo service smb stop 2>/dev/null || true",
            )
            mount_base = self.config.samba_mount_base
            for fs in self.get_fs_names():
                mount_path = f"{mount_base}/{fs}"
                self.executor.run_remote(
                    host_name,
                    f"sudo umount -f {mount_path} || sudo umount -l {mount_path} || true",
                )
        self._provisioned = False

    def _write_empty_smb_conf(self, host_name):
        """Remove share definitions by writing a global-only smb.conf."""
        workgroup = self.config.samba_workgroup
        config_content = (
            "[global]\n"
            f"   workgroup = {workgroup}\n"
            "   server string = Ceph Samba\n"
            "   security = user\n"
            "   map to guest = Bad User\n"
            "   load printers = no\n"
            "   printing = bsd\n"
            "   disable spoolss = yes\n"
        )
        config_path = self.config.samba_config_path
        escaped = config_content.replace("'", "'\\''")
        self.executor.run_remote(host_name, "sudo mkdir -p /etc/samba")
        self.executor.run_remote(
            host_name,
            f"printf '{escaped}' | sudo tee {config_path} > /dev/null",
        )
        self.executor.run_remote(host_name, f"sudo chmod 0644 {config_path}")

    def _mount_filesystems(self, host_name):
        admin_host = self.admin
        mon_dump = self.fs_manager._run_ceph(admin_host, "mon dump")
        mon_addrs = re.findall(
            r"v1:([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+)", mon_dump
        )
        if not mon_addrs:
            raise RuntimeError(
                f"Could not parse mon v1 address from `ceph mon dump` on {admin_host}"
            )
        maddrs = ",".join(dict.fromkeys(mon_addrs))
        key = self.fs_manager._run_ceph(
            admin_host, "auth get-key client.0"
        ).strip()
        if not key:
            raise RuntimeError(
                f"Empty key from `ceph auth get-key client.0` on {admin_host}"
            )

        mount_base = self.config.samba_mount_base
        self.executor.run_remote(host_name, f"sudo mkdir -p {mount_base}")
        for fs in self.get_fs_names():
            mount_path = f"{mount_base}/{fs}"
            backend_path = self._share_backend_path(fs, mount_path)
            opts = f"name=0,secret={key},mds_namespace={fs}"
            self.executor.run_remote(
                host_name,
                f"sudo mkdir -p '{backend_path}' && "
                f"sudo mount -t ceph '{maddrs}:/' '{mount_path}' -o '{opts}' && "
                f"mountpoint -q '{mount_path}'",
                check=True,
            )
            print(
                f"[{host_name}] Kernel-mounted cephfs {fs} at {mount_path} "
                f"(share backend {backend_path})"
            )

    def _share_backend_path(self, fs, mount_path):
        return f"{mount_path.rstrip('/')}{self._subvolume_backend_path(fs)}"

    def _write_smb_conf(self, host_name):
        workgroup = self.config.samba_workgroup
        username = self.config.samba_username
        global_block = (
            "[global]\n"
            f"   workgroup = {workgroup}\n"
            "   server string = Ceph Samba\n"
            "   security = user\n"
            "   map to guest = Bad User\n"
            "   load printers = no\n"
            "   printing = bsd\n"
            "   disable spoolss = yes\n"
        )
        shares = ""
        mount_base = self.config.samba_mount_base
        for fs in self.get_fs_names():
            share_name = self.client_share_name_for_fs(fs)
            mount_path = f"{mount_base}/{fs}"
            backend_path = self._share_backend_path(fs, mount_path)
            shares += (
                f"\n[{share_name}]\n"
                f"   path = {backend_path}\n"
                "   browsable = yes\n"
                "   read only = no\n"
                "   guest ok = no\n"
                f"   valid users = {username}\n"
                f"   force user = {username}\n"
                f"   force group = {username}\n"
                "   create mask = 0664\n"
                "   directory mask = 0775\n"
            )

        config_content = global_block + shares
        config_path = self.config.samba_config_path
        escaped = config_content.replace("'", "'\\''")
        self.executor.run_remote(host_name, "sudo mkdir -p /etc/samba")
        self.executor.run_remote(
            host_name,
            f"printf '{escaped}' | sudo tee {config_path} > /dev/null",
        )
        self.executor.run_remote(host_name, f"sudo chmod 0644 {config_path}")

    def _fix_backend_permissions(self, host_name):
        """Make the CephFS backend writable by the Samba user.

        A fresh kernel ceph mount is root-owned (mode 755). smbd rejects
        writes from valid users that cannot create files on the share path.
        """
        username = self.config.samba_username
        mount_base = self.config.samba_mount_base
        for fs in self.get_fs_names():
            mount_path = f"{mount_base}/{fs}"
            backend_path = self._share_backend_path(fs, mount_path)
            self.executor.run_remote(
                host_name,
                f"sudo chown -R {username}:{username} {backend_path} && "
                f"sudo chmod 0777 {backend_path}",
                check=True,
            )

    def _ensure_samba_user(self, host_name):
        username = self.config.samba_username
        password = self.config.samba_password
        escaped_pass = password.replace("'", "'\\''")
        self.executor.run_remote(
            host_name,
            f"id -u {username} >/dev/null 2>&1 || sudo useradd -M -s /sbin/nologin {username}",
        )
        self.executor.run_remote(
            host_name,
            f"printf '{escaped_pass}\n{escaped_pass}\n' | sudo smbpasswd -a -s {username}",
            check=True,
        )
        self.executor.run_remote(
            host_name, f"sudo smbpasswd -e {username}", check=True
        )

    def _restart_smbd(self, host_name):
        self.executor.run_remote(
            host_name,
            "sudo systemctl enable smbd nmbd 2>/dev/null || true; "
            "sudo systemctl restart smbd nmbd 2>/dev/null || "
            "sudo service smb restart",
            check=True,
        )
