from cephfs_perf_lib import CommonUtils
from lib.rgw.rgw_manager import RgwManager


class RgwSystemdManager(RgwManager):
    """Run local ``radosgw`` daemons on inventory ``rgws`` hosts.

    Starts ``count_per_host`` instances per host on consecutive ports beginning
    at ``frontend_port`` (beast frontend).
    """

    def provision_rgw(self, results_dir=None):
        if self._provisioned:
            print("RGW already provisioned. Skipping.")
            return
        if not self.rgws:
            raise RuntimeError(
                "No RGW hosts configured in inventory group 'rgws'"
            )

        self.open_firewall_ports()
        for host_name in self.rgws:
            self._stop_host_rgw(host_name)
            self._start_host_rgw(host_name)

        self.ensure_s3_user()
        self._provisioned = True

    def cleanup_rgw(self):
        print("Stopping local RGW (radosgw) processes...")
        self._provisioned = False
        for host_name in self.rgws:
            self._stop_host_rgw(host_name)

    def _pid_path(self, host_name, index):
        pid_dir = self.config.rgw_pid_dir.rstrip("/")
        return f"{pid_dir}/radosgw-{host_name}-{index}.pid"

    def _log_path(self, host_name, index):
        return f"/var/log/ceph/radosgw-{host_name}-{index}.log"

    def _keyring_path(self, host_name, index):
        return f"/etc/ceph/ceph.client.rgw.{host_name}.{index}.keyring"

    def _daemon_name(self, host_name, index):
        safe_host = host_name.replace(".", "-")
        return f"client.rgw.{safe_host}.{index}"

    def _ensure_daemon_auth(self, host_name, index):
        name = self._daemon_name(host_name, index)
        keyring = self._keyring_path(host_name, index)
        self._run_ceph(
            f"auth get-or-create {name} "
            f'osd "allow rwx" mon "allow rw" mgr "allow rw" '
            f"-o {keyring}",
            check=False,
        )
        # Ensure keyring exists even if get-or-create was a no-op on an existing entity
        exists = self.executor.run_remote(
            self.admin, f"test -s {keyring} && echo OK || echo MISSING"
        ).strip()
        if "OK" not in exists:
            self._run_ceph(f"auth get {name} -o {keyring}", check=False)

        if host_name != self.admin:
            content = self.executor.run_remote(
                self.admin, f"sudo cat {keyring} 2>/dev/null || true"
            )
            if content.strip():
                escaped = content.replace("'", "'\\''")
                self.executor.run_remote(
                    host_name, "sudo mkdir -p /etc/ceph"
                )
                self.executor.run_remote(
                    host_name,
                    f"printf '%s\\n' '{escaped}' | sudo tee {keyring} > /dev/null",
                )
                self.executor.run_remote(
                    host_name, f"sudo chmod 0640 {keyring}"
                )
        return keyring

    def _stop_host_rgw(self, host_name):
        count = int(self.config.rgw_count_per_host)
        for index in range(count):
            pid_path = self._pid_path(host_name, index)
            self.executor.run_remote(
                host_name,
                f"if [ -f {pid_path} ]; then "
                f"sudo kill $(cat {pid_path}) 2>/dev/null || true; "
                f"sudo rm -f {pid_path}; fi",
            )
        # Only kill daemons we started (pid files), not unrelated radosgw

    def _start_host_rgw(self, host_name):
        binary = self.config.rgw_radosgw_binary_path
        conf = self.config.ceph_conf_path
        ports = self.frontend_ports()
        env_exports = CommonUtils.format_env_exports(self._get_rgw_env())

        self.executor.run_remote(
            host_name,
            f"sudo mkdir -p {self.config.rgw_pid_dir} /var/log/ceph /etc/ceph",
        )

        for index, port in enumerate(ports):
            pid_path = self._pid_path(host_name, index)
            log_path = self._log_path(host_name, index)
            name = self._daemon_name(host_name, index)
            keyring = self._ensure_daemon_auth(host_name, index)
            frontend = f"beast port={port}"
            cmd = (
                f"sudo bash -c '{env_exports} "
                f"nohup {binary} -n {name} -c {conf} -k {keyring} "
                f'--rgw-frontends="{frontend}" '
                f"--pid-file={pid_path} "
                f"> {log_path} 2>&1 &'"
            )
            self.executor.run_remote(host_name, cmd, check=True)
            print(
                f"[{host_name}] Started radosgw {name} on port {port} "
                f"(pid file {pid_path})"
            )
