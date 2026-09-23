import abc
import json
import time

from cephfs_perf_lib import CommonUtils, FSManager


class RgwManager(abc.ABC):
    def __init__(self, executor, config, fs_manager):
        self.executor = executor
        self.config = config
        self.fs_manager = fs_manager
        self.rgws = config.rgws
        self.admin = config.admin_host
        self._provisioned = False
        self._credentials = None

    @abc.abstractmethod
    def provision_rgw(self, results_dir=None):
        pass

    @abc.abstractmethod
    def cleanup_rgw(self):
        pass

    @staticmethod
    def get_rgw_config_str(settings):
        parts = []
        if "count_per_host" in settings:
            parts.append(
                f"{CommonUtils.get_short_name('RGW Count Per Host')}"
                f"{settings['count_per_host']}"
            )
        if "frontend_port" in settings:
            parts.append(
                f"{CommonUtils.get_short_name('RGW Frontend Port')}"
                f"{settings['frontend_port']}"
            )
        return "_".join(parts)

    @staticmethod
    def rgw_payload_keys():
        return [
            "rgw_enabled",
            "rgw_type",
            "rgw_service_id",
            "rgw_count_per_host",
            "rgw_frontend_port",
            "rgw_uid",
            "rgw_access_key",
        ]

    def safe_json_load(self, raw, default=None):
        return FSManager.safe_json_load(self, raw, default)

    def _get_ceph_args(self, include_keyring=True):
        args = []
        if self.config.ceph_conf_path:
            args.append(f"-c {self.config.ceph_conf_path}")
        user_id = self.config.rgw_user_id
        if user_id:
            args.append(f"--user {user_id}")
        if include_keyring and self.config.rgw_keyring_path:
            args.append(f"--keyring {self.config.rgw_keyring_path}")
        return " ".join(args)

    def _get_rgw_env(self):
        default_env = {
            "CEPH_CONF": self.config.ceph_conf_path,
        }
        return self.config.get_merged_env_vars(
            default_env, self.config.rgw_env_vars
        )

    def _sudo_with_rgw_env(self, cmd):
        return CommonUtils.with_env_exports(cmd, self._get_rgw_env(), sudo=True)

    def _ceph_bin(self):
        return self.config.rgw_ceph_binary_path

    def _radosgw_admin_bin(self):
        return self.config.rgw_radosgw_admin_binary_path

    def _run_ceph(self, args, check=False, host=None):
        host = host or self.admin
        ceph_bin = self._ceph_bin()
        return self.executor.run_remote(
            host,
            self._sudo_with_rgw_env(f"{ceph_bin} {self._get_ceph_args()} {args}"),
            check=check,
        )

    def _run_radosgw_admin(self, args, check=False, host=None):
        host = host or self.admin
        admin_bin = self._radosgw_admin_bin()
        return self.executor.run_remote(
            host,
            self._sudo_with_rgw_env(
                f"{admin_bin} {self._get_ceph_args()} {args}"
            ),
            check=check,
        )

    def frontend_ports(self):
        """TCP ports used by RGW instances on each host."""
        start = int(self.config.rgw_frontend_port)
        count = int(self.config.rgw_count_per_host)
        return list(range(start, start + count))

    def host_address(self, host_name):
        meta = self.config.all_hosts_meta.get(host_name, {})
        return meta.get("private_ip") or meta.get("ansible_ssh_host") or host_name

    def s3_endpoints(self):
        endpoints = []
        for host_name in self.rgws:
            addr = self.host_address(host_name)
            for port in self.frontend_ports():
                endpoints.append(f"http://{addr}:{port}")
        return endpoints

    def open_firewall_ports(self):
        if not self.config.rgw_manage_firewall:
            return
        ports = self.frontend_ports()
        if not ports:
            return
        print(
            f"Opening firewall TCP ports {ports[0]}-{ports[-1]} on RGW hosts..."
            if len(ports) > 1
            else f"Opening firewall TCP port {ports[0]} on RGW hosts..."
        )
        CommonUtils.open_firewall_tcp_ports(self.executor, self.rgws, ports)

    def ensure_s3_user(self):
        """Create (or fetch) the S3 user and persist credentials for workloads."""
        uid = self.config.rgw_uid
        display_name = self.config.rgw_display_name
        access_key = self.config.rgw_access_key
        secret_key = self.config.rgw_secret_key

        print(f"Ensuring RGW S3 user '{uid}' exists...")
        info = self.safe_json_load(
            self._run_radosgw_admin(f"user info --uid={uid} --format json"),
            default=None,
        )
        if not info:
            create_args = (
                f"user create --uid={uid} "
                f"--display-name={self._shell_quote(display_name)} "
                f"--format json"
            )
            if access_key:
                create_args += f" --access-key={access_key}"
            if secret_key:
                create_args += f" --secret-key={secret_key}"
            info = self.safe_json_load(
                self._run_radosgw_admin(create_args, check=True),
                default={},
            )
            print(f"Created RGW S3 user '{uid}'")
        else:
            print(f"RGW S3 user '{uid}' already exists")
            if access_key and secret_key:
                # Ensure configured keys are present
                self._run_radosgw_admin(
                    f"key create --uid={uid} --access-key={access_key} "
                    f"--secret-key={secret_key} || true",
                    check=False,
                )
                info = self.safe_json_load(
                    self._run_radosgw_admin(
                        f"user info --uid={uid} --format json"
                    ),
                    default=info,
                )

        keys = info.get("keys") or []
        if access_key:
            matched = next(
                (k for k in keys if k.get("access_key") == access_key), None
            )
            if matched:
                access_key = matched.get("access_key", access_key)
                secret_key = matched.get("secret_key", secret_key)
            elif keys:
                access_key = keys[0].get("access_key", access_key)
                secret_key = keys[0].get("secret_key", secret_key)
        elif keys:
            access_key = keys[0].get("access_key")
            secret_key = keys[0].get("secret_key")

        self._credentials = {
            "uid": uid,
            "display_name": display_name,
            "access_key": access_key,
            "secret_key": secret_key,
            "endpoints": self.s3_endpoints(),
            "frontend_port": self.config.rgw_frontend_port,
            "count_per_host": self.config.rgw_count_per_host,
            "hosts": list(self.rgws),
        }
        self._write_credentials()
        return self._credentials

    @staticmethod
    def _shell_quote(value):
        return "'" + str(value).replace("'", "'\\''") + "'"

    def _write_credentials(self):
        if not self._credentials:
            return
        path = self.config.rgw_credentials_path
        remote_dir = path.rsplit("/", 1)[0] if "/" in path else ""
        if remote_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {remote_dir}")
        payload = json.dumps(self._credentials, indent=2)
        escaped = payload.replace("'", "'\\''")
        self.executor.run_remote(
            self.admin,
            f"printf '%s\\n' '{escaped}' | sudo tee {path} > /dev/null",
        )
        self.executor.run_remote(self.admin, f"sudo chmod 0644 {path}")
        print(f"Wrote S3 credentials to {self.admin}:{path}")

    def wait_for_rgw_running(self, timeout_iters=30, sleep_secs=10):
        sid = self.config.rgw_service_id
        print(f"Waiting for RGW service rgw.{sid} to be running...")
        for _ in range(timeout_iters):
            svcs = self.safe_json_load(
                self._run_ceph("orch ls --service_type rgw --format json"),
                default=[],
            )
            if any(
                (
                    s.get("service_id") == sid
                    or s.get("service_name", "") == f"rgw.{sid}"
                )
                and s.get("status", {}).get("running", 0) > 0
                for s in (svcs or [])
            ):
                return
            time.sleep(sleep_secs)
        print(
            f"Warning: RGW service rgw.{sid} may not be fully running after "
            f"{timeout_iters * sleep_secs}s"
        )
