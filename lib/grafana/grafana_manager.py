import abc
import os
import time

from cephfs_perf_lib import CommonUtils, FSManager, RegistryCredentials


class GrafanaManager(abc.ABC):
    """Deploy and configure the Ceph monitoring stack (Grafana, Prometheus, ceph-exporter)."""

    MONITORING_SERVICE_TYPES = (
        "ceph-exporter",
        "prometheus",
        "grafana",
        "node-exporter",
    )

    # cephadm monitoring services removed before the systemd benchmark stack
    CEPHADM_MONITORING_SERVICE_TYPES = (
        "grafana",
        "prometheus",
        "alertmanager",
        "ceph-exporter",
    )

    def __init__(self, executor, config, fs_manager=None):
        self.executor = executor
        self.config = config
        self.fs_manager = fs_manager
        self.admin = config.admin_host
        self.grafana_hosts = config.grafana_hosts
        self._provisioned = False
        self._registry_logins = {}
        self._registry_credentials = config.grafana_registry_credentials

    @abc.abstractmethod
    def provision_monitoring(self):
        pass

    @abc.abstractmethod
    def cleanup_monitoring(self):
        pass

    def safe_json_load(self, raw, default=None):
        return FSManager.safe_json_load(self, raw, default)

    def _get_ceph_args(self, include_keyring=True):
        args = []
        if self.config.ceph_conf_path:
            args.append(f"-c {self.config.ceph_conf_path}")
        if self.config.ceph_user_id:
            args.append(f"--user {self.config.ceph_user_id}")
        if include_keyring and self.config.ceph_keyring_path:
            args.append(f"--keyring {self.config.ceph_keyring_path}")
        return " ".join(args)

    def _get_grafana_env(self):
        default_env = {
            "CEPH_CONF": self.config.ceph_conf_path,
        }
        return self.config.get_merged_env_vars(
            default_env, self.config.grafana_env_vars
        )

    def _sudo_with_grafana_env(self, cmd):
        return CommonUtils.with_env_exports(cmd, self._get_grafana_env(), sudo=True)

    def _ceph_bin(self):
        return self.config.grafana_ceph_binary_path

    def _run_ceph(self, args, check=False):
        ceph_bin = self._ceph_bin()
        return self.executor.run_remote(
            self.admin,
            self._sudo_with_grafana_env(f"{ceph_bin} {self._get_ceph_args()} {args}"),
            check=check,
        )

    def _host_addr(self, host_name):
        meta = self.config.all_hosts_meta.get(host_name, {})
        return (
            meta.get("private_ip")
            or meta.get("monitor_address")
            or meta.get("ansible_ssh_host")
            or host_name
        )

    def exporter_hosts(self):
        """Hosts that should run ceph-exporter (union of mon/mgr/mds/osd groups)."""
        hosts = []
        for group in ("mons", "mgrs", "mdss", "osds"):
            for host_name in getattr(self.config, group, []):
                if host_name not in hosts:
                    hosts.append(host_name)
        return hosts

    def _enable_prometheus_module(self):
        print("Enabling mgr prometheus module...")
        self._run_ceph("mgr module enable prometheus || true")
        self._run_ceph("mgr module ls | grep -q prometheus || true")

    def _apply_exporter_prio_limit(self):
        prio = self.config.grafana_exporter_prio_limit
        print(f"Setting exporter_prio_limit={prio}...")
        self._run_ceph(
            f"config set global exporter_prio_limit {prio} || true", check=False
        )

    def _remove_cephadm_monitoring(self, wait=True):
        """Remove cephadm monitoring services so the benchmark stack can take over."""
        print("Removing cephadm monitoring services (grafana, prometheus, ...)...")
        for service_type in self.CEPHADM_MONITORING_SERVICE_TYPES:
            self._run_ceph(f"orch rm {service_type} || true", check=False)

        if not wait:
            return

        for service_type in self.CEPHADM_MONITORING_SERVICE_TYPES:
            for _ in range(24):
                svcs = self.safe_json_load(
                    self._run_ceph(
                        f"orch ls --service_type {service_type} --format json"
                    ),
                    default=[],
                )
                if not svcs:
                    break
                running = sum(
                    s.get("status", {}).get("running", 0) for s in svcs
                )
                if running == 0:
                    break
                time.sleep(5)
            else:
                print(
                    f"Warning: cephadm {service_type} may still be running after "
                    "orch rm"
                )

    def _ensure_registry_login(self, host_name, image):
        creds = self._registry_credentials.credentials_for_image(image)
        if creds is None:
            registry = RegistryCredentials.registry_from_image(image)
            print(
                f"[{host_name}] No registry credentials configured for {registry}; "
                f"assuming public pull for {image}"
            )
            return

        cache_key = (host_name, creds["url"])
        if cache_key in self._registry_logins:
            return

        creds_file = self.config.grafana_credentials_file
        if not os.path.exists(creds_file):
            raise RuntimeError(
                f"Registry credentials file not found: {creds_file}. "
                f"Create it or set grafana.credentials_file."
            )

        print(f"[{host_name}] Logging in to {creds['url']} for {image}...")
        login_cmd = RegistryCredentials.podman_login_command(
            creds["url"], creds["username"], creds["password"]
        )
        self.executor.run_remote(host_name, login_cmd, check=True)
        self._registry_logins[cache_key] = True

    def _verify_container_running(self, host_name, container):
        state = self.executor.run_remote(
            host_name,
            f"sudo podman inspect -f '{{{{.State.Running}}}}' {container} 2>/dev/null "
            f"|| echo false",
        ).strip()
        if state == "true":
            return
        logs = self.executor.run_remote(
            host_name,
            f"sudo podman logs {container} 2>&1 | tail -80",
        )
        raise RuntimeError(
            f"{container} failed to start on {host_name}. "
            f"Recent logs:\n{logs}"
        )
