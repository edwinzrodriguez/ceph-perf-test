import os
import subprocess
import time

import yaml

from lib.grafana.grafana_manager import GrafanaManager


class GrafanaCephadmManager(GrafanaManager):
    """Deploy monitoring services via ``ceph orch apply``."""

    def provision_monitoring(self):
        if self._provisioned:
            print("Monitoring stack already provisioned. Skipping.")
            return
        if not self.grafana_hosts:
            raise RuntimeError(
                "No Grafana hosts configured. Add a 'grafanas' inventory group "
                "or ensure at least one mon is defined."
            )

        self._enable_prometheus_module()
        self._apply_exporter_prio_limit()
        self._apply_monitoring_specs()
        self._wait_for_monitoring_services()
        self._provisioned = True

    def cleanup_monitoring(self):
        print("Removing cephadm monitoring services...")
        self._provisioned = False
        for service_type in self.MONITORING_SERVICE_TYPES:
            self._run_ceph(f"orch rm {service_type} || true", check=False)

    def _build_monitoring_specs(self):
        placement_hosts = self.grafana_hosts
        exporter_placement = {"host_pattern": "*"}
        prio = self.config.grafana_exporter_prio_limit

        specs = [
            {
                "service_type": "node-exporter",
                "service_name": "node-exporter",
                "placement": exporter_placement,
                "ssl": False,
            },
            {
                "service_type": "ceph-exporter",
                "service_name": "ceph-exporter",
                "placement": exporter_placement,
                "prio_limit": prio,
                "ssl": False,
            },
            {
                "service_type": "prometheus",
                "service_name": "prometheus",
                "placement": {"hosts": placement_hosts},
                "ssl": False,
            },
            {
                "service_type": "grafana",
                "service_name": "grafana",
                "placement": {"hosts": placement_hosts},
                "spec": {
                    "anonymous_access": self.config.grafana_anonymous_access,
                    "protocol": self.config.grafana_protocol,
                    "ssl": self.config.grafana_ssl,
                },
            },
        ]
        if self.config.grafana_port:
            specs[-1]["port"] = self.config.grafana_port
        if self.config.prometheus_port:
            specs[-2]["port"] = self.config.prometheus_port
        if self.config.grafana_exporter_port:
            specs[1]["port"] = self.config.grafana_exporter_port
        return specs

    def _apply_monitoring_specs(self):
        specs = self._build_monitoring_specs()
        local_path = "monitoring.yaml"
        with open(local_path, "w") as f:
            yaml.safe_dump_all(specs, f, sort_keys=False)

        remote_path = self.config.grafana_yaml_path
        remote_dir = os.path.dirname(remote_path)
        if remote_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {remote_dir}")

        u, h, p = self.executor.get_ssh_details(self.admin)
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(p),
                local_path,
                f"{u}@{h}:{remote_path}",
            ],
            check=True,
        )
        os.remove(local_path)
        print(f"Applying monitoring stack from {self.admin}:{remote_path}...")
        self._run_ceph(f"orch apply -i {remote_path}", check=True)

    def _wait_for_monitoring_services(self, timeout_iters=30, sleep_secs=10):
        for service_type in ("ceph-exporter", "prometheus", "grafana"):
            print(f"Waiting for {service_type} service to be running...")
            for _ in range(timeout_iters):
                svcs = self.safe_json_load(
                    self._run_ceph(
                        f"orch ls --service_type {service_type} --format json"
                    ),
                    default=[],
                )
                if any(
                    s.get("service_type") == service_type
                    and s.get("status", {}).get("running", 0) > 0
                    for s in svcs
                ):
                    break
                time.sleep(sleep_secs)
            else:
                print(
                    f"Warning: {service_type} may not be fully running after "
                    f"{timeout_iters * sleep_secs}s"
                )
