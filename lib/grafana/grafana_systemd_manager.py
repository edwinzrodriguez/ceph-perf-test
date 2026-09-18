import base64
import os
import shlex
import subprocess
from pathlib import Path

import yaml

from lib.grafana.grafana_manager import GrafanaManager


class GrafanaSystemdManager(GrafanaManager):
    """Deploy monitoring for non-cephadm MDS/systemd clusters.

    - ceph-exporter runs as a local process on mon/mgr/mds/osd hosts
    - Prometheus and Grafana run as podman containers on grafana hosts
    """

    PROMETHEUS_CONTAINER = "ceph-perf-prometheus"
    GRAFANA_CONTAINER = "ceph-perf-grafana"
    EXPORTER_PID_PATH = "/var/run/ceph/ceph-exporter.pid"
    GRAFANA_PROVISIONING_DIR = "/etc/ceph/monitoring/grafana/provisioning"
    GRAFANA_DASHBOARDS_HOST_DIR = "/etc/ceph/monitoring/grafana/dashboards"
    CEPH_DASHBOARDS_PATH = "/etc/grafana/dashboards/ceph-dashboard"
    DASHBOARDS_SOURCE_DIR = Path(__file__).resolve().parent / "dashboards"

    def provision_monitoring(self):
        if self._provisioned:
            print("Monitoring stack already provisioned. Skipping.")
            return
        if not self.grafana_hosts:
            raise RuntimeError(
                "No Grafana hosts configured. Add a 'grafanas' inventory group "
                "or ensure at least one mon is defined."
            )

        self._remove_cephadm_monitoring()
        self._enable_prometheus_module()
        self._apply_exporter_prio_limit()
        for host_name in self.exporter_hosts():
            self._start_ceph_exporter(host_name)
        # One Prometheus/Grafana pair scrapes the whole cluster; use the first
        # grafanas host (cephadm does the same).
        primary_host = self.grafana_hosts[0]
        self._deploy_prometheus(primary_host)
        self._deploy_grafana(primary_host)
        self._provisioned = True

    def cleanup_monitoring(self):
        print("Stopping systemd monitoring stack...")
        self._provisioned = False
        for host_name in self.exporter_hosts():
            self._stop_ceph_exporter(host_name)
        for host_name in self.grafana_hosts:
            self._remove_container(host_name, self.PROMETHEUS_CONTAINER)
            self._remove_container(host_name, self.GRAFANA_CONTAINER)

    def _exporter_binary(self):
        return self.config.expand_env(
            self.config.get("grafana", {}).get(
                "exporter_binary_path",
                "${CEPH_INSTALL_PREFIX}/bin/ceph-exporter",
            )
        )

    def _exporter_sock_dir(self):
        """Admin-socket directory for ceph-exporter (must match MDS/OSD run_dir)."""
        return (self.config.get("mds", {}) or {}).get("run_dir", "/var/run/ceph")

    def _start_ceph_exporter(self, host_name):
        binary = self._exporter_binary()
        prio = self.config.grafana_exporter_prio_limit
        port = self.config.grafana_exporter_port
        sock_dir = self._exporter_sock_dir()
        pid_path = self.EXPORTER_PID_PATH
        log_path = "/var/log/ceph/ceph-exporter.log"
        conf = self.config.ceph_conf_path

        self.executor.run_remote(host_name, f"sudo mkdir -p {sock_dir} /var/log/ceph")
        self._stop_ceph_exporter(host_name)

        start_cmd = (
            f"nohup {binary} -c {conf} "
            f"--sock-dir {sock_dir} --prio-limit {prio} "
            f"--tcp-port {port} -f "
            f"> {log_path} 2>&1 & echo $! | sudo tee {pid_path} > /dev/null"
        )
        print(f"[{host_name}] Starting ceph-exporter (prio_limit={prio})...")
        self.executor.run_remote(
            host_name, self._sudo_with_grafana_env(start_cmd), check=True
        )

    def _stop_ceph_exporter(self, host_name):
        pid_path = self.EXPORTER_PID_PATH
        self.executor.run_remote(
            host_name,
            f"if [ -f {pid_path} ]; then "
            f"sudo kill $(cat {pid_path}) 2>/dev/null || true; "
            f"sudo rm -f {pid_path}; fi; "
            f"pids=$(pgrep -f 'ceph-exporter.*--tcp-port' 2>/dev/null); "
            f"if [ -n \"$pids\" ]; then sudo kill $pids || true; fi",
        )

    def _prometheus_config(self):
        mgr_targets = []
        for host_name in self.config.mgrs or self.config.mons:
            addr = self._host_addr(host_name)
            mgr_targets.append(
                f"{addr}:{self.config.grafana_mgr_prometheus_port}"
            )
        exporter_targets = []
        for host_name in self.exporter_hosts():
            addr = self._host_addr(host_name)
            exporter_targets.append(
                f"{addr}:{self.config.grafana_exporter_port}"
            )

        # Ceph Grafana dashboards filter on label ``cluster`` (from
        # ceph_health_status). Attach the same label to every scrape target so
        # ceph-exporter MDS/OSD counters are not dropped by cluster=~"$cluster".
        cluster = self.config.prometheus_cluster_label
        config = {
            "global": {
                "scrape_interval": self.config.prometheus_scrape_interval,
            },
            "scrape_configs": [
                {
                    "job_name": "ceph",
                    "honor_labels": True,
                    "static_configs": [
                        {"targets": mgr_targets, "labels": {"cluster": cluster}}
                    ],
                },
                {
                    "job_name": "ceph-exporter",
                    "static_configs": [
                        {
                            "targets": exporter_targets,
                            "labels": {"cluster": cluster},
                        }
                    ],
                },
            ],
        }
        return yaml.dump(config, default_flow_style=False, sort_keys=False)

    def _deploy_prometheus(self, host_name):
        config_dir = "/etc/ceph/monitoring"
        config_path = f"{config_dir}/prometheus.yml"
        data_dir = "/var/lib/ceph/monitoring/prometheus-data"
        port = self.config.prometheus_port
        image = self.config.grafana_prometheus_image
        container = self.PROMETHEUS_CONTAINER

        encoded = self._prometheus_config().encode()
        b64 = base64.b64encode(encoded).decode()
        self.executor.run_remote(host_name, f"sudo mkdir -p {config_dir} {data_dir}")
        self.executor.run_remote(
            host_name,
            f"echo {b64} | base64 -d | sudo tee {config_path} > /dev/null",
        )
        self.executor.run_remote(host_name, f"sudo chmod 0644 {config_path}")
        # Prometheus runs as UID 65534 (nobody) in the upstream image.
        self.executor.run_remote(
            host_name,
            f"sudo chown 65534:65534 {data_dir}",
        )
        self._remove_container(host_name, container)
        self._ensure_registry_login(host_name, image)

        run_cmd = (
            f"sudo podman run -d --name {container} --network host "
            f"-v {config_path}:/etc/prometheus/prometheus.yml:ro,Z "
            f"-v {data_dir}:/prometheus:Z "
            f"{image} "
            f"--config.file=/etc/prometheus/prometheus.yml "
            f"--storage.tsdb.path=/prometheus "
            f"--web.listen-address=0.0.0.0:{port}"
        )
        print(f"[{host_name}] Starting Prometheus container on port {port}...")
        self.executor.run_remote(host_name, run_cmd, check=True)
        self._verify_container_running(host_name, container)

    def _grafana_provisioning_files(self, prometheus_url):
        datasource = {
            "apiVersion": 1,
            "datasources": [
                {
                    "name": "Dashboard1",
                    "type": "prometheus",
                    "access": "proxy",
                    "orgId": 1,
                    "url": prometheus_url,
                    "basicAuth": False,
                    "isDefault": True,
                    "editable": False,
                }
            ],
        }
        dashboards = {
            "apiVersion": 1,
            "providers": [
                {
                    "name": "Ceph Dashboard",
                    "orgId": 1,
                    "folder": "",
                    "type": "file",
                    "disableDeletion": False,
                    "updateIntervalSeconds": 3,
                    "editable": False,
                    "options": {"path": self.CEPH_DASHBOARDS_PATH},
                }
            ],
        }
        dump = lambda cfg: yaml.dump(cfg, default_flow_style=False, sort_keys=False)
        return {
            "datasources/ceph-dashboard.yml": dump(datasource),
            "dashboards/default.yml": dump(dashboards),
        }

    def _write_grafana_provisioning(self, host_name, prometheus_url):
        base_dir = self.GRAFANA_PROVISIONING_DIR
        for rel_path, content in self._grafana_provisioning_files(
            prometheus_url
        ).items():
            path = f"{base_dir}/{rel_path}"
            parent = os.path.dirname(path)
            self.executor.run_remote(host_name, f"sudo mkdir -p {parent}")
            b64 = base64.b64encode(content.encode()).decode()
            self.executor.run_remote(
                host_name,
                f"echo {b64} | base64 -d | sudo tee {path} > /dev/null",
            )
            self.executor.run_remote(host_name, f"sudo chmod 0644 {path}")

    def _deploy_grafana_dashboards(self, host_name):
        source_dir = self.DASHBOARDS_SOURCE_DIR
        if not any(source_dir.glob("*.json")):
            raise RuntimeError(
                f"No Ceph Grafana dashboards found in {source_dir}. "
                "Expected JSON files from ceph monitoring/ceph-mixin/dashboards_out."
            )

        remote_dir = self.GRAFANA_DASHBOARDS_HOST_DIR
        self.executor.run_remote(host_name, f"sudo mkdir -p {remote_dir}")
        u, h, p = self.executor.get_ssh_details(host_name)
        print(f"[{host_name}] Copying Ceph Grafana dashboards to {remote_dir}...")
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(p),
                "-r",
                f"{source_dir}/.",
                f"{u}@{h}:{remote_dir}/",
            ],
            check=True,
        )
        self.executor.run_remote(
            host_name,
            f"sudo find {remote_dir} -type f -name '*.json' -exec chmod 0644 {{}} +",
        )

    def _deploy_grafana(self, host_name):
        port = self.config.grafana_port
        image = self.config.grafana_image
        container = self.GRAFANA_CONTAINER
        prometheus_url = (
            f"http://127.0.0.1:{self.config.prometheus_port}"
        )
        anon = "true" if self.config.grafana_anonymous_access else "false"
        timezone = shlex.quote(self.config.grafana_timezone)
        provisioning_dir = self.GRAFANA_PROVISIONING_DIR
        dashboards_dir = self.GRAFANA_DASHBOARDS_HOST_DIR

        self._remove_container(host_name, container)
        self._deploy_grafana_dashboards(host_name)
        self._write_grafana_provisioning(host_name, prometheus_url)
        self._ensure_registry_login(host_name, image)
        run_cmd = (
            f"sudo podman run -d --name {container} --network host "
            f"-v {provisioning_dir}:/etc/grafana/provisioning:ro,Z "
            f"-v {dashboards_dir}:{self.CEPH_DASHBOARDS_PATH}:ro,Z "
            f"-e GF_AUTH_ANONYMOUS_ENABLED={anon} "
            f"-e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin "
            f"-e GF_SERVER_HTTP_PORT={port} "
            f"-e GF_DATE_FORMATS_DEFAULT_TIMEZONE={timezone} "
            f"-e GF_SECURITY_ALLOW_EMBEDDING=true "
            f"-e GF_INSTALL_PLUGINS= "
            f"{image}"
        )
        print(
            f"[{host_name}] Starting Grafana container on port {port} "
            f"(prometheus at {prometheus_url})..."
        )
        self.executor.run_remote(host_name, run_cmd, check=True)
        self._verify_container_running(host_name, container)

    def _remove_container(self, host_name, container):
        self.executor.run_remote(
            host_name,
            f"sudo podman rm -f {container} 2>/dev/null || true",
        )
