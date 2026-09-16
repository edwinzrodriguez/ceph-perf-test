import textwrap

from lib.grafana.grafana_manager import GrafanaManager


class GrafanaSystemdManager(GrafanaManager):
    """Deploy monitoring for non-cephadm MDS/systemd clusters.

    - ceph-exporter runs as a local process on mon/mgr/mds/osd hosts
    - Prometheus and Grafana run as podman containers on grafana hosts
    """

    PROMETHEUS_CONTAINER = "ceph-perf-prometheus"
    GRAFANA_CONTAINER = "ceph-perf-grafana"
    EXPORTER_PID_PATH = "/var/run/ceph/ceph-exporter.pid"

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
        for host_name in self.exporter_hosts():
            self._start_ceph_exporter(host_name)
        for host_name in self.grafana_hosts:
            self._deploy_prometheus(host_name)
            self._deploy_grafana(host_name)
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

    def _start_ceph_exporter(self, host_name):
        binary = self._exporter_binary()
        prio = self.config.grafana_exporter_prio_limit
        port = self.config.grafana_exporter_port
        pid_path = self.EXPORTER_PID_PATH
        log_path = "/var/log/ceph/ceph-exporter.log"
        conf = self.config.ceph_conf_path

        self.executor.run_remote(host_name, "sudo mkdir -p /var/run/ceph /var/log/ceph")
        self._stop_ceph_exporter(host_name)

        start_cmd = (
            f"nohup {binary} -c {conf} "
            f"--sock-dir /var/run/ceph --prio-limit {prio} "
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

        return textwrap.dedent(
            f"""\
            global:
              scrape_interval: 15s

            scrape_configs:
              - job_name: ceph
                honor_labels: true
                static_configs:
                  - targets:
            {self._yaml_list(mgr_targets)}
              - job_name: ceph-exporter
                static_configs:
                  - targets:
            {self._yaml_list(exporter_targets)}
            """
        )

    @staticmethod
    def _yaml_list(items):
        if not items:
            return "                    []"
        return "\n".join(f"                      - {item}" for item in items)

    def _deploy_prometheus(self, host_name):
        config_dir = "/etc/ceph/monitoring"
        config_path = f"{config_dir}/prometheus.yml"
        port = self.config.prometheus_port
        image = self.config.grafana_prometheus_image
        container = self.PROMETHEUS_CONTAINER

        encoded = self._prometheus_config().encode()
        import base64

        b64 = base64.b64encode(encoded).decode()
        self.executor.run_remote(host_name, f"sudo mkdir -p {config_dir}")
        self.executor.run_remote(
            host_name,
            f"echo {b64} | base64 -d | sudo tee {config_path} > /dev/null",
        )
        self.executor.run_remote(host_name, f"sudo chmod 0644 {config_path}")
        self._remove_container(host_name, container)
        self._ensure_registry_login(host_name, image)

        run_cmd = (
            f"sudo podman run -d --name {container} --network host "
            f"-v {config_path}:/etc/prometheus/prometheus.yml:ro "
            f"{image} "
            f"--config.file=/etc/prometheus/prometheus.yml "
            f"--web.listen-address=:{port}"
        )
        print(f"[{host_name}] Starting Prometheus container on port {port}...")
        self.executor.run_remote(host_name, run_cmd, check=True)

    def _deploy_grafana(self, host_name):
        port = self.config.grafana_port
        image = self.config.grafana_image
        container = self.GRAFANA_CONTAINER
        prometheus_url = (
            f"http://127.0.0.1:{self.config.prometheus_port}"
        )
        anon = "true" if self.config.grafana_anonymous_access else "false"

        self._remove_container(host_name, container)
        self._ensure_registry_login(host_name, image)
        run_cmd = (
            f"sudo podman run -d --name {container} --network host "
            f"-e GF_AUTH_ANONYMOUS_ENABLED={anon} "
            f"-e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin "
            f"-e GF_SERVER_HTTP_PORT={port} "
            f"-e GF_SECURITY_ALLOW_EMBEDDING=true "
            f"-e GF_INSTALL_PLUGINS= "
            f"{image}"
        )
        print(
            f"[{host_name}] Starting Grafana container on port {port} "
            f"(prometheus at {prometheus_url})..."
        )
        self.executor.run_remote(host_name, run_cmd, check=True)

    def _remove_container(self, host_name, container):
        self.executor.run_remote(
            host_name,
            f"sudo podman rm -f {container} 2>/dev/null || true",
        )
