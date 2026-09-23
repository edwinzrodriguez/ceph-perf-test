import os
import subprocess
import time

import yaml

from lib.rgw.rgw_manager import RgwManager


class RgwCephadmManager(RgwManager):
    def provision_rgw(self, results_dir=None):
        if self._provisioned:
            print("RGW already provisioned. Skipping.")
            return
        if not self.rgws:
            raise RuntimeError(
                "No RGW hosts configured in inventory group 'rgws'"
            )

        sid = self.config.rgw_service_id
        label = self.config.rgw_host_label
        count = self.config.rgw_count_per_host
        port = self.config.rgw_frontend_port

        self.open_firewall_ports()
        self._label_hosts(label)
        self._generate_and_apply_spec(sid, label, count, port)
        self.wait_for_rgw_running()
        # Give beast listeners a moment after orch reports running
        time.sleep(5)
        self.ensure_s3_user()
        self._provisioned = True

    def cleanup_rgw(self):
        print("Cleaning up RGW service...")
        self._provisioned = False
        sid = self.config.rgw_service_id
        print(f"Removing RGW service rgw.{sid}...")
        self._run_ceph(f"orch rm rgw.{sid} || true", check=False)

    def _label_hosts(self, label):
        print(f"Ensuring orch host label '{label}' on RGW hosts...")
        for host_name in self.rgws:
            self._run_ceph(
                f"orch host label add {host_name} {label} || true",
                check=False,
            )

    def _generate_and_apply_spec(self, sid, label, count, port):
        print(
            f"Generating RGW cephadm spec "
            f"(service_id={sid}, label={label}, "
            f"count_per_host={count}, port={port})..."
        )
        rgw_spec = {
            "service_type": "rgw",
            "service_id": sid,
            "placement": {
                "label": label,
                "count_per_host": int(count),
            },
            "spec": {
                "rgw_frontend_port": int(port),
            },
        }

        local_path = "rgw.yaml"
        with open(local_path, "w") as f:
            yaml.safe_dump(rgw_spec, f, sort_keys=False)

        remote_path = self.config.rgw_yaml_path
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

        print(f"Applying RGW service from {self.admin}:{remote_path}...")
        self._run_ceph(f"orch apply -i {remote_path}", check=True)
