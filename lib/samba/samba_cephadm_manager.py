import os
import subprocess
import time

import yaml

from lib.samba.samba_manager import SambaManager


class SambaCephadmManager(SambaManager):
    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)

    def _users_groups_id(self):
        return f"{self.config.samba_cluster_id}-users"

    def _build_provision_resources(self):
        cluster_id = self.config.samba_cluster_id
        ug_id = self._users_groups_id()
        group = self.config.samba_subvolume_group
        resources = [
            {
                "resource_type": "ceph.smb.usersgroups",
                "users_groups_id": ug_id,
                "values": {
                    "users": [
                        {
                            "name": self.config.samba_username,
                            "password": self.config.samba_password,
                        }
                    ],
                    "groups": [],
                },
            },
            {
                "resource_type": "ceph.smb.cluster",
                "cluster_id": cluster_id,
                "auth_mode": "user",
                "user_group_settings": [
                    {"source_type": "resource", "ref": ug_id}
                ],
                "placement": {"hosts": self.sambas},
                "clustering": self.config.samba_clustering,
            },
        ]
        for fs in self.get_fs_names():
            share_id = self.share_id_for_fs(fs)
            share_name = self.client_share_name_for_fs(fs)
            subvol = self.subvolume_name_for_fs(fs)
            share = {
                "resource_type": "ceph.smb.share",
                "cluster_id": cluster_id,
                "share_id": share_id,
                "cephfs": {
                    "volume": fs,
                    "subvolumegroup": group,
                    "subvolume": subvol,
                    "path": "/",
                },
            }
            if share_name != share_id:
                share["name"] = share_name
            resources.append(share)
        return {"resources": resources}

    def _build_cleanup_resources(self, share_ids=None):
        cluster_id = self.config.samba_cluster_id
        ug_id = self._users_groups_id()
        share_ids = set(share_ids or [])
        for fs in self.get_fs_names():
            share_ids.add(self.share_id_for_fs(fs))

        resources = [
            {
                "resource_type": "ceph.smb.share",
                "cluster_id": cluster_id,
                "share_id": share_id,
                "intent": "removed",
            }
            for share_id in sorted(share_ids)
        ]
        resources.extend(
            [
                {
                    "resource_type": "ceph.smb.cluster",
                    "cluster_id": cluster_id,
                    "intent": "removed",
                },
                {
                    "resource_type": "ceph.smb.usersgroups",
                    "users_groups_id": ug_id,
                    "intent": "removed",
                },
            ]
        )
        return {"resources": resources}

    def _apply_resources(self, resources_doc, check=True):
        local_path = "samba-resources.yaml"
        with open(local_path, "w") as f:
            yaml.safe_dump(resources_doc, f, sort_keys=False)

        remote_path = self.config.samba_resources_path
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
        print(f"Applying SMB resources from {self.admin}:{remote_path}...")
        return self._run_ceph(f"smb apply -i {remote_path}", check=check)

    def provision_samba(self, results_dir=None):
        if self._provisioned:
            print("Samba already provisioned. Skipping.")
            return
        if not self.sambas:
            raise RuntimeError("No Samba hosts configured in inventory group 'sambas'")

        cluster_id = self.config.samba_cluster_id
        self._run_ceph("mgr module enable smb || true")
        self._ensure_smb_subvolumes()
        self._apply_resources(self._build_provision_resources(), check=True)

        print(f"Waiting for SMB service {cluster_id} to be running...")
        self._wait_for_smb_running(cluster_id)
        self._provisioned = True

    def cleanup_samba(self):
        self._provisioned = False
        cluster_id = self.config.samba_cluster_id
        share_ids = set()
        shares = self.safe_json_load(
            self._run_ceph(f"smb share ls {cluster_id} --format json"),
            default=[],
        )
        if isinstance(shares, list):
            for share in shares:
                share_id = (
                    share
                    if isinstance(share, str)
                    else share.get("share_id", share)
                )
                if share_id:
                    share_ids.add(share_id)

        cleanup_doc = self._build_cleanup_resources(share_ids)
        if cleanup_doc["resources"]:
            self._apply_resources(cleanup_doc, check=False)

    def _wait_for_smb_running(self, cluster_id, timeout_iters=30, sleep_secs=10):
        for _ in range(timeout_iters):
            svcs = self.safe_json_load(
                self._run_ceph("orch ls --service_type smb --format json"),
                default=[],
            )
            if any(
                s.get("service_name", "").endswith(f".{cluster_id}")
                and s.get("status", {}).get("running", 0) > 0
                for s in svcs
            ):
                return
            time.sleep(sleep_secs)
        print(
            f"Warning: SMB service {cluster_id} may not be fully running after "
            f"{timeout_iters * sleep_secs}s"
        )
