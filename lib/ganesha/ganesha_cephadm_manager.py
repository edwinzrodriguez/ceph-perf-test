import json
import os
import subprocess
import time
import yaml
from lib.ganesha.ganesha_manager import GaneshaManager
from cephfs_perf_lib import CephFSManager, CommonUtils


class GaneshaCephadmManager(GaneshaManager):
    def provision_ganesha(self, use_custom=True, results_dir=None):
        if self._provisioned:
            print("Ganesha already provisioned. Skipping.")
            return
        sid = self.config.ganesha_service_id
        # Ceph CLI might restrict manual creation of pools starting with '.',
        # but the NFS orchestrator often expects it. We'll try to create it,
        # but we'll mainly rely on the orchestrator to handle its own pool if possible.
        ceph_bin = self.config.ganesha_ceph_binary_path
        self.executor.run_remote(
            self.admin, f"sudo {ceph_bin} osd pool create .nfs --yes-i-really-mean-it || true"
        )
        self.executor.run_remote(
            self.admin, f"sudo {ceph_bin} osd pool application enable .nfs nfs || true"
        )
        if use_custom:
            for host_name in self.ganeshas:
                # Create a minimal ceph.conf for this ganesha host
                # We use a pattern here because ganesha-$cluster-$name.asok is used in cephadm
                # However, for simplicity and since we control the volume mount,
                # let's use a fixed path that we can point to in Ceph_Conf.
                # Cephadm ganesha container has /etc/ceph mounted.
                ganesha_ceph_conf = f"/etc/ceph/ganesha-ceph-{host_name}.conf"
                ceph_bin = self.config.ganesha_ceph_binary_path
                self.executor.run_remote(host_name, f"sudo {ceph_bin} config generate-minimal-conf | sudo tee {ganesha_ceph_conf} > /dev/null")
                
                # In cephadm, the asok name is ganesha-$cluster-$name.asok
                client_section = f"\n[client.{self.config.ganesha_user_id}]\n    admin-socket = /var/run/ceph/ganesha-$cluster-$name.asok\n"
                if self.config.ganesha_keyring_path:
                    client_section += f"    keyring = {self.config.ganesha_keyring_path}\n"
                if self.config.ganesha_client_oc_size:
                    oc_size = CommonUtils.parse_si_unit(self.config.ganesha_client_oc_size)
                    client_section += f"    client_oc_size = {oc_size}\n"
                
                escaped_client_section = client_section.replace("'", "'\\''")
                # We use sudo bash to allow variable expansion if needed, but here we want literal $cluster and $name
                # actually, we want them literal in the file so ganesha expands them? 
                # No, ceph library expands them.
                self.executor.run_remote(host_name, f"printf '{escaped_client_section}' | sudo tee -a {ganesha_ceph_conf} > /dev/null")
                self.executor.run_remote(host_name, f"sudo chmod 0644 {ganesha_ceph_conf}")

            self.setup_ganesha_config()
        self.generate_ganesha_yaml(sid, self.ganeshas, use_custom)
        ypath = self.config.ganesha_yaml_path
        ceph_bin = self.config.ganesha_ceph_binary_path
        self.executor.run_remote(self.admin, f"sudo {ceph_bin} orch apply -i {ypath}")

        # Wait for the NFS service to be running BEFORE applying exports
        print(f"Waiting for NFS service {sid} to be running...")
        for _ in range(30):
            ceph_bin = self.config.ganesha_ceph_binary_path
            svcs = self.safe_json_load(
                self.executor.run_remote(
                    self.admin, f"sudo {ceph_bin} orch ls --service_type nfs --format json"
                )
            )
            if any(
                    s.get("service_id") == sid and s.get("status", {}).get("running", 0) > 0
                    for s in svcs
            ):
                break
            time.sleep(10)

        for idx, fs in enumerate(CephFSManager(self.executor, self.config).fs_names):
            exp = {
                "export_id": 100 + idx,
                "path": "/",
                "pseudo": f"/{fs}-export",
                "access_type": "RW",
                "squash": "no_root_squash",
                "protocols": [4],
                "transports": ["TCP"],
                'fsal': {
                    'name': 'CEPH',
                    'fs_name': fs,
                    'cmount_path': '/',
                    'user_id': self.config.ganesha_user_id
                },
                "clients": [
                    {
                        "addresses": ["*"],
                        "access_type": "RW",
                        "squash": "no_root_squash",
                    }
                ],
            }
            with open(f"/tmp/export_{fs}.json", "w") as f:
                json.dump(exp, f)
            u, h, p = self.executor.get_ssh_details(self.admin)
            subprocess.run(
                [
                    "scp",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-P",
                    p,
                    f"/tmp/export_{fs}.json",
                    f"{u}@{h}:/cephfs_perf/sfs2020/export_{fs}.json",
                ]
            )
            # Retry applying export as it may fail if the .nfs pool or NFS cluster is not ready
            for i in range(12):  # Increased retries to 12 (2 mins total)
                try:
                    ceph_bin = self.config.ganesha_ceph_binary_path
                    self.executor.run_remote(
                        self.admin,
                        f"sudo {ceph_bin} nfs export apply {sid} -i /cephfs_perf/sfs2020/export_{fs}.json",
                        check=True,
                    )
                    break
                except Exception as e:
                    if i == 11:
                        raise
                    print(f"Retrying export apply for {fs} ({i + 1}/12): {e}")
                    time.sleep(10)
        ceph_bin = self.config.ganesha_ceph_binary_path
        self.executor.run_remote(self.admin, f"sudo {ceph_bin} orch restart nfs.{sid}")

        # After ganesha starts, run 'config diff' via the admin socket and store results in the output directory
        print("Waiting for Ganesha nodes to start and admin socket to be available...")
        for g_host in self.ganeshas:
            asok_pattern = "/var/run/ceph/ganesha-*.asok"
            print(f"[{g_host}] Waiting for Ganesha admin socket matching {asok_pattern}...")
            for i in range(30):
                cmd = f"ls {asok_pattern} | grep -v 'client.admin.asok' | head -n 1"
                asok_path = self.executor.run_remote(g_host, cmd).strip()
                if asok_path and "No such file" not in asok_path:
                    print(f"[{g_host}] Ganesha admin socket {asok_path} is available.")
                    break
                if i == 29:
                    print(f"[{g_host}] Warning: Ganesha admin socket NOT found after 300 seconds.")
                time.sleep(10)

        print("Collecting Ganesha 'config diff' from all ganesha nodes...")
        if results_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")
        for g_host in self.ganeshas:
            cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
            asok_path = self.executor.run_remote(g_host, cmd).strip()

            if not asok_path or "No such file" in asok_path:
                print(f"[{g_host}] Warning: Ganesha admin socket not found for 'config diff'.")
                continue

            print(f"[{g_host}] Running 'config diff' via {asok_path}...")
            ceph_bin = self.config.ganesha_ceph_binary_path
            diff_output = self.executor.run_remote(g_host, f"sudo {ceph_bin} --admin-daemon {asok_path} config diff")

            filename = f"ganesha_config_diff_{g_host}.json"
            local_temp = f"/tmp/{filename}"
            with open(local_temp, "w") as f:
                f.write(diff_output)

            u, h, p = self.executor.get_ssh_details(self.admin)
            remote_path = f"{results_dir}/{filename}" if results_dir else f"/cephfs_perf/sfs2020/{filename}"
            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", str(p), local_temp, f"{u}@{h}:{remote_path}"])
            os.remove(local_temp)
            print(f"[{g_host}] Config diff saved to {self.admin}:{remote_path}")

        self._provisioned = True

    def cleanup_ganesha(self):
        self._provisioned = False
        sid = self.config.ganesha_service_id
        ceph_bin = self.config.ganesha_ceph_binary_path
        exps = self.safe_json_load(
            self.executor.run_remote(
                self.admin, f"sudo {ceph_bin} nfs export ls {sid} --format json"
            )
        )
        for e in exps:
            self.executor.run_remote(
                self.admin,
                f"sudo {ceph_bin} nfs export rm {sid} {e.get('path') if isinstance(e, dict) else e}",
            )
        self.executor.run_remote(self.admin, f"sudo {ceph_bin} orch rm nfs.{sid} || true")

    def setup_ganesha_config(self):
        print("Setting up custom Ganesha configuration on ganesha nodes...")
        worker_threads = self.config.ganesha_worker_threads
        worker_threads_block = (
            "    _9P {\n"
            f"        Nb_Worker = {worker_threads};\n"
            "    }\n"
        ) if worker_threads else ""

        # Add CEPH block for top-level settings if any are defined
        ceph_block = ""
        ceph_options = ""
        # In cephadm, the custom.conf is mounted at /etc/ganesha/custom.conf
        # The ganesha-ceph-{host_name}.conf is mounted at /etc/ceph/ganesha-ceph-{host_name}.conf
        # But wait, ganesha.conf is global for the service.
        # If we have multiple hosts, we can't easily have different Ceph_Conf in one shared config
        # UNLESS we use a macro or if ganesha supports it.
        # Actually, cephadm mounts /etc/ceph/ceph.conf.
        # If we use a host-specific name, we need to know WHICH host we are on.
        # Ganesha config doesn't easily support host-specific includes based on hostname.
        
        # If we use /etc/ceph/ganesha-ceph.conf and make it host-specific on each host...
        ceph_options += "    Ceph_Conf = /etc/ceph/ganesha-ceph.conf;\n"

        if self.config.ganesha_umask is not None:
            ceph_options += f"    umask = {self.config.ganesha_umask};\n"
        if self.config.ganesha_client_oc is not None:
            val = "true" if self.config.ganesha_client_oc else "false"
            ceph_options += f"    client_oc = {val};\n"
        if self.config.ganesha_async is not None:
            val = "true" if self.config.ganesha_async else "false"
            ceph_options += f"    async = {val};\n"
        if self.config.ganesha_zerocopy is not None:
            val = "true" if self.config.ganesha_zerocopy else "false"
            ceph_options += f"    zerocopy = {val};\n"

        if ceph_options:
            ceph_block = f"CEPH {{\n{ceph_options}}}\n"

        config_content = (
            "NFS_Core_Param {\n"
            "    Protocols = 4;\n"
            "    Enable_NLM = false;\n"
            "    Enable_RQUOTA = false;\n"
            "    NFS_Port = 2049;\n"
            "    allow_set_io_flusher_fail = true;\n"
            "}\n"
            f"{worker_threads_block}"
            "NFSv4 {\n"
            '    RecoveryBackend = "rados_cluster";\n'
            "    Minor_Versions = 1, 2;\n"
            "}\n"
            f"{ceph_block}"
            "RADOS_KV {\n"
            "    nodeid = 0;\n"
            '    pool = ".nfs";\n'
            '    namespace = "ganesha";\n'
            '    UserId = "admin";\n'
            "}\n"
            "RADOS_URLS {\n"
            '    UserId = "admin";\n'
            '    watch_url = "rados://.nfs/ganesha/conf-nfs.ganesha";\n'
            "}\n"
            "# Cephadm will still manage exports via the %url include\n"
            "# but we use our custom global settings\n"
            "%%url rados://.nfs/ganesha/conf-nfs.ganesha\n"
        )

        for host_name in self.ganeshas:
            # Create /etc/ceph if it doesn't exist
            self.executor.run_remote(host_name, "sudo mkdir -p /etc/ceph")

            # Using printf to write the config file
            # We need to escape single quotes and other shell-sensitive characters
            escaped_config = config_content.replace("'", "'\\''")
            cmd = f"printf '{escaped_config}' | sudo tee /etc/ceph/ganesha-custom.conf > /dev/null"
            self.executor.run_remote(host_name, cmd)
            self.executor.run_remote(
                host_name, "sudo chmod 0644 /etc/ceph/ganesha-custom.conf"
            )
            # Also link or copy the host-specific ganesha-ceph-{host_name}.conf to ganesha-ceph.conf
            self.executor.run_remote(
                host_name, f"sudo cp /etc/ceph/ganesha-ceph-{host_name}.conf /etc/ceph/ganesha-ceph.conf"
            )

    def generate_ganesha_yaml(self, sid, hosts, custom=False):
        print(f"Generating ganesha.yaml for {sid} (custom_config={custom})...")

        ganesha_spec = {
            "service_type": "nfs",
            "service_id": sid,
            "placement": {"hosts": hosts},
            "spec": {"port": 2049},
        }

        if custom:
            ganesha_spec.update(
                {
                    "extra_container_args": [
                        "--privileged",
                        "--cap-add",
                        "SYS_MODULE",
                        "-e",
                        "ENABLE_LOCKSTAT=true",
                        "-v",
                        "/sys/kernel/debug:/sys/kernel/debug:rw",
                        "-v",
                        "/usr/src/kernels:/usr/src/kernels:ro",
                        "-v",
                        "/usr/lib/modules:/usr/lib/modules:ro",
                        "-v",
                        "/usr/lib/debug:/usr/lib/debug:ro",
                        "-v",
                        "/etc/ceph:/etc/ceph:z",
                        "-v",
                        "/etc/ceph/ganesha-custom.conf:/etc/ganesha/custom.conf:z",
                        "-v",
                        "/etc/ceph/ganesha-ceph.conf:/etc/ceph/ganesha-ceph.conf:z",
                        "-v",
                        "/var/run/ceph:/var/run/ceph:z",
                        "--env",
                        "GSS_USE_HOSTNAME=0",
                        "--env",
                        "CEPH_CONF=/etc/ceph/ceph.conf",
                        "--entrypoint",
                        "/usr/bin/ganesha.nfsd",
                    ],
                    "extra_entrypoint_args": [
                        "-F",
                        "-L",
                        "STDERR",
                        "-N",
                        "NIV_EVENT",
                        "-f",
                        "/etc/ganesha/custom.conf",
                    ],
                }
            )

        local_ganesha_yaml = "ganesha.yaml"
        with open(local_ganesha_yaml, "w") as f:
            yaml.dump(ganesha_spec, f)

        ypath = self.config.ganesha_yaml_path
        u, h, p = self.executor.get_ssh_details(self.admin)
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                p,
                local_ganesha_yaml,
                f"{u}@{h}:{ypath}",
            ]
        )
