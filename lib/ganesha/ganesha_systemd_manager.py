import json
import os
import subprocess
import time
from lib.ganesha.ganesha_manager import GaneshaManager
from cephfs_perf_lib import CephFSManager


class GaneshaSystemdManager(GaneshaManager):
    def provision_ganesha(self, use_custom=True, results_dir=None):
        if self._provisioned:
            print("Ganesha already provisioned. Skipping.")
            return

        # Ceph CLI might restrict manual creation of pools starting with '.',
        # but the NFS orchestrator often expects it. We'll try to create it,
        # but we'll mainly rely on the orchestrator to handle its own pool if possible.
        self.executor.run_remote(
            self.admin, "sudo ceph osd pool create .nfs --yes-i-really-mean-it || true"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph osd pool application enable .nfs nfs || true"
        )

        print("Setting up Ganesha configuration on ganesha nodes...")
        self.setup_ganesha_config()

        binary = self.config.ganesha_binary_path
        pid_path = self.config.ganesha_pid_path

        # Arguments for environment and parameters
        args = [
            binary,
            "-F",
            "-L",
            "STDOUT",
            "-N",
            "NIV_EVENT",
            "-f",
            "/etc/ganesha/ganesha.conf",
            "-p",
            pid_path,
        ]

        env_vars = (
            "export ENABLE_LOCKSTAT=true; "
            "export GSS_USE_HOSTNAME=0; "
            "export CEPH_CONF=/etc/ceph/ceph.conf; "
        )

        for host_name in self.ganeshas:
            # Create recovery directory
            self.executor.run_remote(host_name, "sudo mkdir -p /usr/local/var/lib/nfs/ganesha")
            self.executor.run_remote(host_name, "sudo chmod 0755 /usr/local/var/lib/nfs/ganesha")

            # If pid_path exists, kill that process then remove the pid file
            cleanup_cmd = f"if [ -f {pid_path} ]; then sudo kill $(cat {pid_path}) || true; sudo rm -f {pid_path}; fi"
            self.executor.run_remote(host_name, cleanup_cmd)

            # We'll use a simpler asok version: ganesha-<hostname>.asok
            ceph_args = f"export CEPH_ARGS='--admin-socket=/var/run/ceph/ganesha-{host_name}.asok'; "

            # Start as a background process with nohup. 
            # We use sudo bash to execute the string with environment variables and background it.
            cmd = f"sudo bash -c '{env_vars} {ceph_args} nohup {' '.join(args)} > /var/log/ganesha.log 2>&1 &'"
            self.executor.run_remote(host_name, cmd, check=True )
            print(f"[{host_name}] Ganesha started with PID file {pid_path}")

        # Collect config diff if results_dir is provided
        if results_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")
            for host_name in self.ganeshas:
                # Wait a bit for asok to appear
                time.sleep(2)

                asok_path = f"/var/run/ceph/ganesha-{host_name}.asok"
                print(f"[{host_name}] Running 'config diff' via {asok_path}...")
                try:
                    diff_output = self.executor.run_remote(
                        host_name, f"sudo ceph --admin-daemon {asok_path} config diff"
                    )
                    filename = f"ganesha_config_diff_{host_name}.json"
                    local_temp = f"/tmp/{filename}"
                    with open(local_temp, "w") as f:
                        f.write(diff_output)

                    u, h, p = self.executor.get_ssh_details(self.admin)
                    remote_path = f"{results_dir}/{filename}"
                    subprocess.run(
                        [
                            "scp",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-P",
                            p,
                            local_temp,
                            f"{u}@{h}:{remote_path}",
                        ]
                    )
                    os.remove(local_temp)
                except Exception as e:
                    print(f"[{host_name}] Failed to collect config diff: {e}")

        self._provisioned = True

    def cleanup_ganesha(self):
        print("Cleaning up Ganesha on ganesha nodes...")
        pid_path = self.config.ganesha_pid_path
        for host_name in self.ganeshas:
            # Kill using pid file
            cmd = f"if [ -f {pid_path} ]; then sudo kill $(cat {pid_path}) || true; sudo rm -f {pid_path}; fi"
            self.executor.run_remote(host_name, cmd)
            # Also cleanup the asok just in case
            self.executor.run_remote(
                host_name, f"sudo rm -f /var/run/ceph/ganesha-{host_name}.asok"
            )
        self._provisioned = False

    def setup_ganesha_config(self):
        # STANDALONE Ganesha configuration without Cephadm URL includes
        worker_threads = self.config.ganesha_worker_threads
        worker_threads_block = (
            "    _9P {\n"
            f"        Nb_Worker = {worker_threads};\n"
            "    }\n"
        ) if worker_threads else ""

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
            "    RecoveryBackend = \"fs\";\n"
            "    Minor_Versions = 1, 2;\n"
            "}\n"
        )

        # Add EXPORT blocks for each filesystem manually
        for idx, fs in enumerate(CephFSManager(self.executor, self.config).fs_names):
            export_block = (
                f"\nEXPORT {{\n"
                f"    Export_ID = {100 + idx};\n"
                f'    Path = "/";\n'
                f'    Pseudo = "/{fs}-export";\n'
                f'    Access_Type = "RW";\n'
                f'    Squash = "no_root_squash";\n'
                f'    Protocols = 4;\n'
                f'    Transports = "TCP";\n'
                f'    FSAL {{\n'
                f'        Name = "CEPH";\n'
                f'        Filesystem = "{fs}";\n'
                f'        User_Id = "admin";\n'
                f'    }}\n'
                f"}}\n"
            )
            config_content += export_block

        for host_name in self.ganeshas:
            self.executor.run_remote(host_name, "sudo mkdir -p /etc/ganesha")
            escaped_config = config_content.replace("'", "'\\''")
            cmd = f"printf '{escaped_config}' | sudo tee /etc/ganesha/ganesha.conf > /dev/null"
            self.executor.run_remote(host_name, cmd)
            self.executor.run_remote(
                host_name, "sudo chmod 0644 /etc/ganesha/ganesha.conf"
            )
