import json
import os
import subprocess
import time
from lib.ganesha.ganesha_manager import GaneshaManager
from cephfs_perf_lib import CommonUtils, FSManager


class GaneshaSystemdManager(GaneshaManager):
    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)

    def provision_ganesha(self, use_custom=True, results_dir=None):
        if self._provisioned:
            print("Ganesha already provisioned. Skipping.")
            return

        # Ceph CLI might restrict manual creation of pools starting with '.',
        # but the NFS orchestrator often expects it. We'll try to create it,
        # but we'll mainly rely on the orchestrator to handle its own pool if possible.
        ceph_bin = self.config.ganesha_ceph_binary_path
        # self.executor.run_remote(
        #     self.admin,
        #     f"sudo {ceph_bin} {self._get_ceph_args()} osd pool create .nfs --yes-i-really-mean-it || true",
        # )
        # self.executor.run_remote(
        #     self.admin, f"sudo {ceph_bin} {self._get_ceph_args()} osd pool application enable .nfs nfs || true"
        # )

        print("Setting up Ganesha configuration on ganesha nodes...")
        # setup_ganesha_config is called per host in the loop below
        # to support per-host Ceph_Conf paths

        binary = self.config.ganesha_binary_path
        pid_path = self.config.ganesha_pid_path

        # Arguments for environment and parameters
        if self.config.ganesha_log_level:
            args = [binary, "-f", "/etc/ganesha/ganesha.conf", "-p", pid_path]
        else:
            args = [binary, "-F", "-L", "STDOUT", "-N", "NIV_EVENT",
                    "-f", "/etc/ganesha/ganesha.conf", "-p", pid_path]

        default_env = {
            "ENABLE_LOCKSTAT": "true",
            "GSS_USE_HOSTNAME": "0",
            "CEPH_CONF": self.config.ceph_conf_path,
        }
        merged_env = {**default_env, **self.config.ganesha_env_vars}
        env_vars = "".join(f'export {k}="{v}"; ' for k, v in merged_env.items())

        for host_name in self.ganeshas:
            # Create recovery directory
            self.executor.run_remote(
                host_name, "sudo mkdir -p /usr/local/var/lib/nfs/ganesha /var/log/ceph"
            )
            self.executor.run_remote(
                host_name, "sudo chmod 0755 /usr/local/var/lib/nfs/ganesha /var/log/ceph"
            )

            # Ensure /var/run/ceph exists for admin sockets
            self.executor.run_remote(host_name, "sudo mkdir -p /var/run/ceph")
            self.executor.run_remote(host_name, "sudo chmod 0755 /var/run/ceph")

            # Create a minimal ceph.conf for this ganesha host
            asok_path = f"/var/run/ceph/ganesha-{host_name}.asok"
            ganesha_ceph_conf = f"/etc/ceph/ganesha-ceph-{host_name}.conf"
            ceph_bin = self.config.ganesha_ceph_binary_path
            self.executor.run_remote(
                host_name,
                f"sudo {ceph_bin} {self._get_ceph_args()} config generate-minimal-conf | sudo tee {ganesha_ceph_conf} > /dev/null",
            )

            client_section = f"\n[client.{self.config.ganesha_user_id}]\n    admin_socket = {asok_path}\n"
            client_section += f"    log_file = /var/log/ceph/ganesha-ceph-{self.config.ganesha_user_id}.log\n"
            client_section += f"    log_to_file = true\n"
            client_section += f"    log_to_stderr = false\n"
            client_section += f"    log_to_syslog = false\n"
            client_section += f"    debug_client = {self.config.ganesha_client_log_level}\n"
            if self.config.ganesha_finisher_log_level is not None:
                client_section += f"    debug_finisher = {self.config.ganesha_finisher_log_level}\n"
            if self.config.ganesha_keyring_path:
                client_section += f"    keyring = {self.config.ganesha_keyring_path}\n"
            if self.config.ganesha_client_oc_size:
                oc_size = CommonUtils.parse_si_unit(self.config.ganesha_client_oc_size)
                client_section += f"    client_oc_size = {oc_size}\n"
            if self.config.ganesha_msgr_workers:
                client_section += f"    ms_async_op_threads = {self.config.ganesha_msgr_workers}\n"

            escaped_client_section = client_section.replace("'", "'\\''")
            self.executor.run_remote(
                host_name,
                f"printf '{escaped_client_section}' | sudo tee -a {ganesha_ceph_conf} > /dev/null",
            )
            self.executor.run_remote(host_name, f"sudo chmod 0644 {ganesha_ceph_conf}")

            self.setup_ganesha_config(host_name=host_name)

            # If pid_path exists, kill that process then remove the pid file
            cleanup_cmd = f"if [ -f {pid_path} ]; then sudo kill -9 $(cat {pid_path}) || true; sudo rm -f {pid_path}; fi"
            self.executor.run_remote(host_name, cleanup_cmd)

            # Kill any remaining ganesha.nfsd processes not tracked by the pid file
            self.executor.run_remote(
                host_name,
                "pids=$(pgrep -f ganesha.nfsd 2>/dev/null); if [ -n \"$pids\" ]; then sudo kill -9 $pids || true; fi",
            )

            # Start as a background process with nohup.
            # We use sudo bash to execute the string with environment variables and background it.
            redirect = "> /dev/null 2>&1" if self.config.ganesha_log_level else "> /var/log/ganesha.log 2>&1"
            cmd = f"sudo bash -c 'ulimit -c unlimited; {env_vars} nohup {' '.join(args)} {redirect} &'"
            self.executor.run_remote(host_name, cmd, check=True)
            print(f"[{host_name}] Ganesha started with PID file {pid_path}")

            # Enable lockstat immediately after start
            self.start_lockstat(host_name)

            # Wait for the admin socket to appear, indicating Ganesha has started
            asok_path = f"/var/run/ceph/ganesha-{host_name}.asok"
            print(f"[{host_name}] Waiting for Ganesha admin socket {asok_path}...")
            for i in range(30):
                check_asok = f"test -S {asok_path}"
                try:
                    self.executor.run_remote(host_name, check_asok, check=True)
                    print(
                        f"[{host_name}] Ganesha admin socket {asok_path} is available."
                    )
                    break
                except Exception:
                    if i == 29:
                        print(
                            f"[{host_name}] Warning: Ganesha admin socket {asok_path} NOT found after 30 seconds."
                        )
                    time.sleep(1)

        # Collect config diff if results_dir is provided
        if results_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")
            for host_name in self.ganeshas:
                asok_path = f"/var/run/ceph/ganesha-{host_name}.asok"
                print(f"[{host_name}] Running 'config diff' via {asok_path}...")
                try:
                    ceph_bin = self.config.ganesha_ceph_binary_path
                    diff_output = self.executor.run_remote(
                        host_name,
                        f"sudo {ceph_bin} {self._get_ceph_args(include_keyring=False)} --admin-daemon {asok_path} config diff",
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
                            str(p),
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

    def setup_ganesha_config(self, host_name=None):
        # STANDALONE Ganesha configuration without Cephadm URL includes
        worker_threads = self.config.ganesha_worker_threads
        worker_threads_block = (
            ("    _9P {\n" f"        Nb_Worker = {worker_threads};\n" "    }\n")
            if worker_threads
            else ""
        )

        # Add CEPH block for top-level settings if any are defined
        ceph_block = ""
        ceph_options = ""
        if host_name:
            ceph_options += (
                f"    Ceph_Conf = /etc/ceph/ganesha-ceph-{host_name}.conf;\n"
            )

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

        # Build NFS_Core_Param block with optional rpc_ioq settings
        nfs_core_params = (
            "    Protocols = 4;\n"
            "    Enable_NLM = false;\n"
            "    Enable_RQUOTA = false;\n"
            "    NFS_Port = 2049;\n"
            "    allow_set_io_flusher_fail = true;\n"
        )

        if self.config.ganesha_rpc_ioq_thrdmin is not None:
            nfs_core_params += f"    rpc_ioq_thrdmin = {self.config.ganesha_rpc_ioq_thrdmin};\n"
        if self.config.ganesha_rpc_ioq_thrdmax is not None:
            nfs_core_params += f"    rpc_ioq_thrdmax = {self.config.ganesha_rpc_ioq_thrdmax};\n"

        log_block = ""
        if self.config.ganesha_log_level:
            lvl = self.config.ganesha_log_level
            log_block = (
                f"LOG {{\n"
                f"    Default_log_level = INFO;\n"
                f"\n"
                f"    FACILITY {{\n"
                f"        name = FILE;\n"
                f"        destination = /var/log/ganesha.log;\n"
                f"        max_level = FULL_DEBUG;\n"
                f"        enable = active;\n"
                f"    }}\n"
                f"\n"
                f"    COMPONENTS {{\n"
                f"        NFS_V4 = {lvl};\n"
                f"        FSAL = {lvl};\n"
                f"        DISPATCH = DEBUG;\n"
                f"\n"
                f"        # Useful for slot/session diagnosis without full noise:\n"
                f"        SESSIONS = DEBUG;\n"
                f"        CLIENTID = DEBUG;\n"
                f"        STATE = DEBUG;\n"
                f"\n"
                f"        # Keep these quiet (main sources of log flood):\n"
                f"        TIRPC = INFO;\n"
                f"        RW_LOCK = EVENT;\n"
                f"        HASHTABLE = EVENT;\n"
                f"        HASHTABLE_CACHE = EVENT;\n"
                f"        HT_CACHE = EVENT;\n"
                f"        MDCACHE = INFO;\n"
                f"        MDCACHE_LRU = EVENT;\n"
                f"        DUPREQ = INFO;\n"
                f"        XPRT = INFO;\n"
                f"        EXPORT = INFO;\n"
                f"        FILEHANDLE = EVENT;\n"
                f"    }}\n"
                f"\n"
                f"    Display_UTC_Timestamp = true;\n"
                f"\n"
                f"    FORMAT {{\n"
                f"        date_format = syslog_usec;\n"
                f"        time_format = syslog_usec;\n"
                f"\n"
                f"        EPOCH = true;\n"
                f"        HOSTNAME = true;\n"
                f"        PROGNAME = true;\n"
                f"        PID = false;\n"
                f"        THREAD_NAME = true;\n"
                f"        FILE_NAME = false;\n"
                f"        LINE_NUM = false;\n"
                f"        FUNCTION_NAME = true;\n"
                f"        COMPONENT = true;\n"
                f"        LEVEL = true;\n"
                f"    }}\n"
                f"}}\n"
            )

        config_content = (
            f"NFS_Core_Param {{\n"
            f"{nfs_core_params}"
            f"}}\n"
            f"{worker_threads_block}"
            "NFSv4 {\n"
            '    RecoveryBackend = "fs";\n'
            "    Minor_Versions = 1, 2;\n"
            "}\n"
            f"{ceph_block}"
            f"{log_block}"
        )

        # Add EXPORT blocks for each filesystem manually
        fs_names = self.get_fs_names()
        for idx, fs in enumerate(fs_names):
            export_block = (
                f"\nEXPORT {{\n"
                f"    Export_ID = {100 + idx};\n"
                f'    Path = "/";\n'
                f'    Pseudo = "/{fs}-export";\n'
                f'    Access_Type = "RW";\n'
                f'    Squash = "no_root_squash";\n'
                f"    Protocols = 4;\n"
                f'    Transports = "TCP";\n'
                f"    FSAL {{\n"
                f'        Name = "CEPH";\n'
                f'        Filesystem = "{fs}";\n'
                f'        User_Id = "{self.config.ganesha_user_id}";\n'
            )

            export_block += f"    }}\n" f"}}\n"
            config_content += export_block

        for host_name in self.ganeshas:
            self.executor.run_remote(host_name, "sudo mkdir -p /etc/ganesha")
            escaped_config = config_content.replace("'", "'\\''")
            cmd = f"printf '{escaped_config}' | sudo tee /etc/ganesha/ganesha.conf > /dev/null"
            self.executor.run_remote(host_name, cmd)
            self.executor.run_remote(
                host_name, "sudo chmod 0644 /etc/ganesha/ganesha.conf"
            )
