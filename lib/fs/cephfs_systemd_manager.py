"""Deploy and manage local ceph-mds processes (non-cephadm).

Mirrors the approach used by ``vstart.sh`` ``start_mds()``: create an MDS
auth keyring, then launch ``ceph-mds -i <id> -c <conf>`` on each MDS host.
"""

import re
import time
from cephfs_perf_lib import CommonUtils
from lib.fs.cephfs_manager import CephFSManager


class CephFSSystemdManager(CephFSManager):
    """CephFS manager that runs local ``ceph-mds`` processes on MDS hosts.

    Configuration (under ``mds:`` in the YAML settings file)::

        mds:
          binary_path: /usr/local/bin/ceph-mds
          ceph_binary_path: /usr/local/bin/ceph   # optional, defaults to ceph
          data_dir: /var/lib/ceph/mds             # keyring parent dir
          log_dir: /var/log/ceph
          run_dir: /var/run/ceph
          env_vars:
            LD_LIBRARY_PATH: "/usr/local/lib:${LD_LIBRARY_PATH}"
            ENABLE_LOCKSTAT: "true"
    """

    def _mds_cfg(self):
        return self.config.get("mds", {}) or {}

    def _binary_path(self):
        # Expand ${CEPH_INSTALL_PREFIX} etc. from top-level env_vars
        return self.config.expand_env(
            self._mds_cfg().get("binary_path", "/usr/local/bin/ceph-mds")
        )

    def _ceph_binary(self):
        # Prefer mds.ceph_binary_path; fall back to base CephFSManager resolution
        path = self._mds_cfg().get("ceph_binary_path")
        if path:
            return self.config.expand_env(path)
        return self._ceph_bin()

    def _data_dir(self):
        return self._mds_cfg().get("data_dir", "/var/lib/ceph/mds")

    def _log_dir(self):
        return self._mds_cfg().get("log_dir", "/var/log/ceph")

    def _run_dir(self):
        return self._mds_cfg().get("run_dir", "/var/run/ceph")

    def _env_vars(self):
        default_env = {
            "ENABLE_LOCKSTAT": "true",
            "CEPH_CONF": self.config.ceph_conf_path,
        }
        user_env = self._mds_cfg().get("env_vars", {}) or {}
        # Top-level env_vars (CEPH_INSTALL_PREFIX, LD_LIBRARY_PATH, PATH) first,
        # then defaults / mds.env_vars so local overrides win.
        return self.config.get_merged_env_vars(default_env, user_env)

    def _mds_id(self, fs, host_name, index):
        # Stable, filesystem-scoped id (host-based to aid multi-host placement)
        safe_host = host_name.replace(".", "-")
        return f"{fs}.{safe_host}.{index}"

    def _keyring_path(self, mds_id):
        return f"{self._data_dir()}/ceph-{mds_id}/keyring"

    def _pid_path(self, mds_id):
        return f"{self._run_dir()}/mds.{mds_id}.pid"

    def _log_path(self, mds_id):
        return f"{self._log_dir()}/ceph-mds.{mds_id}.log"

    def _asok_path(self, mds_id):
        return f"{self._run_dir()}/ceph-mds.{mds_id}.asok"

    def _kill_mds(self, host_name, mds_id):
        pid_path = self._pid_path(mds_id)
        binary = self._binary_path()
        # Prefer pid file, then fall back to matching process (escape for pgrep regex)
        id_re = re.escape(mds_id)
        bin_re = re.escape(binary)
        self.executor.run_remote(
            host_name,
            f"if [ -f {pid_path} ]; then "
            f"sudo kill $(cat {pid_path}) 2>/dev/null || true; "
            f"sudo rm -f {pid_path}; fi",
        )
        # Catch any leftover process for this id; escalate if it does not exit
        self.executor.run_remote(
            host_name,
            f"pids=$(pgrep -f '{bin_re}.*-i {id_re}( |$)' 2>/dev/null || true); "
            f"if [ -n \"$pids\" ]; then sudo kill $pids 2>/dev/null || true; fi; "
            f"for i in 1 2 3 4 5; do "
            f"  pids=$(pgrep -f '{bin_re}.*-i {id_re}( |$)' 2>/dev/null || true); "
            f"  [ -z \"$pids\" ] && break; "
            f"  sleep 1; "
            f"done; "
            f"pids=$(pgrep -f '{bin_re}.*-i {id_re}( |$)' 2>/dev/null || true); "
            f"if [ -n \"$pids\" ]; then sudo kill -9 $pids 2>/dev/null || true; fi",
        )
        self.executor.run_remote(
            host_name, f"sudo rm -f {self._asok_path(mds_id)} || true"
        )

    def _remove_mds_service(self, fs):
        instances = self._mds_instances.get(fs, [])
        if not instances:
            # Best-effort cleanup of any MDS processes whose id starts with fs.
            fs_re = re.escape(fs)
            binary = self._binary_path()
            bin_re = re.escape(binary)
            for host_name in self.mdss:
                self.executor.run_remote(
                    host_name,
                    f"pids=$(pgrep -f '{bin_re}.*-i {fs_re}\\.' 2>/dev/null || true); "
                    f"if [ -n \"$pids\" ]; then sudo kill $pids 2>/dev/null || true; fi",
                )
        else:
            for host_name, mds_id in instances:
                print(f"[{host_name}] Stopping local ceph-mds mds.{mds_id}")
                self._kill_mds(host_name, mds_id)
                # Remove auth entity so recreate is clean
                conf = self.config.ceph_conf_path
                self._run_ceph(
                    self.admin,
                    f"-c {conf} auth rm mds.{mds_id} || true",
                )
        self._mds_instances.pop(fs, None)

    def _create_mds_auth(self, mds_id, keyring_path):
        """Create MDS keyring and register it with the cluster (vstart-style)."""
        conf = self.config.ceph_conf_path
        # Caps mirror vstart.sh start_mds():
        # mon 'allow profile mds' osd 'allow rw tag cephfs *=*' mds 'allow' mgr 'allow profile mds'
        caps = (
            "mon 'allow profile mds' "
            "osd 'allow rw tag cephfs *=*' "
            "mds 'allow' "
            "mgr 'allow profile mds'"
        )
        self._run_ceph(
            self.admin,
            f"-c {conf} auth get-or-create mds.{mds_id} {caps} "
            f"-o {keyring_path}",
            check=True,
        )
        self.executor.run_remote(self.admin, f"sudo chmod 0600 {keyring_path}")

    def _start_mds_process(self, host_name, mds_id, fs, settings=None):
        """Start a single ceph-mds process on host_name (similar to vstart run())."""
        binary = self._binary_path()
        conf = self.config.ceph_conf_path
        keyring = self._keyring_path(mds_id)
        pid_path = self._pid_path(mds_id)
        log_path = self._log_path(mds_id)
        asok_path = self._asok_path(mds_id)
        data_dir = f"{self._data_dir()}/ceph-{mds_id}"

        # Ensure runtime directories exist on the MDS host
        self.executor.run_remote(
            host_name,
            f"sudo mkdir -p {data_dir} {self._log_dir()} {self._run_dir()}",
        )

        # Copy keyring from admin to the MDS host if they differ
        if host_name != self.admin:
            u, h, p = self.executor.get_ssh_details(host_name)
            self.executor.run_remote(
                self.admin,
                f"sudo scp -o StrictHostKeyChecking=no -P {p} {keyring} {u}@{h}:/tmp/mds.{mds_id}.keyring",
            )
            self.executor.run_remote(
                host_name,
                f"sudo mkdir -p {data_dir} && "
                f"sudo mv /tmp/mds.{mds_id}.keyring {keyring} && "
                f"sudo chmod 0600 {keyring}",
            )
        else:
            # Ensure keyring is in the expected path on admin
            self.executor.run_remote(
                host_name,
                f"sudo mkdir -p {data_dir} && "
                f"if [ ! -f {keyring} ]; then echo 'missing keyring {keyring}'; exit 1; fi",
            )

        # Point this MDS at the target filesystem (multi-FS friendly)
        self._run_ceph(
            self.admin,
            f"-c {conf} config set mds.{mds_id} mds_join_fs {fs} || true",
        )

        env = self._env_vars()

        # Optional CPU pinning via taskset when mds_settings.cpus is set
        cpus = settings.get("cpus") if settings else None
        taskset_prefix = ""
        if cpus is not None:
            # cpus is a count; pin to first N CPUs
            cpu_list = ",".join(str(i) for i in range(int(cpus)))
            taskset_prefix = f"taskset -c {cpu_list} "

        # Kill any existing instance first
        self._kill_mds(host_name, mds_id)

        # Launch like vstart: ceph-mds -i <id> -c <conf>
        # Extra args set log/asok/pid/keyring so paths are predictable.
        if self.is_mds_logging_enabled():
            log_args = (
                f"--log-file {log_path} "
                f"--log-to-stderr false "
                f"--err-to-stderr true "
            )
        else:
            log_args = (
                "--log-to-file false "
                "--log-to-stderr false "
                "--err-to-stderr false "
            )
        args = (
            f"{taskset_prefix}{binary} -i {mds_id} -c {conf} "
            f"--keyring {keyring} "
            f"--pid-file {pid_path} "
            f"--admin-socket {asok_path} "
            f"{log_args}"
            f"-f"
        )
        # Expand ${CEPH_INSTALL_PREFIX} etc. and export env for the daemon.
        cmd = CommonUtils.with_env_exports(
            f"ulimit -c unlimited; nohup {args} > /dev/null 2>&1 &",
            env,
            sudo=True,
        )
        print(f"[{host_name}] Starting local ceph-mds mds.{mds_id}")
        self.executor.run_remote(host_name, cmd, check=True)

        # Wait briefly for the admin socket / pid
        for i in range(30):
            try:
                self.executor.run_remote(
                    host_name, f"test -f {pid_path} || test -S {asok_path}", check=True
                )
                print(f"[{host_name}] ceph-mds mds.{mds_id} is up")
                return
            except Exception:
                if i == 29:
                    print(
                        f"[{host_name}] Warning: ceph-mds mds.{mds_id} "
                        f"pid/asok not found after 30s"
                    )
                time.sleep(1)

    def _deploy_mds(self, fs, settings):
        max_mds = settings.get("max_mds", 1) if settings else 1
        selected_hosts = self._select_mds_hosts(fs, max_mds)
        instances = []

        # Create keyrings on admin first (auth is cluster-wide)
        for index, host_name in enumerate(selected_hosts):
            mds_id = self._mds_id(fs, host_name, index)
            keyring_path = self._keyring_path(mds_id)
            data_dir = f"{self._data_dir()}/ceph-{mds_id}"
            self.executor.run_remote(
                self.admin, f"sudo mkdir -p {data_dir} {self._run_dir()} {self._log_dir()}"
            )
            self._create_mds_auth(mds_id, keyring_path)
            instances.append((host_name, mds_id))

        # Start processes on each host
        for host_name, mds_id in instances:
            self._start_mds_process(host_name, mds_id, fs, settings)

        self._mds_instances[fs] = instances

        # Ensure max_mds is applied after daemons are online (vstart does this late)
        if settings and "max_mds" in settings:
            self._run_ceph(
                self.admin, f"fs set {fs} max_mds {settings['max_mds']}"
            )

    def _collect_mds_logs(self, loadpoint, results_dir):
        if not self.is_mds_logging_enabled():
            return
        lp_tag = f"{int(loadpoint):02d}"
        for fs, instances in self._mds_instances.items():
            for host_name, mds_id in instances:
                src_log = self._log_path(mds_id)
                dest_log = f"{host_name}_lp{lp_tag}_mds.{mds_id}.log"
                # Only collect if the log exists
                check = self.executor.run_remote(
                    host_name,
                    f"test -f {src_log} && echo EXISTS || echo MISSING",
                ).strip()
                if "EXISTS" in check:
                    self._copy_log_to_results(
                        host_name, src_log, dest_log, results_dir
                    )
                else:
                    print(
                        f"[{host_name}] Warning: MDS log {src_log} not found; skipping"
                    )
