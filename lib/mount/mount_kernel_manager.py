import re
import shlex

from lib.mount.mount_manager import MountManager
from lib.run_logging import get_logger

log = get_logger(__name__)



class MountKernelManager(MountManager):
    """Mount CephFS via the kernel client (``mount -t ceph``).

    Used when ``ganesha.enabled`` is false. Ceph CLI calls go through the
    filesystem manager helpers so ``CEPH_INSTALL_PREFIX`` / ``mds.ceph_binary_path``
    are honored (bare ``sudo ceph`` fails when ceph lives under a custom prefix).

    Mount options use ``mds_namespace=`` (not ``fs=``): older kernels reject
    ``fs=`` with ``Unknown parameter 'fs'``. ``mds_namespace`` remains accepted
    as a synonym on newer kernels.

    Extra ``-o`` options and optional kernel dynamic debug come from the
    ``mount_ceph`` settings section (see CephFS troubleshooting "Dynamic
    Debugging" / ``src/script/kcon_all.sh``).
    """

    _DEFAULT_DEBUG_MODULES = ("ceph", "libceph", "rbd")

    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)
        self.fs_manager = fs_manager
        self._logging_clients = []

    def _ceph_cfg(self):
        return self.config.get("mount_ceph", {}) or {}

    def _logging_cfg(self):
        return self._ceph_cfg().get("logging") or {}

    def is_logging_enabled(self):
        return bool(self._logging_cfg().get("enabled"))

    def _debug_modules(self):
        cfg = self._logging_cfg()
        modules = cfg.get("modules")
        if modules:
            return list(modules)
        return list(self._DEFAULT_DEBUG_MODULES)

    def _mount_options_extra(self):
        return (self._ceph_cfg().get("mount_options") or "").strip()

    def _set_dynamic_debug(self, host_name, enable):
        """Enable or disable kernel dynamic debug for configured modules.

        Mirrors ``src/script/kcon_all.sh``: raise console loglevel, then
        ``module <name> +p`` / ``-p`` via ``/sys/kernel/debug/dynamic_debug/control``.
        """
        flag = "+p" if enable else "-p"
        modules = self._debug_modules()
        parts = []
        if enable:
            # console_loglevel=9 so dyndebug printk lines reach the ring buffer
            parts.append("echo 9 > /proc/sysrq-trigger")
        for mod in modules:
            # Quote carefully: the control file expects "module NAME +p"
            parts.append(
                f"echo {shlex.quote(f'module {mod} {flag}')} "
                f"> /sys/kernel/debug/dynamic_debug/control"
            )
        cmd = " && ".join(parts)
        action = "Enabling" if enable else "Disabling"
        log.info(
            f"[{host_name}] {action} kernel dynamic debug "
            f"({', '.join(modules)})..."
        )
        self.executor.run_remote(host_name, f"sudo sh -c {shlex.quote(cmd)}", check=True)

    def _enable_logging_on_clients(self):
        if not self.is_logging_enabled():
            return
        seen = []
        for c in self.clients:
            if c in seen:
                continue
            seen.append(c)
            self._set_dynamic_debug(c, enable=True)
            # Start each run with a clean ring buffer for this client
            self.executor.run_remote(c, "sudo dmesg -C || true")
        self._logging_clients = seen

    def _disable_logging_on_clients(self):
        if not self._logging_clients:
            return
        for c in self._logging_clients:
            try:
                self._set_dynamic_debug(c, enable=False)
            except Exception as exc:
                log.info(
                    f"[{c}] Warning: failed to disable kernel dynamic debug: {exc}"
                )
        self._logging_clients = []

    def collect_client_logs(self, loadpoint, results_dir=None):
        """Dump client dmesg (kernel dyndebug) into the results directory."""
        if not self.is_logging_enabled() or not results_dir:
            return
        lp_tag = f"{int(loadpoint):02d}"
        clients = self._logging_clients or list(dict.fromkeys(self.clients))
        for host_name in clients:
            safe = host_name.replace(".", "-")
            dest_log = f"kernel_ceph_{safe}_lp{lp_tag}.dmesg"
            # Capture path must differ from /tmp/{dest_log} used inside
            # _copy_log_to_results (which also truncates the source after copy).
            capture = f"/tmp/kernel_ceph_{safe}_lp{lp_tag}.dmesg.capture"
            log.info(
                f"[{host_name}] Collecting kernel ceph dmesg to "
                f"{results_dir}/{dest_log}..."
            )
            self.executor.run_remote(
                host_name,
                f"sudo dmesg -T | sudo tee {shlex.quote(capture)} > /dev/null",
            )
            self.fs_manager._copy_log_to_results(
                host_name, capture, dest_log, results_dir
            )
            # Clear so the next loadpoint window is distinct
            self.executor.run_remote(host_name, "sudo dmesg -C || true")

    def mount(self):
        admin_host = self.config.admin_host
        # Resolve mon addr + client key with the same env-aware ceph binary as
        # CephFSManager (prefix installs are not on sudo's secure_path).
        mon_dump = self.fs_manager._run_ceph(admin_host, "mon dump")
        mon_addrs = re.findall(
            r"v1:([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+)", mon_dump
        )
        if not mon_addrs:
            raise RuntimeError(
                f"Could not parse mon v1 address from `ceph mon dump` on {admin_host}"
            )
        # Device syntax: mon1:port[,mon2:port,...]:/path
        maddrs = ",".join(dict.fromkeys(mon_addrs))
        key = self.fs_manager._run_ceph(
            admin_host, "auth get-key client.0"
        ).strip()
        if not key:
            raise RuntimeError(
                f"Empty key from `ceph auth get-key client.0` on {admin_host}"
            )

        # Enable dyndebug before mounts so connection setup is captured.
        self._enable_logging_on_clients()

        extra_opts = self._mount_options_extra()
        mpfs = self._mounts_per_fs()
        for fs in self.fs_names:
            for c in self.clients:
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    # mds_namespace selects the CephFS; quote secret (base64 may
                    # contain + / =). check=True so a failed mount aborts the run.
                    opts = f"name=0,secret={key},mds_namespace={fs}"
                    if extra_opts:
                        opts = f"{opts},{extra_opts}"
                    self.executor.run_remote(
                        c,
                        f"sudo mkdir -p {p} && "
                        f"sudo mount -t ceph '{maddrs}:/' '{p}' -o '{opts}' && "
                        f"mountpoint -q '{p}'",
                        check=True,
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(
                        c, f"sudo chown {u}:{u} {p}", check=True
                    )
                    log.info(f"[{c}] Kernel-mounted cephfs {fs} at {p} via {maddrs}")

    def unmount_clients(self):
        super().unmount_clients()
        self._disable_logging_on_clients()

    def display_name(self):
        return "kernel_cephfs"

    def kernel_mount(self):
        self.mount()
