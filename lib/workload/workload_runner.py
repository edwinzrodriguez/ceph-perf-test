import abc
import os
import subprocess
import threading
from cephfs_perf_lib import CommonUtils
from lib.config_loader import role_observability
from lib.run_logging import get_logger

log = get_logger(__name__)


class WorkloadRunner(abc.ABC):
    def __init__(self, executor, config, fs_names):
        self.executor, self.config, self.fs_names = executor, config, fs_names
        self.admin = config.admin_host

    @abc.abstractmethod
    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
        results_dir=None,
        mount_manager=None,
        rgw_manager=None,
    ):
        pass

    @abc.abstractmethod
    def get_results_dir(self, settings, shared_ts=None):
        pass

    @abc.abstractmethod
    def prepare_storage(self):
        pass

    @abc.abstractmethod
    def get_name(self):
        pass

    def execute_mds_perf_record(
        self,
        loadpoint,
        results_dir=None,
        settings=None,
        lp_cfg=None,
    ):
        """Trigger MDS-side perf-record from ``observability.mds`` (legacy ``mds.*``).

        Files are named ``mds_perf_record_<host>_...`` so they stay distinct
        from client and OSD captures.
        """
        perf_cfg = role_observability(self.config._config, "mds").get("perf_record")
        return self.execute_perf_record(
            workload_name="mds",
            target_nodes=self.config.mdss,
            loadpoint=loadpoint,
            results_dir=results_dir,
            settings=settings,
            lp_cfg=lp_cfg,
            perf_cfg=perf_cfg,
        )

    def execute_osd_perf_record(
        self,
        loadpoint,
        results_dir=None,
        settings=None,
        lp_cfg=None,
        target_nodes=None,
    ):
        """Trigger OSD-side perf record for every ``ceph-osd`` on OSD hosts."""
        perf_cfg = role_observability(self.config._config, "osd").get("perf_record")
        hosts = target_nodes
        if hosts is None:
            observer = getattr(self, "osd_observer", None)
            hosts = observer.hosts() if observer is not None else self.config.osds
        return self.execute_perf_record(
            workload_name="osd",
            target_nodes=hosts or [],
            loadpoint=loadpoint,
            results_dir=results_dir,
            settings=settings,
            lp_cfg=lp_cfg,
            perf_cfg=perf_cfg,
        )

    def execute_perf_record(
        self,
        workload_name,
        target_nodes,
        loadpoint,
        results_dir=None,
        settings=None,
        lp_cfg=None,
        config_section=None,
        perf_cfg=None,
    ):
        # `workload_name` drives filename naming and the --workload arg passed
        # to perf_record.py. `perf_cfg` is a normalized
        # {enabled, script, executable, duration, flamegraph_path, stap_script}
        # dict. When omitted, flat keys on `config_section` are still read so
        # older callers keep working.
        if perf_cfg:
            perf_script = perf_cfg.get("script", "/cephfs_perf/perf_record.py")
            perf_exe = perf_cfg.get("executable", "ceph-mds")
            perf_dur = perf_cfg.get("duration", 5)
            fg_path = perf_cfg.get("flamegraph_path", "/cephfs_perf/FlameGraph")
            stap_script = perf_cfg.get("stap_script") or ""
        else:
            section = config_section or workload_name
            workload_cfg = self.config.get(section, {}) or {}
            nested = workload_cfg.get("perf_record")
            if isinstance(nested, dict):
                perf_script = nested.get("script", "/cephfs_perf/perf_record.py")
                perf_exe = nested.get("executable", "ceph-mds")
                perf_dur = nested.get("duration", 5)
                fg_path = nested.get(
                    "flamegraph_path",
                    workload_cfg.get("flamegraph_path", "/cephfs_perf/FlameGraph"),
                )
                stap_script = nested.get("stap_script") or workload_cfg.get(
                    "stap_script", ""
                )
            else:
                perf_script = workload_cfg.get(
                    "perf_record_script", "/cephfs_perf/perf_record.py"
                )
                perf_exe = workload_cfg.get("perf_record_executable", "ceph-mds")
                perf_dur = workload_cfg.get("perf_record_duration", 5)
                fg_path = workload_cfg.get(
                    "perf_record_flamegraph_path",
                    workload_cfg.get("flamegraph_path", "/cephfs_perf/FlameGraph"),
                )
                stap_script = workload_cfg.get("stap_script", "")
        processes = []

        # Construct options string for filename
        options_str = ""
        if settings:
            full_base = CommonUtils.get_workload_base_name(
                workload_name,
                "perf_record",
                "server",
                loadpoint,
                settings,
                lp_cfg,
                self.config,
            )
            lp_tag = f"lp{int(loadpoint):02d}_"
            idx = full_base.find(lp_tag)
            if idx != -1:
                options_str = full_base[idx + len(lp_tag) :]

        for server_name in target_nodes:
            log.info(
                "[%s] Starting parallel perf record for Load Point %s",
                server_name,
                loadpoint,
            )
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            stap_arg = f" --stap-script {stap_script}" if stap_script else ""
            opt_arg = f" --options {options_str}" if options_str else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            ssh_cmd = [
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                "-p",
                str(p),
                f"{u}@{h}",
                f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_exe} --duration {perf_dur} --workload {workload_name}{opt_arg}{fg_arg}{stap_arg}",
            ]
            log.info(
                "[%s] Executing perf record for Load Point %s: %s",
                server_name,
                loadpoint,
                subprocess.list2cmdline(ssh_cmd),
            )
            proc = subprocess.Popen(
                ssh_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False,
                stdin=subprocess.DEVNULL,
            )
            processes.append((server_name, proc))

        def collect_output(s_name, p):
            out_bytes, _ = p.communicate()
            out = out_bytes.decode("utf-8", errors="replace")
            if p.returncode != 0:
                log.error("Error on %s during perf record: %s", s_name, out)
            else:
                if out_bytes:
                    log.info("[%s] Output:\n%s", s_name, out.rstrip("\n"))
                log.info("[%s] Finished perf record for Load Point %s.", s_name, loadpoint)

        threads = []
        for s_name, proc in processes:
            t = threading.Thread(target=collect_output, args=(s_name, proc))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        if results_dir:
            log.info(
                "Copying perf reports and stap traces to %s on %s",
                results_dir,
                self.admin,
            )
            au, ah, ap = self.executor.get_ssh_details(self.admin)
            lp_tag = f"lp{int(loadpoint):02d}"
            for s_name in target_nodes:
                # Service id / PID sits in the basename so multi-OSD hosts do
                # not collide; match either the legacy lp-immediately-after-host
                # form or the id-qualified form.
                check_cmd = (
                    f"ls /tmp/{workload_name}_perf_record_{s_name}_*{lp_tag}* "
                    f"/tmp/{s_name}_{lp_tag}_* 2>/dev/null"
                )
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except Exception:
                    log.info(
                        "[%s] No trace/report files found for Load Point %s, skipping copy.",
                        s_name,
                        loadpoint,
                    )

    def collect_journal_logs(self, results_dir):
        """Collect journal logs from all hosts."""
        all_hosts = list(self.config.all_hosts_meta.keys())
        CommonUtils.collect_journal_logs(self.executor, all_hosts, results_dir)

    def mirror_remote_line(self, line):
        """Log one remote-driver stdout line into the run transcript."""
        log.info("[%s] %s", self.admin, line.rstrip("\n"))

    def consume_remote_stdout(self, process, monitor=None, on_line=None):
        """Stream a remote driver, drive the loadpoint monitor, return output and rc."""
        output = []
        for line in process.stdout:
            self.mirror_remote_line(line)
            output.append(line)
            if monitor is not None:
                monitor.handle_line(line)
            if on_line is not None:
                on_line(line, monitor)
        returncode = process.wait()
        if monitor is not None:
            monitor.finish()
        return "".join(output), returncode

    def make_loadpoint_monitor(self, **kwargs):
        from lib.workload.loadpoint_monitor import LoadpointMonitor

        return LoadpointMonitor(self, **kwargs)
