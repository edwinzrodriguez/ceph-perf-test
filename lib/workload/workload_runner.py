import abc
import os
import subprocess
import threading
from cephfs_perf_lib import CommonUtils


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
    ):
        pass

    @abc.abstractmethod
    def get_results_dir(self, settings, shared_ts=None):
        pass

    @abc.abstractmethod
    def prepare_storage(self):
        pass

    def execute_perf_record(self, workload_name, target_nodes, loadpoint, results_dir=None, settings=None, lp_cfg=None):
        workload_cfg = self.config.get(workload_name, {})
        perf_script = workload_cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")
        perf_exe = workload_cfg.get("perf_record_executable", "ceph-mds")
        perf_dur = workload_cfg.get("perf_record_duration", 5)
        fg_path = workload_cfg.get("perf_record_flamegraph_path", "/cephfs_perf/FlameGraph")
        stap_script = workload_cfg.get("stap_script", "")
        processes = []

        # Construct options string for filename
        options_str = ""
        if settings:
            full_base = CommonUtils.get_workload_base_name(workload_name, 'perf_record', 'server', loadpoint, settings, lp_cfg, self.config)
            lp_tag = f"lp{int(loadpoint):02d}_"
            idx = full_base.find(lp_tag)
            if idx != -1:
                options_str = full_base[idx + len(lp_tag):]

        for server_name in target_nodes:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            stap_arg = f" --stap-script {stap_script}" if stap_script else ""
            opt_arg = f" --options {options_str}" if options_str else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", str(p), f"{u}@{h}",
                       f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_exe} --duration {perf_dur} --workload {workload_name}{opt_arg}{fg_arg}{stap_arg}"]
            print(f"[{server_name}] Executing perf record for Load Point {loadpoint}: {subprocess.list2cmdline(ssh_cmd)}")
            proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False,
                                    stdin=subprocess.DEVNULL)
            processes.append((server_name, proc))

        def collect_output(s_name, p):
            out_bytes, _ = p.communicate()
            out = out_bytes.decode("utf-8", errors="replace")
            if p.returncode != 0:
                print(f"Error on {s_name} during perf record: {out}")
            else:
                if out_bytes:
                    print(f"[{s_name}] Output:\n{out}")
                print(f"[{s_name}] Finished perf record for Load Point {loadpoint}.")

        threads = []
        for s_name, proc in processes:
            t = threading.Thread(target=collect_output, args=(s_name, proc))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        if results_dir:
            print(f"Copying perf reports and stap traces to {results_dir} on {self.admin}...")
            au, ah, ap = self.executor.get_ssh_details(self.admin)
            for s_name in target_nodes:
                check_cmd = f"ls /tmp/{workload_name}_perf_record_{s_name}_lp{int(loadpoint):02d}_* 2>/dev/null"
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except Exception:
                    print(f"[{s_name}] No trace/report files found for Load Point {loadpoint}, skipping copy.")
