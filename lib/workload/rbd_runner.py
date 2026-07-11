import base64
import datetime
import json
import os
import subprocess
import threading
from lib.workload.workload_runner import WorkloadRunner
from cephfs_perf_lib import CommonUtils


class RbdWorkloadRunner(WorkloadRunner):
    """Workload runner for `fio --ioengine=rbd` against a Rados pool.

    Delegates pool lifecycle to CephPoolManager (no CephFS, no ganesha,
    no mount). This runner ships the driver script to the admin, hands it
    the settings/loadpoints/clients as base64-encoded JSON blobs, and
    streams stdout back to trigger perf recording when the RUN phase
    starts, just like FioWorkloadRunner.
    """

    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
        results_dir=None,
    ):
        cfg = self.config.rbd
        loadpoints = cfg.get("loadpoints", [])
        if isinstance(loadpoints, dict):
            loadpoints = [loadpoints]
        expanded_loadpoints = CommonUtils.expand_loadpoints(loadpoints)

        run_cmd = cfg.get("run_command", "/cephfs_perf/rbd/run_rbd_workload.py")
        perf_record_enabled = cfg.get("perf_record", False)
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = results_dir or self.get_results_dir(settings, ts)

        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["results_dir"] = results_dir

        for key, config_val in [
            ("config_path", self.config.ceph_conf_path),
            ("keyring", self.config.ceph_keyring_path),
            ("client_id", self.config.ceph_user_id),
        ]:
            if config_val:
                payload[key] = config_val

        for key in [
            "executable_path",
            "rbd_executable_path",
            "env_vars",
            "config_path",
            "keyring",
            "client_id",
            "pool",
            "image_size",
            "images_per_client",
            "recreate_images",
            "gtod_reduce",
            "ramp_time",
            "threads_fio",
        ]:
            if key in cfg:
                payload[key] = cfg[key]

        settings_json = json.dumps(payload)
        loadpoints_json = json.dumps(expanded_loadpoints)
        clients_json = json.dumps(self.config.clients)

        print(f"Running RBD Workload on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)

        tmp_settings = f"/tmp/rbd_settings_{os.getpid()}.json"
        tmp_loadpoints = f"/tmp/rbd_loadpoints_{os.getpid()}.json"
        tmp_clients = f"/tmp/rbd_clients_{os.getpid()}.json"

        settings_b64 = base64.b64encode(settings_json.encode()).decode()
        loadpoints_b64 = base64.b64encode(loadpoints_json.encode()).decode()
        clients_b64 = base64.b64encode(clients_json.encode()).decode()

        setup_cmd = (
            f"echo '{settings_b64}' | base64 -d > {tmp_settings} && "
            f"echo '{loadpoints_b64}' | base64 -d > {tmp_loadpoints} && "
            f"echo '{clients_b64}' | base64 -d > {tmp_clients}"
        )
        self.executor.run_remote(self.admin, setup_cmd)

        full_cmd = (
            f"python3 {run_cmd} "
            f"--settings '@{tmp_settings}' "
            f"--loadpoints '@{tmp_loadpoints}' "
            f"--clients '@{tmp_clients}' "
            f"--runner-name '{self.get_name()}'; "
            f"rm -f {tmp_settings} {tmp_loadpoints} {tmp_clients}"
        )

        print(f"[{self.admin}] Executing workload with temp files...")
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-p",
            port,
            f"{user}@{host}",
            "bash -s",
        ]

        current_lp, run_phase_started = 0, False
        perf_triggered = False
        perf_threads = []

        process = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        process.stdin.write(full_cmd + "\n")
        process.stdin.close()

        output = []
        for line in process.stdout:
            print(f"[{self.admin}] {line}", end="")
            output.append(line)
            if "Starting tests..." in line:
                current_lp += 1
                run_phase_started, perf_triggered = False, False
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True

            if run_phase_started and not perf_triggered:
                if perf_record_enabled:
                    print(f"Triggering perf recording for Load Point {current_lp}...")
                    lp_cfg = expanded_loadpoints[current_lp - 1]
                    t = threading.Thread(
                        target=self.execute_perf_record,
                        args=(
                            "rbd",
                            self.config.mdss or self.config.mons,
                            current_lp,
                            results_dir,
                            payload,
                            lp_cfg,
                        ),
                    )
                    t.start()
                    perf_threads.append(t)
                perf_triggered = True

        process.wait()
        for t in perf_threads:
            t.join()

        if process.returncode != 0:
            raise RuntimeError(
                f"RBD workload failed on {self.admin} with return code {process.returncode}"
            )

        return "".join(output)

    def get_results_dir(self, settings, shared_ts=None):
        cfg = self.config.rbd
        base = cfg.get("results_base_dir", "/tmp/rbd_results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = (
            f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}"
        )
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}")

    def get_name(self):
        return "rbd"

    def prepare_storage(self):
        cfg = self.config.get("rbd", {})
        run_cmd = cfg.get("run_command", "/cephfs_perf/rbd/run_rbd_workload.py")
        perf_script = cfg.get("perf_record_script", "/cephfs_perf/perf_record.py")

        targets = set(
            [self.admin]
            + self.config.clients
            + self.config.ganeshas
            + self.config.mons
            + self.config.mdss
        )

        for target in targets:
            u, h, p = self.executor.get_ssh_details(target)
            remote_dir = os.path.dirname(run_cmd)
            self.executor.run_remote(
                target,
                f"sudo mkdir -p {remote_dir} && sudo chown {u}:{u} {remote_dir}",
            )

            files_to_copy = [
                ("lib/workload/run_rbd_workload.py", run_cmd),
                ("perf_record.py", perf_script),
                ("cephfs_perf_lib.py", os.path.join(remote_dir, "cephfs_perf_lib.py")),
            ]

            for local_file, remote_path in files_to_copy:
                if os.path.exists(local_file):
                    print(f"Copying local {local_file} to {remote_path} on {target}...")
                    subprocess.run(
                        [
                            "scp",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-P",
                            str(p),
                            local_file,
                            f"{u}@{h}:{remote_path}",
                        ]
                    )
