#!/usr/bin/env python3
#
# /cephfs_perf/sfs2020/perf_record.py --loadpoint 1 --server test --executable ceph-mds --duration 5 --flamegraph-path /cephfs_perf/FlameGraph
#
# perf record -o /cephfs_perf/sfs2020/test_lp01_mds.perf_test_fs_perf.data -p 2 -F 99 -g --call-graph dwarf,128 -- sleep 5
# perf report -i /cephfs_perf/sfs2020/test_lp01_mds.perf_test_fs_perf.data > /cephfs_perf/sfs2020/test_lp01_mds.perf_test_fs_perf_report.txt
#
import subprocess
import argparse
import sys
import os
import threading
import re
import shutil


def _read_file(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return ""


def _detect_container_for_pid(pid: str):
    """
    Best-effort detection of whether a host PID belongs to a container, and which container/runtime.
    Returns (runtime, container_id) or (None, None).
    """
    cgroup_txt = _read_file(f"/proc/{pid}/cgroup")
    if not cgroup_txt:
        return None, None

    # Common patterns:
    # - systemd scopes: .../docker-<64hex>.scope, .../libpod-<64hex>.scope
    # - cgroup paths: .../docker/<64hex>, .../kubepods.../<64hex>, .../libpod/<64hex>
    m = re.search(r"(?:docker|libpod)[-_]([0-9a-f]{64})", cgroup_txt)
    if not m:
        m = re.search(r"/(?:docker|libpod)/([0-9a-f]{64})", cgroup_txt)
    if not m:
        # fallback: any 64-hex-looking token (helps with some kubepods layouts)
        m = re.search(r"([0-9a-f]{64})", cgroup_txt)

    if not m:
        return None, None

    container_id = m.group(1)

    # Choose a runtime to use. Prefer podman if cgroup hints libpod and podman exists.
    runtime = None
    if "libpod" in cgroup_txt and shutil.which("podman"):
        runtime = "podman"
    elif "docker" in cgroup_txt and shutil.which("docker"):
        runtime = "docker"
    else:
        # last resort: try what's installed
        if shutil.which("podman"):
            runtime = "podman"
        elif shutil.which("docker"):
            runtime = "docker"

    if not runtime:
        return None, None

    return runtime, container_id


def _pid_matches_executable(pid: str, executable: str) -> bool:
    exe_link = f"/proc/{pid}/exe"
    try:
        exe_target = os.readlink(exe_link)
    except PermissionError:
        # Fall back to cmdline if /proc/<pid>/exe is not accessible.
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().split(b"\0")
            if not cmdline or not cmdline[0]:
                return False
            exe_target = cmdline[0].decode("utf-8", errors="replace")
        except Exception:
            return False
    except Exception:
        return False

    target_base = os.path.basename(executable)
    exe_base = os.path.basename(exe_target)
    if exe_base == target_base:
        return True
    if os.path.isabs(executable) and exe_target == executable:
        return True
    return False


def _podman_error_needs_sudo(stderr_bytes: bytes) -> bool:
    err = stderr_bytes.decode("utf-8", errors="replace").lower()
    return "container" in err and (
        "does not exist" in err or "no such" in err or "not found" in err
    )


def _run_container_cmd(runtime, cmd_args):
    print(f"Running in container {[runtime] + cmd_args}")
    proc = subprocess.run(
        [runtime] + cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False
    )
    if (
        runtime == "podman"
        and proc.returncode != 0
        and _podman_error_needs_sudo(proc.stderr)
    ):
        sudo = shutil.which("sudo")
        if sudo:
            print(f"Retrying in container {[sudo, '-n', runtime] + cmd_args}")
            return subprocess.run(
                [sudo, "-n", runtime] + cmd_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
            )
    return proc


def _container_cp_to_container(runtime, container_id, src_path, dst_path_in_container):
    return _run_container_cmd(
        runtime, ["cp", src_path, f"{container_id}:{dst_path_in_container}"]
    )


def _container_cp_from_container(
    runtime, container_id, src_path_in_container, dst_path
):
    return _run_container_cmd(
        runtime, ["cp", f"{container_id}:{src_path_in_container}", dst_path]
    )


def _container_exec(runtime, container_id, cmd_argv):
    return _run_container_cmd(runtime, ["exec", container_id] + cmd_argv)


def _resolve_flamegraph_dir(configured_dir):
    default_dir = "/cephfs_perf/FlameGraph"
    if os.path.isdir(default_dir):
        return default_dir
    if configured_dir and os.path.isdir(configured_dir):
        return configured_dir
    return None


def _generate_flamegraph(script_file, flamegraph_dir):
    stackcollapse = os.path.join(flamegraph_dir, "stackcollapse-perf.pl")
    flamegraph = os.path.join(flamegraph_dir, "flamegraph.pl")
    if not (os.path.isfile(stackcollapse) and os.path.isfile(flamegraph)):
        print(
            f"FlameGraph tools not found in {flamegraph_dir}, skipping flamegraph generation."
        )
        return

    folded_file = f"{script_file}-folded.txt"
    svg_file = f"{os.path.splitext(script_file)[0]}.svg"

    with open(folded_file, "w") as folded_out:
        collapse_proc = subprocess.run(
            [stackcollapse, script_file],
            stdout=folded_out,
            stderr=subprocess.PIPE,
            text=False,
        )
    if collapse_proc.returncode != 0:
        print(
            f"Error during stackcollapse for {script_file}: {collapse_proc.stderr.decode('utf-8', errors='replace')}"
        )
        return

    with open(svg_file, "w") as svg_out:
        fg_proc = subprocess.run(
            [flamegraph, folded_file],
            stdout=svg_out,
            stderr=subprocess.PIPE,
            text=False,
        )
    if fg_proc.returncode != 0:
        print(
            f"Error during flamegraph for {script_file}: {fg_proc.stderr.decode('utf-8', errors='replace')}"
        )
        return

    print(f"Generated flamegraph {svg_file}")


def _is_running_in_container():
    """
    Checks if the current process is running inside a container.
    """
    if os.path.exists("/.dockerenv"):
        return True

    try:
        with open("/proc/1/cgroup", "rt") as f:
            content = f.read()
            if "docker" in content or "libpod" in content or "kubepods" in content:
                return True
    except Exception:
        pass

    return False


def run_reports(
    perf_data_file,
    report_file,
    script_file,
    pid,
    server,
    service_id,
    flamegraph_dir,
    in_container_override=False,
):
    print(f"Generating perf report and script for PID {pid} on {server}...")

    if in_container_override:
        runtime, container_id = None, None
        in_container = False
    else:
        runtime, container_id = _detect_container_for_pid(str(pid))
        in_container = bool(runtime and container_id)

    try:
        if not in_container:
            # Generate Report
            print(f"Generating report to {report_file}...")
            with open(report_file, "w") as f:
                report_proc = subprocess.run(
                    ["perf", "report", "-i", perf_data_file, "--header"],
                    stdout=f,
                    stderr=subprocess.PIPE,
                    text=False,
                )
                if report_proc.returncode != 0:
                    print(
                        f"Error during perf report for PID {pid} on {server}: {report_proc.stderr.decode('utf-8', errors='replace')}"
                    )

            # Generate Script
            print(f"Generating script to {script_file}...")
            with open(script_file, "w") as f:
                script_proc = subprocess.run(
                    ["perf", "script", "-i", perf_data_file],
                    stdout=f,
                    stderr=subprocess.PIPE,
                    text=False,
                )
                if script_proc.returncode != 0:
                    print(
                        f"Error during perf script for PID {pid} on {server}: {script_proc.stderr.decode('utf-8', errors='replace')}"
                    )
            if flamegraph_dir:
                _generate_flamegraph(script_file, flamegraph_dir)
            return

        # Check if /cephfs_perf/sfs2020/perf_record.py exists in the container
        print(
            f"Checking for /cephfs_perf/sfs2020/perf_record.py in container {container_id}..."
        )
        check_proc = _container_exec(
            runtime, container_id, ["test", "-f", "/cephfs_perf/sfs2020/perf_record.py"]
        )

        if check_proc.returncode == 0:
            print(
                f"Executing perf_record.py inside container {container_id} for reporting..."
            )

            # Use relative paths for files inside the container's /cephfs_perf/sfs2020 (if that's where we want them)
            # Actually, we can just use the same filenames in /tmp or /cephfs_perf/sfs2020
            # Let's use /cephfs_perf/sfs2020 if it exists, otherwise /tmp
            container_workdir = "/cephfs_perf/sfs2020"

            base_perf = os.path.basename(perf_data_file)
            base_report = os.path.basename(report_file)
            base_script = os.path.basename(script_file)

            perf_in = f"{container_workdir}/{base_perf}"
            report_in = f"{container_workdir}/{base_report}"
            script_in = f"{container_workdir}/{base_script}"

            # Check if perf_data_file exists on host.
            # If we ran 'perf record' in container, it's ALREADY in the container.
            if not os.path.exists(perf_data_file):
                print(
                    f"Perf data {perf_data_file} not found on host. Checking if it's already in container..."
                )
                # Check if it exists in container at the expected path (usually current dir in container)
                # When we ran 'perf record -o perf_data_file', it used the same name.
                check_in_container = _container_exec(
                    runtime, container_id, ["test", "-f", base_perf]
                )
                if check_in_container.returncode == 0:
                    print(
                        f"Found {base_perf} in container. Moving to {perf_in} if needed..."
                    )
                    if base_perf != perf_in and perf_in != f"./{base_perf}":
                        _container_exec(
                            runtime, container_id, ["mv", base_perf, perf_in]
                        )
                else:
                    # Maybe it was already at perf_in?
                    check_at_perf_in = _container_exec(
                        runtime, container_id, ["test", "-f", perf_in]
                    )
                    if check_at_perf_in.returncode != 0:
                        print(
                            f"Error: {base_perf} not found on host OR in container {container_id}"
                        )
                        return
            else:
                # Copy perf.data into container if it exists on host
                cp_in = _container_cp_to_container(
                    runtime, container_id, perf_data_file, perf_in
                )
                if cp_in.returncode != 0:
                    # Try /tmp if /cephfs_perf/sfs2020 fails
                    container_workdir = "/tmp"
                    perf_in = f"{container_workdir}/{base_perf}"
                    report_in = f"{container_workdir}/{base_report}"
                    script_in = f"{container_workdir}/{base_script}"
                    cp_in = _container_cp_to_container(
                        runtime, container_id, perf_data_file, perf_in
                    )
                    if cp_in.returncode != 0:
                        print(
                            f"Error copying perf data into container for PID {pid} on {server}: {cp_in.stderr.decode('utf-8', errors='replace')}"
                        )
                        return

            # Execute itself in container to generate reports
            cmd = [
                "python3",
                "/cephfs_perf/sfs2020/perf_record.py",
                "--loadpoint",
                "99",  # dummy as we don't really use it for reporting only
                "--server",
                server,
                "--only-report",
                "--perf-data",
                perf_in,
                "--report-file",
                report_in,
                "--script-file",
                script_in,
            ]
            if flamegraph_dir:
                cmd += ["--flamegraph-path", "/cephfs_perf/FlameGraph"]

            exec_proc = _container_exec(runtime, container_id, cmd)
            if exec_proc.returncode != 0:
                print(
                    f"Error executing perf_record.py inside container: {exec_proc.stderr.decode('utf-8', errors='replace')}"
                )
            else:
                # Copy results back
                _container_cp_from_container(
                    runtime, container_id, report_in, report_file
                )
                _container_cp_from_container(
                    runtime, container_id, script_in, script_file
                )

                # If flamegraph was generated in container, copy it too
                svg_in = script_in.replace(".txt", ".svg")
                svg_file = script_file.replace(".txt", ".svg")
                _container_cp_from_container(runtime, container_id, svg_in, svg_file)

                # ALSO copy the perf.data file back to the host if it wasn't there
                if not os.path.exists(perf_data_file):
                    _container_cp_from_container(
                        runtime, container_id, perf_in, perf_data_file
                    )

                # Cleanup
                _container_exec(
                    runtime,
                    container_id,
                    ["rm", "-f", perf_in, report_in, script_in, svg_in],
                )
                return

        # Fallback to existing logic if /cephfs_perf/sfs2020/perf_record.py missing or exec failed
        base_perf = os.path.basename(perf_data_file)
        base_report = os.path.basename(report_file)
        base_script = os.path.basename(script_file)

        perf_in = f"/tmp/{base_perf}"
        report_in = f"/tmp/{base_report}"
        script_in = f"/tmp/{base_script}"

        print(
            f"Detected containerized PID {pid}. Running perf report/script inside container {container_id} using {runtime}..."
        )

        # Copy perf.data into container if it exists on host
        if os.path.exists(perf_data_file):
            cp_in = _container_cp_to_container(
                runtime, container_id, perf_data_file, perf_in
            )
            if cp_in.returncode != 0:
                print(
                    f"Error copying perf data into container for PID {pid} on {server}: {cp_in.stderr.decode('utf-8', errors='replace')}"
                )
                return
        else:
            # Check if it exists in container at the expected path (usually current dir in container)
            check_in_container = _container_exec(
                runtime, container_id, ["test", "-f", base_perf]
            )
            if check_in_container.returncode == 0:
                print(f"Found {base_perf} in container. Moving to {perf_in}...")
                _container_exec(runtime, container_id, ["mv", base_perf, perf_in])
            else:
                print(
                    f"Error: {base_perf} not found on host OR in container {container_id}"
                )
                return

        # Run perf report inside container and write to container filesystem
        print(f"Generating report in container to {report_in}...")
        report_proc = _container_exec(
            runtime,
            container_id,
            ["sh", "-lc", f"perf report -i {perf_in} --header > {report_in}"],
        )
        if report_proc.returncode != 0:
            print(
                f"Error during perf report (container) for PID {pid} on {server}: {report_proc.stderr.decode('utf-8', errors='replace')}"
            )

        # Run perf script inside container and write to container filesystem
        print(f"Generating script in container to {script_in}...")
        script_proc = _container_exec(
            runtime,
            container_id,
            ["sh", "-lc", f"perf script -i {perf_in} > {script_in}"],
        )
        if script_proc.returncode != 0:
            print(
                f"Error during perf script (container) for PID {pid} on {server}: {script_proc.stderr.decode('utf-8', errors='replace')}"
            )

        # Copy results out of container
        print(f"Copying report out of container to {report_file}...")
        cp_report = _container_cp_from_container(
            runtime, container_id, report_in, report_file
        )
        if cp_report.returncode != 0:
            print(
                f"Error copying perf report out of container for PID {pid} on {server}: {cp_report.stderr.decode('utf-8', errors='replace')}"
            )

        print(f"Copying script out of container to {script_file}...")
        cp_script = _container_cp_from_container(
            runtime, container_id, script_in, script_file
        )
        if cp_script.returncode != 0:
            print(
                f"Error copying perf script out of container for PID {pid} on {server}: {cp_script.stderr.decode('utf-8', errors='replace')}"
            )

        print(f"Copying perf data out of container to {perf_data_file}...")
        cp_data = _container_cp_from_container(
            runtime, container_id, perf_in, perf_data_file
        )
        if cp_data.returncode != 0:
            print(
                f"Error copying perf data out of container for PID {pid} on {server}: {cp_data.stderr.decode('utf-8', errors='replace')}"
            )

        if flamegraph_dir:
            _generate_flamegraph(script_file, flamegraph_dir)

        # Best-effort cleanup inside container
        _container_exec(
            runtime,
            container_id,
            ["sh", "-lc", f"rm -f {perf_in} {report_in} {script_in}"],
        )

    except Exception as e:
        print(f"Failed to process perf data for PID {pid}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Execute perf record and generate report"
    )
    parser.add_argument("--loadpoint", required=True, help="Current load point number")
    parser.add_argument("--server", required=True, help="Server hostname/IP")
    parser.add_argument(
        "--executable",
        default="ceph-mds",
        help="Executable name to capture (default: ceph-mds)",
    )
    parser.add_argument(
        "--duration", default="5", help="Capture duration in seconds (default: 5)"
    )
    parser.add_argument(
        "--flamegraph-path",
        default="",
        help="Path to FlameGraph project (used if /cephfs_perf/FlameGraph is missing)",
    )
    parser.add_argument(
        "--only-report",
        action="store_true",
        help="Only generate report from existing perf data",
    )
    parser.add_argument("--perf-data", help="Path to perf.data for --only-report")
    parser.add_argument("--report-file", help="Path to report.txt for --only-report")
    parser.add_argument("--script-file", help="Path to script.txt for --only-report")
    parser.add_argument("--stap-script", help="Path to SystemTap script (.stp)")
    parser.add_argument(
        "--output-dir", default="/tmp", help="Output directory for generated files"
    )
    parser.add_argument("--workload", help="Workload name (cephfs_tool, fio, sfs2020)")
    parser.add_argument("--options", help="Workload options string")

    args = parser.parse_args()

    loadpoint = args.loadpoint
    executable = args.executable
    duration = args.duration
    s_name = args.server.split("@")[-1]
    flamegraph_dir = _resolve_flamegraph_dir(args.flamegraph_path)
    output_dir = args.output_dir

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            print(f"Warning: Could not create output directory {output_dir}: {e}")

    if args.only_report:
        if not (args.perf_data and args.report_file and args.script_file):
            print(
                "Error: --only-report requires --perf-data, --report-file, and --script-file"
            )
            sys.exit(1)

        # We need to provide dummy pid and service_id for run_reports
        # But wait, run_reports also detects container. If we are ALREADY in container,
        # _detect_container_for_pid(dummy_pid) will likely return (None, None)
        # because /proc/pid/cgroup won't show the same thing as on host.
        # So we need to make sure run_reports handles being in container.

        # Actually, if we are in container, we want run_reports to just run the "not in_container" logic.
        run_reports(
            args.perf_data,
            args.report_file,
            args.script_file,
            "0",
            args.server,
            "only-report",
            flamegraph_dir,
            in_container_override=True,
        )
        return

    # Check if we are in container. If so, we should do record | report | script locally.
    in_container = _is_running_in_container()

    # Check if executable is running
    pgrep_cmd = ["pgrep", "-f", executable]
    result = subprocess.run(pgrep_cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"{executable} not running on {args.server}. Skipping perf record.")
        return

    raw_pids = [line for line in result.stdout.strip().split("\n") if line]
    pids = [pid for pid in raw_pids if _pid_matches_executable(pid, executable)]

    if not pids:
        print(f"{executable} not running on {args.server}. Skipping perf record.")
        return

    record_processes = []
    for pid in pids:
        # Try to identify the specific service ID if it's ceph-mds
        service_id = pid
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().split(b"\0")
                # For ceph-mds, the service id follows -n or --id
                for i in range(len(cmdline)):
                    if cmdline[i] in [b"-n", b"--id"] and i + 1 < len(cmdline):
                        service_id = cmdline[i + 1].decode("utf-8")
                        # service_id might be mds.perf_test_fs.ceph-server-1.pkcizc
                        # Let's simplify it to mds.perf_test_fs
                        if service_id.startswith("mds."):
                            parts = service_id.split(".")
                            if len(parts) >= 2:
                                service_id = f"{parts[0]}.{parts[1]}"
                        break
        except Exception as e:
            print(f"Could not read cmdline for PID {pid}: {e}")

        lp_tag = f"lp{int(loadpoint):02d}"

        if args.workload and args.options:
            base_filename = (
                f"{args.workload}_perf_record_{s_name}_{lp_tag}_{args.options}"
            )
        else:
            base_filename = f"{s_name}_{lp_tag}_{service_id}"

        report_file = os.path.join(output_dir, f"{base_filename}_perf_report.txt")
        script_file = os.path.join(output_dir, f"{base_filename}_perf_script.txt")
        perf_data_file = os.path.join(output_dir, f"{base_filename}_perf.data")
        stap_out_file = os.path.join(output_dir, f"{base_filename}_stap_trace.txt")

        print(
            f"Starting perf record on {args.server} for PID {pid} ({service_id}) for Load Point {loadpoint}..."
        )

        # perf record
        # If the target process is in a container, we must execute 'perf record'
        # inside that container to correctly capture the symbols and map files.
        runtime, container_id = _detect_container_for_pid(str(pid))

        perf_record_cmd = [
            "perf",
            "record",
            "-o",
            perf_data_file,
            "-p",
            pid,
            "-F",
            "99",
            "-g",
            "--call-graph",
            "dwarf,128",
            "--",
            "sleep",
            duration,
        ]

        if runtime and container_id:
            print(
                f"Detected containerized PID {pid}. Wrapping perf record in {runtime} exec {container_id}..."
            )
            # When executing in container, we need to know the PID INSIDE the container.
            # However, 'perf' on the host can often see container PIDs if namespaces are set up correctly.
            # BUT the requirement says: 'perf_record_cmd should execute within the container'.
            # To execute WITHIN the container, we need the PID as seen by the container.

            target_pid = pid
            # Try to find the PID inside the container
            # pgrep -f executable
            find_pid_cmd = ["pgrep", "-f", executable]
            find_proc = _container_exec(runtime, container_id, find_pid_cmd)
            if find_proc.returncode == 0:
                inner_pids = [
                    p.strip()
                    for p in find_proc.stdout.decode("utf-8").strip().split("\n")
                    if p.strip()
                ]
                if inner_pids:
                    # If we found multiple, try to find one that matches the service_id or at least the cmdline
                    # For now, if only one exists, use it.
                    if len(inner_pids) == 1:
                        target_pid = inner_pids[0]
                    else:
                        # Multiple pids inside container. We are currently in a loop for a specific host PID.
                        # We can look into /proc/PID/status inside the container to find NSpid (Namespace PID).
                        # Cat /proc/<pid>/status | grep NSpid
                        # But wait, we have the host PID. On modern kernels, /proc/<host_pid>/status
                        # on the host contains NSpid: <host_pid> <container_pid>
                        try:
                            with open(f"/proc/{pid}/status", "r") as f:
                                for line in f:
                                    if line.startswith("NSpid:"):
                                        nspids = line.split()[1:]
                                        if len(nspids) > 1:
                                            target_pid = nspids[
                                                -1
                                            ]  # The last one is the most nested namespace PID
                                            break
                        except Exception as e:
                            print(f"Could not read NSpid for PID {pid} from host: {e}")

            perf_record_cmd = [runtime, "exec", container_id] + [
                "perf",
                "record",
                "-o",
                perf_data_file,
                "-p",
                target_pid,
                "-F",
                "99",
                "-g",
                "--call-graph",
                "dwarf,128",
                "--",
                "sleep",
                duration,
            ]

        print(f"Executing perf record command: {' '.join(perf_record_cmd)}")
        p = subprocess.Popen(
            perf_record_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False
        )
        record_processes.append(
            (p, pid, service_id, perf_data_file, report_file, script_file)
        )

        if args.stap_script:
            # SystemTap capture
            # sudo stap rdtsc-para-callgraph-verbose.stp 'process(<pid>).function("*")' for duration
            print(
                f"Starting SystemTap capture on {args.server} for PID {pid} using {args.stap_script}..."
            )

            stap_target_pid = pid
            stap_cmd = [
                "sudo",
                "timeout",
                "-s",
                "SIGINT",
                duration,
                "stap",
                args.stap_script,
                f'process({stap_target_pid}).function("*")',
            ]

            if runtime and container_id:
                # Use NSpid for SystemTap if we can find it
                try:
                    with open(f"/proc/{pid}/status", "r") as f:
                        for line in f:
                            if line.startswith("NSpid:"):
                                nspids = line.split()[1:]
                                if len(nspids) > 1:
                                    stap_target_pid = nspids[-1]
                                    break
                except Exception as e:
                    print(
                        f"Could not read NSpid for SystemTap PID {pid} from host: {e}"
                    )

                print(
                    f"Detected containerized PID {pid}. Wrapping SystemTap in {runtime} exec {container_id}..."
                )

                # Copy SystemTap script to container to ensure it is available
                # Point to a temporary path in the container
                container_stap_script = f"/tmp/{os.path.basename(args.stap_script)}"
                print(
                    f"Copying {args.stap_script} to {container_id}:{container_stap_script}..."
                )
                cp_cmd = [
                    "sudo",
                    runtime,
                    "cp",
                    args.stap_script,
                    f"{container_id}:{container_stap_script}",
                ]
                try:
                    subprocess.run(cp_cmd, check=True)
                except subprocess.CalledProcessError as e:
                    print(f"Warning: Failed to copy stap script to container: {e}")
                    # Fallback to host path and hope it's mounted
                    container_stap_script = args.stap_script

                stap_cmd = [
                    "sudo",
                    runtime,
                    "exec",
                    container_id,
                    "timeout",
                    "-s",
                    "SIGINT",
                    duration,
                    "stap",
                    container_stap_script,
                    f'process({stap_target_pid}).function("*")',
                ]

            print(f"Executing SystemTap command: {' '.join(stap_cmd)}")
            with open(stap_out_file, "w") as outf:
                # We'll run it and wait for it in the same way?
                # Actually, SystemTap can be slow to start, so running it in parallel with perf record is good.
                sp = subprocess.Popen(
                    stap_cmd, stdout=outf, stderr=subprocess.PIPE, text=False
                )
                record_processes.append(
                    (
                        sp,
                        pid,
                        service_id,
                        None,
                        stap_out_file,
                        None,
                    )  # Use None for perf_data and script_file to distinguish
                )

    # Wait for all processes to finish (perf and stap)
    report_data = []
    for (
        p,
        pid,
        service_id,
        perf_data_file,
        report_file,
        script_file,
    ) in record_processes:
        stdout, stderr = p.communicate()
        if p.returncode != 0 and p.returncode != 124:  # timeout exits with 124
            name = "perf record" if perf_data_file else "SystemTap"
            print(
                f"Error during {name} for PID {pid} on {args.server}: {stderr.decode('utf-8', errors='replace')}"
            )
        else:
            name = "perf record" if perf_data_file else "SystemTap"
            print(
                f"{name} for PID {pid} finished successfully. {stderr.decode('utf-8', errors='replace')}"
            )
            if perf_data_file:
                report_data.append(
                    (perf_data_file, report_file, script_file, pid, service_id)
                )

    # Run perf reports serially
    for perf_data_file, report_file, script_file, pid, service_id in report_data:
        # If we are already in container, we want run_reports to treat it as "not in container"
        # so it doesn't try to exec into itself recursively.
        # Consolidated run: record and report in one go to avoid host-to-container copies.
        run_reports(
            perf_data_file,
            report_file,
            script_file,
            pid,
            args.server,
            service_id,
            flamegraph_dir,
            in_container_override=in_container,
        )

    print(
        f"Finished all perf records and reports on {args.server} for Load Point {loadpoint}."
    )


if __name__ == "__main__":
    main()
