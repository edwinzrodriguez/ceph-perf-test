#!/usr/bin/env python3
import argparse
import datetime
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time

# Add project root to sys.path to allow importing cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import CommonUtils

DEFAULT_RBD_ASOK = "/var/run/ceph/fio-rbd.asok"


def rbd_image_exists(rbd_bin, pool, image, config_path, keyring, client_id):
    """Return True if the RBD image already exists in the pool."""
    args = [rbd_bin]
    if config_path:
        args += ["-c", config_path]
    if keyring:
        args += ["-k", keyring]
    if client_id:
        args += ["-n", f"client.{client_id}"]
    args += ["-p", pool, "info", image]
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode == 0


def ensure_rbd_image(
    client, rbd_bin, pool, image, size_bytes, config_path, keyring,
    client_id, recreate, env_vars=None,
):
    """Create the RBD image on `client` if missing (or if recreate=True)."""
    size_mib = max(1, int(size_bytes) // (1024 * 1024))
    env_prefix = ""
    if env_vars:
        env_prefix = "".join(f'export {k}="{v}"; ' for k, v in env_vars.items())
    parts = [rbd_bin]
    if config_path:
        parts += ["-c", config_path]
    if keyring:
        parts += ["-k", keyring]
    if client_id:
        parts += ["-n", f"client.{client_id}"]

    exists_cmd = env_prefix + " ".join(parts + ["-p", pool, "info", image])
    print(f"[{client}] Checking RBD image {pool}/{image}:  {exists_cmd}", flush=True)
    check = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", client, exists_cmd],
        capture_output=True,
        text=True,
    )
    exists = check.returncode == 0

    if exists and recreate:
        rm_cmd = env_prefix + " ".join(parts + ["-p", pool, "rm", image])
        print(f"[{client}] Remove RBD image {pool}/{image}:  {rm_cmd}", flush=True)
        subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", client, rm_cmd],
            capture_output=True,
            text=True,
        )
        exists = False

    if not exists:
        create_cmd = env_prefix + " ".join(
            parts + ["-p", pool, "create", image, "--size", str(size_mib)]
        )
        print(f"[{client}] Creating RBD image {pool}/{image} ({size_mib} MiB): {create_cmd}", flush=True)
        r = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", client, create_cmd],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            print(f"[{client}] rbd create failed: {r.stderr}", flush=True)
            sys.exit(r.returncode)


def _admin_daemon_cmd(ceph_bin, asok, args, env_vars=None):
    """Build ``ceph --admin-daemon`` using a short relative socket path.

    Drop CEPH_ARGS so fio's ``--admin-socket`` does not leak into the dump
    CLI (same rationale as cephfs-tool lockstat).
    """
    asok = (asok or "").rstrip("/")
    if "/" in asok:
        directory, name = asok.rsplit("/", 1)
    else:
        directory, name = ".", asok
    dump_env = dict(env_vars or {})
    dump_env.pop("CEPH_ARGS", None)
    inner = (
        f"cd {shlex.quote(directory)} && "
        f"{ceph_bin} --admin-daemon {shlex.quote(name)} {args}"
    )
    return CommonUtils.with_env_exports(inner, dump_env)


def _run_remote(client, cmd, check=False):
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", client, "bash -s"],
        input=cmd + "\n",
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"[{client}] remote command failed (rc={result.returncode}): "
            f"{result.stderr or result.stdout}"
        )
    return result


def ensure_asok_dir(client, asok_path):
    directory = os.path.dirname(asok_path) or "/var/run/ceph"
    _run_remote(client, f"mkdir -p {shlex.quote(directory)}")


def reset_client_perf(client, ceph_bin, asok, env_vars=None):
    """Reset librados/librbd perf counters at the start of the RUN window."""
    cmd = _admin_daemon_cmd(ceph_bin, asok, "perf reset all", env_vars)
    print(f"[{client}] Resetting RBD client perf counters via {asok}...", flush=True)
    result = _run_remote(client, cmd)
    if result.returncode != 0:
        print(
            f"[{client}] Warning: perf reset failed (rc={result.returncode}): "
            f"{(result.stderr or result.stdout).strip()}",
            flush=True,
        )


def dump_client_perf(
    client, ceph_bin, asok, results_dir, loadpoint, settings, lp_cfg,
    host_tag, env_vars=None,
):
    """Dump client perf counters while fio is still alive; save under results_dir."""
    filename = (
        f"{CommonUtils.get_workload_base_name('rbd', 'perf_dump', host_tag, loadpoint, settings, lp_cfg)}.json"
    )
    remote_tmp = f"/tmp/{filename}"
    local_path = os.path.join(results_dir, filename)
    dump_cmd = _admin_daemon_cmd(ceph_bin, asok, "perf dump", env_vars)
    # Write on the client, then scp back (asok dies when fio exits).
    cmd = f"{dump_cmd} > {shlex.quote(remote_tmp)} 2>/tmp/rbd_perf_dump.err"
    print(f"[{client}] Dumping RBD client perf counters via {asok}...", flush=True)
    result = _run_remote(client, cmd)
    if result.returncode != 0:
        err = _run_remote(client, "cat /tmp/rbd_perf_dump.err 2>/dev/null || true")
        print(
            f"[{client}] Warning: perf dump failed (rc={result.returncode}): "
            f"{(err.stdout or '').strip()}",
            flush=True,
        )
        return None

    scp = subprocess.run(
        [
            "scp",
            "-o",
            "StrictHostKeyChecking=no",
            f"{client}:{remote_tmp}",
            local_path,
        ],
        capture_output=True,
        text=True,
    )
    _run_remote(client, f"rm -f {shlex.quote(remote_tmp)}")
    if scp.returncode != 0:
        print(
            f"[{client}] Warning: failed to copy perf dump: {scp.stderr}",
            flush=True,
        )
        return None

    try:
        with open(local_path, "r") as f:
            data = json.load(f)
        print(f"[{client}] Wrote RBD perf dump {local_path}", flush=True)
        return data
    except Exception as e:
        print(f"[{client}] Warning: invalid perf dump JSON ({e})", flush=True)
        return None


def schedule_perf_dump(
    client, ceph_bin, asok, results_dir, loadpoint, settings, lp_cfg,
    host_tag, duration, env_vars, done_event, dumped_flag,
):
    """Wait until near the end of the measurement window, then dump once."""
    # fio percent advances over --runtime after ramp; dump a few seconds early
    # so the asok is still live.
    delay = max(1, int(duration) - 3) if duration else 10
    if done_event.wait(timeout=delay):
        return
    if dumped_flag["done"]:
        return
    data = dump_client_perf(
        client, ceph_bin, asok, results_dir, loadpoint, settings, lp_cfg,
        host_tag, env_vars=env_vars,
    )
    if data is not None:
        dumped_flag["done"] = True


def main():
    parser = argparse.ArgumentParser(description="RBD (fio --ioengine=rbd) Driver")
    parser.add_argument(
        "--settings", required=True,
        help="JSON settings or path to JSON file (prefix with @)",
    )
    parser.add_argument(
        "--loadpoints", required=True,
        help="JSON loadpoints or path to JSON file (prefix with @)",
    )
    parser.add_argument(
        "--clients", required=True,
        help="JSON client list or path to JSON file (prefix with @)",
    )
    parser.add_argument("--runner-name", help="Name of the workload runner")
    args = parser.parse_args()

    def load_json_arg(v):
        if v.startswith("@"):
            with open(v[1:], "r") as f:
                return json.load(f)
        return json.loads(v)

    try:
        settings = load_json_arg(args.settings)
        loadpoints = load_json_arg(args.loadpoints)
        clients = load_json_arg(args.clients)
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)

    results_dir = settings.get("results_dir")
    if not results_dir:
        print("Error: results_dir is required in settings")
        sys.exit(1)
    os.makedirs(results_dir, exist_ok=True)

    fio_bin = settings.get("executable_path", "/usr/local/bin/fio")
    rbd_bin = settings.get("rbd_executable_path", "/usr/local/bin/rbd")
    base_env_vars = dict(settings.get("env_vars", {}))
    config_path = settings.get("config_path")
    keyring = settings.get("keyring")
    client_id = settings.get("client_id", "admin")
    pool = settings.get("pool")
    if not pool:
        print("Error: 'pool' must be set in rbd config")
        sys.exit(1)

    image_size = CommonUtils.parse_si_unit(settings.get("image_size", "10GiB"))
    images_per_client = int(settings.get("images_per_client", 1))
    recreate_images = bool(settings.get("recreate_images", False))
    timestamp_progress = bool(settings.get("timestamp_progress", False))

    # Client-side librados/librbd admin socket for per-loadpoint perf dumps.
    # Injected into CEPH_ARGS as --admin-socket=... so fio's rbd ioengine
    # creates a reachable asok for `ceph --admin-daemon ... perf dump`.
    perf_dump_enabled = bool(settings.get("rbd_perf_dump_enabled", False))
    perf_dump_asok = settings.get("rbd_perf_dump_asok", DEFAULT_RBD_ASOK)
    ceph_bin = settings.get("ceph_binary_path", "ceph")
    if perf_dump_enabled and perf_dump_asok:
        asok_arg = f"--admin-socket={perf_dump_asok}"
        existing = base_env_vars.get("CEPH_ARGS", "")
        # Avoid duplicating --admin-socket if the user already set one.
        if "--admin-socket" not in existing:
            base_env_vars["CEPH_ARGS"] = (
                f"{existing} {asok_arg}".strip() if existing else asok_arg
            )
        print(
            f"RBD perf dump enabled: CEPH_ARGS will use {asok_arg}",
            flush=True,
        )

    # Create images per client up-front so all subsequent loadpoints reuse them.
    for c in clients:
        if perf_dump_enabled:
            ensure_asok_dir(c, perf_dump_asok)
        for idx in range(images_per_client):
            image = f"{c}_img_{idx:02d}"
            ensure_rbd_image(
                c, rbd_bin, pool, image, image_size, config_path, keyring,
                client_id, recreate_images, env_vars=base_env_vars,
            )

    status_re = re.compile(
        r"Jobs: \d+ \(f=\d+\): \[.*?\]\[(?P<percent>[\d\.-]+)%\](?:\[.*\])?\[eta (?P<eta>.*)\]"
    )

    for idx, lp_cfg in enumerate(loadpoints):
        loadpoint = idx + 1
        print(f"Starting tests... Load Point: {loadpoint}", flush=True)
        print(f"Starting RBD Load Point: {loadpoint}", flush=True)

        loadpoint_results = []
        for c in clients:
            # Ensure results directory exists on each client
            subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", c, f"mkdir -p {results_dir}"]
            )

            for img_idx in range(images_per_client):
                image = f"{c}_img_{img_idx:02d}"

                fio_parts = []
                if base_env_vars:
                    fio_parts.append(
                        "".join(f'export {k}="{v}"; ' for k, v in base_env_vars.items())
                    )

                fio_parts.append(fio_bin)
                fio_parts.append(f"--name=lp{loadpoint:02d}_{c}_{img_idx:02d}")
                fio_parts.append("--ioengine=rbd")
                fio_parts.append(f"--pool={pool}")
                fio_parts.append(f"--rbdname={image}")
                if client_id:
                    fio_parts.append(f"--clientname={client_id}")

                if "size" in lp_cfg:
                    fio_parts.append(f"--size={lp_cfg['size']}")
                if "block-size" in lp_cfg:
                    fio_parts.append(f"--bs={lp_cfg['block-size']}")
                if "iodepth" in lp_cfg:
                    fio_parts.append(f"--iodepth={lp_cfg['iodepth']}")
                if "readwrite" in lp_cfg:
                    fio_parts.append(f"--rw={lp_cfg['readwrite']}")
                if "direct" in lp_cfg:
                    fio_parts.append(f"--direct={lp_cfg['direct']}")
                if "rwmixread" in lp_cfg:
                    fio_parts.append(f"--rwmixread={lp_cfg['rwmixread']}")
                # fio's rbd ioengine crashes with --create_serialize=0 when
                # numjobs > 1: setup_files (which calls fio_rbd_setup, i.e.
                # the librbd connect) runs inside each spawned thread and
                # some threads race to a NULL io_ops_data. Only emit the
                # flag when create_serialize=1 (fio's default), and swallow
                # explicit 0 requests silently.
                cs = lp_cfg.get("create_serialize")
                if cs is not None and int(cs) != 0:
                    fio_parts.append(f"--create_serialize={cs}")
                if "threads" in lp_cfg:
                    fio_parts.append(f"--numjobs={lp_cfg['threads']}")
                # rbd ioengine sets `td->o.use_thread = 1` in its own setup
                # so we don't need to emit --thread; leaving it out avoids
                # fighting the ioengine over process/thread mode.

                duration = lp_cfg.get("duration", settings.get("duration", 0))
                if duration:
                    fio_parts.append("--time_based=1")
                    fio_parts.append(f"--runtime={duration}")

                for key in ["gtod_reduce", "ramp_time", "randrepeat"]:
                    if key in lp_cfg:
                        fio_parts.append(f"--{key}={lp_cfg[key]}")
                    elif key in settings:
                        fio_parts.append(f"--{key}={settings[key]}")

                if "extra_args" in lp_cfg and lp_cfg["extra_args"]:
                    fio_parts.append(lp_cfg["extra_args"])

                filename = f"{CommonUtils.get_workload_base_name('rbd', 'result', c, loadpoint, settings, lp_cfg)}.json"
                remote_path = f"{results_dir}/{filename}"
                cmd = " ".join(fio_parts)
                cmd += f" --group_reporting --output-format=json+ --output={remote_path} --eta=always"

                print(f"[{c}] Executing fio (rbd): {cmd}", flush=True)
                ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", c, "bash -s"]
                proc = subprocess.Popen(
                    ssh_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                proc.stdin.write(cmd + "\n")
                proc.stdin.close()

                run_phase_started = False
                last_status_time = 0.0
                dump_stop = threading.Event()
                dump_thread = None
                dumped_flag = {"done": False}
                host_tag = (
                    f"{c}_img{img_idx:02d}" if images_per_client > 1 else c
                )
                for line in proc.stdout:
                    line = line.strip()
                    if line.startswith("Jobs:"):
                        m = status_re.search(line)
                        if m:
                            percent = m.group("percent")
                            eta = m.group("eta")
                            try:
                                if float(percent) < 0:
                                    percent = "0.0"
                            except ValueError:
                                pass

                            if not run_phase_started and percent != "-.-":
                                print("Starting RUN phase", flush=True)
                                run_phase_started = True
                                if perf_dump_enabled:
                                    reset_client_perf(
                                        c, ceph_bin, perf_dump_asok, base_env_vars
                                    )
                                    dump_thread = threading.Thread(
                                        target=schedule_perf_dump,
                                        args=(
                                            c,
                                            ceph_bin,
                                            perf_dump_asok,
                                            results_dir,
                                            loadpoint,
                                            settings,
                                            lp_cfg,
                                            host_tag,
                                            duration,
                                            base_env_vars,
                                            dump_stop,
                                            dumped_flag,
                                        ),
                                        daemon=True,
                                    )
                                    dump_thread.start()

                            now = time.monotonic()
                            if now - last_status_time >= 1.0:
                                last_status_time = now
                                ts_prefix = ""
                                if timestamp_progress:
                                    ts_prefix = datetime.datetime.now(datetime.timezone.utc).strftime(
                                        "%Y-%m-%dT%H:%M:%S.%f+0000"
                                    ) + " "
                                print(
                                    f"{ts_prefix}[{c}] Fio(rbd) Status: {percent}% complete, ETA: {eta}",
                                    flush=True,
                                )
                    else:
                        print(f"[{c}] {line}", flush=True)

                # fio stdout closed — stop the timer and attempt a final dump
                # if the scheduled one has not already succeeded (short runs).
                dump_stop.set()
                if dump_thread is not None:
                    dump_thread.join(timeout=5)
                if perf_dump_enabled and run_phase_started and not dumped_flag["done"]:
                    data = dump_client_perf(
                        c,
                        ceph_bin,
                        perf_dump_asok,
                        results_dir,
                        loadpoint,
                        settings,
                        lp_cfg,
                        host_tag,
                        env_vars=base_env_vars,
                    )
                    if data is not None:
                        dumped_flag["done"] = True

                proc.wait()
                if proc.returncode != 0:
                    print(
                        f"[{c}] fio (rbd) failed with return code {proc.returncode}",
                        flush=True,
                    )

                    class SimpleExecutor:
                        def run_remote(self, h, cmd, check=False):
                            result = subprocess.run(
                                ["ssh", "-o", "StrictHostKeyChecking=no", h, "bash -s"],
                                input=cmd + "\n",
                                capture_output=True,
                                text=True,
                            )
                            return result.stdout

                    CommonUtils.collect_journal_logs(SimpleExecutor(), clients, results_dir)
                    sys.exit(proc.returncode)

                # Copy result JSON from client to results_dir (admin host)
                print(f"[{c}] Copying results to {results_dir}...", flush=True)
                local_path = f"{results_dir}/{filename}"
                subprocess.run(
                    [
                        "scp",
                        "-o",
                        "StrictHostKeyChecking=no",
                        f"{c}:{remote_path}",
                        local_path,
                    ]
                )

                try:
                    with open(local_path, "r") as f:
                        data = json.load(f)

                    data["test_parameters"] = CommonUtils.get_human_readable_settings(
                        settings, lp_cfg
                    )
                    if args.runner_name:
                        data["test_parameters"]["Workload Runner"] = args.runner_name

                    data["test_results_summary"] = CommonUtils.get_summary(data)

                    with open(local_path, "w") as f:
                        json.dump(data, f, indent=4)
                    print(f"[{c}] Injected test parameters into {local_path}", flush=True)
                    loadpoint_results.append(data)
                except Exception as e:
                    print(f"[{c}] Failed to inject test parameters: {e}", flush=True)

        CommonUtils.write_multi_client_results_summary(
            "rbd",
            loadpoint_results,
            results_dir,
            loadpoint,
            settings,
            lp_cfg,
            num_clients=len(clients),
        )

        print(f"Finished RBD Load Point: {loadpoint}", flush=True)


if __name__ == "__main__":
    main()
