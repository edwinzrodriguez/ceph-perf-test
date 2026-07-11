# CephFS Performance Test Framework

A structured framework for running performance benchmarks on CephFS using various workload drivers, supporting both kernel mounts and NFS-Ganesha.

## Theory of Operation

The framework operates on a **Test Matrix** principle. It iterates through combinations of MDS settings and Ganesha settings, automatically provisioning the environment for each iteration.

1.  **Configuration**: Defined in a YAML file (e.g., `MDSConfigurationSettings.yml`).
2.  **Matrix Expansion**: If any setting in `mds_settings` or `ganesha` is a list, the framework calculates the Cartesian product of all combinations.
3.  **Iteration Lifecycle**:
    -   Unmount clients.
    -   Rebuild/Reset CephFS (optional, based on settings).
    -   Apply MDS configurations.
    -   Provision Ganesha (if enabled).
    -   Mount clients (Kernel or NFS).
    -   Expand and run Workload Loadpoints.
    -   Collect results and performance traces.

## Usage

```bash
./cephfs_perf_runner.py <config.yaml> <ansible_inventory>
```

---

## Configuration Reference

### Top-Level Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `fs_name` | string | required | Name of the CephFS filesystem |
| `num_filesystems` | int | `1` | Number of filesystems to create |
| `fs_manager_type` | string | `CephFSManager` | Manager class for filesystem operations |
| `mount_manager_type` | string | `MountKernelManager` | Mount handler (`MountKernelManager`, `MountNfsManager`, `StubMountManager`) |
| `mds_yaml_path` | string | `/cephfs_perf/mds.yaml` | Path to MDS cephadm spec file |

---

### `ceph`

Connection details for the Ceph cluster.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `conf` | string | `/etc/ceph/ceph.conf` | Path to `ceph.conf` |
| `keyring` | string | | Path to Ceph keyring file |
| `user_id` | string | `admin` | Ceph client user ID |
| `fsid` | string | | Cluster FSID (UUID) |

---

### `ganesha`

Controls NFS-Ganesha deployment. Settings that are lists are expanded across the test matrix.

#### Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable NFS Ganesha |
| `type` | string | `cephadm` | Deployment type (`systemd` or `cephadm`) |
| `service_id` | string | `ganesha` | Service identifier (cephadm only) |
| `binary_path` | string | `/usr/local/ceph/bin/ganesha.nfsd` | Path to `ganesha.nfsd` executable (systemd only) |
| `ceph_binary_path` | string | `/usr/bin/ceph` | Path to the `ceph` CLI |
| `pid_path` | string | `/var/run/ganesha.pid` | PID file location (systemd only) |
| `config_path` | string | `/etc/ganesha/ganesha.conf` | Ganesha config path (systemd only) |
| `ganesha_yaml_path` | string | `/cephfs_perf/ganesha.yaml` | Cephadm spec YAML path (cephadm only) |

#### Authentication

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `user_id` | string | inherits `ceph.user_id` | Ganesha client user ID |
| `keyring_path` | string | inherits `ceph.keyring` | Keyring path for Ganesha |

#### FSAL_CEPH Tuning (support lists for matrix sweep)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `client_oc` | bool or list | | Enable/disable client object cache |
| `client_oc_size` | string or list | | Object cache size (e.g., `16GiB`, `1GiB`) |
| `async` | bool or list | | Enable async FSAL operations |
| `zerocopy` | bool or list | | Enable zero-copy I/O |
| `umask` | int | | File creation umask |

#### Threading & Performance

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker_threads` | int | | Number of Ganesha 9P worker threads |
| `msgr_workers` | int | | Ceph messenger worker threads (`ms_async_op_threads`) |
| `rpc_ioq_thrdmin` | int | | RPC I/O queue minimum threads |
| `rpc_ioq_thrdmax` | int or list | | RPC I/O queue maximum threads |

#### Logging

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `log_level` | string | | When set, writes a `LOG {}` block to `ganesha.conf` with per-component levels. NFS_V4 and FSAL are set to this level; other components use fixed levels (DEBUG for DISPATCH/SESSIONS/CLIENTID/STATE, INFO/EVENT for noisy subsystems). Disables the `-F -L STDOUT -N` command-line flags. Example values: `NIV_DEBUG`, `NIV_INFO`, `NIV_EVENT`. |
| `client_log_level` | int | `1` | Ceph client `debug_client` level written to the per-host `ceph.conf` |

#### Environment Variables

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `env_vars` | dict | `{}` | Environment variables set before launching `ganesha.nfsd`. Values are double-quoted, allowing `$VAR` and `${VAR}` expansion. Merged with framework defaults (`ENABLE_LOCKSTAT`, `GSS_USE_HOSTNAME`, `CEPH_CONF`); user-provided keys override defaults. Example: `LD_LIBRARY_PATH: "/usr/local/lib:${LD_LIBRARY_PATH}"` |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling on Ganesha |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `ganesha.nfsd` | Executable name to attach perf to |
| `perf_record_duration` | int | | Profiling duration in seconds |

#### Lock Statistics

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `lockstat.enabled` | bool | `false` | Collect lock contention statistics |
| `lockstat.path` | string | `/usr/local/bin/ceph-lockstat` | Path to `ceph-lockstat` binary |
| `lockstat.threshold` | int | `0` | Lock contention reporting threshold |

---

### `cephfs_tool`

Configuration for the `cephfs-tool bench` workload runner.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/cephfs_tool/run_cephfs_workload.py` | Remote driver script path |
| `executable_path` | string | `/usr/local/bin/cephfs-tool` | Path to `cephfs-tool` binary |
| `config_path` | string | `/etc/ceph/ceph.conf` | ceph.conf path |
| `keyring` | string | | Keyring path |
| `client_id` | string | `admin` | Ceph client ID |
| `root_path` | string | `/` | Root path within the filesystem |
| `duration` | int | `0` | Global test duration limit in seconds (0 = unlimited) |
| `progress` | bool | `true` | Show progress output |
| `progress_interval` | int | `10` | Progress update interval in seconds |
| `msgr_workers` | int | | Default messenger worker count (overridable per loadpoint) |
| `env_vars` | dict | `{}` | Environment variables passed to `cephfs-tool`. Values are double-quoted, allowing `$VAR`/`${VAR}` expansion. Common use: `CEPH_ARGS`, `LD_LIBRARY_PATH`. |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `cephfs-tool` | Executable to profile |
| `perf_record_duration` | int | `30` | Profiling duration in seconds |

#### Lock Statistics

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `lockstat.enabled` | bool | `false` | Collect lock contention statistics |
| `lockstat.path` | string | `/usr/local/bin/ceph-lockstat` | Path to `ceph-lockstat` |
| `lockstat.asok` | string | `/var/run/ceph/cephfs-tool.asok` | Admin socket path |
| `lockstat.threshold` | int | `0` | Reporting threshold |

#### Loadpoint Options

Each entry in `loadpoints` is a dict (or expanded from lists via Cartesian product):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `files` | int | `1024` | Number of files per thread |
| `size` | string | `128MiB` | File size (SI units: `1GiB`, `256MiB`) |
| `iterations` | int | `3` | Number of write+read iterations |
| `threads` | int or list | `32` | Number of concurrent threads |
| `msgr_workers` | int or list | | Messenger workers (overrides global) |
| `block-size` | string or list | | I/O block size (e.g., `4MiB`, `256KiB`) |
| `client-oc` | int or list | | Client object cache (`0`=off, `1`=on) |
| `client-oc-size` | string or list | | Object cache size |
| `async` | bool | | Enable async I/O mode |
| `queue-depth` | int or list | | Queue depth for async mode |
| `extra_args` | string | | Additional arguments passed to `cephfs-tool bench` |

---

### `fio`

Configuration for the fio workload runner.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/fio/run_fio_workload.py` | Remote driver script path |
| `mounts_per_fs` | int | `1` | Number of mount points per filesystem per client |
| `gtod_reduce` | int | `1` | Enable `gtod_reduce` to reduce gettimeofday overhead |
| `ramp_time` | int | `5` | Warmup time in seconds before measuring |
| `threads_fio` | bool | | Use threads instead of forked processes |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `ganesha.nfsd` | Executable to profile |
| `perf_record_duration` | int | | Profiling duration in seconds |
| `flamegraph_path` | string | | Path to FlameGraph tools for SVG generation |
| `stap_script` | string | | Path to a SystemTap script to run alongside fio |

#### Loadpoint Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `duration` | int | | Test runtime in seconds |
| `size` | string | | Total I/O size per job (e.g., `5GiB`) |
| `block-size` | string or list | | I/O block size (maps to `--bs`) |
| `iodepth` | int or list | | I/O queue depth |
| `readwrite` | string or list | | Access pattern (`randread`, `randwrite`, `randrw`, `read`, `write`) |
| `rwmixread` | int or list | | Read percentage for `randrw` (e.g., `75`) |
| `ioengine` | string or list | | I/O engine (e.g., `libaio`) |
| `direct` | int or list | | Direct I/O (`1`=on, `0`=off) |
| `buffered` | int | | Buffered I/O mode |
| `create_serialize` | int | | Serialize file creation (`0` or `1`) |
| `threads` | int or list | | Number of fio jobs |
| `ramp_time` | int | | Per-loadpoint ramp time override |
| `extra_args` | string | | Additional fio command-line arguments |

---

### `rados_bench`

Configuration for the `rados bench` workload runner. This workload targets a RADOS pool directly (not CephFS), making it useful for isolating OSD-level performance from MDS overhead.

Set `mount_manager_type: StubMountManager` when running rados bench only — no filesystem mounts are needed.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/rados_bench/run_rados_workload.py` | Remote driver script path |
| `executable_path` | string | `/usr/local/bin/rados` | Path to the `rados` binary |
| `config_path` | string | `/etc/ceph/ceph.conf` | Path to `ceph.conf` |
| `keyring` | string | | Path to Ceph keyring file |
| `client_id` | string | `admin` | Ceph client user ID |
| `pool` | string | required | RADOS pool to benchmark. Created automatically by `CephPoolManager` if it does not exist. |
| `pool_pg_num` | int | | PG count for the pool (optional) |
| `pool_size` | int | | Replication size (optional) |
| `pool_min_size` | int | | Minimum replication size (optional) |
| `pool_recreate` | bool | `false` | Wipe and recreate the pool before each iteration |
| `no_cleanup` | bool | `true` | Keep bench objects after write phase so subsequent read loadpoints can find them |
| `duration` | int | `30` | Default bench duration in seconds (overridable per loadpoint) |
| `env_vars` | dict | `{}` | Environment variables passed to `rados`. Values are double-quoted, allowing `$VAR`/`${VAR}` expansion. |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `rados` | Executable to profile |
| `perf_record_duration` | int | `30` | Profiling duration in seconds |

#### Loadpoint Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `readwrite` | string or list | `seqwrite` | Access pattern: `seqwrite`, `randwrite`, `seqread`, `randread` |
| `threads` | int or list | `16` | Number of concurrent I/O streams (`--concurrent-ios`) |
| `duration` | int | global `duration` | Per-loadpoint bench duration in seconds |
| `min-object-size` | string | | Minimum object size for write loadpoints (e.g., `4KiB`). SI units supported. |
| `max-object-size` | string | | Maximum object size for write loadpoints (e.g., `4MiB`). SI units supported. |
| `read-percent` | int | | Read percentage (passed as `--read-percent`; for mixed workloads) |
| `no_cleanup` | bool | global `no_cleanup` | Per-loadpoint override for `--no-cleanup` |
| `run_name` | string | `<fs_name>_<client>` | Override the `--run-name` prefix. Useful when chaining write and read loadpoints across separate runs. |
| `extra_args` | string | | Additional arguments appended verbatim to the `rados bench` command |

> **Note on read loadpoints**: `seqread` and `randread` require objects from a prior `seqwrite`/`randwrite` run with the same `--run-name`. Set `no_cleanup: true` on the write loadpoint so objects persist for subsequent reads.

#### Example Configuration

```yaml
mount_manager_type: "StubMountManager"

rados_bench:
  results_base_dir: "/cephfs_perf/results"
  run_command: "/cephfs_perf/rados_bench/run_rados_workload.py"
  executable_path: "/usr/local/bin/rados"
  config_path: "/etc/ceph/ceph.conf"
  keyring: "/etc/ceph/ceph.client.admin.keyring"
  client_id: "admin"
  pool: "rados_bench_pool"
  no_cleanup: true
  duration: 30
  loadpoints:
    - readwrite: ["seqwrite", "randwrite", "seqread", "randread"]
      threads: [16, 32]
      duration: 30
      min-object-size: "4KiB"
      max-object-size: "4MiB"
      extra_args: ""
```

---

### `specstorage`

Configuration for the SPECstorage 2020 workload runner.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/sfs2020/run_sfs2020_workload.py` | Remote driver script path |
| `output_path` | string | `/cephfs_perf/sfs2020/spec_2020.txt` | Path where the generated spec file is written |
| `workload_dir` | string | `/cephfs_perf/sfs2020/SPECstorage2020` | SPECstorage installation directory on admin host |
| `netmist_env` | string | `netmist.env` | Path to a local YAML file with license key and paths |
| `benchmark` | string | `SWBUILD` | Workload type (`SWBUILD`, `VDA`, `EDA_BLENDED`, `AI_IMAGE`, `ENOMICS`) |
| `mounts_per_fs` | int | `1` | Number of mount points per filesystem per client |
| `increment` | int | `1` | Increment between load points (maps to `INCR_LOAD`) |
| `num_runs` | int | `1` | Number of runs per load point (maps to `NUM_RUNS`) |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `ceph-mds` | Executable to profile |
| `perf_record_duration` | int | | Profiling duration in seconds |

#### Lock Statistics

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `lockstat.enabled` | bool | `false` | Collect lock contention statistics |
| `lockstat.path` | string | `/usr/local/bin/ceph-lockstat` | Path to `ceph-lockstat` |
| `lockstat.threshold` | int | `0` | Reporting threshold |

#### Loadpoint Options

| Key | Type | Description |
|-----|------|-------------|
| `loadpoints` | list of int | Load metric values to test (e.g., `[1, 2, 4, 8]`) |

#### `netmist_env` File

A local YAML file providing SPECstorage licensing information:

```yaml
netmist_license_key: 1234
netmist_license_path: "/tmp/netmist_license_key"
sfs2020_archive: "/path/to/SPECstorage2020.tgz"  # optional
```

---

### `logging`

Controls MDS debug logging during tests.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable MDS debug logging |
| `debug_mds` | int | `5` | MDS subsystem debug level |
| `debug_ms` | int | `1` | Messenger subsystem debug level |

---

### `mds_settings`

MDS parameters swept across the test matrix. Each key maps to a single value or list of values; lists are expanded into the Cartesian product with other list settings.

| Key | Type | Example | Description |
|-----|------|---------|-------------|
| `mds_cache_memory_limit` | string or list | `[128Gi]` | MDS cache memory limit |
| `max_mds` | int or list | `[1, 2]` | Maximum number of active MDSs |
| `cpus` | int or list | `[4, 8]` | CPU cores allocated to MDS |
| `mds_max_caps_per_client` | string or list | `[100k, 200k]` | Max capabilities per client |
| `mds_recall_max_caps` | string or list | `[5k, 10k]` | Max caps to recall at once |
| `mds_recall_max_decay_rate` | int or list | `[1, 2]` | Decay rate for cap recall |
| `mds_cache_trim_threshold` | string or list | `[64Ki, 128Ki]` | Cache trim threshold |
| `mds_cache_reservation` | int or list | `[5, 10]` | Cache reservation percentage |
| `mds_log_max_segments` | int or list | `[30, 60]` | Maximum MDS log segments |

---

## Mount Managers

Selected via `mount_manager_type`.

### `MountKernelManager`

Mounts CephFS directly via the kernel client.

- No additional configuration keys.
- Uses `fs_name`, the cluster's monitor addresses, and the Ceph keyring.

### `MountNfsManager`

Mounts via NFS through Ganesha.

- Requires `ganesha.enabled: true`.
- Distributes clients across Ganesha nodes in round-robin order.
- `mounts_per_fs` controls the number of mount points per client per filesystem.

Configured via the `mount_nfs` section:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mount_options` | string | `nfsvers=4.1,proto=tcp` | Options passed to `mount -t nfs -o` |

```yaml
mount_nfs:
  mount_options: "nfsvers=4.1,proto=tcp,sec=sys"
```

### `StubMountManager`

No-op mount manager. Used for workloads that manage their own connectivity (e.g., `cephfs_tool` uses its own libcephfs handle).

---

## Inventory Providers

### `DirectInventoryProvider`

Parses a YAML-based inventory defined directly in the config file under `inventory`. Used automatically when no external Ansible inventory file is provided.

> **Note**: A `mons` group is required. The first host in `mons` is designated the **admin host**.

Supported groups: `mons`, `mgrs`, `clients`, `ganeshas`, `mdss`, `osds`.

Per-host fields:

| Field | Required | Description |
|-------|----------|-------------|
| `ansible_ssh_host` | yes | IP or hostname for SSH |
| `ansible_ssh_user` | yes | SSH username |
| `ansible_ssh_port` | no | SSH port (default: `22`) |
| `ansible_ssh_private_key_file` | no | Path to SSH private key |
| `private_ip` | no | Cluster-internal IP for client-to-cluster traffic |

```yaml
inventory:
  mons:
    mon-000:
      ansible_ssh_host: 169.63.188.95
      ansible_ssh_user: root
      private_ip: 10.241.64.69
  clients:
    client-000:
      ansible_ssh_host: 169.63.179.214
      ansible_ssh_user: root
      private_ip: 10.241.64.70
```

### `AnsibleInventoryProvider`

Parses an Ansible-style INI inventory file.

> **Note**: A `[mons]` group is required.

- Loads global variables from `group_vars/all.yml` and `cluster.json` relative to the project parent directory.
- Supports `{{ var_name }}` template syntax.

```ini
[mons]
mon-000 ansible_ssh_user=root ansible_ssh_host=10.241.64.69

[clients]
client-000 ansible_ssh_user=root ansible_ssh_host=10.241.64.70
```

---

## Performance Recording

| Option | Description |
|--------|-------------|
| `perf_record: true` | Captures `perf.data`, generates text reports and SVG flamegraphs via FlameGraph |
| `perf_record_executable` | Name of the process to attach `perf record` to |
| `perf_record_duration` | How long to record in seconds |
| `flamegraph_path` | Path to FlameGraph tool directory (fio only) |
| `stap_script` | Path to a SystemTap `.stp` script to run alongside the workload (fio only) |

---

## Result Naming Convention

Output files follow the pattern:

```
<workload>_<output_type>_<client>_lp<N>_<encoded_settings>.json
```

- `workload`: `cephfs_tool`, `fio`, or `sfs2020`
- `output_type`: `result`, `perf_dump`
- `N`: load point number
- `encoded_settings`: abbreviated key-value pairs for the active parameters (e.g., `s5GiB_t32_oc1_ocs16GiB_bs4MiB_mw8`)

Results are stored in a timestamped directory under `results_base_dir`.
