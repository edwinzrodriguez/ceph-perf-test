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

## Workload Runners

The framework supports three main workload runners. Each supports **Loadpoint Expansion**: if a parameter (like `threads`) is a list, it will run separate benchmarks for each value.

### 1. CephFS-Tool (`cephfs_tool`)
Uses the internal `cephfs-tool bench` for low-level metadata and I/O testing.

**Loadpoint Options:**
-   `files`: Number of files per thread.
-   `size`: File size (supports SI units: `128MiB`, `1GiB`).
-   `threads`: Number of concurrent threads.
-   `iterations`: Number of times to run the loadpoint.
-   `block-size`: I/O block size (e.g., `4MB`, `1MiB`).
-   `client-oc`: Enable/disable object cache (`0` or `1`).
-   `client-oc-size`: Size of object cache (supports SI units).
-   `duration`: Limit each phase to N seconds.

### 2. Fio (`fio`)
Flexible I/O tester for industry-standard benchmarking.

**Loadpoint Options:**
-   `size`: Total I/O size.
-   `block-size`: I/O block size (maps to `--bs`).
-   `threads`: Number of jobs (maps to `--numjobs`).
-   `iodepth`: Number of I/O units to keep in flight.
-   `readwrite`: I/O pattern (`read`, `write`, `randread`, `randrw`, etc.).
-   `ioengine`: I/O engine to use (e.g., `libaio`).
-   `direct`: Use non-buffered I/O (`0` or `1`).
-   `buffered`: Use buffered I/O (`0` or `1`).
-   `duration`: Runtime in seconds.
-   `ramp_time`: Warmup time.
-   `gtod_reduce`: Enable `gtod_reduce` for reduced overhead.

### 3. SPECstorage 2020 (`spec_storage`)
Standardized benchmark for storage solution performance.

## Ganesha Options

When `ganesha_enabled` is true, the framework provisions NFS-Ganesha on specified hosts.

**Global Options:**
-   `type`: `systemd` or `cephadm`.
-   `worker_threads`: Number of Ganesha worker threads.
-   `ceph_binary_path`: Path to `ceph` binary (default: `/usr/bin/ceph`).
-   `user_id`: Ceph user ID for Ganesha (default: `admin`).
-   `keyring_path`: Path to the keyring for the specified user.

**FSAL_CEPH Specific Options (can be lists for matrix testing):**
-   `umask`: File mode creation mask.
-   `client_oc`: Enable/disable Ceph object cache.
-   `client_oc_size`: Size of the object cache.
-   `async`: Enable Ceph async operations.
-   `zerocopy`: Enable Ceph zero-copy I/O.

## Performance Recording

The framework can automatically capture performance data during benchmarks:
-   **Perf Record**: Captures `perf.data`, generates reports, and SVG flamegraphs.
-   **SystemTap**: Can execute custom `.stp` scripts (e.g., `stap_script` option).
-   **Perf Dump**: Collects internal Ceph performance counters.

## Result Naming Convention

Output files are standardized for easy analysis:
`<workload>_<output_type>_<client>_<lp>_<options>`

-   `workload`: `cephfs_tool`, `fio`, or `sfs2020`.
-   `output_type`: `result`, `perf_dump`, `perf_record`.
-   `options`: Encodes the specific settings used (e.g., `s128MiB_t32_oc1`).

Results are stored in a timestamped directory under `results/`.
