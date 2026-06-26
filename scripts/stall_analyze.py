#!/usr/bin/env python3
"""
Correlate FIO progress drops with merged Ganesha+Ceph stall logs.

Designed for large merged logs (streaming, single pass). Complements
io_correl_scan.py, which tracks individual async write lifecycles.

Typical usage:
  stall_analyze.py stall-merged-1.log --fio-log mon-output.txt
  stall_analyze.py stall-merged-1.log --window 19:28:50,19:28:55
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

RE_MERGED_TS = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)"
)
RE_FIO_TS = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+).*Fio Status: ([\d.]+)%"
)
RE_CLIENT = re.compile(r"client\.(\d+)")
RE_THREAD = re.compile(r"\] (\S+) \d+ (?:client\.|mark_caps|wait_sync|_put)")
RE_INO = re.compile(r"on (0x[0-9a-f]+\.head)")
RE_FLUSH_WANT = re.compile(r"want (\d+) last (\d+)")


@dataclass
class SecondBucket:
    mark_caps_clean: int = 0
    put_inode: int = 0
    wait_sync_caps: int = 0
    waiting_data: int = 0
    cap_delay_requeue: int = 0
    io_alloc: int = 0
    io_complete: int = 0
    ceph_io_correl: int = 0
    finisher_log: int = 0
    dispose_stale_stall: int = 0
    delay_put: int = 0
    clients: Counter = field(default_factory=Counter)
    threads: Counter = field(default_factory=Counter)
    stale_inodes: Counter = field(default_factory=Counter)
    cap_lag_sum: int = 0
    cap_lag_count: int = 0


@dataclass
class FioSample:
    ts: datetime
    pct: float
    line: int


@dataclass
class ProgressDrop:
    ts: datetime
    from_pct: float
    to_pct: float
    delta: float
    line: int


@dataclass
class Analysis:
    total_lines: int
    first_ts: Optional[datetime]
    last_ts: Optional[datetime]
    buckets: Dict[str, SecondBucket]
    fio_samples: List[FioSample]
    progress_drops: List[ProgressDrop]
    dominant_client: Optional[str]
    dominant_thread: Optional[str]
    top_stale_inodes: List[Tuple[str, int]]


def parse_ts(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("+0000", "+00:00"))


def in_window(ts: datetime, start: Optional[datetime], end: Optional[datetime]) -> bool:
    if start and ts < start:
        return False
    if end and ts > end:
        return False
    return True


def parse_fio_log(lines: Iterable[str]) -> List[FioSample]:
    samples: List[FioSample] = []
    for line_no, line in enumerate(lines, start=1):
        m = RE_FIO_TS.search(line)
        if not m:
            continue
        samples.append(FioSample(
            ts=parse_ts(m.group(1)),
            pct=float(m.group(2)),
            line=line_no,
        ))
    return samples


def find_progress_drops(samples: List[FioSample], min_drop: float = 5.0) -> List[ProgressDrop]:
    drops: List[ProgressDrop] = []
    for prev, cur in zip(samples, samples[1:]):
        delta = cur.pct - prev.pct
        if delta <= -min_drop:
            drops.append(ProgressDrop(
                ts=cur.ts,
                from_pct=prev.pct,
                to_pct=cur.pct,
                delta=delta,
                line=cur.line,
            ))
    return drops


def classify_stall(bucket: SecondBucket) -> str:
    """Heuristic root-cause label for a one-second bucket."""
    if bucket.dispose_stale_stall:
        return "dispose_stale_stall"
    if bucket.mark_caps_clean > 50000:
        return "stale_inode_spin"
    if bucket.wait_sync_caps > 500 and bucket.mark_caps_clean > 1000:
        return "client_lock_contention"
    if bucket.wait_sync_caps > 800:
        return "cap_flush_backlog"
    if bucket.waiting_data > 400:
        return "data_flush_wait"
    if bucket.finisher_log > 100:
        return "finisher_pressure"
    if bucket.io_alloc > 0 and bucket.io_complete < bucket.io_alloc * 0.5:
        return "io_pipeline_stall"
    return "unknown"


def correlate_drop(
    drop: ProgressDrop,
    buckets: Dict[str, SecondBucket],
    radius: int = 3,
) -> List[Tuple[str, SecondBucket, str]]:
    """Return buckets within +/- radius seconds of a FIO drop."""
    hits: List[Tuple[str, SecondBucket, str]] = []
    for offset in range(-radius, radius + 1):
        key = (drop.ts.replace(microsecond=0)).isoformat()
        # step by offset seconds
        ts = drop.ts.replace(microsecond=0)
        from datetime import timedelta
        ts_key = (ts + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%S")
        b = buckets.get(ts_key)
        if b:
            hits.append((ts_key, b, classify_stall(b)))
    return hits


def scan_merged_log(
    path: str,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Analysis:
    buckets: Dict[str, SecondBucket] = defaultdict(SecondBucket)
    total_lines = 0
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None
    global_clients: Counter = Counter()
    global_threads: Counter = Counter()
    global_stale: Counter = Counter()

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            total_lines += 1
            m = RE_MERGED_TS.match(line)
            if not m:
                continue
            ts = parse_ts(m.group(1))
            if not in_window(ts, window_start, window_end):
                continue
            if first_ts is None:
                first_ts = ts
            last_ts = ts
            sec = ts.strftime("%Y-%m-%dT%H:%M:%S")
            b = buckets[sec]

            if "mark_caps_clean" in line:
                b.mark_caps_clean += 1
                cm = RE_CLIENT.search(line)
                if cm:
                    b.clients[cm.group(1)] += 1
                    global_clients[cm.group(1)] += 1
                im = RE_INO.search(line)
                if im:
                    b.stale_inodes[im.group(1)] += 1
                    global_stale[im.group(1)] += 1
                if "client." not in line:
                    tm = RE_THREAD.search(line)
                    if tm:
                        b.threads[tm.group(1)] += 1
                        global_threads[tm.group(1)] += 1
            elif "_put_inode" in line:
                b.put_inode += 1
                cm = RE_CLIENT.search(line)
                if cm:
                    b.clients[cm.group(1)] += 1
                    global_clients[cm.group(1)] += 1
                if "lp01" not in line and "lp02" not in line:
                    im = RE_INO.search(line)
                    if im:
                        b.stale_inodes[im.group(1)] += 1
                        global_stale[im.group(1)] += 1
                tm = RE_THREAD.search(line)
                if tm:
                    b.threads[tm.group(1)] += 1
                    global_threads[tm.group(1)] += 1
            elif "wait_sync_caps" in line:
                b.wait_sync_caps += 1
                wm = RE_FLUSH_WANT.search(line)
                if wm:
                    want, last = int(wm.group(1)), int(wm.group(2))
                    b.cap_lag_sum += want - last
                    b.cap_lag_count += 1
            elif "waiting on data" in line:
                b.waiting_data += 1
            elif "cap_delay_requeue" in line:
                b.cap_delay_requeue += 1
            elif "io_correl alloc" in line:
                b.io_alloc += 1
            elif "io_correl nfs4_write_cb" in line:
                b.io_complete += 1
            elif "[ceph" in line and "io_correl" in line:
                b.ceph_io_correl += 1
            elif "finisher_thread" in line or "io_correl client_finisher" in line:
                b.finisher_log += 1
            elif "io_correl dispose_stale_inodes stalled" in line:
                b.dispose_stale_stall += 1
            elif "io_correl delay_put_inodes" in line:
                b.delay_put += 1

    dom_client = global_clients.most_common(1)[0][0] if global_clients else None
    dom_thread = global_threads.most_common(1)[0][0] if global_threads else None

    return Analysis(
        total_lines=total_lines,
        first_ts=first_ts,
        last_ts=last_ts,
        buckets=dict(buckets),
        fio_samples=[],
        progress_drops=[],
        dominant_client=dom_client,
        dominant_thread=dom_thread,
        top_stale_inodes=global_stale.most_common(10),
    )


def parse_window(spec: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    parts = spec.split(",")
    start = parse_ts(parts[0]) if parts[0] else None
    end = parse_ts(parts[1]) if len(parts) > 1 and parts[1] else None
    return start, end


def format_report(
    analysis: Analysis,
    fio_samples: List[FioSample],
    drops: List[ProgressDrop],
    top_n: int = 10,
) -> str:
    out: List[str] = []
    out.append(f"Lines scanned: {analysis.total_lines}")
    if analysis.first_ts and analysis.last_ts:
        span = (analysis.last_ts - analysis.first_ts).total_seconds()
        out.append(
            f"Log window: {analysis.first_ts.isoformat()} .. "
            f"{analysis.last_ts.isoformat()} ({span:.1f}s)"
        )
    out.append("")

    # Overall event totals
    totals = Counter()
    for b in analysis.buckets.values():
        totals["mark_caps_clean"] += b.mark_caps_clean
        totals["put_inode"] += b.put_inode
        totals["wait_sync_caps"] += b.wait_sync_caps
        totals["waiting_data"] += b.waiting_data
        totals["cap_delay_requeue"] += b.cap_delay_requeue
        totals["io_alloc"] += b.io_alloc
        totals["io_complete"] += b.io_complete
        totals["ceph_io_correl"] += b.ceph_io_correl
        totals["finisher_log"] += b.finisher_log
    out.append("Event totals:")
    for k, v in totals.most_common():
        out.append(f"  {k}: {v}")
    out.append("")

    if totals["mark_caps_clean"] > 100000:
        out.append(
            "DIAGNOSIS: stale_inode_spin — mark_caps_clean/_put_inode dominate the log."
        )
        out.append(
            "  A tick/upkeep thread is spinning in dispose_stale_inodes() on inodes"
        )
        out.append(
            "  that cannot be deleted (nref>1). This holds client_lock and blocks"
        )
        out.append(
            "  wait_sync_caps / fsync on active I/O threads."
        )
        if analysis.dominant_client:
            out.append(f"  Spinning client: {analysis.dominant_client}")
        if analysis.dominant_thread:
            out.append(f"  Spinning thread: {analysis.dominant_thread}")
        if analysis.top_stale_inodes:
            out.append("  Stuck inodes (no active fio path):")
            for ino, cnt in analysis.top_stale_inodes[:5]:
                out.append(f"    {ino}: {cnt} events")
        out.append("")

    if totals["wait_sync_caps"] > 1000:
        out.append(
            "DIAGNOSIS: cap_flush_backlog — many threads in wait_sync_caps."
        )
        out.append(
            "  Cap flush TID has not advanced; MDS acks are behind client writes."
        )
        out.append("")

    if totals["waiting_data"] > 500:
        out.append(
            "DIAGNOSIS: data_flush_wait — fsync threads blocked on object cache flush."
        )
        out.append("")

    if totals["finisher_log"] == 0 and totals["ceph_io_correl"] == 0:
        out.append(
            "LOG GAP: no ceph-side io_correl or finisher_thread lines present."
        )
        out.append(
            "  Cannot confirm finisher-queue bottleneck from this log alone."
        )
        out.append(
            "  Enable: debug client=10 and debug finisher=10 (or rebuild with"
        )
        out.append(
            "  io_correl client_finisher logging)."
        )
        out.append("")

    # Per-second peaks
    ranked = sorted(
        analysis.buckets.items(),
        key=lambda kv: (
            kv[1].mark_caps_clean
            + kv[1].wait_sync_caps * 10
            + kv[1].waiting_data * 10
        ),
        reverse=True,
    )
    out.append(f"Top {top_n} seconds by stall pressure:")
    out.append(
        "  time                  mark  put   wait_caps wait_data io_alloc complete label"
    )
    for sec, b in ranked[:top_n]:
        label = classify_stall(b)
        out.append(
            f"  {sec}  {b.mark_caps_clean:6d} {b.put_inode:5d}"
            f" {b.wait_sync_caps:9d} {b.waiting_data:9d}"
            f" {b.io_alloc:8d} {b.io_complete:8d}  {label}"
        )
    out.append("")

    # FIO correlation
    if fio_samples:
        out.append(f"FIO samples: {len(fio_samples)}")
        if drops:
            out.append(f"FIO progress drops (>={5}%): {len(drops)}")
            for d in drops:
                out.append(
                    f"  {d.ts.isoformat()}  {d.from_pct:.1f}% -> {d.to_pct:.1f}%"
                    f"  (delta {d.delta:.1f}%)  line {d.line}"
                )
                corr = correlate_drop(d, analysis.buckets)
                if corr:
                    worst = max(
                        corr,
                        key=lambda x: x[1].mark_caps_clean + x[1].wait_sync_caps,
                    )
                    out.append(
                        f"    nearest log activity: {worst[0]} -> {worst[2]}"
                        f" (mark={worst[1].mark_caps_clean}"
                        f" wait_caps={worst[1].wait_sync_caps})"
                    )
                elif analysis.first_ts and analysis.last_ts:
                    if d.ts < analysis.first_ts or d.ts > analysis.last_ts:
                        out.append(
                            f"    WARNING: FIO drop at {d.ts.isoformat()} is outside"
                            f" merged log window"
                            f" ({analysis.first_ts.isoformat()}"
                            f" .. {analysis.last_ts.isoformat()})."
                            f" Re-extract bracket around the drop."
                        )
        else:
            out.append("  No large FIO progress drops in supplied fio log.")
        out.append("")
    elif drops:
        out.append("FIO drops detected but no fio log supplied for correlation.")
        out.append("")

    out.append("Recommendations:")
    if totals["mark_caps_clean"] > 100000:
        out.append(
            "  1. Fix dispose_stale_inodes() spin (break when inode_map size"
            " does not shrink)."
        )
        out.append(
            "  2. Investigate why stale lp01/lp02 files remain at nref=2 in inode_map."
        )
    if totals["wait_sync_caps"] > 1000:
        out.append(
            "  3. Check MDS cap flush latency and client cap_delay_requeue rate."
        )
    if totals["finisher_log"] == 0:
        out.append(
            "  4. Re-run with io_correl client_finisher + delay_put_inodes logging."
        )

    return "\n".join(out)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("logfile", help="Merged Ganesha+Ceph log")
    parser.add_argument(
        "--fio-log",
        help="Monitor output containing 'Fio Status: N% complete' lines",
    )
    parser.add_argument(
        "--window",
        help="Limit analysis to TIME_START,TIME_END (ISO timestamps)",
    )
    parser.add_argument(
        "--min-drop",
        type=float,
        default=5.0,
        help="Minimum FIO %% drop to report (default: 5)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of peak seconds to show (default: 10)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    win_start, win_end = None, None
    if args.window:
        win_start, win_end = parse_window(args.window)

    analysis = scan_merged_log(args.logfile, win_start, win_end)

    fio_samples: List[FioSample] = []
    drops: List[ProgressDrop] = []
    if args.fio_log:
        with open(args.fio_log, encoding="utf-8", errors="replace") as f:
            fio_samples = parse_fio_log(f)
        drops = find_progress_drops(fio_samples, args.min_drop)
    analysis.fio_samples = fio_samples
    analysis.progress_drops = drops

    print(format_report(analysis, fio_samples, drops, top_n=args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main())