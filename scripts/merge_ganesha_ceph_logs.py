#!/usr/bin/env python3
"""
Merge NFS-Ganesha and libcephfs client logs by timestamp.

Supports:
  - Ceph:  2026-06-15T13:01:52.964+0000 ...
  - Ganesha (syslog_usec): 2026-06-15T16:58:29.123456+0000 ...
  - Ganesha (legacy):      15/06/2026 16:58:29 : epoch ... (UTC if --ganesha-utc)
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, TextIO


CEPH_TS = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{4}))\s"
)
GANESHA_ISO_TS = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{4}))\s"
)
GANESHA_LEGACY_TS = re.compile(
    r"^(?:(?:\S+\s+){0,2})?(?P<ts>\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\s*:"
)


@dataclass(order=True)
class LogEntry:
    ts: datetime
    source: str
    line: str


def parse_iso_ts(raw: str) -> datetime:
    ts = raw
    if ts.endswith("Z"):
        ts = ts[:-1] + "+0000"
    if ts[-5] in "+-" and ts[-3] != ":":
        # Normalize +0000 / -0500 to +00:00 for fromisoformat.
        ts = ts[:-5] + ts[-5:-2] + ":" + ts[-2:]
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def parse_legacy_ganesha_ts(raw: str, assume_utc: bool) -> datetime:
    dt = datetime.strptime(raw, "%d/%m/%Y %H:%M:%S")
    if assume_utc:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_timestamp(line: str, source: str, ganesha_utc: bool) -> datetime | None:
    if source == "ceph":
        m = CEPH_TS.match(line)
        if m:
            return parse_iso_ts(m.group("ts"))
        return None

    m = GANESHA_ISO_TS.match(line)
    if m:
        return parse_iso_ts(m.group("ts"))

    m = GANESHA_LEGACY_TS.match(line)
    if m:
        return parse_legacy_ganesha_ts(m.group("ts"), ganesha_utc)

    return None


def iter_entries(
    fh: TextIO,
    source: str,
    ganesha_utc: bool,
    carry_continuations: bool,
) -> Iterator[LogEntry]:
    pending: list[str] = []
    pending_ts: datetime | None = None

    for raw in fh:
        line = raw.rstrip("\n")
        if not line:
            continue

        ts = extract_timestamp(line, source, ganesha_utc)
        if ts is None:
            if carry_continuations and pending_ts is not None:
                pending.append(line)
            continue

        if pending_ts is not None:
            yield LogEntry(pending_ts, source, "\n".join(pending))
            pending = []
            pending_ts = None

        pending = [line]
        pending_ts = ts

    if pending_ts is not None:
        yield LogEntry(pending_ts, source, "\n".join(pending))


def open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        import gzip

        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def merge_logs(
    files: Iterable[tuple[str, Path]],
    ganesha_utc: bool,
    carry_continuations: bool,
) -> Iterator[LogEntry]:
    import heapq

    handles = []
    iterators = []
    for source, path in files:
        fh = open_text(path)
        handles.append(fh)
        iterators.append(iter_entries(fh, source, ganesha_utc, carry_continuations))

    try:
        yield from heapq.merge(*iterators)
    finally:
        for fh in handles:
            fh.close()


def strip_leading_ts(line: str) -> str:
    m = CEPH_TS.match(line)  # same pattern as GANESHA_ISO_TS
    if m:
        return line[m.end():]
    m = GANESHA_LEGACY_TS.match(line)
    if m:
        return line[m.end():]
    return line


def format_entry(entry: LogEntry, prefix_source: bool) -> str:
    ts = entry.ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
    if prefix_source:
        return f"{ts} [{entry.source:7}] {strip_leading_ts(entry.line)}"
    return entry.line


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge Ganesha and Ceph client logs by timestamp."
    )
    parser.add_argument(
        "--ganesha",
        action="append",
        type=Path,
        default=[],
        help="Ganesha log file (repeatable; .gz supported)",
    )
    parser.add_argument(
        "--ceph",
        action="append",
        type=Path,
        default=[],
        help="Ceph client log file (repeatable; .gz supported)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("-"),
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--ganesha-utc",
        action="store_true",
        help="Treat legacy Ganesha DD/MM/YYYY timestamps as UTC",
    )
    parser.add_argument(
        "--no-prefix",
        action="store_true",
        help="Do not add [ganesha]/[ceph] source tags",
    )
    parser.add_argument(
        "--no-continuations",
        action="store_true",
        help="Drop lines without a leading timestamp",
    )
    args = parser.parse_args()

    if not args.ganesha and not args.ceph:
        parser.error("provide at least one of --ganesha or --ceph")

    files: list[tuple[str, Path]] = []
    for path in args.ganesha:
        files.append(("ganesha", path))
    for path in args.ceph:
        files.append(("ceph", path))

    for _, path in files:
        if not path.exists():
            print(f"error: {path} does not exist", file=sys.stderr)
            return 1

    out: TextIO | BinaryIO
    if args.output == Path("-"):
        out = sys.stdout
    else:
        out = args.output.open("w", encoding="utf-8")

    prefix = not args.no_prefix
    count = 0
    for entry in merge_logs(files, args.ganesha_utc, not args.no_continuations):
        print(format_entry(entry, prefix), file=out)
        count += 1

    if args.output != Path("-"):
        out.close()
        print(f"merged {count} entries -> {args.output}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())