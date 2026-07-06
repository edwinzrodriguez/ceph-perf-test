#!/usr/bin/env python3
"""
Scan merged Ganesha + Ceph logs for mismatched async write I/O.

Correlates operations via io_correl tags (instrumented builds) or legacy
patterns where C_Write_Finisher::try_complete 'this' equals Ganesha
write_data / cbi pointers.

Reports incomplete operations and line ranges that bracket each issue cluster.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple

HEX = r"(0x[0-9a-fA-F]+)"
HEX_NC = r"(?:0x[0-9a-fA-F]+)"

# --- io_correl (instrumented builds) ---------------------------------------

RE_IO_ALLOC = re.compile(rf"io_correl alloc write_data={HEX} offset=(\d+)")
RE_IO_SUBMIT = re.compile(
    rf"io_correl submit write caller_arg={HEX} cbi={HEX} io_info={HEX}"
    rf" fileid=(\d+) offset=(\d+)"
)
RE_IO_SUBMIT_RET = re.compile(
    rf"io_correl submit write returned caller_arg={HEX} cbi={HEX} result=(-?\d+)"
)
RE_IO_GANESHA_CB = re.compile(
    rf"io_correl callback write caller_arg={HEX} cbi={HEX} io_info={HEX}"
)
RE_IO_GANESHA_FSAL = re.compile(
    rf"io_correl callback write fsal_complete_io caller_arg={HEX} cbi={HEX}"
)
RE_IO_NFS4_CB = re.compile(rf"io_correl nfs4_write_cb write_data={HEX}")
RE_IO_CEPH_ENTRY = re.compile(
    rf"io_correl ceph_ll_nonblocking_readv_writev io_info={HEX} priv={HEX}"
    rf" onfinish={HEX} fh={HEX} off=(-?\d+)"
)
RE_IO_CEPH_RET = re.compile(
    rf"io_correl ceph_ll_nonblocking_readv_writev return"
    rf" io_info={HEX} priv={HEX} onfinish={HEX} r=(-?\d+)"
)
RE_IO_CWF_CREATED = re.compile(
    rf"io_correl CWF created CWF={HEX} onfinish={HEX} ino=(\S+) offset=(-?\d+)"
    rf"(?: size=\d+)? do_fsync=(\d+)"
)
RE_IO_CWF_QUEUE_FINISH_IO = re.compile(
    rf"io_correl CWF queue_finish_io CWF={HEX} onfinish={HEX} ino=(\S+)"
    rf" offset=(-?\d+) r=(-?\d+)"
)
RE_IO_CWF_FINISH_IO = re.compile(
    rf"io_correl CWF finish_io_complete CWF={HEX} onfinish={HEX} ino=(\S+)"
    rf" offset=(-?\d+) r=(-?\d+)"
)
RE_IO_CWF_KICKOFF_FSYNC = re.compile(
    rf"io_correl CWF kickoff fsync CWF={HEX} onfinish={HEX}"
    rf"(?: fsync_finish_ctx={HEX_NC})? fsync_state={HEX} ino=(\S+) offset=(-?\d+)"
)
RE_IO_CWF_FINISH_FSYNC = re.compile(
    rf"io_correl CWF finish_fsync CWF={HEX} onfinish={HEX} ino=(\S+)"
    rf" offset=(-?\d+) r=(-?\d+)"
)
RE_IO_CWF_COMPLETE = re.compile(
    rf"io_correl CWF complete CWF={HEX} onfinish={HEX} r=(-?\d+)"
)
RE_IO_FSYNC_WAIT_FLUSH = re.compile(
    rf"io_correl fsync wait flush fsync_state={HEX} onfinish={HEX} ino=(\S+)"
)
RE_IO_FSYNC_WAIT_CAPS = re.compile(
    rf"io_correl fsync wait caps fsync_state={HEX} onfinish={HEX} ino=(\S+)"
    rf" flush_tid=(\d+) oldest_flushing=(\d+)"
)
RE_IO_FSYNC_STATE_COMPLETE = re.compile(
    rf"io_correl fsync state complete fsync_state={HEX} onfinish={HEX} ino=(\S+)"
    rf" r=(-?\d+)"
)
RE_IO_LL_FINISH = re.compile(
    rf"io_correl LL_Onfinish finish onfinish={HEX} io_info={HEX} priv={HEX}"
)
RE_IO_DISPATCH = re.compile(rf"io_correl dispatch callback io_info={HEX} priv={HEX}")

# --- legacy ----------------------------------------------------------------

RE_LEGACY_ALLOC = re.compile(rf"Allocated write_data {HEX}")
RE_LEGACY_OFFSET = re.compile(r"nfs4_op_write :NFS4 :F_DBG :offset = (\d+)")
RE_LEGACY_CEPH_CALL = re.compile(
    r"ceph_fsal_write2 :FSAL :F_DBG :Calling ceph_ll_nonblocking_readv_writev for write"
)
RE_LEGACY_CEPH_RET = re.compile(
    r"ceph_fsal_write2 :FSAL :F_DBG :ceph_ll_nonblocking_readv_writev for write returned (-?\d+)"
)
RE_LEGACY_CWF_TRY = re.compile(
    rf"C_Write_Finisher::try_complete this {HEX} .* fsync_finished (\d+)"
)
RE_LEGACY_CWF_DONE = re.compile(r" complete with iofinished_r (-?\d+)")
RE_LEGACY_LL_QUEUE = re.compile(
    rf"LL_Onfinish::finish queuing on client_finisher io_info={HEX} priv={HEX}"
)
RE_LEGACY_LL_DISPATCH = re.compile(
    rf"LL_Onfinish dispatching callback io_info={HEX} priv={HEX}"
)
RE_LEGACY_WRITE_CB = re.compile(
    r"ceph_write2_cb :FSAL :F_DBG :Write returned (\d+)"
)
RE_LEGACY_IO_WORK = re.compile(
    rf"fsal_complete_io :FSAL :F_DBG :{HEX} done io_work \(-1\) = (-?\d+)"
)
RE_GANESHA_THREAD = re.compile(r"ganesha\.nfsd\[([^\]]+)\]")

RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)")

UPKEEP_MARKERS = (
    "upkeep thread waiting",
    "trim_cache size",
    "collect_and_send_metrics",
    "collect_and_send_global_metrics",
    " tick",
    "reaper_run",
    "Now checking NFS4 clients",
    "renew_caps",
)


@dataclass
class OpRecord:
    key: str
    caller_arg: Optional[str] = None
    cbi: Optional[str] = None
    io_info: Optional[str] = None
    onfinish: Optional[str] = None
    cwf: Optional[str] = None
    ino: Optional[str] = None
    offset: Optional[int] = None
    first_line: int = 0
    last_line: int = 0
    events: List[str] = field(default_factory=list)

    allocated: bool = False
    submitted: bool = False
    async_accepted: bool = False
    cwf_started: bool = False
    cwf_completed: bool = False
    ll_queued: bool = False
    dispatched: bool = False
    ganesha_cb: bool = False
    fsal_complete: bool = False
    nfs4_cb: bool = False

    do_fsync: bool = False
    io_queued: bool = False
    io_finished: bool = False
    fsync_kickoff: bool = False
    fsync_finished: bool = False
    fsync_state: Optional[str] = None

    # legacy: generation bumps on pointer reuse
    generation: int = 0

    def touch(self, line_no: int, event: str) -> None:
        if self.first_line == 0:
            self.first_line = line_no
        self.last_line = line_no
        self.events.append(f"L{line_no}:{event}")

    def is_complete(self) -> bool:
        if self.nfs4_cb:
            return True
        if self.ganesha_cb and self.fsal_complete:
            return True
        # Ceph write pipeline finished (legacy: CWF 'complete with' is definitive)
        if self.cwf_completed:
            return True
        return False

    def missing_stages(self) -> List[str]:
        stages: List[str] = []
        if self.allocated and not self.submitted:
            stages.append("submit")
        elif self.submitted and not self.async_accepted:
            stages.append("ceph_accept")
        elif self.async_accepted and not self.cwf_started:
            stages.append("cwf_start")
        elif self.cwf_started and not self.io_queued and not self.io_finished:
            stages.append("queue_finish_io")
        elif self.io_queued and not self.io_finished:
            stages.append("io_finish")
        elif self.io_finished and self.do_fsync and not self.fsync_kickoff:
            stages.append("fsync_kickoff")
        elif self.fsync_kickoff and not self.fsync_finished:
            stages.append("fsync_finish")
        elif self.fsync_finished and not self.cwf_completed:
            stages.append("cwf_complete")
        elif self.cwf_started and not self.cwf_completed:
            stages.append("cwf_complete")
        elif self.cwf_completed and not self.ll_queued:
            stages.append("ll_queue")
        elif self.ll_queued and not self.dispatched:
            stages.append("dispatch")
        elif self.dispatched and not self.ganesha_cb:
            stages.append("ganesha_cb")
        elif self.ganesha_cb and not self.fsal_complete:
            stages.append("fsal_complete")
        return stages


def parse_ts(line: str) -> Optional[datetime]:
    m = RE_TS.match(line)
    return datetime.fromisoformat(m.group(1)) if m else None


def is_upkeep(line: str) -> bool:
    return any(m in line for m in UPKEEP_MARKERS)


def is_io_meaningful(line: str) -> bool:
    if is_upkeep(line):
        return False
    markers = (
        "io_correl",
        "Allocated write_data",
        "ceph_ll_nonblocking_readv_writev",
        "CWF queue_finish_io",
        "CWF kickoff fsync",
        "CWF finish_fsync",
        "fsync wait flush",
        "fsync wait caps",
        "fsync state complete",
        "C_Write_Finisher",
        "LL_Onfinish",
        "ceph_write2_cb",
        "fsal_complete_io",
        "nfs4_op_write",
        "wait_to_start_io",
    )
    return any(m in line for m in markers)


def ptr_key(ptr: str) -> str:
    return ptr.lower()


def merge_op_records(primary: OpRecord, secondary: OpRecord) -> OpRecord:
    """Merge secondary into primary; primary key is retained."""
    for field_name in (
        "caller_arg", "cbi", "io_info", "onfinish", "cwf", "ino", "offset",
        "fsync_state",
    ):
        pv = getattr(primary, field_name)
        sv = getattr(secondary, field_name)
        if pv is None and sv is not None:
            setattr(primary, field_name, sv)

    for field_name in (
        "allocated", "submitted", "async_accepted", "cwf_started",
        "cwf_completed", "ll_queued", "dispatched", "ganesha_cb",
        "fsal_complete", "nfs4_cb", "do_fsync", "io_queued", "io_finished",
        "fsync_kickoff", "fsync_finished",
    ):
        if getattr(secondary, field_name):
            setattr(primary, field_name, True)

    if secondary.first_line:
        if primary.first_line == 0 or secondary.first_line < primary.first_line:
            primary.first_line = secondary.first_line
    primary.last_line = max(primary.last_line, secondary.last_line)
    primary.events.extend(secondary.events)
    primary.generation = max(primary.generation, secondary.generation)
    return primary


class OpRegistry:
    """Track async writes; merge aliased pointers (priv/onfinish/CWF/cbi)."""

    def __init__(self) -> None:
        self.ops: Dict[str, OpRecord] = {}
        self.alias: Dict[str, str] = {}

    def _canonical(self, ptr: str) -> str:
        k = ptr_key(ptr)
        while k in self.alias:
            k = self.alias[k]
        return k

    def get(self, ptr: str) -> OpRecord:
        k = self._canonical(ptr)
        if k not in self.ops:
            self.ops[k] = OpRecord(key=ptr)
        return self.ops[k]

    def link(self, *ptrs: str) -> OpRecord:
        if not ptrs:
            raise ValueError("link requires at least one pointer")
        canon = self._canonical(ptrs[0])
        op = self.get(ptrs[0])
        for ptr in ptrs[1:]:
            k = ptr_key(ptr)
            other_canon = self._canonical(ptr)
            if other_canon == canon:
                self.alias[k] = canon
                continue
            if other_canon in self.ops:
                op = merge_op_records(op, self.ops[other_canon])
                self.ops[canon] = op
                del self.ops[other_canon]
                for alias_k, alias_v in list(self.alias.items()):
                    if alias_v == other_canon:
                        self.alias[alias_k] = canon
            self.alias[k] = canon
            self.alias[other_canon] = canon
        self.ops[canon] = op
        return op

    def unique_ops(self) -> List[OpRecord]:
        return list(self.ops.values())


def ganesha_thread(line: str) -> Optional[str]:
    m = RE_GANESHA_THREAD.search(line)
    return m.group(1) if m else None


def bump_generation(op: OpRecord, line_no: int) -> None:
    """New alloc reusing a pointer invalidates prior completion state."""
    op.generation += 1
    op.allocated = True
    op.submitted = False
    op.async_accepted = False
    op.cwf_started = False
    op.cwf_completed = False
    op.ll_queued = False
    op.dispatched = False
    op.ganesha_cb = False
    op.fsal_complete = False
    op.nfs4_cb = False
    op.do_fsync = False
    op.io_queued = False
    op.io_finished = False
    op.fsync_kickoff = False
    op.fsync_finished = False
    op.fsync_state = None
    op.first_line = line_no
    op.touch(line_no, f"alloc/gen{op.generation}")


@dataclass
class FsyncStateRecord:
    key: str
    cwf: Optional[str] = None
    kickoff_line: int = 0
    wait_flush: bool = False
    wait_caps: bool = False
    completed: bool = False
    last_line: int = 0


@dataclass
class PipelineCounts:
    ceph_entry: int = 0
    ceph_return_async: int = 0
    cwf_created: int = 0
    queue_finish_io: int = 0
    finish_io: int = 0
    fsync_kickoff: int = 0
    fsync_finish: int = 0
    fsync_state_complete: int = 0
    cwf_complete: int = 0
    ll_finish: int = 0
    dispatch: int = 0


@dataclass
class ScanResult:
    total_lines: int
    io_correl_hits: int
    legacy_mode: bool
    pipeline: PipelineCounts
    pending_ops: List[OpRecord]
    stall_ops: List[OpRecord]
    orphan_fsync_states: List[FsyncStateRecord]
    stall_line: Optional[int]
    stall_gap_secs: Optional[float]
    last_io_line: int
    final_io_work: Optional[int]
    bracket_ranges: List[Tuple[int, int, str]]


def get_fsync_state(
    fsync_states: Dict[str, FsyncStateRecord], ptr: str
) -> FsyncStateRecord:
    k = ptr_key(ptr)
    if k not in fsync_states:
        fsync_states[k] = FsyncStateRecord(key=ptr)
    return fsync_states[k]


def scan_log(lines: List[str], stall_threshold: float = 5.0) -> ScanResult:
    registry = OpRegistry()
    fsync_states: Dict[str, FsyncStateRecord] = {}
    pipeline = PipelineCounts()
    io_correl_hits = 0

    # per ganesha worker thread: alloc awaiting submit / submit awaiting accept
    thread_pending_alloc: Dict[str, str] = {}
    thread_pending_submit: Dict[str, str] = {}

    # CWF try without following complete (per ptr, per line)
    pending_cwf_line: Dict[str, int] = {}

    # LL queue/dispatch balance by io_info (orphan detection)
    ll_queued: Dict[str, int] = defaultdict(int)
    ll_dispatched: Dict[str, int] = defaultdict(int)

    last_io_ts: Optional[datetime] = None
    last_io_line = 0
    stall_line: Optional[int] = None
    stall_gap: Optional[float] = None
    final_io_work: Optional[int] = None

    pending_offset: Optional[int] = None

    for line_no, line in enumerate(lines, start=1):
        ts = parse_ts(line)

        if "io_correl" in line:
            io_correl_hits += 1

        if ts and last_io_ts and stall_line is None:
            gap = (ts - last_io_ts).total_seconds()
            if gap >= stall_threshold:
                stall_line = line_no
                stall_gap = gap

        if is_io_meaningful(line):
            if ts:
                last_io_ts = ts
            last_io_line = line_no

        m = RE_LEGACY_IO_WORK.search(line)
        if m:
            final_io_work = int(m.group(2))

        # ---- io_correl path -------------------------------------------------
        m = RE_IO_ALLOC.search(line)
        if m:
            op = registry.get(m.group(1))
            bump_generation(op, line_no)
            op.offset = int(m.group(2))
            continue

        m = RE_IO_SUBMIT.search(line)
        if m:
            caller_arg, cbi, io_info = m.group(1), m.group(2), m.group(3)
            op = registry.link(caller_arg, cbi, io_info)
            op.caller_arg = caller_arg
            op.cbi = cbi
            op.io_info = io_info
            op.offset = int(m.group(5))
            op.submitted = True
            op.touch(line_no, "submit")
            continue

        m = RE_IO_SUBMIT_RET.search(line)
        if m:
            caller_arg, cbi, result = m.group(1), m.group(2), int(m.group(3))
            op = registry.link(caller_arg, cbi)
            op.cbi = cbi
            if result == 0:
                op.async_accepted = True
                op.touch(line_no, "submit_return_async")
            else:
                op.async_accepted = True
                op.cwf_completed = True
                op.dispatched = True
                op.ganesha_cb = True
                op.fsal_complete = True
                op.touch(line_no, f"submit_return_sync r={result}")
            continue

        m = RE_IO_CEPH_ENTRY.search(line)
        if m:
            io_info, priv, onfinish = m.group(1), m.group(2), m.group(3)
            op = registry.link(priv, onfinish, io_info)
            op.io_info = io_info
            op.onfinish = onfinish
            op.cbi = priv
            op.touch(line_no, "ceph_entry")
            pipeline.ceph_entry += 1
            continue

        m = RE_IO_CEPH_RET.search(line)
        if m:
            io_info, priv, onfinish, result = (
                m.group(1), m.group(2), m.group(3), int(m.group(4)),
            )
            op = registry.link(priv, onfinish, io_info)
            if result == 0:
                op.async_accepted = True
                op.touch(line_no, "ceph_return_async")
                pipeline.ceph_return_async += 1
            continue

        m = RE_IO_CWF_CREATED.search(line)
        if m:
            cwf, onfinish, ino, offset, do_fsync = (
                m.group(1), m.group(2), m.group(3), int(m.group(4)),
                int(m.group(5)),
            )
            op = registry.link(cwf, onfinish)
            op.cwf = cwf
            op.onfinish = onfinish
            op.ino = ino
            op.offset = offset
            op.cwf_started = True
            op.do_fsync = bool(do_fsync)
            op.touch(line_no, f"cwf_created do_fsync={do_fsync}")
            pipeline.cwf_created += 1
            continue

        m = RE_IO_CWF_QUEUE_FINISH_IO.search(line)
        if m:
            cwf, onfinish, ino = m.group(1), m.group(2), m.group(3)
            op = registry.link(cwf, onfinish)
            op.cwf = cwf
            op.ino = ino
            op.io_queued = True
            op.touch(line_no, "queue_finish_io")
            pipeline.queue_finish_io += 1
            continue

        m = RE_IO_CWF_FINISH_IO.search(line)
        if m:
            cwf, onfinish, ino = m.group(1), m.group(2), m.group(3)
            op = registry.link(cwf, onfinish)
            op.cwf = cwf
            op.ino = ino
            op.io_finished = True
            op.touch(line_no, "cwf_finish_io")
            pipeline.finish_io += 1
            continue

        m = RE_IO_CWF_KICKOFF_FSYNC.search(line)
        if m:
            cwf, onfinish, fsync_state = m.group(1), m.group(2), m.group(3)
            op = registry.link(cwf, onfinish, fsync_state)
            op.cwf = cwf
            op.io_finished = True
            op.fsync_kickoff = True
            op.fsync_state = fsync_state
            op.touch(line_no, f"fsync_kickoff state={fsync_state}")
            st = get_fsync_state(fsync_states, fsync_state)
            st.cwf = cwf
            st.kickoff_line = line_no
            st.last_line = line_no
            pipeline.fsync_kickoff += 1
            continue

        m = RE_IO_CWF_FINISH_FSYNC.search(line)
        if m:
            cwf = m.group(1)
            op = registry.get(cwf)
            op.fsync_finished = True
            op.touch(line_no, "cwf_finish_fsync")
            pipeline.fsync_finish += 1
            continue

        m = RE_IO_FSYNC_WAIT_FLUSH.search(line)
        if m:
            fsync_state = m.group(1)
            st = get_fsync_state(fsync_states, fsync_state)
            st.wait_flush = True
            st.last_line = line_no
            continue

        m = RE_IO_FSYNC_WAIT_CAPS.search(line)
        if m:
            fsync_state = m.group(1)
            st = get_fsync_state(fsync_states, fsync_state)
            st.wait_caps = True
            st.last_line = line_no
            continue

        m = RE_IO_FSYNC_STATE_COMPLETE.search(line)
        if m:
            fsync_state = m.group(1)
            st = get_fsync_state(fsync_states, fsync_state)
            st.completed = True
            st.last_line = line_no
            pipeline.fsync_state_complete += 1
            continue

        m = RE_IO_CWF_COMPLETE.search(line)
        if m:
            cwf, onfinish = m.group(1), m.group(2)
            op = registry.link(cwf, onfinish)
            op.cwf_completed = True
            op.touch(line_no, "cwf_complete")
            pipeline.cwf_complete += 1
            continue

        m = RE_IO_LL_FINISH.search(line)
        if m:
            onfinish, io_info, priv = m.group(1), m.group(2), m.group(3)
            op = registry.link(priv, onfinish, io_info)
            op.ll_queued = True
            op.io_info = io_info
            op.onfinish = onfinish
            op.touch(line_no, "ll_finish")
            pipeline.ll_finish += 1
            continue

        m = RE_IO_DISPATCH.search(line)
        if m:
            io_info, priv = m.group(1), m.group(2)
            op = registry.link(priv, io_info)
            op.dispatched = True
            op.touch(line_no, "dispatch")
            pipeline.dispatch += 1
            continue

        m = RE_IO_GANESHA_CB.search(line)
        if m:
            caller_arg, cbi = m.group(1), m.group(2)
            op = registry.link(caller_arg, cbi)
            op.ganesha_cb = True
            op.touch(line_no, "ganesha_cb")
            continue

        m = RE_IO_GANESHA_FSAL.search(line)
        if m:
            caller_arg, cbi = m.group(1), m.group(2)
            op = registry.link(caller_arg, cbi)
            op.fsal_complete = True
            op.touch(line_no, "fsal_complete")
            continue

        m = RE_IO_NFS4_CB.search(line)
        if m:
            op = registry.get(m.group(1))
            op.nfs4_cb = True
            op.touch(line_no, "nfs4_cb")
            continue

        # ---- legacy path ----------------------------------------------------
        m = RE_LEGACY_OFFSET.search(line)
        if m:
            pending_offset = int(m.group(1))
            continue

        m = RE_LEGACY_ALLOC.search(line)
        if m:
            ptr = m.group(1)
            op = registry.get(ptr)
            bump_generation(op, line_no)
            if pending_offset is not None:
                op.offset = pending_offset
            thd = ganesha_thread(line)
            if thd:
                thread_pending_alloc[thd] = ptr_key(ptr)
            continue

        if RE_LEGACY_CEPH_CALL.search(line):
            thd = ganesha_thread(line)
            if thd and thd in thread_pending_alloc:
                k = thread_pending_alloc.pop(thd)
                op = registry.ops[k]
                op.submitted = True
                op.touch(line_no, "legacy_submit")
                thread_pending_submit[thd] = k
            continue

        m = RE_LEGACY_CEPH_RET.search(line)
        if m:
            result = int(m.group(1))
            thd = ganesha_thread(line)
            if result == 0 and thd and thd in thread_pending_submit:
                op = registry.ops[thread_pending_submit.pop(thd)]
                op.async_accepted = True
                op.touch(line_no, "legacy_accept")
            continue

        m = RE_LEGACY_CWF_TRY.search(line)
        if m:
            ptr, fsync_done = m.group(1), int(m.group(2))
            k = ptr_key(ptr)
            op = registry.ops.get(k)
            if op and op.allocated and line_no >= op.first_line:
                op.cwf_started = True
                op.touch(line_no, f"cwf_try fsync={fsync_done}")
                pending_cwf_line[k] = line_no
            continue

        if RE_LEGACY_CWF_DONE.search(line):
            # Pair with nearest preceding CWF try for same pointer.
            best_k: Optional[str] = None
            best_line = -1
            for k, cwf_line in pending_cwf_line.items():
                if cwf_line < line_no <= cwf_line + 5 and cwf_line > best_line:
                    op = registry.ops.get(k)
                    if op and op.allocated and cwf_line >= op.first_line:
                        best_k, best_line = k, cwf_line
            if best_k is not None:
                registry.ops[best_k].cwf_completed = True
                registry.ops[best_k].touch(line_no, "cwf_done")
                del pending_cwf_line[best_k]
            continue

        m = RE_LEGACY_LL_QUEUE.search(line)
        if m:
            io_info, priv = m.group(1), m.group(2)
            ll_queued[io_info] += 1
            op = registry.link(priv, io_info)
            if op.first_line and line_no >= op.first_line:
                op.ll_queued = True
                op.io_info = io_info
                op.touch(line_no, "ll_queue")
            continue

        m = RE_LEGACY_LL_DISPATCH.search(line)
        if m:
            io_info, priv = m.group(1), m.group(2)
            ll_dispatched[io_info] += 1
            op = registry.link(priv, io_info)
            if op.first_line and line_no >= op.first_line:
                op.dispatched = True
                op.touch(line_no, "dispatch")
            continue

        # legacy ganesha cb: only pair when exactly one in-flight lacks cb
        if RE_LEGACY_WRITE_CB.search(line):
            candidates = [
                op for op in registry.unique_ops()
                if op.submitted and op.dispatched and not op.ganesha_cb
                and op.first_line and line_no >= op.first_line
            ]
            if len(candidates) == 1:
                candidates[0].ganesha_cb = True
                candidates[0].touch(line_no, "ganesha_cb")
            continue

    # ---- classify pending ops -----------------------------------------------
    pending: List[OpRecord] = []

    for op in registry.unique_ops():
        if op.is_complete():
            continue
        # Ganesha path: alloc+submit. Ceph-only (e.g. cephfs-bench): CWF created.
        if not (op.submitted or op.cwf_started or op.async_accepted):
            continue
        if op.allocated and not op.submitted:
            continue
        # Instrumented logs duplicate Ganesha alloc/submit as separate pointers.
        if io_correl_hits and op.allocated and op.submitted and not op.cwf_started:
            continue
        pending.append(op)

    # orphan LL queues (queued but never dispatched)
    for io_info, q in ll_queued.items():
        d = ll_dispatched.get(io_info, 0)
        if q > d:
            # already covered if priv-matched op is pending
            pass

    pending.sort(key=lambda o: o.first_line)

    orphan_fsync_states = [
        st for st in fsync_states.values()
        if st.kickoff_line and not st.completed
    ]
    orphan_fsync_states.sort(key=lambda s: s.kickoff_line)

    stall_ops = select_stall_ops(pending, final_io_work, stall_line)

    bracket_ranges = compute_brackets(
        pending=stall_ops or pending,
        total_lines=len(lines),
        stall_line=stall_line,
        last_io_line=last_io_line,
    )

    return ScanResult(
        total_lines=len(lines),
        io_correl_hits=io_correl_hits,
        legacy_mode=io_correl_hits == 0,
        pipeline=pipeline,
        pending_ops=pending,
        stall_ops=stall_ops,
        orphan_fsync_states=orphan_fsync_states,
        stall_line=stall_line,
        stall_gap_secs=stall_gap,
        last_io_line=last_io_line,
        final_io_work=final_io_work,
        bracket_ranges=bracket_ranges,
    )


def select_stall_ops(
    pending: List[OpRecord],
    final_io_work: Optional[int],
    stall_line: Optional[int],
) -> List[OpRecord]:
    """Pick ops most likely tied to the observed stall."""
    if not pending:
        return []

    if final_io_work and final_io_work > 0:
        return pending[-final_io_work:]

    # fsync pipeline stuck: kickoff without finish is the usual hang class
    fsync_stuck = [
        op for op in pending
        if op.fsync_kickoff and not op.fsync_finished
    ]
    if fsync_stuck:
        return fsync_stuck

    finisher_stuck = [
        op for op in pending
        if op.io_queued and not op.io_finished
    ]
    if finisher_stuck:
        return finisher_stuck

    if stall_line is not None:
        return pending

    return pending


def diagnose_fsync_blocking(pending: List[OpRecord]) -> List[str]:
    """Heuristic notes for _fsync_enter / fsync_active serialization stalls."""
    notes: List[str] = []
    by_ino: Dict[str, List[OpRecord]] = defaultdict(list)
    for op in pending:
        if op.ino:
            by_ino[op.ino].append(op)

    for ino, ops in sorted(by_ino.items()):
        kickoff = [op for op in ops if op.fsync_kickoff and not op.fsync_finished]
        waiting_io = [op for op in ops if op.io_queued and not op.io_finished]
        waiting_fsync_enter = [
            op for op in ops
            if op.do_fsync and op.io_finished and not op.fsync_kickoff
        ]
        if kickoff:
            holder = min(kickoff, key=lambda o: o.last_line)
            blocked = len(waiting_fsync_enter) + len(
                [op for op in ops if op.cwf_started and not op.io_finished]
            )
            notes.append(
                f"ino={ino}: fsync holder CWF={holder.cwf} state={holder.fsync_state}"
                f" kickoff=L{holder.last_line}"
                f"; {blocked} ops likely blocked in _fsync_enter or finisher queue"
            )
        elif waiting_io:
            notes.append(
                f"ino={ino}: {len(waiting_io)} ops queued on client finisher"
                f" (queue_finish_io without finish_io_complete)"
            )

    return notes


def compute_brackets(
    pending: List[OpRecord],
    total_lines: int,
    stall_line: Optional[int],
    last_io_line: int,
    context: int = 50,
) -> List[Tuple[int, int, str]]:
    if not pending and stall_line is None:
        return []

    spans: List[Tuple[int, int, str]] = []

    for op in pending:
        start = max(1, op.first_line - context)
        end = min(total_lines, max(op.last_line, op.first_line) + context)
        missing = ",".join(op.missing_stages()) or "unknown"
        spans.append((
            start,
            end,
            f"incomplete write_data={op.key} offset={op.offset} missing={missing}",
        ))

    if stall_line is not None and last_io_line:
        start = max(1, last_io_line - context)
        end = min(total_lines, max(stall_line, last_io_line) + context)
        spans.append((start, end, f"stall after last I/O near L{last_io_line}"))

    spans.sort(key=lambda s: s[0])
    merged: List[Tuple[int, int, Set[str]]] = []
    for start, end, reason in spans:
        if not merged or start > merged[-1][1] + 1:
            merged.append((start, end, {reason}))
        else:
            ps, pe, rs = merged[-1]
            merged[-1] = (ps, max(pe, end), rs | {reason})

    return [(s, e, "; ".join(sorted(r))) for s, e, r in merged]


def format_report(result: ScanResult) -> str:
    out: List[str] = []
    out.append(f"Lines scanned: {result.total_lines}")
    out.append(
        f"Mode: {'legacy' if result.legacy_mode else 'io_correl'}"
        f" (io_correl hits: {result.io_correl_hits})"
    )
    out.append(f"Last I/O activity: line {result.last_io_line}")
    if result.final_io_work is not None:
        out.append(f"Final io_work count: {result.final_io_work}")
    p = result.pipeline
    out.append(
        "Pipeline stage counts:"
        f" ceph_entry={p.ceph_entry} ceph_return_async={p.ceph_return_async}"
        f" cwf_created={p.cwf_created} queue_finish_io={p.queue_finish_io}"
        f" finish_io={p.finish_io} fsync_kickoff={p.fsync_kickoff}"
        f" fsync_finish={p.fsync_finish} fsync_state_complete={p.fsync_state_complete}"
        f" cwf_complete={p.cwf_complete} ll_finish={p.ll_finish}"
        f" dispatch={p.dispatch}"
    )
    if p.cwf_created and p.finish_io < p.cwf_created:
        out.append(
            f"Pipeline gap: {p.cwf_created - p.finish_io} CWFs never reached"
            f" finish_io_complete ({p.queue_finish_io} queued on finisher)"
        )
    if p.fsync_kickoff and not p.fsync_finish:
        out.append(
            f"Pipeline gap: {p.fsync_kickoff} fsync kickoff(s) with 0 finish_fsync"
            f" — matches _fsync_enter / fsync_active hang class"
        )
    if result.stall_line:
        out.append(
            f"Stall detected: line {result.stall_line}"
            f" (gap {result.stall_gap_secs:.3f}s after prior I/O)"
        )
    out.append("")

    if not result.pending_ops:
        out.append("No mismatched async writes detected.")
    else:
        out.append(f"Incomplete / mismatched operations: {len(result.pending_ops)}")
        stall_keys = {op.key for op in result.stall_ops}
        for op in result.pending_ops:
            tag = " [STALL]" if id(op) in {id(s) for s in result.stall_ops} else ""
            label = op.cwf or op.cbi or op.key
            out.append(
                f"  op={label} ino={op.ino} offset={op.offset}"
                f" lines={op.first_line}-{op.last_line}{tag}"
            )
            out.append(f"    missing: {', '.join(op.missing_stages())}")
            if op.events:
                out.append(f"    trail: {' -> '.join(op.events[-10:])}")
        if result.stall_ops:
            out.append("")
            if result.final_io_work and result.final_io_work > 0:
                out.append(
                    f"Likely stall culprits (last {result.final_io_work}"
                    f" matching final io_work):"
                )
            else:
                out.append("Likely stall culprits (fsync/finisher pipeline):")
            for op in result.stall_ops:
                label = op.cwf or op.cbi or op.key
                out.append(f"  {label} ino={op.ino} offset={op.offset}")
        fsync_notes = diagnose_fsync_blocking(result.pending_ops)
        if fsync_notes:
            out.append("")
            out.append("Fsync / finisher diagnosis:")
            for note in fsync_notes:
                out.append(f"  {note}")
        out.append("")

    if result.orphan_fsync_states:
        out.append(
            f"Orphan fsync states (kickoff without state complete): "
            f"{len(result.orphan_fsync_states)}"
        )
        for st in result.orphan_fsync_states:
            flags = []
            if st.wait_flush:
                flags.append("wait_flush")
            if st.wait_caps:
                flags.append("wait_caps")
            extra = f" [{','.join(flags)}]" if flags else ""
            out.append(
                f"  fsync_state={st.key} cwf={st.cwf}"
                f" kickoff=L{st.kickoff_line}{extra}"
            )
        out.append("")

    if result.bracket_ranges:
        out.append("Suggested bracket ranges:")
        for start, end, reason in result.bracket_ranges:
            out.append(f"  L{start}-L{end}  ({end - start + 1} lines)")
            out.append(f"    {reason}")
        out.append("")
        s, e, _ = result.bracket_ranges[0]
        out.append("Extract:")
        out.append(f"  sed -n '{s},{e}p' LOGFILE > bracket.log")

    return "\n".join(out)


def self_test() -> int:
    """Synthetic log exercising fsync kickoff-without-finish detection."""
    stuck_cwf = "0x7ffb6f8378e0"
    stuck_onfinish = "0x7fee77d3c6b0"
    stuck_state = "0x7fffa40261e0"
    stuck_priv = "0x7fee75411070"
    lines = [
        "2026-06-21T19:25:44.719+0000 [ceph] io_correl ceph_ll_nonblocking_readv_writev "
        f"io_info=0x7fee75411088 priv={stuck_priv} onfinish={stuck_onfinish} fh=0x1 off=1822208000 write=1 fsync=1",
        "2026-06-21T19:25:44.719+0000 [ceph] io_correl CWF created "
        f"CWF={stuck_cwf} onfinish={stuck_onfinish} ino=0x10000000310 offset=1822208000 size=256000 do_fsync=1",
        "2026-06-21T19:25:44.722+0000 [ceph] io_correl CWF finish_io_complete "
        f"CWF={stuck_cwf} onfinish={stuck_onfinish} ino=0x10000000310 offset=1822208000 r=256000",
        "2026-06-21T19:25:44.722+0000 [ceph] io_correl CWF kickoff fsync "
        f"CWF={stuck_cwf} onfinish={stuck_onfinish} fsync_finish_ctx=0xdeadbeef "
        f"fsync_state={stuck_state} ino=0x10000000310 offset=1822208000",
        "2026-06-21T19:25:44.723+0000 [ceph] io_correl fsync wait caps "
        f"fsync_state={stuck_state} onfinish=0x7ffc2a7ec2e0 ino=0x10000000310 flush_tid=981932 oldest_flushing=981932",
    ]
    result = scan_log(lines)
    pending = [op for op in result.pending_ops if op.fsync_kickoff]
    if len(pending) != 1:
        print(f"self-test: expected 1 pending fsync op, got {len(pending)}", file=sys.stderr)
        return 1
    op = pending[0]
    if op.fsync_finished or op.cwf_completed:
        print("self-test: expected incomplete fsync op", file=sys.stderr)
        return 1
    if "fsync_finish" not in op.missing_stages():
        print(f"self-test: missing_stages={op.missing_stages()}", file=sys.stderr)
        return 1
    if len(result.orphan_fsync_states) != 1:
        print(
            f"self-test: expected 1 orphan fsync state, got {len(result.orphan_fsync_states)}",
            file=sys.stderr,
        )
        return 1
    print("io_correl_scan.py self-test passed")
    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "logfile",
        nargs="?",
        help="Merged Ganesha+Ceph log file (not required with --self-test)",
    )
    parser.add_argument(
        "-c", "--context",
        type=int,
        default=50,
        help="Context lines around each issue (default: 50)",
    )
    parser.add_argument(
        "--stall-threshold",
        type=float,
        default=5.0,
        help="Seconds without I/O activity to mark stall (default: 5)",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Print only start:end line ranges, one per line",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in regression checks and exit",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.self_test:
        return self_test()

    if not args.logfile:
        parser.error("logfile is required unless --self-test is used")

    with open(args.logfile, encoding="utf-8", errors="replace") as f:
        log_lines = f.read().splitlines()

    result = scan_log(log_lines, stall_threshold=args.stall_threshold)
    bracket_ops = result.stall_ops or result.pending_ops
    result.bracket_ranges = compute_brackets(
        pending=bracket_ops,
        total_lines=result.total_lines,
        stall_line=result.stall_line,
        last_io_line=result.last_io_line,
        context=args.context,
    )

    if args.extract:
        for start, end, _ in result.bracket_ranges:
            print(f"{start}:{end}")
        return 0 if result.pending_ops or result.stall_line else 1

    print(format_report(result))
    return 0 if result.pending_ops or result.stall_line else 1


if __name__ == "__main__":
    sys.exit(main())