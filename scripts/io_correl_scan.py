#!/usr/bin/env python3
"""
Scan merged Ganesha + Ceph logs for mismatched async write I/O.

Tracks in-flight writes only (bounded memory for multi-million-line logs).
Correlates pointers across layers: write_data, caller_arg, cbi/priv, CWF,
onfinish, io_info.

An op is pending at EOF when submitted but fsal_complete_io has not run
(matches Ganesha io_work semantics).
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

HEX = r"(0x[0-9a-fA-F]+)"

# --- io_correl (instrumented builds) ---------------------------------------

RE_IO_ALLOC = re.compile(
    rf"io_correl alloc write_data={HEX} offset=(\d+)(?:\s|$)"
)
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
    rf"io_correl ceph_ll_nonblocking_readv_writev "
    rf"io_info={HEX} priv={HEX} onfinish={HEX}"
)
RE_IO_CEPH_RET = re.compile(
    rf"io_correl ceph_ll_nonblocking_readv_writev return "
    rf"io_info={HEX} priv={HEX} onfinish={HEX} r=(-?\d+)"
)
RE_IO_CWF_CREATED = re.compile(
    rf"io_correl CWF created CWF={HEX} onfinish={HEX}"
)
RE_IO_CWF_COMPLETE = re.compile(
    rf"io_correl CWF complete CWF={HEX} onfinish={HEX} r=(-?\d+)"
)
RE_IO_CWF_KICKOFF_FSYNC = re.compile(
    rf"io_correl CWF kickoff fsync CWF={HEX} onfinish={HEX}"
)
RE_IO_CWF_FINISH_FSYNC = re.compile(
    rf"io_correl CWF finish_fsync CWF={HEX} onfinish={HEX}"
)
RE_IO_LL_FINISH = re.compile(
    rf"io_correl LL_Onfinish finish onfinish={HEX} io_info={HEX} priv={HEX}"
)
RE_IO_DISPATCH = re.compile(
    rf"io_correl dispatch callback io_info={HEX} priv={HEX}"
)

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
    write_data: Optional[str] = None
    caller_arg: Optional[str] = None
    cbi: Optional[str] = None
    io_info: Optional[str] = None
    cwf: Optional[str] = None
    onfinish: Optional[str] = None
    ceph_onfinish: Optional[str] = None
    offset: Optional[int] = None
    first_line: int = 0
    last_line: int = 0
    events: List[str] = field(default_factory=list)

    allocated: bool = False
    submitted: bool = False
    async_accepted: bool = False
    cwf_started: bool = False
    cwf_fsync_kickoff: bool = False
    cwf_fsync_done: bool = False
    cwf_completed: bool = False
    ll_queued: bool = False
    dispatched: bool = False
    ganesha_cb: bool = False
    fsal_complete: bool = False
    nfs4_cb: bool = False

    def touch(self, line_no: int, event: str) -> None:
        if self.first_line == 0:
            self.first_line = line_no
        self.last_line = line_no
        if len(self.events) < 32:
            self.events.append(f"L{line_no}:{event}")

    def ganesha_done(self) -> bool:
        """Matches io_work decrement (fsal_complete_io)."""
        return self.fsal_complete

    def fully_done(self) -> bool:
        return self.nfs4_cb or (self.ganesha_cb and self.fsal_complete)

    def missing_stages(self) -> List[str]:
        if not self.submitted:
            return ["submit"]
        if not self.async_accepted:
            return ["ceph_accept"]
        if not self.cwf_started:
            return ["cwf_start"]
        if not self.cwf_fsync_kickoff:
            return ["cwf_fsync_kickoff"]
        if not self.cwf_fsync_done:
            return ["cwf_finish_fsync"]
        if not self.cwf_completed:
            return ["cwf_complete"]
        if not self.dispatched:
            return ["dispatch"]
        if not self.ganesha_cb:
            return ["ganesha_cb"]
        if not self.fsal_complete:
            return ["fsal_complete"]
        if not self.nfs4_cb:
            return ["nfs4_cb"]
        return []


class OpTracker:
    """One OpRecord per Ganesha submit; pointers (cbi/CWF/caller_arg) are reused."""

    def __init__(self) -> None:
        self.ops: Dict[int, OpRecord] = {}
        self.inflight: Set[int] = set()
        self.next_seq = 1
        self.cbi_active: Dict[str, int] = {}
        self.caller_active: Dict[str, int] = {}
        self.cwf_active: Dict[str, int] = {}
        self.pending_ceph: Dict[str, dict] = {}
        self.alloc_pending: Dict[str, OpRecord] = {}

    def _new_op(self) -> OpRecord:
        seq = self.next_seq
        self.next_seq += 1
        op = OpRecord(key=str(seq))
        self.ops[seq] = op
        return op

    def _active(self, seq: int) -> OpRecord:
        return self.ops[seq]

    def _apply_pending_ceph(self, op: OpRecord, cbi: str) -> None:
        pending = self.pending_ceph.pop(ptr_key(cbi), None)
        if not pending:
            return
        seq = int(op.key)
        op.cbi = cbi
        op.io_info = pending.get("io_info")
        op.ceph_onfinish = pending.get("onfinish")
        op.async_accepted = pending.get("async_accepted", False)
        for flag in (
            "cwf_started", "cwf_fsync_kickoff", "cwf_fsync_done",
            "cwf_completed", "ll_queued", "dispatched",
        ):
            if pending.get(flag):
                setattr(op, flag, True)
        if pending.get("cwf"):
            op.cwf = pending["cwf"]
            op.onfinish = pending.get("onfinish")
            self.cwf_active[ptr_key(op.cwf)] = seq
        for ev in pending.get("events", []):
            op.events.append(ev)

    def start_alloc(self, write_data: str, offset: int) -> OpRecord:
        op = self._new_op()
        op.write_data = write_data
        op.offset = offset
        op.allocated = True
        self.alloc_pending[ptr_key(write_data)] = op
        return op

    def start_submit(
        self, caller_arg: str, cbi: str, io_info: str, write_data: Optional[str]
    ) -> OpRecord:
        op = self.alloc_pending.pop(ptr_key(write_data), None) if write_data else None
        if op is None:
            op = self._new_op()
        seq = int(op.key)
        self.ops[seq] = op

        op.caller_arg = caller_arg
        op.cbi = cbi
        op.io_info = io_info
        op.submitted = True
        self.inflight.add(seq)
        self.cbi_active[ptr_key(cbi)] = seq
        self.caller_active[ptr_key(caller_arg)] = seq
        self._apply_pending_ceph(op, cbi)
        return op

    def note_ceph_entry(
        self, priv: str, io_info: str, onfinish: str
    ) -> Optional[OpRecord]:
        seq = self.cbi_active.get(ptr_key(priv))
        if seq is not None:
            op = self._active(seq)
            op.cbi = priv
            op.io_info = io_info
            op.ceph_onfinish = onfinish
            return op
        self.pending_ceph[ptr_key(priv)] = {
            "io_info": io_info,
            "onfinish": onfinish,
            "async_accepted": False,
        }
        return None

    def note_ceph_return(self, priv: str, result: int) -> Optional[OpRecord]:
        seq = self.cbi_active.get(ptr_key(priv))
        if seq is not None:
            op = self._active(seq)
            if result == 0:
                op.async_accepted = True
            return op
        pending = self.pending_ceph.get(ptr_key(priv))
        if pending and result == 0:
            pending["async_accepted"] = True
        return None

    def _pending_for_onfinish(self, onfinish: str) -> Optional[dict]:
        of_k = ptr_key(onfinish)
        for pending in self.pending_ceph.values():
            if ptr_key(pending.get("onfinish", "")) == of_k:
                return pending
        return None

    def attach_cwf(self, cwf: str, onfinish: str) -> Optional[OpRecord]:
        seq = self.cwf_active.get(ptr_key(cwf))
        if seq is not None and seq in self.inflight:
            op = self._active(seq)
            op.cwf = cwf
            op.onfinish = onfinish
            return op
        seq = self.caller_active.get(ptr_key(onfinish))
        if seq is not None and seq in self.inflight:
            op = self._active(seq)
            op.cwf = cwf
            op.onfinish = onfinish
            self.cwf_active[ptr_key(cwf)] = seq
            return op
        for candidate in self.inflight:
            op = self._active(candidate)
            if op.ceph_onfinish and ptr_key(op.ceph_onfinish) == ptr_key(onfinish):
                op.cwf = cwf
                op.onfinish = onfinish
                self.cwf_active[ptr_key(cwf)] = candidate
                return op
        pending = self._pending_for_onfinish(onfinish)
        if pending is not None:
            pending["cwf"] = cwf
            pending.setdefault("events", [])
            return None
        return None

    def by_cbi(self, cbi: str) -> Optional[OpRecord]:
        seq = self.cbi_active.get(ptr_key(cbi))
        return self._active(seq) if seq is not None else None

    def by_caller(self, caller_arg: str) -> Optional[OpRecord]:
        seq = self.caller_active.get(ptr_key(caller_arg))
        return self._active(seq) if seq is not None else None

    def by_write_data(self, wd: str) -> Optional[OpRecord]:
        op = self.alloc_pending.get(ptr_key(wd))
        if op:
            return op
        for seq in self.inflight:
            op = self._active(seq)
            if op.write_data and ptr_key(op.write_data) == ptr_key(wd):
                return op
        return None

    def finish_ganesha(self, caller_arg: str, cbi: str) -> None:
        seq = self.caller_active.pop(ptr_key(caller_arg), None)
        if seq is None:
            seq = self.cbi_active.get(ptr_key(cbi))
        if seq is None:
            return
        op = self._active(seq)
        self.inflight.discard(seq)
        self.cbi_active.pop(ptr_key(cbi), None)
        if op.cwf:
            self.cwf_active.pop(ptr_key(op.cwf), None)
        if op.write_data:
            self.alloc_pending.pop(ptr_key(op.write_data), None)

    def pending_submitted(self) -> List[OpRecord]:
        return sorted(
            (self._active(s) for s in self.inflight if self._active(s).submitted),
            key=lambda o: o.first_line,
        )


def ptr_key(ptr: str) -> str:
    return ptr.lower()


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
        "C_Write_Finisher",
        "LL_Onfinish",
        "ceph_write2_cb",
        "fsal_complete_io",
        "nfs4_op_write",
        "wait_to_start_io",
    )
    return any(m in line for m in markers)


def ganesha_thread(line: str) -> Optional[str]:
    m = RE_GANESHA_THREAD.search(line)
    return m.group(1) if m else None


def iter_lines(path: str) -> Iterator[Tuple[int, str]]:
    with open(path, encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            yield line_no, line.rstrip("\n")


@dataclass
class ScanResult:
    total_lines: int
    io_correl_hits: int
    legacy_mode: bool
    pending_ops: List[OpRecord]
    stall_ops: List[OpRecord]
    stall_line: Optional[int]
    stall_gap_secs: Optional[float]
    last_io_line: int
    final_io_work: Optional[int]
    inflight_at_eof: int
    bracket_ranges: List[Tuple[int, int, str]] = field(default_factory=list)
    scan_start_line: int = 1
    scan_end_line: Optional[int] = None
    backsearch_ops: List[OpRecord] = field(default_factory=list)


def scan_log(
    path: str,
    stall_threshold: float = 5.0,
    start_line: int = 1,
    end_line: Optional[int] = None,
    track_stall: bool = True,
) -> ScanResult:
    tracker = OpTracker()
    io_correl_hits = 0
    legacy_mode = True

    thread_pending_alloc: Dict[str, str] = {}
    thread_pending_submit: Dict[str, str] = {}
    pending_cwf_line: Dict[str, int] = {}
    pending_offset: Optional[int] = None

    last_io_ts: Optional[datetime] = None
    last_io_line = 0
    stall_line: Optional[int] = None
    stall_gap: Optional[float] = None
    final_io_work: Optional[int] = None
    total_lines = 0

    for line_no, line in iter_lines(path):
        total_lines = line_no
        if end_line is not None and line_no > end_line:
            break
        if line_no < start_line:
            continue

        if "io_correl" in line:
            io_correl_hits += 1
            legacy_mode = False

        ts = parse_ts(line)
        if track_stall and ts and last_io_ts and stall_line is None:
            gap = (ts - last_io_ts).total_seconds()
            if gap >= stall_threshold:
                stall_line = line_no
                stall_gap = gap

        if track_stall and is_io_meaningful(line):
            if ts:
                last_io_ts = ts
            last_io_line = line_no

        m = RE_LEGACY_IO_WORK.search(line)
        if m:
            final_io_work = int(m.group(2))

        # ---- io_correl ------------------------------------------------------
        m = RE_IO_ALLOC.search(line)
        if m:
            wd, offset = m.group(1), int(m.group(2))
            op = tracker.start_alloc(wd, offset)
            op.touch(line_no, "alloc")
            thd = ganesha_thread(line)
            if thd:
                thread_pending_alloc[thd] = wd
            continue

        m = RE_IO_SUBMIT.search(line)
        if m:
            caller_arg, cbi, io_info = m.group(1), m.group(2), m.group(3)
            offset = int(m.group(5))
            wd = None
            thd = ganesha_thread(line)
            if thd and thd in thread_pending_alloc:
                wd = thread_pending_alloc.pop(thd)
            op = tracker.start_submit(caller_arg, cbi, io_info, wd)
            op.offset = offset
            op.touch(line_no, "submit")
            if thd:
                thread_pending_submit[thd] = cbi
            continue

        m = RE_IO_SUBMIT_RET.search(line)
        if m:
            caller_arg, cbi, result = m.group(1), m.group(2), int(m.group(3))
            op = tracker.by_cbi(cbi)
            if op is None:
                continue
            if result == 0:
                op.async_accepted = True
                op.touch(line_no, "submit_return_async")
            else:
                op.async_accepted = True
                op.fsal_complete = True
                op.nfs4_cb = True
                op.touch(line_no, f"submit_return_sync r={result}")
                tracker.finish_ganesha(caller_arg, cbi)
            continue

        m = RE_IO_CEPH_ENTRY.search(line)
        if m:
            io_info, priv, onfinish = m.group(1), m.group(2), m.group(3)
            op = tracker.note_ceph_entry(priv, io_info, onfinish)
            if op:
                op.touch(line_no, "ceph_entry")
            continue

        m = RE_IO_CEPH_RET.search(line)
        if m:
            priv, result = m.group(2), int(m.group(4))
            op = tracker.note_ceph_return(priv, result)
            if op and result == 0:
                op.touch(line_no, "ceph_return_async")
            continue

        def _cwf_update(cwf: str, onfinish: str, flag: str, event: str) -> None:
            op = tracker.attach_cwf(cwf, onfinish)
            if op:
                setattr(op, flag, True)
                op.touch(line_no, event)
                return
            pending = tracker._pending_for_onfinish(onfinish)
            if pending is not None:
                pending[flag] = True
                pending["cwf"] = cwf
                pending.setdefault("events", []).append(f"L{line_no}:{event}")

        m = RE_IO_CWF_CREATED.search(line)
        if m:
            _cwf_update(m.group(1), m.group(2), "cwf_started", "cwf_created")
            continue

        m = RE_IO_CWF_COMPLETE.search(line)
        if m:
            _cwf_update(m.group(1), m.group(2), "cwf_completed", "cwf_complete")
            continue

        m = RE_IO_CWF_KICKOFF_FSYNC.search(line)
        if m:
            _cwf_update(m.group(1), m.group(2), "cwf_fsync_kickoff", "cwf_fsync_kickoff")
            continue

        m = RE_IO_CWF_FINISH_FSYNC.search(line)
        if m:
            _cwf_update(m.group(1), m.group(2), "cwf_fsync_done", "cwf_finish_fsync")
            continue

        m = RE_IO_LL_FINISH.search(line)
        if m:
            priv = m.group(3)
            op = tracker.by_cbi(priv)
            if op:
                op.ll_queued = True
                op.touch(line_no, "ll_finish")
            continue

        m = RE_IO_DISPATCH.search(line)
        if m:
            priv = m.group(2)
            op = tracker.by_cbi(priv)
            if op:
                op.dispatched = True
                op.touch(line_no, "dispatch")
            continue

        m = RE_IO_GANESHA_CB.search(line)
        if m:
            caller_arg, cbi, io_info = m.group(1), m.group(2), m.group(3)
            op = tracker.by_cbi(cbi) or tracker.by_caller(caller_arg)
            if op:
                op.ganesha_cb = True
                op.touch(line_no, "ganesha_cb")
            continue

        m = RE_IO_GANESHA_FSAL.search(line)
        if m:
            caller_arg, cbi = m.group(1), m.group(2)
            op = tracker.by_cbi(cbi) or tracker.by_caller(caller_arg)
            if op:
                op.fsal_complete = True
                op.touch(line_no, "fsal_complete")
            tracker.finish_ganesha(caller_arg, cbi)
            continue

        m = RE_IO_NFS4_CB.search(line)
        if m:
            wd = m.group(1)
            op = tracker.by_write_data(wd)
            if op is not None:
                op.nfs4_cb = True
                op.touch(line_no, "nfs4_cb")
            continue

        # ---- legacy ---------------------------------------------------------
        m = RE_LEGACY_OFFSET.search(line)
        if m:
            pending_offset = int(m.group(1))
            continue

        m = RE_LEGACY_ALLOC.search(line)
        if m:
            ptr = m.group(1)
            offset = pending_offset or 0
            op = tracker.start_alloc(ptr, offset)
            op.touch(line_no, "legacy_alloc")
            thd = ganesha_thread(line)
            if thd:
                thread_pending_alloc[thd] = ptr
            continue

        if RE_LEGACY_CEPH_CALL.search(line):
            thd = ganesha_thread(line)
            if thd and thd in thread_pending_alloc:
                wd = thread_pending_alloc.pop(thd)
                op = tracker.start_submit(wd, wd, wd, wd)
                op.touch(line_no, "legacy_submit")
                thread_pending_submit[thd] = wd
            continue

        m = RE_LEGACY_CEPH_RET.search(line)
        if m:
            result = int(m.group(1))
            thd = ganesha_thread(line)
            if result == 0 and thd and thd in thread_pending_submit:
                op = tracker.by_write_data(thread_pending_submit.pop(thd))
                if op:
                    op.async_accepted = True
                    op.touch(line_no, "legacy_accept")
            continue

        m = RE_LEGACY_CWF_TRY.search(line)
        if m:
            ptr = m.group(1)
            op = tracker.by_write_data(ptr)
            if op and line_no >= op.first_line:
                op.cwf_started = True
                op.touch(line_no, f"cwf_try fsync={m.group(2)}")
                pending_cwf_line[ptr_key(ptr)] = line_no
            continue

        if RE_LEGACY_CWF_DONE.search(line):
            best_k: Optional[str] = None
            best_line = -1
            for k, cwf_line in pending_cwf_line.items():
                if cwf_line < line_no <= cwf_line + 5 and cwf_line > best_line:
                    op = tracker.by_write_data(k)
                    if op and cwf_line >= op.first_line:
                        best_k, best_line = k, cwf_line
            if best_k:
                op = tracker.by_write_data(best_k)
                if op:
                    op.cwf_completed = True
                    op.touch(line_no, "cwf_done")
                del pending_cwf_line[best_k]
            continue

        m = RE_LEGACY_LL_QUEUE.search(line)
        if m:
            priv = m.group(2)
            op = tracker.by_cbi(priv)
            if op and op.first_line and line_no >= op.first_line:
                op.ll_queued = True
                op.touch(line_no, "ll_queue")
            continue

        m = RE_LEGACY_LL_DISPATCH.search(line)
        if m:
            priv = m.group(2)
            op = tracker.by_cbi(priv)
            if op and op.first_line and line_no >= op.first_line:
                op.dispatched = True
                op.touch(line_no, "dispatch")
            continue

        if RE_LEGACY_WRITE_CB.search(line):
            candidates = [
                tracker._active(s) for s in tracker.inflight
                if tracker._active(s).dispatched and not tracker._active(s).ganesha_cb
            ]
            if len(candidates) == 1:
                candidates[0].ganesha_cb = True
                candidates[0].touch(line_no, "ganesha_cb")

    pending = tracker.pending_submitted()
    stall_ops: List[OpRecord] = []
    if final_io_work and final_io_work > 0:
        stall_ops = sorted(pending, key=lambda o: o.last_line)[-final_io_work:]
    elif stall_line is not None:
        stall_ops = pending

    bracket_ranges = compute_brackets(
        pending=stall_ops or pending,
        total_lines=total_lines,
        stall_line=stall_line,
        last_io_line=last_io_line,
    )

    return ScanResult(
        total_lines=total_lines,
        io_correl_hits=io_correl_hits,
        legacy_mode=legacy_mode and io_correl_hits == 0,
        pending_ops=pending,
        stall_ops=stall_ops,
        stall_line=stall_line,
        stall_gap_secs=stall_gap,
        last_io_line=last_io_line,
        final_io_work=final_io_work,
        inflight_at_eof=len(pending),
        bracket_ranges=bracket_ranges,
        scan_start_line=start_line,
        scan_end_line=end_line,
    )


def abs_line(line_no: int, slice_start: int) -> int:
    """Map a line number inside a slice file to stall-merged.log line numbers."""
    return slice_start + line_no - 1


def backsearch_before_start(
    path: str,
    start_line: int,
    back_lines: int,
    stall_threshold: float,
) -> List[OpRecord]:
    """Find submits still in-flight at start_line (started before the scan window)."""
    if start_line <= 1:
        return []
    bs_start = max(1, start_line - back_lines)
    bs_end = start_line - 1
    result = scan_log(
        path,
        stall_threshold=stall_threshold,
        start_line=bs_start,
        end_line=bs_end,
        track_stall=False,
    )
    return result.pending_ops


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
        label = op.write_data or op.caller_arg or op.key
        missing = ",".join(op.missing_stages()) or "unknown"
        spans.append((
            start, end,
            f"incomplete key={label} offset={op.offset} missing={missing}",
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


def format_report(
    result: ScanResult,
    logfile: str,
    slice_start: int = 0,
) -> str:
    out: List[str] = []
    out.append(f"Scanned file: {logfile}")
    out.append(
        "Line numbers below are 1-based within that file only"
        " (not stall-merged.log unless you scanned it directly)."
    )
    out.append(f"Lines scanned: {result.total_lines}")
    if result.scan_start_line > 1:
        out.append(
            f"Tracking window: L{result.scan_start_line}"
            f"{f'-L{result.scan_end_line}' if result.scan_end_line else ''}"
            f" (submits before L{result.scan_start_line} are invisible)"
        )
    if slice_start > 0:
        out.append(
            f"Slice offset: line 1 of this file = L{slice_start} in stall-merged.log"
        )
    out.append(
        f"Mode: {'legacy' if result.legacy_mode else 'io_correl'}"
        f" (io_correl hits: {result.io_correl_hits})"
    )
    out.append(f"Last I/O activity: line {result.last_io_line}")
    out.append(f"In-flight at EOF: {result.inflight_at_eof}")
    if result.final_io_work is not None:
        out.append(f"Final io_work count: {result.final_io_work}")
        if result.inflight_at_eof != result.final_io_work:
            out.append(
                f"NOTE: in-flight ({result.inflight_at_eof}) != "
                f"io_work ({result.final_io_work}); check merged log scope"
            )
    if result.stall_line:
        out.append(
            f"Stall detected: line {result.stall_line}"
            f" (gap {result.stall_gap_secs:.3f}s after prior I/O)"
        )
    out.append("")

    display_ops = result.pending_ops or result.backsearch_ops
    if not display_ops:
        out.append("No in-flight async writes at EOF.")
        if result.final_io_work and result.final_io_work > 0:
            out.append(
                f"  (but io_work={result.final_io_work}: stuck submit is before"
                f" this scan window — widen sed or use --backsearch)"
            )
            if result.scan_start_line > 1:
                out.append(
                    f"  Try: widen slice to at least {result.total_lines - result.scan_start_line + 5_000_000:,}"
                    f" lines before EOF, or run without --start-line"
                )
    else:
        if result.backsearch_ops and not result.pending_ops:
            out.append(
                "In-flight ops found via backsearch (submit before --start-line):"
            )
        else:
            out.append(f"In-flight / incomplete operations: {len(display_ops)}")
        stall_keys = {op.key for op in result.stall_ops}
        for op in display_ops:
            tag = " [STALL]" if op.key in stall_keys else ""
            label = op.write_data or op.caller_arg or op.key
            line_range = f"lines={op.first_line}-{op.last_line}"
            if slice_start > 0:
                line_range += (
                    f" (merged L{abs_line(op.first_line, slice_start)}"
                    f"-L{abs_line(op.last_line, slice_start)})"
                )
            elif result.scan_start_line > 1 and result.backsearch_ops:
                line_range += (
                    f" (merged L{op.first_line}-L{op.last_line};"
                    f" backsearch from L{result.scan_start_line})"
                )
            out.append(
                f"  key={label} caller_arg={op.caller_arg} cbi={op.cbi}"
                f" offset={op.offset} {line_range}{tag}"
            )
            out.append(f"    missing: {', '.join(op.missing_stages())}")
            if op.events:
                out.append(f"    trail: {' -> '.join(op.events[-10:])}")
        stall_list = result.stall_ops or result.backsearch_ops[: (result.final_io_work or 0)]
        if stall_list and result.final_io_work:
            out.append("")
            out.append(
                f"Likely stall culprits (last {result.final_io_work} in-flight):"
            )
            for op in stall_list:
                out.append(
                    f"  {op.caller_arg or op.key} offset={op.offset}"
                )
        out.append("")

    if result.bracket_ranges:
        out.append("Suggested bracket ranges:")
        for start, end, reason in result.bracket_ranges:
            out.append(f"  L{start}-L{end}  ({end - start + 1} lines)")
            out.append(f"    {reason}")
        out.append("")
        extract_range = result.bracket_ranges[0]
        for start, end, reason in result.bracket_ranges:
            if "incomplete key=" in reason or "missing=" in reason:
                extract_range = (start, end, reason)
                break
        s, e, reason = extract_range
        out.append("Extract (use the same file you scanned):")
        if slice_start > 0:
            ms, me = abs_line(s, slice_start), abs_line(e, slice_start)
            out.append(f"  sed -n '{s},{e}p' {logfile} > bracket.log")
            out.append(f"  # or in stall-merged.log: sed -n '{ms},{me}p' stall-merged.log")
        elif "incomplete" in reason and result.backsearch_ops:
            out.append(f"  sed -n '{s},{e}p' {logfile} > bracket.log")
            out.append(
                f"  # merged-log lines {s}-{e} (backsearch; same numbers as stall-merged.log)"
            )
        else:
            out.append(f"  sed -n '{s},{e}p' {logfile} > bracket.log")
        out.append(f"  # {reason}")

    return "\n".join(out)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("logfile", help="Merged Ganesha+Ceph log file")
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
        "--start-line", "--start",
        type=int,
        default=1,
        dest="start_line",
        help="Begin tracking at this line (submits before it are invisible unless --backsearch)",
    )
    parser.add_argument(
        "--slice-start",
        type=int,
        default=0,
        help="Absolute line in stall-merged.log where line 1 of logfile begins",
    )
    parser.add_argument(
        "--backsearch",
        type=int,
        default=5_000_000,
        metavar="LINES",
        help="When io_work>0 but nothing in-flight, scan this many lines before --start-line (0=off)",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Print only start:end line ranges, one per line",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    result = scan_log(
        args.logfile,
        stall_threshold=args.stall_threshold,
        start_line=args.start_line,
    )

    if (
        result.final_io_work
        and result.final_io_work > 0
        and not result.pending_ops
        and args.backsearch > 0
        and args.start_line > 1
    ):
        result.backsearch_ops = backsearch_before_start(
            args.logfile,
            args.start_line,
            args.backsearch,
            args.stall_threshold,
        )
        if result.backsearch_ops:
            # Prefer ops stuck well before the main window (not boundary noise).
            margin = 1000
            candidates = [
                op for op in result.backsearch_ops
                if not op.fsal_complete
                and op.last_line < args.start_line - margin
            ] or [op for op in result.backsearch_ops if not op.fsal_complete]
            result.stall_ops = sorted(
                candidates, key=lambda o: o.first_line
            )[: result.final_io_work]

    bracket_ops = result.stall_ops or result.pending_ops or result.backsearch_ops
    result.bracket_ranges = compute_brackets(
        pending=bracket_ops,
        total_lines=result.total_lines,
        stall_line=result.stall_line,
        last_io_line=result.last_io_line,
        context=args.context,
    )

    if args.extract:
        for start, end, _ in result.bracket_ranges:
            if args.slice_start > 0:
                print(f"{abs_line(start, args.slice_start)}:{abs_line(end, args.slice_start)}")
            else:
                print(f"{start}:{end}")
        found = result.pending_ops or result.backsearch_ops or result.stall_line
        return 0 if found else 1

    print(format_report(result, args.logfile, args.slice_start))
    found = result.pending_ops or result.backsearch_ops or result.stall_line
    return 0 if found else 1


if __name__ == "__main__":
    sys.exit(main())