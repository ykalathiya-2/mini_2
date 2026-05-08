"""Per-node telemetry writer.

Emits one JSON object per line to logs/<run>/telemetry-<node>.jsonl.
The bench harness merges these by t_ns to attribute end-to-end query
latency to specific RPCs / hops / nodes.

Format (every event has these keys):
    t_ns      : monotonic nanoseconds since arbitrary epoch (per-process)
    wall_ns   : wall-clock nanoseconds since UNIX epoch
    node      : this node's name
    event     : "rpc_send" | "rpc_recv" | "range_search" | "produce" |
                "chunk_out" | "chunk_in" | "lifecycle"
    Additional event-specific fields are passed in **kwargs.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Optional


class Telemetry:
    """Thread-safe append-only JSONL writer."""

    def __init__(self, path: Path, node: str):
        self.path = path
        self.node = node
        self._mu = threading.Lock()
        # Line-buffered text mode so each flush writes a complete line and
        # `tail -f` shows it immediately.
        self._f = open(path, "a", buffering=1)

    def emit(self, event: str, **kw) -> None:
        rec = {
            "t_ns":    time.perf_counter_ns(),
            "wall_ns": time.time_ns(),
            "node":    self.node,
            "event":   event,
            **kw,
        }
        line = json.dumps(rec, separators=(",", ":"))
        with self._mu:
            self._f.write(line + "\n")

    def close(self) -> None:
        with self._mu:
            try:
                self._f.flush()
                self._f.close()
            except Exception:
                pass


_GLOBAL: Optional[Telemetry] = None


def init(node: str, run_dir: Optional[str] = None) -> Telemetry:
    """Create the per-node telemetry log inside MINI2_RUN_DIR (or run_dir).

    Falls back to logs/run-default if neither is set, so unit tests still
    work.
    """
    global _GLOBAL
    if _GLOBAL is not None:
        return _GLOBAL
    base = run_dir or os.environ.get("MINI2_RUN_DIR")
    if not base:
        # Default fallback for ad-hoc runs.
        root = Path(__file__).resolve().parents[2]
        base = str(root / "logs" / "run-default")
    Path(base).mkdir(parents=True, exist_ok=True)
    _GLOBAL = Telemetry(Path(base) / f"telemetry-{node}.jsonl", node)
    return _GLOBAL


def get() -> Optional[Telemetry]:
    return _GLOBAL
