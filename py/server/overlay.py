"""Tiny config loader shared by Python server + bench scripts."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import yaml


@dataclass(frozen=True)
class NodeSpec:
    name: str
    host: str
    port: int
    team: str
    role: str
    impl: str
    # Virtual-host id used by the latency interceptor and the multihost
    # launcher: RPCs that cross a virtual_host boundary get the
    # inter_host_latency added; RPCs within the same virtual_host get
    # intra_host_latency. Defaults to "h1" so old overlay.yaml still works.
    virtual_host: str = "h1"

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class NetworkSim:
    simulate: bool = False
    inter_host_latency_ms: float = 0.0
    intra_host_latency_ms: float = 0.0


@dataclass(frozen=True)
class Overlay:
    nodes: Dict[str, NodeSpec]
    edges: List[List[str]]
    routing: Dict[str, Dict[str, str]]
    data_owners: List[str]
    chunking: dict
    scheduler: dict
    network: NetworkSim = NetworkSim()

    def neighbors(self, node: str) -> List[str]:
        out = []
        for a, b in self.edges:
            if a == node:
                out.append(b)
            elif b == node:
                out.append(a)
        return out

    def next_hop(self, src: str, dst: str) -> str:
        return self.routing[src][dst]


def _expand_hostnames(raw: dict) -> dict:
    """Allow HOST_<NAME> env overrides so the same overlay.yaml works on
    localhost and multi-host deploys."""
    for name, spec in raw.get("nodes", {}).items():
        env_host = os.environ.get(f"MINI2_HOST_{name}")
        env_port = os.environ.get(f"MINI2_PORT_{name}")
        if env_host:
            spec["host"] = env_host
        if env_port:
            spec["port"] = int(env_port)
    return raw


def load_overlay(path: str | Path | None = None) -> Overlay:
    if path is None:
        path = os.environ.get("MINI2_OVERLAY") or (
            Path(__file__).resolve().parents[2] / "config" / "overlay.yaml"
        )
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    raw = _expand_hostnames(raw)

    nodes = {
        name: NodeSpec(
            name=name,
            host=spec["host"],
            port=int(spec["port"]),
            team=spec["team"],
            role=spec["role"],
            impl=spec["impl"],
            virtual_host=spec.get("virtual_host", "h1"),
        )
        for name, spec in raw["nodes"].items()
    }
    net_raw = raw.get("network", {}) or {}
    network = NetworkSim(
        simulate=bool(net_raw.get("simulate", False)),
        inter_host_latency_ms=float(net_raw.get("inter_host_latency_ms", 0.0)),
        intra_host_latency_ms=float(net_raw.get("intra_host_latency_ms", 0.0)),
    )
    return Overlay(
        nodes=nodes,
        edges=[list(e) for e in raw["edges"]],
        routing={k: dict(v) for k, v in raw["routing"].items()},
        data_owners=list(raw["data_owners"]),
        chunking=dict(raw.get("chunking", {})),
        scheduler=dict(raw.get("scheduler", {})),
        network=network,
    )
