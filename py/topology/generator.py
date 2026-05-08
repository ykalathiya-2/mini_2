"""Topology generator: produces overlay.yaml variants for the bench.

Eight named structures over the 9 nodes A..I. The gateway is always A,
data-owners are always {C, D, F, G, H, I}, and intermediates are {B, E}.
Every output:

    nodes:        host/port/team/role/impl + virtual_host (for the latency
                  simulator and multihost deploy)
    edges:        undirected graph
    routing:      next-hop tables computed via BFS (lex-deterministic on ties)
    data_owners:  same six everywhere
    network:      simulate flag + per-link latency knobs
    chunking, scheduler: defaults

Topologies:
    star          A connected directly to every other node
    tree          the original mini_2 spec edges (A,B,C,D,E,F,G,H,I)
    binary_tree   balanced binary tree rooted at A, depth 3
    chain         A-B-C-D-E-F-G-H-I (linear chain)
    ring          chain wrapped (A also connects to I)
    grid          3x3 grid, A in corner: {A,B,C}/{D,E,F}/{G,H,I} rows
    mesh          Hamiltonian cycle + 4 random chords (avg degree ~2.9 on
                  9 nodes; true 3-regular is impossible by the handshake
                  lemma when |V| is odd)
    random_k      random k-regular graph (requires k*|V| even)
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Hand-rolled YAML emit so we don't take a yaml dep. Format mirrors the
# existing config/overlay.yaml so the loaders keep working unchanged.

NODES = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]
GATEWAY = "A"
DATA_OWNERS = ["C", "D", "F", "G", "H", "I"]

# Default impl assignment matches the existing overlay.yaml so behaviour
# is comparable. Three Python nodes (D, F, H), six C++.
DEFAULT_IMPL = {
    "A": "cpp",    "B": "cpp",    "C": "cpp",
    "D": "python", "E": "cpp",    "F": "python",
    "G": "cpp",    "H": "python", "I": "cpp",
}

DEFAULT_TEAM = {
    "A": "blue",   "B": "blue",   "C": "yellow",
    "D": "blue",   "E": "yellow", "F": "yellow",
    "G": "yellow", "H": "blue",   "I": "yellow",
}

# Two virtual hosts in the 5/4 split. Host h1 is "this Mac"; host h2 is
# the Arch box. The latency simulator (or real network) only adds delay
# when an RPC crosses h1<->h2. Multihost launcher uses these to decide
# which nodes to ssh-launch on the remote machine.
DEFAULT_HOST_SPLIT = {
    # 5 here (this Mac)
    "A": "h1", "B": "h1", "C": "h1", "D": "h1", "E": "h1",
    # 4 on the Arch box
    "F": "h2", "G": "h2", "H": "h2", "I": "h2",
}

PORTS = {n: 50051 + i for i, n in enumerate(NODES)}


# ---------------------------------------------------------------------------
# Edge sets per topology.
# ---------------------------------------------------------------------------

def edges_star() -> List[Tuple[str, str]]:
    return [(GATEWAY, n) for n in NODES if n != GATEWAY]


def edges_tree() -> List[Tuple[str, str]]:
    # The original mini_2 spec edges.
    return [
        ("A", "B"), ("B", "C"), ("B", "D"), ("B", "E"),
        ("E", "F"), ("E", "D"), ("E", "G"),
        ("A", "H"), ("A", "G"), ("A", "I"),
    ]


def edges_binary_tree() -> List[Tuple[str, str]]:
    # Balanced binary tree, root A.
    #              A
    #            /   \
    #           B     C
    #          / \   / \
    #         D   E F   G
    #        / \
    #       H   I
    return [
        ("A", "B"), ("A", "C"),
        ("B", "D"), ("B", "E"),
        ("C", "F"), ("C", "G"),
        ("D", "H"), ("D", "I"),
    ]


def edges_chain() -> List[Tuple[str, str]]:
    return [(NODES[i], NODES[i + 1]) for i in range(len(NODES) - 1)]


def edges_ring() -> List[Tuple[str, str]]:
    e = edges_chain()
    e.append((NODES[-1], NODES[0]))   # close the loop
    return e


def edges_grid() -> List[Tuple[str, str]]:
    # 3x3 grid laid out as
    #   A B C
    #   D E F
    #   G H I
    rows = [["A", "B", "C"], ["D", "E", "F"], ["G", "H", "I"]]
    e = []
    for r in range(3):
        for c in range(3):
            if c + 1 < 3:
                e.append((rows[r][c], rows[r][c + 1]))
            if r + 1 < 3:
                e.append((rows[r][c], rows[r + 1][c]))
    return e


def edges_random_k(k: int = 4, seed: int = 0xC0FFEE) -> List[Tuple[str, str]]:
    """Configuration-model k-regular graph over NODES (no self-loops, no
    parallel edges). Requires k * |V| to be even; for |V|=9 use even k.
    Retries with a bumped seed if the random pairing fails.
    """
    if (k * len(NODES)) % 2 != 0:
        raise ValueError(
            f"k * |V| must be even for a k-regular graph "
            f"(got k={k}, |V|={len(NODES)}); pick an even k for an "
            f"odd-sized vertex set"
        )
    for attempt in range(200):
        rng = random.Random(seed + attempt)
        stubs = []
        for n in NODES:
            stubs.extend([n] * k)
        rng.shuffle(stubs)
        edges = set()
        bad = False
        for i in range(0, len(stubs), 2):
            a, b = stubs[i], stubs[i + 1]
            if a == b:
                bad = True; break
            e = tuple(sorted((a, b)))
            if e in edges:
                bad = True; break
            edges.add(e)
        if not bad:
            return [tuple(sorted(e)) for e in sorted(edges)]
    raise RuntimeError("could not generate random k-regular graph after 200 tries")


def edges_mesh(seed: int = 0xC0FFEE, target_edges: int = 13) -> List[Tuple[str, str]]:
    """Hamiltonian cycle + random chords. Default target=13 gives average
    degree (2*13)/9 ≈ 2.89 — eight nodes at degree 3, one at degree 2.
    """
    base = edges_ring()
    existing = {tuple(sorted(e)) for e in base}
    rng = random.Random(seed)
    while len(existing) < target_edges:
        a, b = rng.sample(NODES, 2)
        e = tuple(sorted((a, b)))
        if e in existing:
            continue
        existing.add(e)
    return [tuple(sorted(e)) for e in sorted(existing)]


GENERATORS = {
    "star":        edges_star,
    "tree":        edges_tree,
    "binary_tree": edges_binary_tree,
    "chain":       edges_chain,
    "ring":        edges_ring,
    "grid":        edges_grid,
    "mesh":        edges_mesh,
}


# ---------------------------------------------------------------------------
# Routing: BFS shortest path from every node to every other.
# ---------------------------------------------------------------------------

def bfs_next_hop(adj: Dict[str, List[str]], src: str) -> Dict[str, str]:
    """For every dst, return the next-hop neighbour of src on a shortest
    path. Ties broken lexicographically on the neighbour name so the
    routing table is deterministic across runs.
    """
    next_hop: Dict[str, str] = {src: src}
    visited = {src}
    # frontier carries (node, first_neighbour_taken_from_src)
    q = collections.deque()
    for nb in sorted(adj[src]):           # lex-deterministic tie-break
        if nb not in visited:
            visited.add(nb)
            next_hop[nb] = nb
            q.append((nb, nb))
    while q:
        u, first = q.popleft()
        for v in sorted(adj[u]):
            if v in visited:
                continue
            visited.add(v)
            next_hop[v] = first
            q.append((v, first))
    return next_hop


def build_adj(edges: List[Tuple[str, str]]) -> Dict[str, List[str]]:
    adj: Dict[str, List[str]] = {n: [] for n in NODES}
    for a, b in edges:
        if b not in adj[a]:
            adj[a].append(b)
        if a not in adj[b]:
            adj[b].append(a)
    return adj


def validate_connectivity(adj: Dict[str, List[str]]) -> None:
    visited = {NODES[0]}
    q = collections.deque([NODES[0]])
    while q:
        u = q.popleft()
        for v in adj[u]:
            if v not in visited:
                visited.add(v)
                q.append(v)
    missing = set(NODES) - visited
    if missing:
        raise ValueError(f"graph is not connected; unreachable: {sorted(missing)}")


# ---------------------------------------------------------------------------
# YAML emit (hand-rolled to match config/overlay.yaml shape exactly).
# ---------------------------------------------------------------------------

def emit_yaml(
    name: str,
    edges: List[Tuple[str, str]],
    *,
    host_map: Dict[str, str],
    impl_map: Dict[str, str],
    team_map: Dict[str, str],
    real_hosts: Dict[str, str],
    inter_host_latency_ms: float,
    intra_host_latency_ms: float,
    simulate_network: bool,
) -> str:
    adj = build_adj(edges)
    validate_connectivity(adj)

    # Compact unique edge list.
    seen = set()
    canonical_edges: List[Tuple[str, str]] = []
    for a, b in edges:
        e = tuple(sorted((a, b)))
        if e in seen:
            continue
        seen.add(e)
        canonical_edges.append(e)
    canonical_edges.sort()

    routing = {src: bfs_next_hop(adj, src) for src in NODES}

    out = []
    out.append(f"# Auto-generated overlay: topology = {name}")
    out.append(f"# Edges ({len(canonical_edges)}): "
               + " ".join(f"{a}{b}" for a, b in canonical_edges))
    out.append("# Generated by py/topology/generator.py — DO NOT hand-edit.")
    out.append("")

    # nodes
    out.append("nodes:")
    for n in NODES:
        host = real_hosts.get(host_map[n], "127.0.0.1")
        port = PORTS[n]
        team = team_map[n]
        role = "gateway" if n == GATEWAY else "peer"
        impl = impl_map[n]
        vhost = host_map[n]
        out.append(f"  {n}: {{ host: {host}, port: {port}, team: {team}, "
                   f"role: {role}, impl: {impl}, virtual_host: {vhost} }}")
    out.append("")

    # edges
    out.append("edges:")
    for a, b in canonical_edges:
        out.append(f"  - [{a}, {b}]")
    out.append("")

    # routing
    out.append("routing:")
    for src in NODES:
        items = ", ".join(f"{dst}: {routing[src][dst]}" for dst in NODES)
        out.append(f"  {src}: {{ {items} }}")
    out.append("")

    out.append(f"data_owners: [{', '.join(DATA_OWNERS)}]")
    out.append("")

    out.append("network:")
    out.append(f"  simulate: {'true' if simulate_network else 'false'}")
    out.append(f"  inter_host_latency_ms: {inter_host_latency_ms}")
    out.append(f"  intra_host_latency_ms: {intra_host_latency_ms}")
    out.append("")

    out.append("chunking:")
    out.append("  initial_rows: 64")
    out.append("  max_rows: 4096")
    out.append("  min_rows: 16")
    out.append("  target_chunk_ms: 25")
    out.append("")

    out.append("scheduler:")
    out.append("  mode: weighted_round_robin")
    out.append("  max_concurrent_requests: 32")
    out.append("")

    return "\n".join(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Generate Mini 2 overlay yamls.")
    ap.add_argument("--topo", choices=list(GENERATORS) + ["all"],
                    default="all", help="which topology (or 'all')")
    ap.add_argument("--out-dir", type=Path, default=Path("config/topo"))
    ap.add_argument("--host-h1", default="127.0.0.1",
                    help="real IP for virtual host h1")
    ap.add_argument("--host-h2", default="127.0.0.1",
                    help="real IP for virtual host h2")
    ap.add_argument("--simulate-network", action="store_true",
                    help="enable the latency interceptor at run time")
    ap.add_argument("--inter-host-latency-ms", type=float, default=0.0,
                    help="one-way delay added by the interceptor")
    ap.add_argument("--intra-host-latency-ms", type=float, default=0.0)
    ap.add_argument("--random-k", type=int, default=4,
                    help="k for the random_k topology (must satisfy k*|V| even)")
    ap.add_argument("--random-seed", type=int, default=0xC0FFEE)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    real_hosts = {"h1": args.host_h1, "h2": args.host_h2}

    targets = list(GENERATORS) if args.topo == "all" else [args.topo]
    # random_k is parameterised — emit it as an extra topology when "all".
    if args.topo == "all":
        targets.append("random_k")

    written = []
    for t in targets:
        if t == "random_k":
            edges = edges_random_k(k=args.random_k, seed=args.random_seed)
        else:
            edges = GENERATORS[t]()
        body = emit_yaml(
            name=t,
            edges=edges,
            host_map=DEFAULT_HOST_SPLIT,
            impl_map=DEFAULT_IMPL,
            team_map=DEFAULT_TEAM,
            real_hosts=real_hosts,
            inter_host_latency_ms=args.inter_host_latency_ms,
            intra_host_latency_ms=args.intra_host_latency_ms,
            simulate_network=args.simulate_network,
        )
        path = args.out_dir / f"{t}.yaml"
        path.write_text(body)
        written.append(path)

    summary = {
        "wrote": [str(p) for p in written],
        "real_hosts": real_hosts,
        "host_split": DEFAULT_HOST_SPLIT,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
