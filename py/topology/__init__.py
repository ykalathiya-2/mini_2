"""Topology generators for Mini 2.

Each generator produces an undirected edge set over the 9 nodes; the same
codepath then attaches BFS routing tables, virtual-host assignments, and
the standard chunking / scheduler config so every produced overlay yaml
is drop-in compatible with the existing C++ / Python node binaries.
"""
