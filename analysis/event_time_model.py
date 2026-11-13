#!/usr/bin/env python3
"""The inserts workload, modelled (docs/HYPOTHESES.md, 2026-11-12 and 2026-11-13): what each way of
placing the inserts writes, and what a 1% range read by event time and the sorted read then touch.

  mor      MoR: each write's inserts are a file of their own, one row group at these sizes
  compact  MoR (compacted) as Iceberg plans a bin-pack after every write (compactMinInputFiles 5):
           the files under 0.75 of the target size, rewritten together once there are 5 of them,
           sorted, into files of the target size cut into row groups
  density  RMoR-density: the event-time merge (EventTimeRegions), a row group merged once the rows
           joining it, those waiting included, reach theta of it; until then they wait in delta row
           groups of their own, one a row group a write
  merge    RMoR, RMoW-external, RMoW-append: the event-time merge with theta 0, every late row
           merged at once

It draws the op log's inserts as OpLogGenerator does (event time = id - lateness, lateness lognormal
by its median and 99th percentile, in writes), with numpy's generator rather than the op log's, so
it matches the harness in distribution, not row for row. Event times are in rows: an initial row's is
its id. A row group is its event-time range and rows, at `bpr` bytes a row; it holds at most `cap`
rows, the initial load's (the newest is cut into full row groups and the rest, an older one at twice
that into equal parts; their ranges are split in proportion to their rows). A range read reads every
row group whose range overlaps it, whole; the sorted read holds every row group whose range spans the
event time it has reached. Requests, footers and pages are left out.

    .venv/bin/python analysis/event_time_model.py                 # the 128 MiB suite's inserts
    .venv/bin/python analysis/event_time_model.py --json out.json # each design's bytes per write
    .venv/bin/python analysis/event_time_model.py --initial 400000 --per-write 20000 \\
        --writes 12 --row-groups 39 --rg-per-file 4 --bpr 33     # the scale first checked

Checked against the harness on 2026-11-12 at that small scale (400,000 rows, 1 MiB row groups, 39
of them, 12 writes of 20,000, lateness median 0.05 and p99 5 writes): the event-time merge wrote
15 MB a write at theta 0 against the harness's 12.4-16.8 (mean 14.5), 1 MB at 0.06 against 1.0-2.0
(1.4), and ended with 61 row groups against 59.
"""
import argparse
import json

import numpy as np

Z99 = 2.3263478740408408
WINDOWS = (0.1, 0.3, 0.5, 0.7, 0.9)


class Mor:
    """Files (MoR), each a list of row groups (lo, hi, rows); small ones keep their event times."""

    def __init__(self, m):
        self.m = m
        self.files = []
        for f in range(0, len(m.initial_groups), m.rg_per_file):
            groups = m.initial_groups[f : f + m.rg_per_file]
            rows = sum(g[2] for g in groups)
            ts = np.arange(groups[0][0], groups[-1][1] + 1, dtype=float)
            self.files.append({"groups": groups, "rows": rows, "ts": ts})

    def write(self, ts):
        self.files.append(self.m.file_of(np.sort(ts)))
        return len(ts) * self.m.bpr

    def groups(self):
        return [g for f in self.files for g in f["groups"]]


class Compact(Mor):
    """MoR with Iceberg's bin-pack rule checked after every write (MorArm.icebergRewrites)."""

    def write(self, ts):
        written = super().write(ts)
        m = self.m
        small = [f for f in self.files if f["rows"] * m.bpr < m.small_fraction * m.target]
        total = sum(f["rows"] for f in small) * m.bpr
        if not (len(small) > 1 and (len(small) >= m.min_files or total > m.target)):
            return written
        self.files = [f for f in self.files if not any(f is s for s in small)]
        group, size = [], 0.0
        for f in small + [None]:
            if f is None or (group and size + f["rows"] * m.bpr > m.target):
                rows = np.sort(np.concatenate([g["ts"] for g in group]))
                self.files.append(m.file_of(rows))
                written += len(rows) * m.bpr
                group, size = [], 0.0
            if f is not None:
                group.append(f)
                size += f["rows"] * m.bpr
        return written


class Merge:
    """The event-time merge: regions (row groups) by their first event time, with deltas waiting."""

    def __init__(self, m, theta):
        self.m = m
        self.theta = theta
        self.regions = [
            {"lo": g[0], "hi": g[1], "rows": g[2], "waiting": 0, "deltas": []}
            for g in m.initial_groups
        ]

    def write(self, ts):
        m = self.m
        ts = np.sort(ts)
        los = np.array([r["lo"] for r in self.regions])
        at = np.maximum(np.searchsorted(los, ts, side="right") - 1, 0)
        cuts = np.flatnonzero(np.diff(at)) + 1
        starts = np.concatenate(([0], cuts))
        ends = np.concatenate((cuts, [len(ts)]))
        joining = {int(at[s]): (int(e - s), ts[s], ts[e - 1]) for s, e in zip(starts, ends)}
        newest = len(self.regions) - 1
        written = 0.0
        out = []
        for i, r in enumerate(self.regions):
            if i not in joining:
                out.append(r)
                continue
            c, lo, hi = joining[i]
            if i != newest and self.theta > 0 and c + r["waiting"] < self.theta * r["rows"]:
                r["waiting"] += c
                r["deltas"].append((lo, hi, c))
                written += c * m.bpr
                out.append(r)
                continue
            rows = r["rows"] + c + r["waiting"]
            lo = min([r["lo"], lo] + [d[0] for d in r["deltas"]])
            hi = max([r["hi"], hi] + [d[1] for d in r["deltas"]])
            written += rows * m.bpr
            if i == newest:
                parts = [m.cap] * int((rows - 1) // m.cap) + [rows - m.cap * int((rows - 1) // m.cap)]
            elif rows >= 2 * m.cap:
                n = int(rows // m.cap)
                parts = [rows / n] * n
            else:
                parts = [rows]
            at_ts = lo
            for p in parts:
                span = (hi - lo) * p / rows
                out.append({"lo": at_ts, "hi": at_ts + span, "rows": p, "waiting": 0, "deltas": []})
                at_ts += span
        self.regions = out
        return written

    def groups(self):
        out = []
        for r in self.regions:
            out.append((r["lo"], r["hi"], r["rows"]))
            out.extend(r["deltas"])
        return out


class Model:
    def __init__(self, a):
        self.bpr = a.bpr
        self.cap = a.initial // a.row_groups
        self.rg_per_file = a.rg_per_file
        self.target = a.target_mib * 1048576
        self.small_fraction = 0.75
        self.min_files = 5
        self.initial = a.initial
        self.initial_groups = [
            (i * self.cap, min((i + 1) * self.cap, a.initial) - 1,
             min((i + 1) * self.cap, a.initial) - i * self.cap)
            for i in range(a.row_groups)
        ]

    def file_of(self, sorted_ts):
        """A file of rows sorted by event time, cut into row groups of `cap` rows."""
        groups = []
        for s in range(0, len(sorted_ts), self.cap):
            chunk = sorted_ts[s : s + self.cap]
            groups.append((float(chunk[0]), float(chunk[-1]), len(chunk)))
        small = len(sorted_ts) * self.bpr < self.small_fraction * self.target
        return {"groups": groups, "rows": len(sorted_ts), "ts": sorted_ts if small else None}

    def reads(self, groups, span):
        """Each window's bytes read over its live bytes, and the sorted read's peak bytes held."""
        out = {}
        live = 0.01 * span * self.bpr
        for f in WINDOWS:
            lo, hi = (f - 0.005) * span, (f + 0.005) * span
            read = sum(g[2] for g in groups if g[0] <= hi and g[1] >= lo) * self.bpr
            out[f] = read / live
        events = sorted([(g[0], g[2]) for g in groups] + [(g[1] + 1e-9, -g[2]) for g in groups])
        held = peak = 0.0
        for _, d in events:
            held += d
            peak = max(peak, held)
        return out, peak * self.bpr


def run(a):
    m = Model(a)
    designs = {"mor": Mor(m), "compact": Compact(m), "density": Merge(m, a.theta),
               "merge": Merge(m, 0.0)}
    if a.designs:
        designs = {k: v for k, v in designs.items() if k in a.designs.split(",")}
    rng = np.random.default_rng(a.seed)
    mu = np.log(a.late_median)
    sigma = (np.log(a.late_p99) - mu) / Z99
    written = {k: [] for k in designs}
    reads = {k: {} for k in designs}
    nid = a.initial
    for w in range(1, a.writes + 1):
        ids = np.arange(nid, nid + a.per_write, dtype=float)
        nid += a.per_write
        late = np.exp(mu + sigma * rng.standard_normal(a.per_write))
        ts = np.maximum(0, ids - late * a.per_write)
        for k, d in designs.items():
            written[k].append(d.write(ts))
            if w % a.read_every == 0 or w == a.writes:
                reads[k][w] = m.reads(d.groups(), nid)
    return designs, written, reads


def parser():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--initial", type=int, default=205_480_000)
    ap.add_argument("--per-write", type=int, default=350_000)
    ap.add_argument("--writes", type=int, default=240)
    ap.add_argument("--row-groups", type=int, default=58, help="the initial load's")
    ap.add_argument("--rg-per-file", type=int, default=5, help="the initial load's")
    ap.add_argument("--target-mib", type=float, default=512)
    ap.add_argument("--bpr", type=float, default=30.1, help="bytes a row on disk")
    ap.add_argument("--theta", type=float, default=0.06, help="RMoR-density's")
    ap.add_argument("--late-median", type=float, default=0.05)
    ap.add_argument("--late-p99", type=float, default=5.0)
    ap.add_argument("--read-every", type=int, default=40)
    ap.add_argument("--designs", default="", help="a comma-separated subset")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--json", help="write each design's bytes a write and reads here")
    return ap


def project(**overrides):
    """The model run with its defaults (the 128 MiB suite's inserts) but for `overrides`, by
    argument name (per_write, theta, ...): each design's bytes a write, its reads at each read
    point ({write: (window ratios by position, sorted read's bytes held)}), and its row groups at
    the end."""
    a = parser().parse_args([])
    for k, v in overrides.items():
        if not hasattr(a, k):
            raise ValueError(f"no model argument {k}")
        setattr(a, k, v)
    designs, written, reads = run(a)
    return {"args": vars(a), "written": written, "reads": reads,
            "row_groups": {k: len(d.groups()) for k, d in designs.items()}}


def main():
    a = parser().parse_args()
    designs, written, reads = run(a)
    for k in designs:
        mb = np.array(written[k]) / 1e6
        last = max(reads[k])
        windows, held = reads[k][last]
        print(f"{k:8s} {mb.sum() / 1e3:7.1f} GB written, {mb.mean():7.1f} MB a write"
              f" (first 30 {mb[:30].mean():.1f}, last 30 {mb[-30:].mean():.1f});"
              f" {len(designs[k].groups())} row groups; after write {last}: windows "
              + " ".join(f"{windows[f]:.1f}" for f in WINDOWS)
              + f" times live, sorted read holds {held / 1e9:.2f} GB")
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"args": vars(a), "written": written,
                       "reads": {k: {w: {"windows": {str(x): y for x, y in r[0].items()},
                                         "held": r[1]} for w, r in v.items()}
                                 for k, v in reads.items()}}, f)


if __name__ == "__main__":
    main()
