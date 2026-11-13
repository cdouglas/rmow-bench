#!/usr/bin/env python3
"""The event-time merge's write, modelled (docs/HYPOTHESES.md, 2026-11-12): the bytes a write
rewrites when each insert joins the row group its event time falls in (EventTimeRegions), and with
deferred merges (RMoR-density) when a row group merges only once the rows that join it reach theta
of its rows, the rest waiting in delta row groups.

It draws the op log's inserts as OpLogGenerator does (event time = id - lateness, lateness
lognormal by its median and 99th percentile, in writes), with numpy's generator rather than the
op log's, so it matches the harness in distribution, not row for row. Row groups are cut as the
arms cut them: the newest into full row groups of `cap` rows and the rest, an older one only once it
holds two full ones, into equal parts. A row group merged counts its rows, its inserts and the rows
that waited for it at `bpr` bytes a row; a deferred insert counts its own row.

    .venv/bin/python analysis/event_time_model.py               # the 128 MiB suite's inserts
    .venv/bin/python analysis/event_time_model.py --theta 0.06  # RMoR-density
    .venv/bin/python analysis/event_time_model.py --initial 400000 --per-write 20000 \\
        --writes 12 --row-groups 39 --bpr 33        # the scale the harness was checked at

Checked against the harness on 2026-11-12 at that small scale (400,000 rows, 1 MiB row groups, 39
of them, 12 writes of 20,000, lateness median 0.05 and p99 5 writes): the model writes 15 MB a
write at theta 0 against the harness's 12.4-16.8 (mean 14.5), 1 MB at 0.06 against 1.0-2.0 (1.4),
and ends with 61 row groups against 59.
"""
import argparse

import numpy as np

Z99 = 2.3263478740408408


def model(initial, per_write, writes, row_groups, bpr, theta, median, p99, seed):
    rng = np.random.default_rng(seed)
    cap = initial // row_groups
    mu = np.log(median)
    sigma = (np.log(p99) - mu) / Z99
    # regions: first event time (in rows: ts = id - lateness * per_write), rows, rows waiting
    mins = [i * cap for i in range(row_groups)]
    rows = [cap] * (row_groups - 1) + [initial - (row_groups - 1) * cap]
    waiting = [0] * row_groups
    nid = initial
    per = []
    for _ in range(writes):
        ids = np.arange(nid, nid + per_write)
        nid += per_write
        late = np.exp(mu + sigma * rng.standard_normal(per_write))
        ts = np.maximum(0, ids - late * per_write)
        at = np.searchsorted(np.array(mins), ts, side="right") - 1
        at[at < 0] = 0
        counts = np.bincount(at, minlength=len(mins))
        newest = len(mins) - 1
        written = 0
        m2, r2, w2 = [], [], []
        for r in range(len(mins)):
            c, n = int(counts[r]), rows[r]
            merge = c > 0 and (r == newest or theta == 0 or c + waiting[r] >= theta * n)
            if not merge:
                if c > 0:
                    waiting[r] += c
                    written += c * bpr  # a delta row group
                m2.append(mins[r])
                r2.append(n)
                w2.append(waiting[r])
                continue
            n2 = n + c + waiting[r]
            written += n2 * bpr
            if r == newest:
                k = 0
                while n2 - k * cap > cap:
                    m2.append(mins[r] + k * cap)
                    r2.append(cap)
                    w2.append(0)
                    k += 1
                m2.append(mins[r] + k * cap)
                r2.append(n2 - k * cap)
                w2.append(0)
            elif n2 >= 2 * cap:
                parts = n2 // cap
                for j in range(parts):
                    m2.append(mins[r] + j * (n2 // parts))
                    r2.append(n2 // parts)
                    w2.append(0)
            else:
                m2.append(mins[r])
                r2.append(n2)
                w2.append(0)
        mins, rows, waiting = m2, r2, w2
        per.append(written)
    return np.array(per), len(mins)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--initial", type=int, default=205_480_000)
    ap.add_argument("--per-write", type=int, default=350_000)
    ap.add_argument("--writes", type=int, default=240)
    ap.add_argument("--row-groups", type=int, default=58, help="the initial load's")
    ap.add_argument("--bpr", type=float, default=30.1, help="bytes a row on disk")
    ap.add_argument("--theta", type=float, default=0.0)
    ap.add_argument("--late-median", type=float, default=0.05)
    ap.add_argument("--late-p99", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    per, groups = model(a.initial, a.per_write, a.writes, a.row_groups, a.bpr, a.theta,
                        a.late_median, a.late_p99, a.seed)
    mb = per / 1e6
    print(f"theta {a.theta}: {a.writes} writes, {per.sum() / 1e9:.1f} GB written, MB a write: "
          f"mean {mb.mean():.0f}, median {np.median(mb):.0f}, max {mb.max():.0f}, "
          f"first 30 {mb[:30].mean():.0f}; {groups} row groups at the end; "
          f"MoR {a.per_write * a.bpr / 1e6:.1f} MB a write")


if __name__ == "__main__":
    main()
