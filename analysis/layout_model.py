"""A layout model of rewrite policies at stride granularity, replayed over an op log.

The table is strides: runs of up to STRIDE rows that every column cuts its pages at (the writer's
20,000-row page limit), inside row groups of up to RG rows, inside files. A row lives in one
stride; a stride's rows die by deletion vector (row-level), or the whole stride dies when it is
rewritten (a run in a deletion vector, or a row group dropped from a footer). Bytes are rows times
B. The model predicts what each policy writes and reads back per write, and what a full read opens
at the end; it is a model of the policies, not of encodings or requests.

Policies:
  mor      MoR: a write's inserts and new versions are a new file sorted by (ts, id); changed rows
           die by deletion vector.
  morc     MoR with compaction every `every` writes, as the harness's (Iceberg's bin-packing
           defaults): files under 0.75 of the 128 MiB target, or with at least 0.3 of their rows
           dead, packed in file order into files of up to the target, live rows sorted by ts.
  rg       RMo* with row groups kept whole (W1-W3): every row group holding a changed row is
           rewritten as one row group; inserts apart.
  stride   Variant A: every stride holding a changed row is rewritten; gaps of at most `gap` live
           strides between rewritten strides of a file are rewritten too (contiguity); each run of
           rewritten strides becomes a row group; inserts apart.
  clean    Variant B: a stride is rewritten when the write changes at least `theta` of its live
           rows (with `gap` as above); below, its changed rows die by deletion vector and their new
           versions join the inserts. Then a cleaner carries spans forward: a span is a file's live
           strides separated by at most `gap` dead strides; one whose occupancy (live rows over
           the rows of its extent) is below `low` and untouched for `age` writes is carried, by
           cost-benefit (1 - u) * age / (1 + u), within a budget of `budget` times the write's own
           rows; a file left below `drain` of its rows has the rest carried too.

  python analysis/layout_model.py <run dir or dir with oplog.bin> [--policy ...]
"""
import argparse
import os
import sys

import numpy as np

STRIDE, RG, INITIAL_FILES = 20_000, 532_000, 6
B = 15.2e6 / 532_000  # bytes a row on disk, from a 16 MiB row group of the initial load


def load(path):
    buf = open(os.path.join(path, "oplog.bin"), "rb").read()
    pos = [0]

    def take(fmt, n=1):
        v = np.frombuffer(buf, dtype=np.dtype(fmt).newbyteorder(">"), count=n, offset=pos[0])
        pos[0] += np.dtype(fmt).itemsize * n
        return v

    n0 = int(take("q")[0])
    n = int(take("i")[0])
    ts = take("q", n).astype(np.int64)
    writes = []
    for _ in range(int(take("i")[0])):
        take("i")
        ins = take("q", int(take("i")[0])).astype(np.int64)
        upd = take("q", int(take("i")[0])).astype(np.int64)
        take("i", int(take("i")[0]))
        dele = take("q", int(take("i")[0])).astype(np.int64)
        writes.append((ins, upd, dele))
    return n0, n, ts, writes


class Table:
    """Strides, row groups and files, and the stride each live row is in."""

    def __init__(self, n):
        self.s_of = np.full(n, -1, np.int64)
        self.file, self.rows0, self.live, self.last, self.rg = [], [], [], [], []
        self.files = []  # per file: [first stride, end stride)
        self.nrg = 0

    def add_file(self, groups, w):
        """A new file of row groups, each an id array in file order, cut into strides."""
        f, first = len(self.files), len(self.file)
        for ids in groups:
            for lo in range(0, len(ids), RG):
                part = ids[lo:lo + RG]
                for so in range(0, len(part), STRIDE):
                    s = len(self.file)
                    rows = part[so:so + STRIDE]
                    self.s_of[rows] = s
                    self.file.append(f); self.rows0.append(len(rows)); self.live.append(len(rows))
                    self.last.append(w); self.rg.append(self.nrg)
                self.nrg += 1
        self.files.append((first, len(self.file)))
        return f

    def arrays(self):
        return (np.array(self.file), np.array(self.rows0), np.array(self.live), np.array(self.last))


TARGET = 128 * 2**20 / B  # the target file size, in rows


def run(path, policy, theta=0.0, gap=0, low=0.5, age=2, budget=0.5, drain=0.1, sort_by_ts=True,
        every=10):
    n0, n, ts, writes = load(path)
    t = Table(n)
    per = -(-n0 // INITIAL_FILES)
    for f in range(INITIAL_FILES):
        t.add_file([np.arange(f * per, min((f + 1) * per, n0))], -1)
    out = []
    for w, (ins, upd, dele) in enumerate(writes):
        changed = np.concatenate([upd, dele])
        olds = t.s_of[changed]
        touched, counts = np.unique(olds, return_counts=True)
        fresh = [ins]  # rows of the write's own row group(s): inserts, and new versions below theta
        rewrite = set()
        if policy in ("mor", "morc"):
            fresh.append(upd)
        elif policy == "rg":
            rgs = {t.rg[s] for s in touched}
            rewrite = {s for s in range(len(t.file)) if t.rg[s] in rgs and t.live[s] > 0}
        else:
            th = theta if policy == "clean" else 0.0
            for s, c in zip(touched, counts):
                if c >= th * t.live[s]:
                    rewrite.add(int(s))
            rewrite |= gaps(t, rewrite, gap)
            below = np.isin(olds, list(rewrite), invert=True)
            fresh.append(upd[below[: len(upd)]])
        # changed rows leave their strides
        for s, c in zip(touched, counts):
            t.live[s] -= int(c); t.last[s] = w
        t.s_of[dele] = -1
        read_back = 0
        groups = []
        # rewritten strides: their live rows (new versions in place), in file order; a run of
        # rewritten strides is one row group (under rg, each old row group stays one)
        if rewrite:
            rows_of = collect(t, rewrite)
            units = by_rg(t, sorted(rewrite)) if policy == "rg" else runs(t, sorted(rewrite))
            for unit in units:
                read_back += sum(t.rows0[s] for s in unit)
                ids = [rows_of[s] for s in unit if s in rows_of]
                groups.append(np.sort(np.concatenate(ids)) if ids else np.array([], np.int64))
                for s in unit:
                    t.live[s] = 0
        # the cleaner
        carried = 0
        if policy == "clean":
            own = sum(len(g) for g in groups) + sum(len(x) for x in fresh)
            chosen = clean(t, w, gap, low, age, int(budget * own), drain)
            if chosen:
                rows_of = collect(t, {s for sp in chosen for s in sp})
                for sp in chosen:
                    ids = np.sort(np.concatenate([rows_of[s] for s in sp if s in rows_of]))
                    read_back += sum(t.rows0[s] for s in sp)
                    carried += len(ids)
                    for s in sp:
                        t.live[s] = 0
                    groups.append(ids)
        new = np.concatenate(fresh) if fresh else np.array([], np.int64)
        if sort_by_ts and len(new):
            new = new[np.lexsort((new, ts[new]))]
        groups = [g for g in groups if len(g)] + ([new] if len(new) else [])
        written = sum(len(g) for g in groups)
        t.add_file(groups, w)
        compacted = 0
        if policy == "morc" and (w + 1) % every == 0:
            compacted = compact(t, w, ts)
        rc = read_cost(t)
        out.append(dict(write=w + 1, written_mb=written * B / 1e6, read_back_mb=read_back * B / 1e6,
                        carried_mb=carried * B / 1e6, compacted_mb=compacted * B / 1e6,
                        files=rc["files"], read_skip_gb=rc["read_skip_gb"],
                        read_noskip_gb=rc["read_noskip_gb"], live_gb=rc["live_gb"]))
    return t, out


def compact(t, w, ts):
    """Bin-packing compaction; returns the rows it wrote."""
    groups, cur, cur_rows = [], [], 0
    for f, (a, b) in enumerate(t.files):
        total = sum(t.rows0[a:b]); live = sum(t.live[a:b])
        if live == 0 or not (total < 0.75 * TARGET or (total - live) >= 0.3 * total):
            continue
        if cur and cur_rows + live > TARGET:
            groups.append(cur); cur, cur_rows = [], 0
        cur.append(f); cur_rows += live
    if cur:
        groups.append(cur)
    wrote = 0
    for g in groups:
        a0 = [s for f in g for s in range(*t.files[f]) if t.live[s] > 0]
        if len(g) == 1:
            a, b = t.files[g[0]]
            if sum(t.rows0[a:b]) - sum(t.live[a:b]) < 0.3 * sum(t.rows0[a:b]):
                continue
        rows_of = collect(t, a0)
        ids = np.concatenate([rows_of[s] for s in a0 if s in rows_of])
        ids = ids[np.lexsort((ids, ts[ids]))]
        for s in a0:
            t.live[s] = 0
        t.add_file([ids], w)
        wrote += len(ids)
    return wrote


def collect(t, strides):
    """The live rows of each of `strides`, as id arrays in id order."""
    arr = np.array(sorted(strides), dtype=np.int64)
    mark = np.zeros(len(t.file) + 1, bool)
    mark[arr] = True
    ids = np.nonzero(mark[t.s_of])[0]  # s_of -1 indexes the last, false
    sid = t.s_of[ids]
    order = np.argsort(sid, kind="stable")
    ids, sid = ids[order], sid[order]
    u, start = np.unique(sid, return_index=True)
    end = np.append(start[1:], len(sid))
    return {int(x): ids[a:b] for x, a, b in zip(u, start, end)}


def by_rg(t, strides):
    out = {}
    for s in strides:
        out.setdefault(t.rg[s], []).append(s)
    return list(out.values())


def gaps(t, rewrite, gap):
    """Live strides lying between two rewritten strides of a file with at most `gap` live between."""
    if gap <= 0:
        return set()
    add = set()
    by_file = {}
    for s in sorted(rewrite):
        by_file.setdefault(t.file[s], []).append(s)
    for f, ss in by_file.items():
        for a, b in zip(ss, ss[1:]):
            between = [s for s in range(a + 1, b) if t.live[s] > 0]
            if 0 < len(between) <= gap:
                add.update(between)
    return add


def runs(t, strides):
    """Sorted strides grouped into runs: consecutive live strides of one file (dead ones skipped)."""
    out, cur = [], []
    for s in strides:
        if cur and (t.file[s] != t.file[cur[-1]] or any(t.live[x] > 0 for x in range(cur[-1] + 1, s))):
            out.append(cur); cur = []
        cur.append(s)
    if cur:
        out.append(cur)
    return out


def spans(t, f, gap):
    """A file's live strides grouped into spans separated by at most `gap` dead strides."""
    a, b = t.files[f]
    out, cur, dead = [], [], 0
    for s in range(a, b):
        if t.live[s] > 0:
            if cur and dead > gap:
                out.append(cur); cur = []
            cur.append(s); dead = 0
        elif cur:
            dead += 1
    if cur:
        out.append(cur)
    return out


def clean(t, w, gap, low, age_min, budget, drain):
    cands = []
    for f in range(len(t.files)):
        a, b = t.files[f]
        total = sum(t.rows0[a:b]); live = sum(t.live[a:b])
        if live == 0:
            continue
        if live < drain * total and all(w - t.last[s] >= age_min for s in range(a, b) if t.live[s] > 0):
            cands.append((float("inf"), [s for s in range(a, b) if t.live[s] > 0]))
            continue
        for sp in spans(t, f, gap):
            extent = sum(t.rows0[sp[0]:sp[-1] + 1])
            lv = sum(t.live[s] for s in sp)
            u = lv / extent
            ag = min(w - t.last[s] for s in sp)
            if u < low and ag >= age_min:
                cands.append(((1 - u) * ag / (1 + u), sp))
    cands.sort(key=lambda c: -c[0])
    chosen, used = [], 0
    for score, sp in cands:
        lv = sum(t.live[s] for s in sp)
        if used + lv > budget and score != float("inf"):
            continue
        chosen.append(sp); used += lv
    return chosen


def read_cost(t):
    """What a full read opens at the end: files with a live row, live fragments (runs of live
    strides), bytes with page skipping (live strides) and without (every stride of an open file)."""
    file_, rows0, live, _ = t.arrays()
    open_files = [f for f, (a, b) in enumerate(t.files) if live[a:b].sum() > 0]
    frags = sum(len(spans(t, f, 0)) for f in open_files)
    skip = rows0[live > 0].sum() * B / 1e9
    noskip = sum(rows0[t.files[f][0]:t.files[f][1]].sum() for f in open_files) * B / 1e9
    sparse = int(((live > 0) & (live < 0.1 * rows0)).sum())
    shares = [live[t.files[f][0]:t.files[f][1]].sum() / rows0[t.files[f][0]:t.files[f][1]].sum()
              for f in open_files]
    # row groups a footer lists: each run of live strides inside a row group (a split row group's
    # pieces are virtual row groups)
    rg_, prev, prev_live, row_groups = np.array(t.rg), None, False, 0
    for s in range(len(rg_)):
        lv = live[s] > 0
        if lv and not (prev == rg_[s] and prev_live):
            row_groups += 1
        prev, prev_live = rg_[s], lv
    return dict(files=len(open_files), files_lt50=int(sum(x < 0.5 for x in shares)), fragments=frags,
                row_groups=row_groups, read_skip_gb=round(skip, 2),
                read_noskip_gb=round(noskip, 2), live_gb=round(live.sum() * B / 1e9, 2),
                sparse_strides=sparse)


def main():
    global STRIDE
    p = argparse.ArgumentParser()
    p.add_argument("path")
    p.add_argument("--policy", default="mor,rg,stride")
    p.add_argument("--theta", type=float, default=0.25)
    p.add_argument("--gap", type=int, default=1)
    p.add_argument("--low", type=float, default=0.5)
    p.add_argument("--age", type=int, default=2)
    p.add_argument("--budget", type=float, default=0.5)
    p.add_argument("--drain", type=float, default=0.5)
    p.add_argument("--every", type=int, default=10)
    p.add_argument("--stride", type=int, default=20_000, help="rows a stride (532000: a row group)")
    a = p.parse_args()
    STRIDE = a.stride
    for pol in a.policy.split(","):
        name, _, g = pol.partition(":")
        gap = int(g) if g else (a.gap if name in ("stride", "clean") else 0)
        t, out = run(a.path, name, theta=a.theta, gap=gap, low=a.low, age=a.age, budget=a.budget,
                     drain=a.drain, every=a.every)
        mb = np.array([o["written_mb"] for o in out]); rb = np.array([o["read_back_mb"] for o in out])
        car = np.array([o["carried_mb"] for o in out])
        cmp_ = np.array([o["compacted_mb"] for o in out])
        cost = {k: (round(float(v), 2) if isinstance(v, (float, np.floating)) else v)
                for k, v in read_cost(t).items()}
        amp = np.array([o["read_skip_gb"] / o["live_gb"] for o in out])
        amp_ns = np.array([o["read_noskip_gb"] / o["live_gb"] for o in out])
        fl = np.array([o["files"] for o in out])
        print(f"{pol:10s} write MB mean {mb.mean():5.0f} max {mb.max():5.0f}  read back {rb.mean():5.0f}"
              f"  carried {car.mean():4.0f}  compaction total {cmp_.sum():5.0f} max {cmp_.max():5.0f}"
              f"  | over writes: files {fl.mean():4.1f} (max {fl.max()}), read/live skip {amp.mean():.2f}"
              f" (max {amp.max():.2f}) noskip {amp_ns.mean():.2f} (max {amp_ns.max():.2f})"
              f"  | at the end {cost}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
