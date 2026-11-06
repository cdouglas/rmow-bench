"""A piece-level layout model: row groups re-cut where writes change them, and deletion vectors
with merges (the hybrid), replayed over an op log.

A piece is a row group: rows of one object in (ts, id) order, cut into strides of STRIDE rows (the
writer's page limit, at which every column starts a page). A write changes rows of some pieces.
What it does with a piece it changes is the policy:

  mor             every change a deletion vector; new versions join the write's inserts.
  rg              RMoW-append with row groups kept whole: the piece is rewritten as one piece.
  recut:M         the piece is rewritten, cut around the strides this write changes (and any
                  holding rows a DV hid): a run of at least M unchanged strides becomes a piece
                  of its own, a shorter one joins the changed strides beside it. A later write
                  rewrites only the pieces it touches.
  hybrid:M:T      deletion vectors until a piece's hidden rows plus the write's changes reach T of
                  its rows; then the piece is rewritten as recut:M does, dropping its hidden rows.

Placement (`--place`): `append` keeps a rewritten piece in its object, in the old piece's place in
the footer (RMoW-append), and inserts go to a tail object of up to TARGET rows; `file` writes the
write's rewritten pieces and inserts to a new file (RMoR, row groups kept whole).

Packing (`--pack-age A`, 0 for never; the inverse of cracking): after each write, runs of adjacent
pieces of an object, each under half of RG rows, created at least A writes ago and unchanged for A
writes, are packed into pieces of up to RG rows.

What it reports, per write: bytes written and read back (rewrites and packing); bytes an update
reads if it reads the old row's every column (the strides holding the rows it updates, in pieces
it does not rewrite: a rewrite reads its whole piece anyway); pieces whose id range holds a
changed id but no changed row (what locate by statistics reads for nothing). And for a full read
after it (dead pieces skipped, as the `cached-skip` reader): objects, pieces, deletion vectors,
bytes against live bytes, and requests, one a footer, a piece and a deletion vector.

  .venv/bin/python analysis/recut_model.py <run dir> [--policy ...] [--place append] [--pack-age 2]
"""
import argparse
import sys

import numpy as np

from layout_model import B, INITIAL_FILES, RG, STRIDE, TARGET, load


class Pieces:
    def __init__(self, n):
        self.piece_of = np.full(n, -1, np.int32)  # the piece holding the id's live version
        self.pos_of = np.zeros(n, np.int32)  # its position in that piece
        self.rows, self.dead, self.ndead = [], [], []  # ids in order; DV mask (or None)
        self.obj, self.born, self.last, self.lo, self.hi = [], [], [], [], []
        self.objects = []  # per object: its live pieces in footer order
        self.ts, self.tlo, self.thi = None, [], []  # event times, and each piece's range

    def new(self, ids, w, last):
        p = len(self.rows)
        self.rows.append(ids); self.dead.append(None); self.ndead.append(0)
        self.obj.append(-1); self.born.append(w); self.last.append(last)
        self.lo.append(int(ids.min())); self.hi.append(int(ids.max()))
        tv = self.ts[ids]
        self.tlo.append(int(tv.min())); self.thi.append(int(tv.max()))
        self.piece_of[ids] = p
        self.pos_of[ids] = np.arange(len(ids), dtype=np.int32)
        return p

    def new_object(self):
        self.objects.append([])
        return len(self.objects) - 1

    def place(self, o, ps):
        for p in ps:
            self.obj[p] = o
        self.objects[o].extend(ps)

    def replace(self, p, ps):
        o = self.obj[p]
        lst = self.objects[o]
        i = lst.index(p)
        for q in ps:
            self.obj[q] = o
        lst[i:i + 1] = ps


def cut(hot, m):
    """Strides flagged changed, as runs: a run of fewer than m unchanged strides joins its
    neighbours. Returns (first stride, end stride, changed) per run."""
    flags = hot.copy()
    ns = len(flags)
    i = 0
    while i < ns:
        j = i
        while j < ns and flags[j] == flags[i]:
            j += 1
        if not flags[i] and j - i < m:
            flags[i:j] = True
        i = j
    out, i = [], 0
    while i < ns:
        j = i
        while j < ns and flags[j] == flags[i]:
            j += 1
        out.append((i, j, bool(flags[i])))
        i = j
    return out


def run(path, policy, m, theta, place, pack_age, data=None, ingest="tail", horizon=float("inf")):
    n0, n, ts, writes = data or load(path)
    # a write's span of event time: its inserts' ids at the initial rows' spacing
    span_ts = len(writes[0][0]) * (ts[1] - ts[0])
    t = Pieces(n)
    t.ts = ts
    per = -(-n0 // INITIAL_FILES)
    for f in range(INITIAL_FILES):
        o = t.new_object()
        ids = np.arange(f * per, min((f + 1) * per, n0))
        t.place(o, [t.new(ids[a:a + RG], -1, -1) for a in range(0, len(ids), RG)])
    tail = None
    out = []
    merging = ingest == "merge" and policy in ("rg", "recut")
    for w, (ins, upd, dele) in enumerate(writes):
        changed = np.concatenate([upd, dele])
        is_upd = np.zeros(len(changed), bool); is_upd[: len(upd)] = True
        cp, cpos = t.piece_of[changed], t.pos_of[changed]
        if (cp < 0).any():
            raise ValueError(f"write {w + 1} changes a row the model has no live version of")
        # locate by statistics: pieces whose id range holds a changed id
        alive = [p for o in t.objects for p in o]
        lo = np.array([t.lo[p] for p in alive]); hi = np.array([t.hi[p] for p in alive])
        cs = np.sort(changed)
        k = np.searchsorted(cs, lo)
        candidates = int(((k < len(cs)) & (cs[np.minimum(k, len(cs) - 1)] <= hi)).sum())
        order = np.argsort(cp, kind="stable")
        cp, cpos, is_upd, changed = cp[order], cpos[order], is_upd[order], changed[order]
        touched, start = np.unique(cp, return_index=True)
        end = np.append(start[1:], len(cp))
        locate_extra = candidates - len(touched)
        # inserts: on time (after every row written), or late; merging, a late row goes into the
        # piece its event time falls in (the last piece starting at or before it)
        late_of = {}
        late_rows = 0
        stale = []
        if merging:
            ps_live = np.array([p for p in alive if live(t, p) > 0])
            tlo = np.array([t.tlo[p] for p in ps_live]); o_ = np.argsort(tlo, kind="stable")
            ps_live, tlo = ps_live[o_], tlo[o_]
            its = ts[ins]
            newest = max(t.thi[p] for p in ps_live)
            late = its <= newest
            # rows later than the horizon go to a piece of the write's own, apart from on-time rows
            too_late = its < newest - horizon * span_ts
            stale.append(ins[too_late])
            late &= ~too_late
            homes = ps_live[np.maximum(np.searchsorted(tlo, its[late], side="right") - 1, 0)]
            lids = ins[late]
            o_ = np.argsort(homes, kind="stable")
            homes, lids = homes[o_], lids[o_]
            hu, hs = np.unique(homes, return_index=True)
            he = np.append(hs[1:], len(homes))
            late_of = {int(p): lids[x:y] for p, x, y in zip(hu, hs, he)}
            late_rows = int(late.sum())
            fresh = [ins[~late & ~too_late]]
        else:
            fresh = [ins]
        written = read_back = upd_read = 0
        rewritten = dv_pieces = 0
        new_file = []  # place == "file": the write's rewritten pieces
        t.piece_of[dele] = -1
        span = {p: (x, y) for p, x, y in zip(touched.tolist(), start, end)}
        for p in sorted(set(span) | set(late_of)):
            a, b = span.get(p, (0, 0))
            rows = t.rows[p]
            nrows = len(rows)
            pos, upd_here = cpos[a:b], is_upd[a:b]
            if policy == "mor":
                rewrite = False
            elif policy in ("rg", "recut"):
                rewrite = True
            else:
                rewrite = t.ndead[p] + (b - a) >= theta * nrows
            if not rewrite:
                if t.dead[p] is None:
                    t.dead[p] = np.zeros(nrows, bool)
                t.dead[p][pos] = True
                t.ndead[p] += b - a
                t.last[p] = w
                fresh.append(changed[a:b][upd_here])
                strides = np.unique(pos[upd_here] // STRIDE)
                upd_read += int(np.minimum(STRIDE, nrows - strides * STRIDE).sum())
                dv_pieces += 1
                continue
            # the piece's rows after the write, in (ts, id) order, each flagged when the write
            # (or an earlier DV) changed it; a dropped row passes its flag to the next row kept
            flag = np.zeros(nrows, bool); flag[pos] = True
            keep = np.ones(nrows, bool)
            if t.dead[p] is not None:
                flag |= t.dead[p]; keep &= ~t.dead[p]
            keep[pos[~upd_here]] = False
            ids = rows
            late = late_of.get(p)
            if late is not None:
                ids = np.concatenate([rows, late])
                flag = np.concatenate([flag, np.ones(len(late), bool)])
                keep = np.concatenate([keep, np.ones(len(late), bool)])
                o_ = np.lexsort((ids, ts[ids]))
                ids, flag, keep = ids[o_], flag[o_], keep[o_]
            idx = np.nonzero(keep)[0]
            ps = []
            if len(idx):
                nf = flag[idx]
                dropped = np.nonzero(flag & ~keep)[0]
                if len(dropped):
                    nf[np.minimum(np.searchsorted(idx, dropped), len(idx) - 1)] = True
                ids = ids[idx]
                hot = np.zeros(-(-len(ids) // STRIDE), bool)
                hot[np.nonzero(nf)[0] // STRIDE] = True
                runs = [(0, len(hot), True)] if policy == "rg" else cut(hot, m)
                for s0, s1, changed_run in runs:
                    run_ids = ids[s0 * STRIDE:min(s1 * STRIDE, len(ids))]
                    for part in np.array_split(run_ids, -(-len(run_ids) // RG)):
                        ps.append(t.new(part, w, w if changed_run else t.last[p]))
            read_back += nrows
            written += sum(len(t.rows[q]) for q in ps)
            rewritten += 1
            if place == "append":
                t.replace(p, ps)
            else:
                t.objects[t.obj[p]].remove(p)
                new_file.extend(ps)
        new = np.concatenate(fresh)
        new = new[np.lexsort((new, ts[new]))]
        fps = [t.new(new[a:a + RG], w, w) for a in range(0, len(new), RG)] if len(new) else []
        written += len(new)
        if stale and len(stale[0]):
            st = stale[0][np.lexsort((stale[0], ts[stale[0]]))]
            fps.insert(0, t.new(st, w, w))
            written += len(st)
        if place == "append":
            if tail is None or sum(len(t.rows[q]) for q in t.objects[tail]) >= TARGET:
                tail = t.new_object()
            t.place(tail, fps)
        else:
            t.place(t.new_object(), new_file + fps)
        packed_w = packed_r = 0
        if pack_age > 0:
            packed_r, packed_w = pack(t, w, pack_age)
        out.append(dict(written=written * B / 1e6, read_back=read_back * B / 1e6,
                        packed=packed_w * B / 1e6, packed_read=packed_r * B / 1e6,
                        upd_read=upd_read * B / 1e6, rewritten=rewritten, dv_pieces=dv_pieces,
                        late_merged=late_rows,
                        locate_extra=locate_extra, **read_cost(t), **range_cost(t)))
    return out


def pack(t, w, age):
    read = wrote = 0
    for o, lst in enumerate(t.objects):
        i, new_lst = 0, []
        while i < len(lst):
            p = lst[i]
            if not small_cold(t, p, w, age):
                new_lst.append(p); i += 1
                continue
            j, group, rows = i, [], 0
            while j < len(lst) and small_cold(t, lst[j], w, age) and \
                    rows + live(t, lst[j]) <= RG:
                group.append(lst[j]); rows += live(t, lst[j]); j += 1
            if len(group) < 2:
                new_lst.append(p); i += 1
                continue
            ids = np.concatenate([t.rows[q] if t.dead[q] is None else t.rows[q][~t.dead[q]]
                                  for q in group])
            q = t.new(ids, w, max(t.last[x] for x in group))
            t.obj[q] = o
            new_lst.append(q)
            read += sum(len(t.rows[x]) for x in group)
            wrote += len(ids)
            i = j
        t.objects[o] = new_lst
    return read, wrote


def live(t, p):
    return len(t.rows[p]) - t.ndead[p]


def small_cold(t, p, w, age):
    return len(t.rows[p]) < RG // 2 and w - t.born[p] >= age and w - t.last[p] >= age


def range_cost(t, windows=100):
    """Reads of 1% of the live rows by event time: the pieces whose ts range overlaps the window
    (pruned by row-group statistics), with their objects' footers and deletion vectors, and the
    bytes of those pieces against the window's live bytes. Means over `windows` windows."""
    ps = [p for lst in t.objects for p in lst if live(t, p) > 0]
    lv_ids = np.concatenate([t.rows[p] if t.dead[p] is None else t.rows[p][~t.dead[p]] for p in ps])
    edges = np.quantile(t.ts[lv_ids], np.linspace(0, 1, windows + 1))
    tlo = np.array([t.tlo[p] for p in ps]); thi = np.array([t.thi[p] for p in ps])
    phys = np.array([len(t.rows[p]) for p in ps], float)
    obj = np.array([t.obj[p] for p in ps]); dv = np.array([t.ndead[p] > 0 for p in ps])
    req = amp = 0.0
    for a, b in zip(edges[:-1], edges[1:]):
        hit = (tlo <= b) & (thi >= a)
        objs = np.unique(obj[hit])
        dvs = len(np.unique(obj[hit & dv]))
        req += hit.sum() + len(objs) + dvs
        amp += phys[hit].sum() / (len(lv_ids) / windows)
    return dict(range_requests=req / windows, range_read_live=amp / windows)


def read_cost(t):
    objs = pieces = dvs = 0
    phys = lv = 0
    most = 0
    tiny = runs = 0
    for lst in t.objects:
        rd = [p for p in lst if live(t, p) > 0]
        if not rd:
            continue
        objs += 1
        pieces += len(rd)
        most = max(most, len(lst))
        dvs += any(t.ndead[p] for p in rd)
        phys += sum(len(t.rows[p]) for p in rd)
        lv += sum(live(t, p) for p in rd)
        tiny += sum(len(t.rows[p]) < 2 * STRIDE for p in rd)
        # a reader that coalesces adjacent ranges reads a run of pieces one write wrote together
        # (adjacent in the footer and in the object) in one request
        runs += sum(1 for i, p in enumerate(rd) if i == 0 or t.born[rd[i - 1]] != t.born[p]
                    or lst.index(rd[i - 1]) != lst.index(p) - 1)
    return dict(objects=objs, pieces=pieces, dvs=dvs, requests=objs + pieces + dvs,
                coalesced=objs + runs + dvs,
                read_live=phys / lv, most_pieces=most, tiny=tiny, live_gb=lv * B / 1e9)


def summarize(name, out, skip=0):
    o = out[skip:]
    col = lambda k: np.array([r[k] for r in o], float)
    last = out[-1]
    wr = col("written") + col("packed")
    print(f"{name:22s} write MB mean {wr.mean():5.0f} max {wr.max():5.0f}"
          f"  read back {(col('read_back') + col('packed_read')).mean():5.0f}"
          f"  update reads {col('upd_read').mean():4.0f}  packed {col('packed').mean():4.0f}"
          f"  rewritten {col('rewritten').mean():5.1f} dv'd {col('dv_pieces').mean():5.1f}"
          f"  locate extra {col('locate_extra').mean():5.1f}"
          f" | full read: requests {col('requests').mean():5.0f} (end {last['requests']};"
          f" coalesced {col('coalesced').mean():4.0f})"
          f" pieces {last['pieces']} objects {last['objects']} dvs {last['dvs']}"
          f" read/live {col('read_live').mean():.3f} (max {col('read_live').max():.3f})"
          f" most/object {last['most_pieces']} tiny {last['tiny']}"
          f" | 1% by ts: requests {col('range_requests').mean():5.1f}"
          f" read/live {col('range_read_live').mean():5.1f}", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("path")
    p.add_argument("--policy", default="mor,rg,recut:1,recut:4,hybrid:4:0.25")
    p.add_argument("--place", default="append")
    p.add_argument("--pack-age", type=int, default=0)
    p.add_argument("--ingest", default="tail", help="tail, or merge: late rows into their piece")
    p.add_argument("--horizon", type=float, default=float("inf"),
                   help="merging, rows later than this many writes go to a piece of their own")
    a = p.parse_args()
    data = load(a.path)
    for pol in a.policy.split(","):
        name, *args = pol.split(":")
        m = int(args[0]) if args else 1
        theta = float(args[1]) if len(args) > 1 else 0.0
        place = "file" if name == "mor" else a.place
        out = run(a.path, name, m, theta, place, a.pack_age, data, a.ingest, a.horizon)
        summarize(f"{pol}/{place}" + (f"/pack{a.pack_age}" if a.pack_age else "")
                  + (f"/merge<{a.horizon:g}" if a.ingest == "merge" and name in ("rg", "recut")
                     else ""), out)


if __name__ == "__main__":
    sys.exit(main())
