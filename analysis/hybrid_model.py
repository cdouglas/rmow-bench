"""Two workloads modeled apart, each with deletion vectors or deltas combined with merges.

The two claims for region merges are separate, and so are their workloads:

(1) updates: a statement updates rows clustered in a key region, on data laid out by key, with no
    order to keep. An update reads every column of the rows it changes (it writes them whole), so
    copying the rest of their unit (a stride or a row group) into the write costs only the
    write, and spares readers the deletion vectors and the dead rows. `run_updates`.
(2) ingest: inserts only, sorted by event time, some late. Appended, a late row widens its row
    group's event-time range, and range readers interleave row groups; merged by the writer into
    the row group it belongs in, readers read one sorted region. `run_ingest`.

Units, as in layout_model.py: rows of 28.6 bytes, strides of STRIDE rows (a page of every
column), row groups (pieces) of up to RG rows, objects of up to TARGET rows.

Update policies (`--policy`):
  mor            deletion vectors; new versions in the write's new file, by id.
  morc:K         mor, with Iceberg's bin-packing compaction every K writes (files under 0.75 of
                 the target or with 0.3 of their rows dead, live rows packed by id).
  rmor           each row group the write changes, copied (live rows, new versions in place) into
                 the write's new file; a DV covers the old row group.
  rmor-stride    each stride the write changes, copied into the write's new file (a stride's rows
                 of one row group as one row group); DVs cover the old strides whole.
  rmow           each row group the write changes, copied in place (RMoW-append's footer drops it).
  defer:T[:region] deferred merges: DVs, until a row group's dead rows and the write's changes
                 reach T of its rows; then it is copied as rmor does, with its region's new
                 versions folded in from the deltas (the write's new versions, in one row group
                 sorted by id, or with `region` one per row group they left). A delta row group
                 never merges on its own (rmor-density).
  defer-stride:T the same, stride by stride.

Ingest policies:
  tail           inserts appended as row groups sorted by event time, to a tail object until it
                 holds TARGET rows (RMoW-append); readers merge late rows.
  mor            the same, a new object a write (MoR, and RMoR: at the base point a write's
                 inserts are sealed at once, so tail policy C never merges them).
  compact:K      mor, with Iceberg's bin-packing every K writes: objects under 0.75 of the target
                 packed in order into groups of up to TARGET rows, each sorted by event time.
  merge          a late row merged into the row group its event time falls in (rewritten whole).
  trunc          the user's re-cut: that row group is truncated in the footer to its strides
                 before the first late row's place; the rest, with the late rows, is rewritten
                 as a new row group. The prefix keeps its column chunks where they were, which
                 are no longer adjacent: a reader fetches one range per column (COLUMNS).
  defer:K:M[:H]  deferred merges: rows late by at most H writes (default 0) merged at once by M
                 (merge or trunc); later rows go to a delta row group of the write's own, and every
                 K writes the deltas are merged into their row groups by M.

  .venv/bin/python analysis/hybrid_model.py updates <dir with oplog.bin> [--policy ...]
  .venv/bin/python analysis/hybrid_model.py ingest <dir with oplog.bin> [--policy ...]
"""
import argparse
import sys

import numpy as np

from layout_model import B, INITIAL_FILES, RG, STRIDE, TARGET, load

ID_SHARE = 0.059  # the id column's share of a row group's bytes (lab: rg_meta, row groups > 100k)
COLUMNS = 11
WINDOWS = 100


class Table:
    def __init__(self, n, ts):
        self.ts = ts
        self.piece_of = np.full(n, -1, np.int32)
        self.pos_of = np.zeros(n, np.int32)
        self.rows, self.dead, self.ndead, self.obj = [], [], [], []
        self.lo, self.hi, self.tlo, self.thi = [], [], [], []
        self.scattered, self.delta = [], []
        self.objects = {}  # object -> its pieces in footer order
        self.next_obj = 0

    def new(self, ids, delta=False):
        p = len(self.rows)
        self.rows.append(ids); self.dead.append(None); self.ndead.append(0); self.obj.append(-1)
        self.scattered.append(False); self.delta.append(delta)
        self.lo.append(0); self.hi.append(0); self.tlo.append(0); self.thi.append(0)
        self.set_rows(p, ids)
        return p

    def set_rows(self, p, ids):
        self.rows[p] = ids
        self.lo[p], self.hi[p] = int(ids.min()), int(ids.max())
        tv = self.ts[ids]
        self.tlo[p], self.thi[p] = int(tv.min()), int(tv.max())
        self.piece_of[ids] = p
        self.pos_of[ids] = np.arange(len(ids), dtype=np.int32)

    def new_object(self, ps=()):
        o = self.next_obj
        self.next_obj += 1
        self.objects[o] = []
        self.add(o, ps)
        return o

    def add(self, o, ps):
        for p in ps:
            self.obj[p] = o
        self.objects[o].extend(ps)

    def replace(self, p, ps):
        lst = self.objects[self.obj[p]]
        i = lst.index(p)
        for q in ps:
            self.obj[q] = self.obj[p]
        lst[i:i + 1] = ps

    def remove(self, p):
        self.objects[self.obj[p]].remove(p)

    def live(self, p):
        return len(self.rows[p]) - self.ndead[p]

    def live_rows(self, p):
        return self.rows[p] if self.dead[p] is None else self.rows[p][~self.dead[p]]

    def kill(self, p, pos):
        if self.dead[p] is None:
            self.dead[p] = np.zeros(len(self.rows[p]), bool)
        fresh = ~self.dead[p][pos]
        self.dead[p][pos] = True
        self.ndead[p] += int(fresh.sum())

    def drop_empty_objects(self):
        for o in [o for o, lst in self.objects.items() if not any(self.live(p) for p in lst)]:
            del self.objects[o]

    def pieces(self):
        return [p for lst in self.objects.values() for p in lst]


def chunks(ids):
    """A run of rows as row groups of at most RG rows, of even size."""
    return np.array_split(ids, -(-len(ids) // RG)) if len(ids) else []


def initial(t, n0):
    per = -(-n0 // INITIAL_FILES)
    for f in range(INITIAL_FILES):
        ids = np.arange(f * per, min((f + 1) * per, n0))
        t.new_object([t.new(ids[a:a + RG]) for a in range(0, len(ids), RG)])


def strides_of(n):
    return -(-n // STRIDE)


def stride_rows(n, s):
    return np.minimum(STRIDE, n - s * STRIDE)


# ---------------------------------------------------------------------------------------------
# (1) updates

def run_updates(data, policy, arg, by_region=False):
    n0, n, ts, writes = data
    t = Table(n, ts)
    initial(t, n0)
    out = []
    for w, (ins, upd, dele) in enumerate(writes):
        assert len(ins) == 0, "the update workload has no inserts"
        changed = np.concatenate([upd, dele])
        is_upd = np.zeros(len(changed), bool); is_upd[:len(upd)] = True
        cp, cpos = t.piece_of[changed], t.pos_of[changed]
        assert (cp >= 0).all()
        # what finding and reading the changed rows costs, before the write moves anything
        ps = [p for p in t.pieces() if t.live(p) > 0]
        lo = np.array([t.lo[p] for p in ps]); hi = np.array([t.hi[p] for p in ps])
        cs = np.sort(changed)
        k = np.searchsorted(cs, lo)
        cand = (k < len(cs)) & (cs[np.minimum(k, len(cs) - 1)] <= hi)
        cand_rows = int(sum(len(t.rows[p]) for p, c in zip(ps, cand) if c))
        o_ = np.argsort(cp, kind="stable")
        cp, cpos, is_upd, changed = cp[o_], cpos[o_], is_upd[o_], changed[o_]
        touched, start = np.unique(cp, return_index=True)
        end = np.append(start[1:], len(cp))
        t.piece_of[dele] = -1
        fresh = []  # new versions of rows a DV hides, as (row group they left, ids)
        copies = []  # (source piece, ids copied, in place?)
        read_units = 0  # rows of the units copied (a write reads a unit it copies whole)
        upd_strides = 0  # rows of strides holding an updated row, in units not copied
        upd_groups = 0  # rows of row groups holding an updated row, not copied whole
        for p, a, b in zip(touched.tolist(), start, end):
            rows, nr = t.rows[p], len(t.rows[p])
            pos, uh = cpos[a:b], is_upd[a:b]
            name = policy
            if name in ("mor", "morc"):
                copy = np.zeros(strides_of(nr), bool)
            elif name in ("rmor", "rmow"):
                copy = np.ones(strides_of(nr), bool)
            elif name == "rmor-stride":
                copy = np.zeros(strides_of(nr), bool); copy[pos // STRIDE] = True
            elif name == "defer":
                # a delta row group never merges on its own: its rows wait for their region's
                # (rmor-density does the same)
                whole = not t.delta[p] and t.ndead[p] + (b - a) >= arg * nr
                copy = np.full(strides_of(nr), whole)
            elif name == "defer-stride":
                ch = np.bincount(pos // STRIDE, minlength=strides_of(nr))
                dd = np.zeros(strides_of(nr), int) if t.dead[p] is None else \
                    np.add.reduceat(t.dead[p].astype(int), np.arange(0, nr, STRIDE))
                copy = (ch > 0) & (ch + dd >= arg * stride_rows(nr, np.arange(strides_of(nr))))
            else:
                raise ValueError(policy)
            in_copy = copy[pos // STRIDE]
            # rows in units not copied: deletion vectors, new versions to the write's file
            if (~in_copy).any():
                t.kill(p, pos[~in_copy])
                fresh.append((p, changed[a:b][~in_copy & uh]))
                s = np.unique(pos[~in_copy & uh] // STRIDE)
                upd_strides += int(stride_rows(nr, s).sum())
                if (~in_copy & uh).any() and not copy.all():
                    upd_groups += nr
            if copy.any():
                keep = np.ones(nr, bool) if t.dead[p] is None else ~t.dead[p]
                keep[pos[in_copy & ~uh]] = False
                sel = np.repeat(copy, STRIDE)[:nr]
                ids = rows[sel & keep]
                read_units += int(sel.sum())
                if name == "defer":
                    # a deferred merge folds in the region's new versions from earlier deltas
                    folded, rd = fold(t, t.lo[p], t.hi[p])
                    ids = np.sort(np.concatenate([ids, folded]))
                    read_units += rd
                if copy.all() and name == "rmow":
                    copies.append((p, ids, True))
                else:
                    copies.append((p, ids, False))
                    t.kill(p, np.nonzero(sel)[0])
        # the write's new file: copied units (each a row group), then the new versions by id
        new_file = []
        for p, ids, in_place in copies:
            qs = [t.new(c) for c in chunks(ids)]
            if in_place:
                t.replace(p, qs)
            else:
                new_file.extend(qs)
        fr = np.sort(np.concatenate([ids for _, ids in fresh])) if fresh else np.array([], np.int64)
        if by_region and len(fr):  # a delta row group per region: the base row group whose id
            # range holds the row (ranges are disjoint: no inserts), merged this write or not
            bases = [q for q in t.pieces() if not t.delta[q] and t.live(q) > 0]
            bases += [q for q in new_file if not t.delta[q]]
            blo = np.sort(np.array([t.lo[q] for q in bases]))
            region = np.searchsorted(blo, fr, side="right") - 1
            for g in np.unique(region):
                new_file.extend(t.new(c, delta=True) for c in chunks(fr[region == g]))
        else:
            new_file.extend(t.new(c, delta=True) for c in chunks(fr))
        if new_file:
            t.new_object(new_file)
        written = sum(len(ids) for _, ids, _ in copies) + len(fr)
        compacted = 0
        if policy == "morc" and (w + 1) % arg == 0:
            compacted = compact(t)
        t.drop_empty_objects()
        r = dict(written=written * B / 1e6, compacted=compacted * B / 1e6,
                 # the reads an update costs, by engine: blind (this harness: the id column of the
                 # row groups that may hold a target, and the units copied), Iceberg's reader (every
                 # column of those row groups), and a reader that fetches only the pages it needs
                 read_blind=(ID_SHARE * cand_rows + read_units) * B / 1e6,
                 read_iceberg=max(cand_rows, read_units) * B / 1e6,
                 read_pages=(ID_SHARE * cand_rows + read_units + upd_strides) * B / 1e6,
                 # in two passes (updateRead row): the id column of those row groups, then whole
                 # every row group holding an updated row
                 read_rows=(ID_SHARE * cand_rows + read_units + upd_groups) * B / 1e6)
        r.update(full_read(t))
        r.update(window_reads(t, by="id"))
        out.append(r)
    return out


def fold(t, lo, hi):
    """The live rows with ids in [lo, hi] of delta row groups, taken out of them (DVs), and the
    rows of the delta row groups read to find them."""
    out, read = [], 0
    for q in t.pieces():
        if not t.delta[q] or t.live(q) == 0 or t.hi[q] < lo or t.lo[q] > hi:
            continue
        rows = t.rows[q]
        sel = (rows >= lo) & (rows <= hi)
        if t.dead[q] is not None:
            sel &= ~t.dead[q]
        if sel.any():
            out.append(rows[sel])
            t.kill(q, np.nonzero(sel)[0])
            read += len(rows)
    return (np.concatenate(out) if out else np.array([], np.int64)), read


def compact(t):
    """Iceberg's bin-packing: files under 0.75 of the target or with 0.3 of their rows dead,
    their live rows packed by id into files of up to the target."""
    pick = []
    for o, lst in t.objects.items():
        total = sum(len(t.rows[p]) for p in lst); lv = sum(t.live(p) for p in lst)
        if lv and (total < 0.75 * TARGET or total - lv >= 0.3 * total):
            pick.append(o)
    if len(pick) < 2 and not any(
            sum(t.ndead[p] for p in t.objects[o]) for o in pick):
        return 0
    ids = np.sort(np.concatenate([t.live_rows(p) for o in pick for p in t.objects[o]]))
    for o in pick:
        del t.objects[o]
    for a in range(0, len(ids), int(TARGET)):
        t.new_object([t.new(c) for c in chunks(ids[a:a + int(TARGET)])])
    return len(ids)


# ---------------------------------------------------------------------------------------------
# (2) ingest

def run_ingest(data, policy, args, watermark=float("inf")):
    n0, n, ts, writes = data
    t = Table(n, ts)
    initial(t, n0)
    span = len(writes[0][0]) * (ts[1] - ts[0])  # a write's event time
    tail = None
    newest = int(ts[:n0].max())
    every = int(args[0]) if policy in ("defer", "compact") else 0
    mode = args[1] if policy == "defer" else policy
    horizon = float(args[2]) if policy == "defer" and len(args) > 2 else 0.0
    pending_obj = None
    out = []
    for w, (ins, upd, dele) in enumerate(writes):
        assert len(upd) == 0 and len(dele) == 0, "the ingest workload has only inserts"
        ins = ins[np.lexsort((ins, ts[ins]))]
        dropped = ts[ins] < newest - watermark * span  # behind the watermark: not ingested
        ins = ins[~dropped]
        late = ts[ins] <= newest
        written = read_back = merged_units = 0
        now_rows, later = ins[late], np.array([], np.int64)
        if policy == "defer":
            soon = ts[now_rows] >= newest - horizon * span
            now_rows, later = now_rows[soon], now_rows[~soon]
        if policy in ("tail", "mor", "compact"):
            on_time = ins
        else:
            on_time = ins[~late]
            wr, rb, mu = merge_late(t, now_rows, mode)
            written += wr; read_back += rb; merged_units += mu
            if len(later):
                if pending_obj is None:
                    pending_obj = t.new_object()
                t.add(pending_obj, [t.new(c, delta=True) for c in chunks(later)])
                written += len(later)
            if policy == "defer" and (w + 1) % every == 0 and pending_obj is not None:
                ids = np.concatenate([t.rows[p] for p in t.objects[pending_obj]])
                del t.objects[pending_obj]
                pending_obj = None
                ids = ids[np.lexsort((ids, ts[ids]))]
                wr, rb, mu = merge_late(t, ids, mode)
                written += wr; read_back += rb + len(ids); merged_units += mu
        compacted = 0
        if len(on_time):
            if policy in ("mor", "compact"):
                t.new_object([t.new(c) for c in chunks(on_time)])
            else:
                if tail is None or sum(len(t.rows[p]) for p in t.objects[tail]) >= TARGET:
                    tail = t.new_object()
                t.add(tail, [t.new(c) for c in chunks(on_time)])
            written += len(on_time)
        if policy == "compact" and (w + 1) % every == 0:
            compacted = compact_ingest(t)
        newest = max(newest, int(ts[ins].max()))
        r = dict(written=written * B / 1e6, compacted=compacted * B / 1e6,
                 read_back=read_back * B / 1e6,
                 merged_units=merged_units, late=int(late.sum()), dropped=int(dropped.sum()))
        r.update(full_read(t))
        r.update(window_reads(t, by="ts"))
        out.append(r)
    return out


def compact_ingest(t):
    """Iceberg's bin-packing on an insert-only table: objects under 0.75 of the target, packed in
    order into groups of up to TARGET rows, each rewritten sorted by event time. Returns rows
    written."""
    small = [o for o, lst in t.objects.items()
             if sum(len(t.rows[p]) for p in lst) < 0.75 * TARGET]
    groups, cur, n = [], [], 0
    for o in small:
        r = sum(len(t.rows[p]) for p in t.objects[o])
        if cur and n + r > TARGET:
            groups.append(cur); cur, n = [], 0
        cur.append(o); n += r
    if cur:
        groups.append(cur)
    written = 0
    for g in groups:
        if len(g) < 2:
            continue
        ids = np.concatenate([t.rows[p] for o in g for p in t.objects[o]])
        ids = ids[np.lexsort((ids, t.ts[ids]))]
        for o in g:
            del t.objects[o]
        t.new_object([t.new(c) for c in chunks(ids)])
        written += len(ids)
    return written


def merge_late(t, ids, mode):
    """Merges late rows (sorted by event time) into the row groups their event times fall in:
    the last row group, of those not deltas, starting at or before each. Returns rows written,
    rows read back and row groups merged into."""
    if not len(ids):
        return 0, 0, 0
    ps = np.array([p for p in t.pieces() if not t.delta[p]])
    tlo = np.array([t.tlo[p] for p in ps]); o_ = np.argsort(tlo, kind="stable")
    ps, tlo = ps[o_], tlo[o_]
    homes = ps[np.maximum(np.searchsorted(tlo, t.ts[ids], side="right") - 1, 0)]
    o_ = np.argsort(homes, kind="stable")
    homes, ids = homes[o_], ids[o_]
    hu, hs = np.unique(homes, return_index=True)
    he = np.append(hs[1:], len(homes))
    written = read = 0
    for p, a, b in zip(hu.tolist(), hs, he):
        rows, late = t.rows[p], ids[a:b]
        keep = 0
        if mode == "trunc":
            first = int(np.searchsorted(t.ts[rows], t.ts[late].min(), side="left"))
            keep = (first // STRIDE) * STRIDE
        suffix = rows[keep:]
        merged = np.concatenate([suffix, late])
        merged = merged[np.lexsort((merged, t.ts[merged]))]
        qs = [t.new(c) for c in chunks(merged)]
        if keep:
            t.set_rows(p, rows[:keep])
            t.scattered[p] = True
            t.replace(p, [p] + qs)
        else:
            t.replace(p, qs)
        written += len(merged)
        read += len(suffix)
    return written, read, len(hu)


# ---------------------------------------------------------------------------------------------
# reads

def ranges(t, p):
    """Ranged reads of a row group: one, or one per column for a truncated prefix."""
    return COLUMNS if t.scattered[p] else 1


def full_read(t):
    """A full read that skips row groups whose rows are all deleted: objects, row groups (their
    ranges), deletion vectors (one per object holding a deleted row), and bytes against live."""
    objs = reqs = dvs = 0
    phys = lv = page_phys = 0
    pieces = scattered = 0
    for lst in t.objects.values():
        rd = [p for p in lst if t.live(p) > 0]
        if not rd:
            continue
        objs += 1
        pieces += len(rd)
        scattered += sum(t.scattered[p] for p in rd)
        reqs += sum(ranges(t, p) for p in rd)
        dvs += any(t.ndead[p] for p in rd)
        for p in rd:
            nr = len(t.rows[p])
            phys += nr
            lv += t.live(p)
            if t.dead[p] is None:
                page_phys += nr
            else:  # a reader that skips the pages (strides) a DV covers whole
                alive = np.add.reduceat((~t.dead[p]).astype(int), np.arange(0, nr, STRIDE))
                page_phys += int(stride_rows(nr, np.nonzero(alive)[0]).sum())
    open_n, open_rows = sorted_open(t)
    return dict(objects=objs, pieces=pieces, scattered=scattered, dvs=dvs,
                requests=objs + reqs + dvs, read_live=phys / lv, page_read_live=page_phys / lv,
                live_gb=lv * B / 1e9, sorted_open=open_n, sorted_held=open_rows / lv)


def sorted_open(t):
    """A read in event-time order (read_sorted) merges the row groups, each a sorted run, opened
    when the merge reaches its first event time and held until its last: the most row groups open
    at once, and their rows."""
    ev = []
    for p in t.pieces():
        if t.live(p) > 0:
            ev.append((t.tlo[p], 0, len(t.rows[p])))  # opens before a run that ends at the same ts
            ev.append((t.thi[p], 1, -len(t.rows[p])))
    ev.sort()
    n = rows = best_n = best_rows = 0
    for _, kind, r in ev:
        n += 1 if kind == 0 else -1
        rows += r
        if rows > best_rows:
            best_n, best_rows = n, rows
    return best_n, best_rows


def window_reads(t, by):
    """Reads of 1% of the live rows by `by` (id or ts), pruned by row-group statistics: requests
    (objects, row-group ranges, DVs) and bytes against the window's live bytes, as means over
    WINDOWS windows; and the newest window alone."""
    ps = [p for p in t.pieces() if t.live(p) > 0]
    lv = np.concatenate([t.live_rows(p) for p in ps])
    key = lv if by == "id" else t.ts[lv]
    edges = np.quantile(key, np.linspace(0, 1, WINDOWS + 1))
    lo = np.array([t.lo[p] if by == "id" else t.tlo[p] for p in ps])
    hi = np.array([t.hi[p] if by == "id" else t.thi[p] for p in ps])
    phys = np.array([len(t.rows[p]) for p in ps], float)
    rng = np.array([ranges(t, p) for p in ps])
    obj = np.array([t.obj[p] for p in ps]); dv = np.array([t.ndead[p] > 0 for p in ps])
    per = len(lv) / WINDOWS
    reqs, amps = [], []
    for a, b in zip(edges[:-1], edges[1:]):
        hit = (lo <= b) & (hi >= a)
        reqs.append(rng[hit].sum() + len(np.unique(obj[hit])) + len(np.unique(obj[hit & dv])))
        amps.append(phys[hit].sum() / per)
    return dict(win_requests=float(np.mean(reqs)), win_read_live=float(np.mean(amps)),
                newest_requests=float(reqs[-1]), newest_read_live=float(amps[-1]))


# ---------------------------------------------------------------------------------------------

def summarize(name, out, keys):
    col = lambda k: np.array([r[k] for r in out], float)
    last = out[-1]
    parts = [f"{name:20s}"]
    for k in keys:
        if k.startswith("end:"):
            v = last[k[4:]]
            parts.append(f"{k[4:]} {v:.2f}" if v < 10 else f"{k[4:]} {v:.0f}")
        elif k.startswith("max:"):
            parts.append(f"{k[4:]} max {col(k[4:]).max():.0f}")
        else:
            v = col(k).mean()
            parts.append(f"{k} {v:.2f}" if v < 10 else f"{k} {v:.0f}")
    print("  ".join(parts), flush=True)


UPDATE_KEYS = ["written", "max:written", "compacted", "read_blind", "read_rows", "read_iceberg",
               "read_pages",
               "requests", "end:requests", "end:objects", "end:pieces", "read_live",
               "page_read_live", "win_requests", "win_read_live", "newest_requests",
               "newest_read_live", "end:sorted_open", "end:sorted_held"]
INGEST_KEYS = ["written", "max:written", "compacted", "read_back", "merged_units", "requests", "end:requests",
               "end:pieces", "end:scattered", "win_requests", "win_read_live", "newest_requests",
               "newest_read_live", "end:sorted_open", "end:sorted_held"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("workload", choices=["updates", "ingest"])
    p.add_argument("path")
    p.add_argument("--policy")
    p.add_argument("--watermark", type=float, default=float("inf"),
                   help="ingest: rows later than this many writes are dropped")
    a = p.parse_args()
    data = load(a.path)
    if a.workload == "updates":
        for pol in (a.policy or "mor,morc:10,rmor,rmor-stride,rmow,defer:0.25,defer-stride:0.5"
                    ).split(","):
            name, *args = pol.split(":")
            arg = float(args[0]) if args else 0
            out = run_updates(data, name, int(arg) if name == "morc" else arg,
                              by_region="region" in args)
            summarize(pol, out, UPDATE_KEYS)
    else:
        for pol in (a.policy or "tail,merge,trunc,defer:10:trunc").split(","):
            name, *args = pol.split(":")
            summarize(pol, run_ingest(data, name, args, a.watermark), INGEST_KEYS + ["dropped"])


if __name__ == "__main__":
    sys.exit(main())
