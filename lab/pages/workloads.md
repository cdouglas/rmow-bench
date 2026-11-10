---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Updates and inserts: writes against reads'
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
from labbook.db import connect
theme()
fig = figures_module()
tune = fig.tuner()
con = connect()

# Simulated time per phase (analysis/tune.py): the laptop's measured work at its pace, plus havoc's
# requests priced by size from each store's latency test, for every configuration of the windows and
# tune campaigns. The tuner's own set of insert configurations leaves out the lateness twins.
train = tune.records(con, tune.TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
formula = tune.Formula(train)
ns = tune.pace(con)
tune.WORKLOADS["ing_all"] = "regexp_matches(label, '^(win-ing-|tune-ing-)')"
P = pd.concat([tune.policies(con, "upd", formula, ns, True), tune.policies(con, "ing_all", formula, ns, True)])
STORES = ["S3 Express", "Azure Blob"]
S3X, AZ = STORES
P["Store"] = P.store.map({"s3x": S3X, "azure": AZ})

COLOR = {"MoR": "#2a78d6", "RMoR": "#eda100", "RMoW-external": "#008300", "RMoW-append": "#1baf7a",
         "deferred merges": "#b8478f", "appending": "#5f6b75"}
SHAPE = {"MoR": "circle", "RMoR": "triangle-up", "RMoW-external": "square", "RMoW-append": "diamond",
         "deferred merges": "triangle-down", "appending": "circle"}

# (configuration, arm, family, curve it lies on, its knob's value, its name if a main point, order on
# the curve). A curve runs through one family's settings of one knob.
UPD = [
    ("win-upd-base", "mor-v3", "MoR", "MoR", "never", "MoR", 0),
    ("tune-upd-mor-c12", "mor-v3", "MoR", "MoR", "12", None, 1),
    ("win-upd-base-compact", "mor-v3", "MoR", "MoR", "10", "MoR compacted", 2),
    ("tune-upd-mor-c6", "mor-v3", "MoR", "MoR", "6", None, 3),
    ("tune-upd-mor-c3", "mor-v3", "MoR", "MoR", "3", None, 4),
    ("win-upd-base", "rmor-c-kept", "RMoR", "", "", "RMoR", 0),
    ("win-upd-base", "rmor-c-footer-kept", "RMoW-external", "", "", "RMoW-external", 0),
    ("win-upd-base", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
    ("tune-upd-defer-p100", "rmor-density", "deferred merges", "deferred merges", "θ 1", None, 0),
    ("tune-upd-defer-p50", "rmor-density", "deferred merges", "deferred merges", "0.5", None, 1),
    ("win-upd-density-p25", "rmor-density", "deferred merges", "deferred merges", "0.25", None, 2),
    ("tune-upd-defer-p12", "rmor-density", "deferred merges", "deferred merges", "0.12", None, 3),
    ("win-upd-base", "rmor-density", "deferred merges", "deferred merges", "0.06", "deferred merges", 4),
]
ING = [
    ("win-ing-base", "mor-v3", "appending", "MoR", "", "appending", 0),
    ("tune-ing-mor-c12", "mor-v3", "MoR", "MoR", "12", None, 1),
    ("win-ing-base-compact", "mor-v3", "MoR", "MoR", "10", "MoR compacted", 2),
    ("tune-ing-mor-c6", "mor-v3", "MoR", "MoR", "6", None, 3),
    ("tune-ing-mor-c3", "mor-v3", "MoR", "MoR", "3", None, 4),
    ("tune-ing-seal160-f8", "rmor-c", "RMoR", "", "160 ×8", None, 0),
    ("win-ing-seal160", "rmor-c", "RMoR", "", "", "RMoR sealed", 0),
    ("tune-ing-seal256", "rmor-c", "RMoR", "", "256 ×4", None, 0),
    ("tune-ing-seal160-f2", "rmor-c", "RMoR", "", "160 ×2", None, 0),
    ("win-ing-seal160", "rmow-c", "RMoW-append", "", "", "RMoW-append sealed", 0),
]


def frame(spec):
    rows = []
    for label, arm, family, curve, knob, name, order in spec:
        for _, r in P[(P.label == label) & (P.arm == arm)].iterrows():
            rows.append(dict(Store=r.Store, label=label, arm=arm, family=family, curve=curve, knob=knob,
                             name=name or "", main=name is not None, order=order,
                             write_s=(r.write_ms + r.compact_ms) / 1e3, compact_s=r.compact_ms / 1e3,
                             window_s=r.window_ms / 1e3, full_s=r.full_ms / 1e3, sorted_s=r.sorted_ms / 1e3))
    return pd.DataFrame(rows)


U, I = frame(UPD), frame(ING)


def v(d, store, label, arm, col):
    return d[(d.Store == store) & (d.label == label) & (d.arm == arm)][col].iloc[0]


def f1(x):
    return f"{x:.1f}"


def f2(x):
    return f"{x:.2f}"


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


READ = {"window_s": "a range read (1% of the table)", "full_s": "a full scan", "sorted_s": "a sorted read"}


def tradeoff(d, reads, place, width=300, height=220):
    """Seconds a write (compactions spread over the writes) against seconds a read, a panel per kind
    of read and a row per store; each family's settings of one knob joined in order. The designs
    are named in the first panel, each placed by `place` (name: (align, dy))."""
    L = d.melt(id_vars=[c for c in d.columns if c not in READ], value_vars=reads,
               var_name="kind", value_name="read_s")
    L["read"] = L.kind.map(READ)
    L["place"] = [f"{place[n][0]} {place[n][1]}" if n in place and k == reads[0] else ""
                  for n, k in zip(L.name, L.kind)]
    fams = [f for f in COLOR if f in set(L.family) | set(L.curve)]
    legend = alt.Legend(title=None, orient="bottom", direction="horizontal")
    color = alt.Color("family:N", legend=legend, scale=alt.Scale(domain=fams, range=[COLOR[f] for f in fams]))
    lcolor = alt.Color("curve:N", legend=legend, scale=alt.Scale(domain=fams, range=[COLOR[f] for f in fams]))
    shape = alt.Shape("family:N", legend=legend, scale=alt.Scale(domain=fams, range=[SHAPE[f] for f in fams]))
    x = alt.X("write_s:Q", title="seconds a write, with its compactions",
              scale=alt.Scale(domain=[0, 1.18 * L.write_s.max()], nice=False))
    y = alt.Y("read_s:Q", title="seconds a read", scale=alt.Scale(zero=True))
    tip = ["Store", "family", "knob", "name", alt.Tooltip("write_s:Q", format=".2f"),
           alt.Tooltip("compact_s:Q", format=".2f"), "read", alt.Tooltip("read_s:Q", format=".2f")]
    line = alt.Chart().transform_filter(alt.datum.curve != "").mark_line(strokeWidth=1.5, opacity=0.55).encode(
        x=x, y=y, color=lcolor, detail="curve:N", order="order:Q")
    small = alt.Chart().transform_filter(~alt.datum.main).mark_point(size=28, filled=True, opacity=0.8).encode(
        x=x, y=y, color=color, shape=shape, tooltip=tip)
    big = alt.Chart().transform_filter(alt.datum.main).mark_point(
        size=110, filled=True, opacity=1, stroke="white", strokeWidth=0.8).encode(
        x=x, y=y, color=color, shape=shape, tooltip=tip)
    labels = []
    for p in sorted(set(L.place) - {""}):
        align, dy = p.split()
        labels.append(alt.Chart().transform_filter(alt.datum.place == p).mark_text(
            align=align, dx=8 if align == "left" else -8, dy=int(dy), fontSize=11, fontWeight="bold").encode(
            x=x, y=y, text="name:N", color=color))
    return alt.layer(line, small, big, *labels, data=L).properties(width=width, height=height).facet(
        row=alt.Row("Store:N", sort=STORES, title=None),
        column=alt.Column("read:N", sort=[READ[r] for r in reads], title=None)).resolve_scale(y="independent")


def twin(d):
    t = d[["Store", "family", "knob", "name", "label", "arm", "write_s", "compact_s", "window_s", "full_s",
           "sorted_s"]].copy()
    t["_s"] = t.Store.map(STORES.index)
    return t.sort_values(["_s", "family", "order" if "order" in t else "write_s"]).drop(columns="_s").round(3)


# Where the time goes: the laptop's CPU split by kind (phase_breakdown, the phase's own thread) applies
# to each phase's work; the store's part is its client's CPU plus the priced requests.
B = q("""
SELECT r.label, b.arm, b.phase, b.component, sum(b.ms) AS ms
FROM phase_breakdown b JOIN canonical_runs r USING (run_id)
WHERE r.host = 'bearyak' AND (list_contains(r.campaigns, 'windows') OR list_contains(r.campaigns, 'tune'))
  AND b.thread = 'phase' AND b.component LIKE '%CPU'
  AND ((b.phase = 'batch' AND b.step >= 1)
       OR (b.step >= 5 AND ((b.phase IN ('read_full', 'read_window') AND b.reader = 'cached-skip')
                            OR (b.phase = 'read_sorted' AND b.reader = 'cached'))))
GROUP BY ALL""")
CPU_PART = {"decode CPU": "decoding", "encode CPU": "encoding", "footer CPU": "deletion vectors and footers",
            "deletes CPU": "deletion vectors and footers", "other CPU": "rows and planning",
            "store client CPU": "the store"}
B["part"] = B.component.map(CPU_PART)
SH = B.groupby(["label", "arm", "phase", "part"]).ms.sum()
SH = SH / SH.groupby(level=["label", "arm", "phase"]).transform("sum")
PARTS = ["decoding", "encoding", "deletion vectors and footers", "rows and planning", "the store",
         "compactions, spread over the writes"]
PCOL = {**fig.PART_COLORS, "compactions, spread over the writes": fig.PART_COLORS["paused (GC, JIT)"]}
WHAT = {"write": ("batch", "a write"), "window": ("read_window", READ["window_s"]),
        "full": ("read_full", READ["full_s"]), "sorted": ("read_sorted", READ["sorted_s"])}


def parts(spec):
    rows = []
    for label, arm, family, curve, knob, name, order in spec:
        if name is None:
            continue
        for _, r in P[(P.label == label) & (P.arm == arm)].iterrows():
            for kind, (phase, what) in WHAT.items():
                work, total = r[f"{kind}_work_ms"], r[f"{kind}_ms"]
                share = SH.loc[(label, arm, phase)]
                for part, s in share.items():
                    ms = work * s + (total - work if part == "the store" else 0)
                    rows.append(dict(Store=r.Store, name=name, what=what, part=part, s=ms / 1e3))
                if kind == "write" and r.compact_ms > 0:
                    rows.append(dict(Store=r.Store, name=name, what=what, part=PARTS[-1], s=r.compact_ms / 1e3))
    d = pd.DataFrame(rows)
    d["pord"] = d.part.map(PARTS.index)
    return d


def parts_chart(d, whats, names):
    def panel(w):
        return alt.Chart(d[d.what == w]).mark_bar().encode(
            x=alt.X("s:Q", title="seconds", stack="zero"),
            y=alt.Y("name:N", title=None, sort=names),
            color=alt.Color("part:N", title=None, sort=PARTS,
                            scale=alt.Scale(domain=PARTS, range=[PCOL[p] for p in PARTS]),
                            legend=alt.Legend(orient="top", columns=3, labelLimit=260)),
            order=alt.Order("pord:Q"),
            tooltip=["Store", "name", "what", "part", alt.Tooltip("s:Q", format=".2f")],
        ).properties(width=190).facet(row=alt.Row("Store:N", sort=STORES, title=None)).properties(title=w)
    return alt.hconcat(*[panel(w) for w in whats]).resolve_scale(x="independent", y="independent")


UP, IP = parts(UPD), parts(ING)


def share_of(d, store, name, what, part):
    g = d[(d.Store == store) & (d.name == name) & (d.what == what)]
    return g[g.part == part].s.sum() / g.s.sum()


def part_s(d, store, name, what, part):
    return d[(d.Store == store) & (d.name == name) & (d.what == what) & (d.part == part)].s.sum()


def by_part(d, store, name, what):
    return d[(d.Store == store) & (d.name == name) & (d.what == what)].groupby("part").s.sum()


seal_add = (by_part(IP, S3X, "RMoR sealed", "a write") - by_part(IP, S3X, "appending", "a write")).fillna(0)


rb_ = q("""SELECT s.arm, s.phase, avg(s.bytes_read) / 1e6 AS mb
           FROM steps s JOIN canonical_runs r USING (run_id)
           WHERE r.label = 'win-upd-base' AND r.host = 'havoc' AND s.step >= 5
             AND ((s.phase = 'read_sorted' AND s.reader = 'cached') OR (s.phase = 'read_full' AND s.reader = 'cached-skip'))
           GROUP BY ALL""")
sorted_mb = rb_[rb_.phase == "read_sorted"].set_index("arm").mb
full_mb = rb_[rb_.phase == "read_full"].set_index("arm").mb

runs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
            WHERE (list_contains(campaigns, 'windows') OR list_contains(campaigns, 'tune'))
              AND list_contains($labels, label) ORDER BY label, host""",
         labels=sorted({s[0] for s in UPD + ING} | {"win-ing-late99-1", "win-ing-late99-20",
                                                    "win-ing-seal160-late99-1", "win-ing-seal160-late99-20",
                                                    "tune-upd-rg4m", "tune-upd-rg8m", "tune-upd-rg32m"}))
```

What each design costs a writer and its readers on two workloads apart, **updates** and
**inserts**, on S3 Express and Azure Blob. The times are simulated from local runs, at 16 MiB row
groups.

```{code-cell} ipython3
:tags: [remove-cell]

def u(label, arm, col, s=S3X):
    return v(U, s, label, arm, col)


def i_(label, arm, col, s=S3X):
    return v(I, s, label, arm, col)
```

## What it shows

- **Updates: the region designs write in about twice MoR's time and read a range 2 to 4 times as
  fast.** On S3 Express RMoW-append writes in
  {eval}`f"{u('win-upd-base', 'rmow-c', 'write_s') / u('win-upd-base', 'mor-v3', 'write_s'):.1f}"`
  times MoR's time, and reads 1% of the table
  {eval}`f"{u('win-upd-base', 'mor-v3', 'window_s') / u('win-upd-base', 'rmow-c', 'window_s'):.1f}"`
  times as fast as MoR, or
  {eval}`f"{u('win-upd-base-compact', 'mor-v3', 'window_s') / u('win-upd-base', 'rmow-c', 'window_s'):.1f}"`
  times as fast as MoR compacted every 10 writes, which costs MoR no more a write. A full scan
  gains {eval}`f"{1 - u('win-upd-base', 'rmow-c', 'full_s') / u('win-upd-base-compact', 'mor-v3', 'full_s'):.0%}"`.
- **Inserts: the designs do not differ; what matters is whether late rows are merged.**
  Compacting MoR or sealing RMoR's tail at 160 MiB reads a range
  {eval}`f"{i_('win-ing-base', 'mor-v3', 'window_s') / i_('win-ing-base-compact', 'mor-v3', 'window_s'):.1f}"`
  and {eval}`f"{i_('win-ing-base', 'mor-v3', 'window_s') / i_('win-ing-seal160', 'rmor-c', 'window_s'):.1f}"`
  times as fast as appending, for
  {eval}`f"{i_('win-ing-base-compact', 'mor-v3', 'write_s') - i_('win-ing-base', 'mor-v3', 'write_s'):.2f}"`
  and {eval}`f"{i_('win-ing-seal160', 'rmor-c', 'write_s') - i_('win-ing-base', 'mor-v3', 'write_s'):.2f}"`
  s more a write; it pays only once rows arrive more than a write late.
- **Whole-row-group copies get dearer as row groups grow:** RMoW-append's write rises
  {eval}`f"{v(P.assign(write_s=P.write_ms), S3X, 'tune-upd-rg32m', 'rmow-c', 'write_s') / v(P.assign(write_s=P.write_ms), S3X, 'tune-upd-rg4m', 'rmow-c', 'write_s'):.1f}"`
  times from 4 to 32 MiB. Iceberg's default, 128 MiB, and copying parts of row groups are not
  tested here.
- **Where the time goes:** a region design's write is mostly encoding the row groups it copies,
  MoR's mostly finding the rows to delete and the store; reads are mostly the store.

## Setup

**The workloads**, on one table (11 columns, about 1 GB, 25.7 million rows), 30 writes each:
- **updates:** 150,000 changes to existing rows a write, five updates to each delete, clustered
  in regions of 513,700 rows (about one 16 MiB row group each), some regions far more often than
  others;
- **inserts:** 350,000 new rows a write, sorted by event time; most arrive on time, and 99% within
  5 writes of their event time (with twins at 1 and 20 writes).

**The designs:** MoR (deletion vectors, new rows in new files), RMoR (each changed row group
copied into the write's new file, a deletion vector over the old one), RMoW-external (each changed
row group rewritten, footers kept apart), RMoW-append (each changed row group appended to its own
file with a new footer), and RMoR with deferred merges (deletion vectors until a row group's
changed rows reach a share θ of it, then the whole row group copied; the new versions wait in a
delta row group per region). Each copies **whole row groups**: none copies part of one.

**The knobs drawn as curves:** MoR compacted every 12, 10, 6 or 3 writes; the deferred merges at
θ of 1 (never merging), 0.5, 0.25, 0.12 and 0.06; on the inserts, RMoR's tail sealed at 160 MiB
with 2, 4 or 8 units merged at a time, or at 256 MiB.

**The reads**, one at a time:
- **a range read:** 1% of the table, by id on the updates and by event time on the inserts, timed
  as the mean of five at each probe;
- **a full scan**, skipping row groups whose rows are all dead;
- **a sorted read:** the whole table in key order.

Each read's time is its mean over steps 5 to 30, and a write's over the 30 writes.

**How the times are simulated.** Each run's requests and bytes were counted on havoc and its CPU
on the laptop, at {eval}`", ".join(sorted(runs.build.unique()))`. A phase's time is its CPU at the
laptop's speed, plus its requests priced by size from a latency test of the store (S3 Express on
a `c7a.4xlarge` in usw2-az3, Azure Blob on a `D8as_v6` in westus). A cloud run of seven of these
policies came within 3% of this on S3 Express once CPU was priced at the cloud VM's speed, which
is slower than the laptop's; at the laptop's speed, as here, S3 Express times run 4–12% low
({doc}`tune-cloud`).

```{code-cell} ipython3
:tags: [remove-input]

table(runs)
```

## 1. Updates: writing against reading

```{code-cell} ipython3
:label: workloads-updates
:tags: [remove-input]
:class: col-page-right

tradeoff(U, ["window_s", "full_s"], {
    "MoR": ("left", -10), "MoR compacted": ("right", 0), "deferred merges": ("right", 14),
    "RMoW-append": ("right", -10), "RMoW-external": ("right", 14), "RMoR": ("left", -10)})
```

Each design's seconds a write (with its compactions spread over the writes) against seconds a
read, simulated. The large marks are the designs. MoR's line runs from never compacted through
compaction every 12, 10 (the large mark), 6 and 3 writes; the deferred merges' from θ 1 (never
merging) through 0.5, 0.25 and 0.12 to 0.06 (the large mark). The sorted read is left out here
(Limits).

```{code-cell} ipython3
:tags: [remove-input]

table(twin(U).drop(columns="sorted_s"))
```

```{code-cell} ipython3
:tags: [remove-cell]

B_ = "win-upd-base"
w = {s: {a: v(U, s, B_, a, "write_s") for a in ("mor-v3", "rmor-c-kept", "rmor-c-footer-kept", "rmow-c", "rmor-density")}
     for s in STORES}
wc = {s: v(U, s, "win-upd-base-compact", "mor-v3", "write_s") for s in STORES}


def rd(s, label, arm, kind):
    return v(U, s, label, arm, kind)


def repay(s, kind):
    """Reads of a kind per write that repay RMoW-append's extra write against compacted MoR."""
    extra = w[s]["rmow-c"] - wc[s]
    saved = rd(s, "win-upd-base-compact", "mor-v3", kind) - rd(s, B_, "rmow-c", kind)
    return extra / saved if saved > 0 else float("inf")


dz = U[U.family == "deferred merges"]
dz_w = {s: dz[dz.Store == s].write_s for s in STORES}
dz1 = {s: dz[(dz.Store == s) & (dz.knob == "θ 1")].iloc[0] for s in STORES}
```

- **The region designs write in about twice MoR's time.** On S3 Express RMoW-append takes
  {eval}`f1(w[S3X]["rmow-c"])` s a write, RMoW-external {eval}`f1(w[S3X]["rmor-c-footer-kept"])` and
  RMoR {eval}`f1(w[S3X]["rmor-c-kept"])`, against MoR's {eval}`f1(w[S3X]["mor-v3"])` (on Azure,
  {eval}`f1(w[AZ]["rmow-c"])`, {eval}`f1(w[AZ]["rmor-c-footer-kept"])` and
  {eval}`f1(w[AZ]["rmor-c-kept"])` against {eval}`f1(w[AZ]["mor-v3"])`).
- **Compaction makes MoR the baseline to beat.** Compacting every 10 writes costs MoR nothing
  over the run: {eval}`f1(wc[S3X])` s a write with its compactions, against
  {eval}`f1(w[S3X]["mor-v3"])` without, since a compacted table is cheaper to update. It halves
  MoR's range read, from {eval}`f2(rd(S3X, B_, "mor-v3", "window_s"))` s to
  {eval}`f2(rd(S3X, "win-upd-base-compact", "mor-v3", "window_s"))`.
- **Against compacted MoR, RMoW-append reads a range
  {eval}`f1(rd(S3X, "win-upd-base-compact", "mor-v3", "window_s") / rd(S3X, B_, "rmow-c", "window_s"))`
  times as fast and scans
  {eval}`f2(rd(S3X, "win-upd-base-compact", "mor-v3", "full_s") / rd(S3X, B_, "rmow-c", "full_s"))`
  times as fast** on S3 Express
  ({eval}`f1(rd(AZ, "win-upd-base-compact", "mor-v3", "window_s") / rd(AZ, B_, "rmow-c", "window_s"))`
  and {eval}`f2(rd(AZ, "win-upd-base-compact", "mor-v3", "full_s") / rd(AZ, B_, "rmow-c", "full_s"))`
  on Azure). Its extra write is repaid by about {eval}`f"{repay(S3X, 'window_s'):.0f}"` range reads
  or {eval}`f1(repay(S3X, 'full_s'))` full scans a write on S3 Express
  ({eval}`f"{repay(AZ, 'window_s'):.0f}"` or {eval}`f1(repay(AZ, 'full_s'))` on Azure).
- **The deferred merges' threshold barely moves their write**
  ({eval}`span(dz_w[S3X], "{:.1f}")` s from θ 1 to 0.06 on S3 Express). At θ 1 they never merge,
  yet they read a range in {eval}`f2(dz1[S3X].window_s)` s against MoR's
  {eval}`f2(rd(S3X, B_, "mor-v3", "window_s"))`, for {eval}`f1(dz1[S3X].write_s)` s a write: the gain
  comes from the new versions waiting in a delta row group per region, not from merging.

## 2. Updates: where the time goes

```{code-cell} ipython3
:label: workloads-updates-parts
:tags: [remove-input]
:class: col-page-right

UN = [s[5] for s in UPD if s[5]]
parts_chart(UP, ["a write", READ["window_s"], READ["full_s"]], UN)
```

Each design's simulated seconds by part: Parquet's decoding and encoding, deletion vectors and
footers, rows and planning (the rest of the CPU: making and merging rows, planning scans), and the
store (its client's CPU and the priced requests). Compacted MoR's write carries its compactions.

```{code-cell} ipython3
:tags: [remove-input]

table(UP.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
      .reindex(columns=[p for p in PARTS if p in set(UP.part)]).reset_index().round(3))
```

- **A region design's write is mostly encoding:** the row groups it copies are re-encoded, which
  is {eval}`f"{share_of(UP, S3X, 'RMoW-append', 'a write', 'encoding'):.0%}"` of RMoW-append's write
  on S3 Express and {eval}`f"{share_of(UP, S3X, 'RMoR', 'a write', 'encoding'):.0%}"` of RMoR's.
  MoR's write is mostly decoding (finding the rows to delete) and the store
  ({eval}`f"{share_of(UP, S3X, 'MoR', 'a write', 'decoding'):.0%}"` and
  {eval}`f"{share_of(UP, S3X, 'MoR', 'a write', 'the store'):.0%}"`).
- **A read is mostly the store:**
  {eval}`span([share_of(UP, S3X, n, READ['full_s'], 'the store') for n in UN], "{:.0%}")` of a full
  scan on S3 Express and
  {eval}`span([share_of(UP, AZ, n, READ['full_s'], 'the store') for n in UN], "{:.0%}")` on Azure;
  {eval}`span([share_of(UP, S3X, n, READ['window_s'], 'the store') for n in UN], "{:.0%}")` of a range
  read on S3 Express.

## 3. Inserts: when to merge late rows

```{code-cell} ipython3
:tags: [remove-cell]

base = {a: {s: P[(P.label == "win-ing-base") & (P.arm == a) & (P.Store == s)].iloc[0] for s in STORES}
        for a in ("mor-v3", "rmor-c-kept", "rmow-c")}


def spread(col, s=S3X):
    xs = [base[a][s][col] for a in base]
    return max(xs) / min(xs) - 1


iw = {s: v(I, s, "win-ing-base", "mor-v3", "write_s") for s in STORES}
ir = {s: v(I, s, "win-ing-base", "mor-v3", "window_s") for s in STORES}
```

On the inserts the designs do not differ: each appends a write's rows as new row groups, and none
merges a late row into the row group its event time falls in. MoR, RMoR and RMoW-append come within
{eval}`f"{max(spread(c) for c in ('window_ms', 'full_ms', 'sorted_ms')):.0%}"` of one another on every
read on S3 Express, and their writes within {eval}`f"{spread('write_ms'):.0%}"`. What changes the
reads is whether late rows are merged afterwards: by compacting MoR, or by sealing RMoR's or
RMoW-append's tail in larger units.

```{code-cell} ipython3
:label: workloads-inserts
:tags: [remove-input]
:class: col-page-right

tradeoff(I, ["window_s", "sorted_s"], {
    "appending": ("right", 0), "RMoW-append sealed": ("left", 0), "RMoR sealed": ("right", -10),
    "MoR compacted": ("left", 14)})
```

Seconds a write against seconds a read on the inserts (99% within 5 writes late), simulated.
Appending alone (any design); MoR's line runs from appending through compaction every 12, 10 (the
large mark), 6 and 3 writes; RMoR's tail sealed at 160 MiB merging 4 units at a time (the large
mark; small marks: 8, 2, or sealed at 256 MiB); RMoW-append's sealed at 160 MiB. A full scan takes
about the same in every policy (table below).

```{code-cell} ipython3
:tags: [remove-input]

table(twin(I))
```

```{code-cell} ipython3
:tags: [remove-cell]

mc = {s: I[(I.Store == s) & (I.label == "win-ing-base-compact")].iloc[0] for s in STORES}
rs = {s: I[(I.Store == s) & (I.label == "win-ing-seal160") & (I.arm == "rmor-c")].iloc[0] for s in STORES}
ws = {s: I[(I.Store == s) & (I.label == "win-ing-seal160") & (I.arm == "rmow-c")].iloc[0] for s in STORES}
fulls = I[I.Store == S3X].full_s
```

- **Merging late rows buys range reads for a fraction of a second a write.** Appending writes in
  {eval}`f2(iw[S3X])` s and reads a range in {eval}`f2(ir[S3X])` s on S3 Express. Compacting MoR
  every 10 writes costs {eval}`f2(mc[S3X].write_s - iw[S3X])` s more a write and reads a range in
  {eval}`f2(mc[S3X].window_s)` s; RMoR sealed at 160 MiB costs {eval}`f2(rs[S3X].write_s - iw[S3X])` s
  more and reads in {eval}`f2(rs[S3X].window_s)` s. On Azure: {eval}`f2(ir[AZ])` s appending,
  {eval}`f2(mc[AZ].window_s)` compacted, {eval}`f2(rs[AZ].window_s)` sealed.
- **RMoW-append gains little from its seal:** {eval}`f2(ws[S3X].window_s)` s a range read on S3
  Express and {eval}`f2(ws[AZ].window_s)` on Azure, against appending's {eval}`f2(ir[S3X])` and
  {eval}`f2(ir[AZ])`. Its merged unit is one row group, which stretches back to its oldest late row
  as a single write's did.
- **A full scan does not care:** {eval}`span(fulls, "{:.1f}")` s in every policy on S3 Express.

```{code-cell} ipython3
:label: workloads-lateness
:tags: [remove-input]

LATE = {"win-ing-late99-1": 1, "win-ing-base": 5, "win-ing-late99-20": 20,
        "win-ing-seal160-late99-1": 1, "win-ing-seal160": 5, "win-ing-seal160-late99-20": 20,
        "win-ing-base-compact": 5}
POLICY = {("win-ing", "mor-v3"): "appending", ("win-ing-seal160", "rmor-c"): "RMoR sealed",
          ("win-ing-seal160", "rmow-c"): "RMoW-append sealed", ("compact", "mor-v3"): "MoR compacted"}


def late_policy(label, arm):
    if label == "win-ing-base-compact":
        return "MoR compacted"
    key = ("win-ing-seal160" if label.startswith("win-ing-seal160") else "win-ing", arm)
    return POLICY.get(key)


Lt = P[P.label.isin(LATE)].copy()
Lt["policy"] = [late_policy(l, a) for l, a in zip(Lt.label, Lt.arm)]
Lt = Lt[Lt.policy.notna()].assign(lateness=lambda d: d.label.map(LATE), range_read_s=lambda d: d.window_ms / 1e3,
                                  write_s=lambda d: (d.write_ms + d.compact_ms) / 1e3)
LP = ["appending", "MoR compacted", "RMoR sealed", "RMoW-append sealed"]
lc = alt.Color("policy:N", title=None, sort=LP,
               scale=alt.Scale(domain=LP, range=[COLOR["appending"], COLOR["MoR"], COLOR["RMoR"], COLOR["RMoW-append"]]))
ls = alt.Shape("policy:N", title=None, sort=LP,
               scale=alt.Scale(domain=LP, range=[SHAPE["appending"], SHAPE["MoR"], SHAPE["RMoR"], SHAPE["RMoW-append"]]))
lx = alt.X("lateness:Q", title="lateness, p99 (writes)", scale=alt.Scale(type="log", domain=[0.8, 25]),
           axis=alt.Axis(values=[1, 5, 20], format="~g"))
ly = alt.Y("range_read_s:Q", title="seconds a range read", scale=alt.Scale(zero=True))
lb = alt.Chart().encode(x=lx, y=ly, color=lc)
alt.layer(lb.mark_line(strokeWidth=1.5, opacity=0.6),
          lb.mark_point(size=80, filled=True, opacity=1).encode(
              shape=ls, tooltip=["Store", "policy", "lateness", alt.Tooltip("range_read_s:Q", format=".2f"),
                                 alt.Tooltip("write_s:Q", format=".2f")]),
          data=Lt[["Store", "policy", "lateness", "range_read_s", "write_s"]]).properties(
    width=260, height=180).facet(column=alt.Column("Store:N", sort=STORES, title=None))
```

A range read's simulated seconds against how late the inserts arrive (the 99th percentile, in
writes); MoR compacted every 10 writes ran at 5 only.

```{code-cell} ipython3
:tags: [remove-input]

table(Lt[["Store", "policy", "lateness", "write_s", "range_read_s"]].sort_values(["Store", "policy", "lateness"]).round(3))
```

```{code-cell} ipython3
:tags: [remove-cell]

def lt(s, policy, late):
    return Lt[(Lt.Store == s) & (Lt.policy == policy) & (Lt.lateness == late)].range_read_s.iloc[0]
```

- **How late the rows are decides whether merging is worth it.** When 99% arrive within a write,
  appending reads a range in {eval}`f2(lt(S3X, "appending", 1))` s on S3 Express, about what
  compacted MoR reads at 5 writes ({eval}`f2(lt(S3X, "MoR compacted", 5))`). At 5 or 20 writes it
  takes {eval}`f2(lt(S3X, "appending", 5))` and {eval}`f2(lt(S3X, "appending", 20))` s: a few late
  rows widen every row group's event-time range.
- **RMoR's seal holds at every lateness:** {eval}`f2(lt(S3X, "RMoR sealed", 1))`,
  {eval}`f2(lt(S3X, "RMoR sealed", 5))` and {eval}`f2(lt(S3X, "RMoR sealed", 20))` s.

## 4. Inserts: where the time goes

```{code-cell} ipython3
:label: workloads-inserts-parts
:tags: [remove-input]
:class: col-page-right

IN = [s[5] for s in ING if s[5]]
parts_chart(IP, ["a write", READ["window_s"], READ["sorted_s"]], IN)
```

Each merge policy's simulated seconds by part (as in section 2), on the inserts at lateness p99 5.

```{code-cell} ipython3
:tags: [remove-input]

table(IP.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
      .reindex(columns=[p for p in PARTS if p in set(IP.part)]).reset_index().round(3))
```

- **An appended write is mostly CPU:** encoding
  ({eval}`f"{share_of(IP, S3X, 'appending', 'a write', 'encoding'):.0%}"`) and making the rows
  ({eval}`f"{share_of(IP, S3X, 'appending', 'a write', 'rows and planning'):.0%}"`), with the store
  {eval}`f"{share_of(IP, S3X, 'appending', 'a write', 'the store'):.0%}"` on S3 Express. RMoR's seal
  adds {eval}`f2(seal_add.sum())` s a write, spread over the same parts
  ({eval}`", ".join(f"{p} {s:.2f} s" for p, s in seal_add.sort_values(ascending=False).items() if s >= 0.01)`).
- **A range read is mostly the store** in every policy
  ({eval}`span([share_of(IP, S3X, n, READ['window_s'], 'the store') for n in IN], "{:.0%}")` on S3
  Express). Merging cuts the store's time and the decoding alike: compacted MoR's range read takes
  {eval}`f2(part_s(IP, S3X, 'MoR compacted', READ['window_s'], 'the store'))` s at the store and
  {eval}`f2(part_s(IP, S3X, 'MoR compacted', READ['window_s'], 'decoding'))` s decoding, against
  {eval}`f2(part_s(IP, S3X, 'appending', READ['window_s'], 'the store'))` and
  {eval}`f2(part_s(IP, S3X, 'appending', READ['window_s'], 'decoding'))` s appending.

## 5. Row-group size, 4 to 32 MiB (updates)

```{code-cell} ipython3
:tags: [remove-cell]

RG = {"tune-upd-rg4m": 4, "tune-upd-rg8m": 8, "win-upd-base": 16, "tune-upd-rg32m": 32}
R = P[P.label.isin(RG) & P.arm.isin(["rmow-c", "rmor-density"])].copy()
R["design"] = R.arm.map({"rmow-c": "RMoW-append", "rmor-density": "deferred merges"})
R["rg_mib"] = R.label.map(RG)
R = R.assign(**{"a write": R.write_ms / 1e3, READ["window_s"]: R.window_ms / 1e3, READ["full_s"]: R.full_ms / 1e3})
RL = R.melt(id_vars=["Store", "design", "rg_mib"], value_vars=["a write", READ["window_s"], READ["full_s"]],
            var_name="what", value_name="s")


def rg(s, design, mib, what):
    return RL[(RL.Store == s) & (RL.design == design) & (RL.rg_mib == mib) & (RL.what == what)].s.iloc[0]
```

```{code-cell} ipython3
:label: workloads-row-groups
:tags: [remove-input]
:class: col-page-right

RD = ["RMoW-append", "deferred merges"]
rb = alt.Chart().encode(
    x=alt.X("rg_mib:Q", title="row-group size (MiB)", scale=alt.Scale(type="log", base=2, domain=[3.5, 36]),
            axis=alt.Axis(values=[4, 8, 16, 32], format="~g")),
    y=alt.Y("s:Q", title="seconds", scale=alt.Scale(zero=True)),
    color=alt.Color("design:N", title=None, sort=RD, scale=alt.Scale(domain=RD, range=[COLOR[d] for d in RD])))
alt.layer(rb.mark_line(strokeWidth=1.5, opacity=0.6),
          rb.mark_point(size=60, filled=True, opacity=1).encode(
              shape=alt.Shape("design:N", title=None, sort=RD, scale=alt.Scale(domain=RD, range=[SHAPE[d] for d in RD])),
              tooltip=["Store", "design", "rg_mib", "what", alt.Tooltip("s:Q", format=".2f")]),
          data=RL).properties(width=200, height=150).facet(
    row=alt.Row("Store:N", sort=STORES, title=None),
    column=alt.Column("what:N", sort=["a write", READ["window_s"], READ["full_s"]], title=None)
).resolve_scale(y="independent")
```

RMoW-append and the deferred merges (θ 0.06) on the updates at four row-group sizes, simulated:
seconds a write, a range read and a full scan.

```{code-cell} ipython3
:tags: [remove-input]

table(R[["Store", "design", "rg_mib", "a write", READ["window_s"], READ["full_s"]]].sort_values(
    ["Store", "design", "rg_mib"]).round(3))
```

- **Copying whole row groups gets dearer as they grow.** RMoW-append's write takes
  {eval}`f1(rg(S3X, "RMoW-append", 4, "a write"))` s at 4 MiB and
  {eval}`f1(rg(S3X, "RMoW-append", 32, "a write"))` s at 32 MiB on S3 Express, and its range read
  {eval}`f2(rg(S3X, "RMoW-append", 4, READ["window_s"]))` and
  {eval}`f2(rg(S3X, "RMoW-append", 32, READ["window_s"]))` s. The deferred merges, which seldom
  merge, stay at {eval}`span([rg(S3X, "deferred merges", m, "a write") for m in RG.values()], "{:.1f}")` s
  a write.
- **On Azure, full scans get much faster as row groups grow:** RMoW-append's takes
  {eval}`f1(rg(AZ, "RMoW-append", 4, READ["full_s"]))` s at 4 MiB and
  {eval}`f1(rg(AZ, "RMoW-append", 32, READ["full_s"]))` s at 32 MiB, as its requests get fewer and
  larger ({eval}`f1(rg(S3X, "RMoW-append", 4, READ["full_s"]))` and
  {eval}`f1(rg(S3X, "RMoW-append", 32, READ["full_s"]))` s on S3 Express, whose requests cost less).
- **Iceberg's default is 128 MiB, four times the largest size here.** A design that copies whole
  row groups pays for the whole row group whatever share of it a write changes; the deferred merges
  could copy only the part a write changed, but as built they cannot.

## Limits

- **One row-group size for the comparison, 16 MiB, on a narrow table.** The update regions are
  about one 16 MiB row group each, so copying a whole row group copies about the region a write
  changed. At Iceberg's default of 128 MiB a row group holds about six regions; that setting, and
  copying parts of row groups, are not tested (the next experiment).
- **The deferred merges copy whole row groups.** They choose when to copy, from the deletion
  vector's share of the row group, but not what: no part of a row group, no splitting or merging of
  row groups.
- **The sorted read ran only with a reader that reads dead row groups** (`cached`), so on the
  updates it charges RMoR and the deferred merges for the row groups they copied out: RMoR's sorted
  read moves {eval}`f"{sorted_mb['rmor-c-kept']:.0f}"` MB against its full scan's
  {eval}`f"{full_mb['rmor-c-kept']:.0f}"` MB with skipping. It is left out of the updates'
  comparison. On the inserts no row group dies, so it stands there.
- **Simulated**, from one seed and 30 writes, CPU at the laptop's speed; reads one at a time on one
  thread.
- **RMoW-external did not run on the inserts.**
