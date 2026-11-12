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
# requests priced by size from each store's latency test.
train = tune.records(con, tune.TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
formula = tune.Formula(train)
ns = tune.pace(con)
STORES = fig.WL_STORES
S3X, AZ = STORES


def seconds(d):
    """Each write's, compaction's and read's simulated time (s), for the whiskers and the snapshots."""
    return d.assign(Store=d.store.map(fig.WL_STORE), s=d.ms / 1e3)


# The inserts' curves and lateness twins: the windows and tune campaigns' runs, before the locate by
# key, their sorted read through the reader that reads every row group (none dies on the inserts).
# The tuner's own set of insert configurations leaves out the lateness twins.
tune.READS["sorted"] = ("read_sorted", "cached")
tune.WORKLOADS["ing_all"] = "regexp_matches(label, '^(win-ing-|tune-ing-)')"
SIO = seconds(tune.steps(con, "ing_all", formula, ns, True))
PO = tune.policies(con, "ing_all", formula, ns, True, SIO)
WO = fig.wl_whiskers(SIO)
# Everything else: the configurations run in the cloud (campaign workloads-ref) and the update curves
# rerun with their settings (workloads-curves): the locate pruned by each changed key, and every
# read, the sorted one too, through the reader that skips dead row groups.
REF = "(list_contains(campaigns, 'workloads-ref') OR list_contains(campaigns, 'workloads-curves'))"
tune.READS["sorted"] = ("read_sorted", "cached-skip")
tune.WORKLOADS["wl_upd"] = "regexp_matches(label, '^(wl-upd-|wlc-upd-)')"
tune.WORKLOADS["wl_ing"] = "regexp_matches(label, '^wl-ing-')"
SU, SI = (seconds(tune.steps(con, wl, formula, ns, True, REF)) for wl in ("wl_upd", "wl_ing"))
P = pd.concat([tune.policies(con, "wl_upd", formula, ns, True, SU), tune.policies(con, "wl_ing", formula, ns, True, SI),
               PO])
P["Store"] = P.store.map(fig.WL_STORE)
S = pd.concat([SU, SI])

# the designs' colors and shapes (figures.py, shared with the measured page)
COLOR, SHAPE, NCOLOR, READ = fig.WL_COLOR, fig.WL_SHAPE, fig.WL_NCOLOR, fig.WL_READ

# (configuration, arm, family, curve it lies on, its knob's value, its name if a main point, order on
# the curve). A curve runs through one family's settings of one knob.
UPD = [
    ("wl-upd-base", "mor-v3", "MoR", "MoR", "never", "MoR", 0),
    ("wlc-upd-mor-c12", "mor-v3", "MoR", "MoR", "12", None, 1),
    ("wl-upd-compact", "mor-v3", "MoR", "MoR", "10", "MoR compacted", 2),
    ("wlc-upd-mor-c6", "mor-v3", "MoR", "MoR", "6", None, 3),
    ("wlc-upd-mor-c3", "mor-v3", "MoR", "MoR", "3", None, 4),
    ("wl-upd-base", "rmor-c-kept", "RMoR", "", "", "RMoR", 0),
    ("wl-upd-base", "rmor-c-footer-kept", "RMoW-external", "", "", "RMoW-external", 0),
    ("wl-upd-base", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
    ("wlc-upd-defer-p100", "rmor-density", "deferred merges", "deferred merges", "θ 1", None, 0),
    ("wlc-upd-defer-p50", "rmor-density", "deferred merges", "deferred merges", "0.5", None, 1),
    ("wlc-upd-defer-p25", "rmor-density", "deferred merges", "deferred merges", "0.25", None, 2),
    ("wlc-upd-defer-p12", "rmor-density", "deferred merges", "deferred merges", "0.12", None, 3),
    ("wl-upd-base", "rmor-density", "deferred merges", "deferred merges", "0.06", "deferred merges", 4),
]
# the inserts' main points from workloads-ref, their curves' other points from the tune campaign
ING = [
    ("wl-ing-base", "mor-v3", "appending", "MoR", "", "appending", 0),
    ("tune-ing-mor-c12", "mor-v3", "MoR", "MoR", "12", None, 1),
    ("wl-ing-compact", "mor-v3", "MoR", "MoR", "10", "MoR compacted", 2),
    ("tune-ing-mor-c6", "mor-v3", "MoR", "MoR", "6", None, 3),
    ("tune-ing-mor-c3", "mor-v3", "MoR", "MoR", "3", None, 4),
    ("tune-ing-seal160-f8", "rmor-c", "RMoR", "", "160 ×8", None, 0),
    ("wl-ing-seal160", "rmor-c", "RMoR", "", "", "RMoR sealed", 0),
    ("tune-ing-seal256", "rmor-c", "RMoR", "", "256 ×4", None, 0),
    ("tune-ing-seal160-f2", "rmor-c", "RMoR", "", "160 ×2", None, 0),
    ("wl-ing-seal160", "rmow-c", "RMoW-append", "", "", "RMoW-append sealed", 0),
]
# the same main points in the windows campaign, before the locate by key and the sorted read's skip
WIN = {"wl-upd-base": "win-upd-base", "wl-upd-compact": "win-upd-base-compact", "wl-ing-base": "win-ing-base",
       "wl-ing-compact": "win-ing-base-compact", "wl-ing-seal160": "win-ing-seal160"}


W = pd.concat([fig.wl_whiskers(S), WO])


def frame(spec):
    return fig.wl_frame(spec, P, W)


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


snapshot_chart = fig.wl_snapshot_chart


def snapshots(spec, reads):
    """Each design's (main point's) simulated time at every write, compaction and read probe."""
    return fig.wl_snapshots(spec, S, reads)


def snapshot_table(d, what):
    return fig.wl_snapshot_table(d, what, "simulated")


# Where the time goes: the laptop's CPU split by kind (phase_breakdown, the phase's own thread) applies
# to each phase's work; the store's part is its client's CPU plus the priced requests.
B = q("""
SELECT r.label, b.arm, b.phase, b.component, sum(b.ms) AS ms
FROM phase_breakdown b JOIN canonical_runs r USING (run_id)
WHERE r.host = 'bearyak' AND list_contains(r.campaigns, 'workloads-ref')
  AND b.thread = 'phase' AND b.component LIKE '%CPU'
  AND ((b.phase = 'batch' AND b.step >= 1)
       OR (b.step >= 5 AND b.phase IN ('read_full', 'read_window', 'read_sorted') AND b.reader = 'cached-skip'))
GROUP BY ALL""")
CPU_PART = {k: v for k, v in fig.WL_CPU_PART.items() if k.endswith("CPU")}
B["part"] = B.component.map(CPU_PART)
SH = B.groupby(["label", "arm", "phase", "part"]).ms.sum()
SH = SH / SH.groupby(level=["label", "arm", "phase"]).transform("sum")
PARTS = [p for p in fig.WL_PARTS if p != "paused (GC, JIT)"]
PCOL = fig.WL_PCOL
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
    return fig.wl_parts_chart(d, whats, names, PARTS)


UP, IP = parts(UPD), parts(ING)


def share_of(d, store, name, what, part):
    g = d[(d.Store == store) & (d.name == name) & (d.what == what)]
    return g[g.part == part].s.sum() / g.s.sum()


def part_s(d, store, name, what, part):
    return d[(d.Store == store) & (d.name == name) & (d.what == what) & (d.part == part)].s.sum()


def by_part(d, store, name, what):
    return d[(d.Store == store) & (d.name == name) & (d.what == what)].groupby("part").s.sum()


seal_add = (by_part(IP, S3X, "RMoR sealed", "a write") - by_part(IP, S3X, "appending", "a write")).fillna(0)


# how far the locate by key and the sorted read's skip moved each insert main point: its time over
# the windows campaign's (the sorted read's, with skipping over without)
MV = P.merge(P.assign(label=P.label.map({v: k for k, v in WIN.items()})).dropna(subset=["label"]),
             on=["Store", "label", "arm"], suffixes=("", "_win"))
for c in ("window_ms", "full_ms", "sorted_ms"):
    MV[c + "_x"] = MV[c] / MV[c + "_win"]
MV["write_x"] = (MV.write_ms + MV.compact_ms) / (MV.write_ms_win + MV.compact_ms_win)
ing_moved = (MV[["write_x", "window_ms_x", "full_ms_x", "sorted_ms_x"]] - 1).abs().max().max()

LATE_RUNS = ["win-ing-base", "win-ing-base-compact", "win-ing-seal160", "win-ing-late99-1", "win-ing-late99-20",
             "win-ing-seal160-late99-1", "win-ing-seal160-late99-20"]
runs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
            WHERE (list_contains(campaigns, 'windows') OR list_contains(campaigns, 'tune')
                   OR list_contains(campaigns, 'workloads-ref') OR list_contains(campaigns, 'workloads-curves'))
              AND list_contains($labels, label) ORDER BY label, host""",
         labels=sorted({s[0] for s in UPD + ING} | set(LATE_RUNS) | {"wlc-upd-rg4m", "wlc-upd-rg8m", "wlc-upd-rg32m"}))
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


# the region designs' write over MoR's, per store, and their range read's speed-up over MoR's and
# compacted MoR's
REGION = ["rmor-c-kept", "rmor-c-footer-kept", "rmow-c"]
reg_write = {s: [u("wl-upd-base", a, "write_s", s) / u("wl-upd-base", "mor-v3", "write_s", s) for a in REGION]
             for s in STORES}
reg_window = [u("wl-upd-base", "mor-v3", "window_s", s) / u("wl-upd-base", a, "window_s", s)
              for s in STORES for a in REGION]


# each design's latency at every write, compaction and read (the snapshot sections)
SNU = snapshots(UPD, ["window_s", "full_s", "sorted_s"])


def at(d, name, what, step, store=S3X):
    return d[(d.Store == store) & (d.name == name) & (d.what == what) & (d.step == step)].s.iloc[0]


def avg_write(d, name, first, last, store=S3X):
    w = d[(d.Store == store) & (d.name == name) & (d.what == "a write") & d.step.between(first, last)]
    return w.s.mean()


def reads_of(d, name, what, store=S3X):
    return d[(d.Store == store) & (d.name == name) & (d.what == what)].sort_values("step").s


SNI = snapshots(ING, ["window_s", "sorted_s"])
IN = [s[5] for s in ING if s[5]]


def seal_writes(name, store=S3X):
    """A sealed tail's ordinary write (s), its write that merges (s), and those writes' steps."""
    w = SNI[(SNI.Store == store) & (SNI.name == name) & (SNI.what == "a write")]
    slow = w.s > 2 * w.s.median()
    return w[~slow].s.mean(), w[slow].s.mean(), sorted(w[slow].step)


# The designs compared as snapshots are added, with nothing compacted: compaction has sections of
# its own (3 and 9)
UN = [s[5] for s in UPD if s[5]]
UN_M = [n for n in UN if n != "MoR compacted"]
IN_M = [n for n in IN if n != "MoR compacted"]
SNU_M = SNU[SNU.name.isin(UN_M)]
SNI_M = SNI[SNI.name.isin(IN_M)]

# Compaction: MoR at each interval, never to every 3 writes. The inserts' intervals but every 10
# writes are the tune campaign's runs (SIO, priced as the insert curves were)
CU = [("wl-upd-base", "never"), ("wlc-upd-mor-c12", "every 12 writes"), ("wl-upd-compact", "every 10 writes"),
      ("wlc-upd-mor-c6", "every 6 writes"), ("wlc-upd-mor-c3", "every 3 writes")]
CI = [("wl-ing-base", "never"), ("tune-ing-mor-c12", "every 12 writes"), ("wl-ing-compact", "every 10 writes"),
      ("tune-ing-mor-c6", "every 6 writes"), ("tune-ing-mor-c3", "every 3 writes")]
S_ALL = pd.concat([S, SIO])


def compaction(spec, reads, ref):
    """MoR's time at every write, compaction and read probe at each interval of spec, and ref's
    (label, arm, family, name), never compacted."""
    sp = [(l, "mor-v3", "MoR", "", "", n, 0) for l, n in spec] + [(ref[0], ref[1], ref[2], "", "", ref[3], 0)]
    return fig.wl_snapshots(sp, S_ALL, reads)


def stored_of(spec):
    return fig.wl_stored(spec)


def costs(spec, cm, st):
    return fig.wl_compaction_costs(spec, P, cm, st)


def knob_chart(L, field, title, values, domain):
    """The latency of a write, a range read and a full scan (L: Store, design, field, what, s)
    against a knob's setting on a log axis: a row per store, a column per operation."""
    designs = [d for d in COLOR if d in set(L.design)]
    legend = None if len(designs) == 1 else alt.Legend(orient="bottom")
    enc = alt.Chart().encode(
        x=alt.X(f"{field}:Q", title=title, scale=alt.Scale(type="log", domain=domain),
                axis=alt.Axis(values=values, format="~g")),
        y=alt.Y("s:Q", title="latency (s)", scale=alt.Scale(zero=True)),
        color=alt.Color("design:N", title=None, sort=designs, legend=legend,
                        scale=alt.Scale(domain=designs, range=[COLOR[d] for d in designs])))
    return alt.layer(
        enc.mark_line(strokeWidth=2, opacity=0.7),
        enc.mark_point(size=60, filled=True, opacity=1).encode(
            shape=alt.Shape("design:N", title=None, sort=designs, legend=legend,
                            scale=alt.Scale(domain=designs, range=[SHAPE[d] for d in designs])),
            tooltip=["Store", "design", field, "what", alt.Tooltip("s:Q", format=".2f")]),
        data=L).properties(width=200, height=150).facet(
        row=alt.Row("Store:N", sort=STORES, title=None),
        column=alt.Column("what:N", sort=["a write", READ["window_s"], READ["full_s"]], title=None)
    ).resolve_scale(y="independent")


CMU = compaction(CU, ["window_s", "full_s"], ("wl-upd-base", "rmow-c", "RMoW-append", "RMoW-append"))
STU = stored_of(CU)
TU = costs(CU, CMU, STU)


def cu(store, name, col):
    return TU[(TU.Store == store) & (TU.compacted == name)][col].iloc[0]
```

## What it shows

- **Updates: the region designs take
  {eval}`span(reg_write[S3X] + reg_write[AZ], "{:.1f}")` times MoR's time to write, and read a range
  {eval}`span(reg_window, "{:.1f}")` times as fast.** On S3 Express RMoW-append writes in
  {eval}`f"{u('wl-upd-base', 'rmow-c', 'write_s') / u('wl-upd-base', 'mor-v3', 'write_s'):.1f}"`
  times MoR's time, and reads 1% of the table
  {eval}`f"{u('wl-upd-base', 'mor-v3', 'window_s') / u('wl-upd-base', 'rmow-c', 'window_s'):.1f}"`
  times as fast; a full scan gains
  {eval}`f"{1 - u('wl-upd-base', 'rmow-c', 'full_s') / u('wl-upd-base', 'mor-v3', 'full_s'):.0%}"`
  and a sorted read
  {eval}`f"{1 - u('wl-upd-base', 'rmow-c', 'sorted_s') / u('wl-upd-base', 'mor-v3', 'sorted_s'):.0%}"`.
- **As snapshots are added, MoR slows and the region designs do not:** MoR's write and range read
  grow as its deletes pile up (its range read
  {eval}`f"{at(SNU_M, 'MoR', READ['window_s'], 30) / at(SNU_M, 'MoR', READ['window_s'], 5):.1f}"`
  times as long after 30 writes as after 5 on S3 Express), while RMoW-append's stay level.
- **Compaction brings MoR's reads back in line, at the cost of storage:** compacted every 10
  writes, its range read after 30 writes takes
  {eval}`f2(at(CMU, "every 10 writes", READ["window_s"], 30))` s against
  {eval}`f2(at(CMU, "never", READ["window_s"], 30))` never compacted, for no slower writes, but
  while older snapshots are kept its table stores
  {eval}`f2(cu(S3X, "every 10 writes", "stored ÷ live"))` times its live bytes (section 3).
- **Inserts: the designs do not differ; what matters is whether late rows are merged.** Sealing
  RMoR's tail at 160 MiB reads a range
  {eval}`f"{i_('wl-ing-base', 'mor-v3', 'window_s') / i_('wl-ing-seal160', 'rmor-c', 'window_s'):.1f}"`
  times as fast as appending for
  {eval}`f"{i_('wl-ing-seal160', 'rmor-c', 'write_s') - i_('wl-ing-base', 'mor-v3', 'write_s'):.2f}"`
  s more a write, as compacting MoR every 10 writes does; it pays only once rows arrive more than a
  write late.
- **Whole-row-group copies get dearer as row groups grow:** RMoW-append's write rises
  {eval}`f"{v(P.assign(write_s=P.write_ms), S3X, 'wlc-upd-rg32m', 'rmow-c', 'write_s') / v(P.assign(write_s=P.write_ms), S3X, 'wlc-upd-rg4m', 'rmow-c', 'write_s'):.1f}"`
  times from 4 to 32 MiB. Iceberg's default, 128 MiB, and copying parts of row groups are not
  tested here.
- **Where the time goes:** on S3 Express the largest part of a region design's write is encoding
  the row groups it copies, and MoR's is mostly the store and finding the rows to delete; reads are
  mostly the store.

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
delta row group per region). Each copies **whole row groups**: none copies part of one. Each
writer finds the rows a write changes by key, reading only the row groups whose statistics may hold
a changed key ([our changes to readers and writers](./reader-changes.md)).

**The designs are compared with nothing compacted,** as snapshots are added. The knobs each have
a section of their own: MoR compacted every 12, 10, 6 or 3 writes (sections 3 and 9); the deferred
merges at θ of 1 (never merging), 0.5, 0.25, 0.12 and 0.06 (section 4); the row-group size
(section 5); on the inserts, RMoR's tail sealed at 160 MiB with 2, 4 or 8 units merged at a time,
or at 256 MiB (section 9).

**The reads**, one at a time:
- **a range read:** 1% of the table, by id on the updates and by event time on the inserts, timed
  as the mean of five at each probe;
- **a full scan**, skipping row groups whose rows are all dead;
- **a sorted read:** the whole table in key order, skipping them too.

Where a section gives one time, a read's is its mean over the reads after 5 to 30 writes, and a
write's over the 30 writes.

**How the times are simulated.** Each run's requests and bytes were counted on havoc and its CPU
on the laptop, at {eval}`", ".join(sorted(runs.build.unique()))`. A phase's time is its CPU at the
laptop's speed, plus its requests priced by size from a latency test of the store (S3 Express on
a `c7a.4xlarge` in usw2-az3, Azure Blob on a `D8as_v6` in westus). A cloud run of seven of these
policies came within 3% of this on S3 Express once CPU was priced at the cloud VM's speed, which
is slower than the laptop's; at the laptop's speed, as here, S3 Express times run 4–12% low
(the tuner's picks in the cloud, 2026-11-09: docs/LESSONS.md). The main points here ran in the cloud on
2026-11-11; [the measured page](./workloads-cloud.md) sets them beside these times.

```{code-cell} ipython3
:tags: [remove-input]

table(runs)
```

## Updates

### 1. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-updates
:tags: [remove-input]
:class: col-page-right

snapshot_chart(SNU_M, ["window_s", "full_s", "sorted_s"])
```

Each design's simulated write latency at every write, and its read latency after every 5 writes,
on the updates with nothing compacted (section 3 compacts MoR). Each graph has one latency axis for
both stores.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

snapshot_table(SNU_M, "a write")
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNU_M, READ["window_s"])
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNU_M, READ["full_s"])
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNU_M, READ["sorted_s"])
```

```{code-cell} ipython3
:tags: [remove-cell]

B_ = "wl-upd-base"
w = {s: {a: v(U, s, B_, a, "write_s") for a in ("mor-v3", "rmor-c-kept", "rmor-c-footer-kept", "rmow-c", "rmor-density")}
     for s in STORES}


def rd(s, label, arm, kind):
    return v(U, s, label, arm, kind)


def repay(s, kind):
    """Reads of a kind per write that repay RMoW-append's extra write against MoR."""
    extra = w[s]["rmow-c"] - w[s]["mor-v3"]
    saved = rd(s, B_, "mor-v3", kind) - rd(s, B_, "rmow-c", kind)
    return extra / saved if saved > 0 else float("inf")
```

- **The region designs take {eval}`span(reg_write[S3X], "{:.1f}")` times MoR's time to write on S3
  Express and {eval}`span(reg_write[AZ], "{:.1f}")` on Azure**, where the store's slower requests
  weigh more on MoR's write, which is mostly the store's (section 2). On S3 Express RMoW-append takes
  {eval}`f1(w[S3X]["rmow-c"])` s a write on average, RMoW-external {eval}`f1(w[S3X]["rmor-c-footer-kept"])`
  and RMoR {eval}`f1(w[S3X]["rmor-c-kept"])`, against MoR's {eval}`f1(w[S3X]["mor-v3"])` (on Azure,
  {eval}`f1(w[AZ]["rmow-c"])`, {eval}`f1(w[AZ]["rmor-c-footer-kept"])` and
  {eval}`f1(w[AZ]["rmor-c-kept"])` against {eval}`f1(w[AZ]["mor-v3"])`).
- **MoR slows as its deletes pile up, in its writes and its reads.** On S3 Express its write takes
  {eval}`f1(avg_write(SNU_M, "MoR", 1, 10))` s on average over the first 10 writes and
  {eval}`f1(avg_write(SNU_M, "MoR", 21, 30))` over the last 10, and its range read
  {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 5))` s after 5 writes and
  {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 30))` after 30 (on Azure
  {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 5, AZ))` and {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 30, AZ))`).
- **The region designs stay level:** RMoW-append's write takes
  {eval}`f1(avg_write(SNU_M, "RMoW-append", 1, 10))` and {eval}`f1(avg_write(SNU_M, "RMoW-append", 21, 30))` s
  over the first and last 10 writes, and its range read
  {eval}`span(reads_of(SNU_M, "RMoW-append", READ["window_s"]))` s throughout. The deferred merges climb
  as MoR does ({eval}`f1(avg_write(SNU_M, "deferred merges", 1, 10))` to
  {eval}`f1(avg_write(SNU_M, "deferred merges", 21, 30))` s a write, a range read from
  {eval}`f2(at(SNU_M, "deferred merges", READ["window_s"], 5))` to
  {eval}`f2(at(SNU_M, "deferred merges", READ["window_s"], 30))` s), as more of their changes wait in
  deletion vectors and delta row groups.
- **Against MoR, RMoW-append reads a range
  {eval}`f1(rd(S3X, B_, "mor-v3", "window_s") / rd(S3X, B_, "rmow-c", "window_s"))` times as fast and
  scans {eval}`f2(rd(S3X, B_, "mor-v3", "full_s") / rd(S3X, B_, "rmow-c", "full_s"))` times as fast**
  on S3 Express over the 30 writes
  ({eval}`f1(rd(AZ, B_, "mor-v3", "window_s") / rd(AZ, B_, "rmow-c", "window_s"))` and
  {eval}`f2(rd(AZ, B_, "mor-v3", "full_s") / rd(AZ, B_, "rmow-c", "full_s"))` on Azure). Its extra
  write is repaid by about {eval}`f"{repay(S3X, 'window_s'):.0f}"` range reads or
  {eval}`f1(repay(S3X, 'full_s'))` full scans a write on S3 Express
  ({eval}`f"{repay(AZ, 'window_s'):.0f}"` or {eval}`f1(repay(AZ, 'full_s'))` on Azure), fewer as
  MoR's reads slow.

### 2. Where the time goes

```{code-cell} ipython3
:label: workloads-updates-parts
:tags: [remove-input]
:class: col-page-right

parts_chart(UP[UP.name.isin(UN_M)], ["a write", READ["window_s"], READ["full_s"]], UN_M)
```

Each design's simulated seconds by part, on average over the 30 writes and the reads after 5 to
30: Parquet's decoding and encoding, deletion vectors and footers, rows and planning (the rest of
the CPU: making and merging rows, planning scans), and the store (its client's CPU and the priced
requests).

```{code-cell} ipython3
:tags: [remove-input]

UPM = UP[UP.name.isin(UN_M)]
table(UPM.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
      .reindex(columns=[p for p in PARTS if p in set(UPM.part)]).reset_index().round(3))
```

- **On S3 Express the largest part of a region design's write is encoding:** the row groups it
  copies are re-encoded, which is
  {eval}`f"{share_of(UP, S3X, 'RMoW-append', 'a write', 'encoding'):.0%}"` of RMoW-append's write
  and {eval}`f"{share_of(UP, S3X, 'RMoR', 'a write', 'encoding'):.0%}"` of RMoR's, against the
  store's {eval}`f"{share_of(UP, S3X, 'RMoW-append', 'a write', 'the store'):.0%}"` and
  {eval}`f"{share_of(UP, S3X, 'RMoR', 'a write', 'the store'):.0%}"` (on Azure the store's part is
  the larger, {eval}`span([share_of(UP, AZ, n, 'a write', 'the store') for n in ('RMoR', 'RMoW-external', 'RMoW-append')], "{:.0%}")`).
  MoR's write is mostly the store and decoding, which finds the rows to delete
  ({eval}`f"{share_of(UP, S3X, 'MoR', 'a write', 'the store'):.0%}"` and
  {eval}`f"{share_of(UP, S3X, 'MoR', 'a write', 'decoding'):.0%}"`).
- **A read is mostly the store:**
  {eval}`span([share_of(UP, S3X, n, READ['full_s'], 'the store') for n in UN_M], "{:.0%}")` of a full
  scan on S3 Express and
  {eval}`span([share_of(UP, AZ, n, READ['full_s'], 'the store') for n in UN_M], "{:.0%}")` on Azure;
  {eval}`span([share_of(UP, S3X, n, READ['window_s'], 'the store') for n in UN_M], "{:.0%}")` of a range
  read on S3 Express.

### 3. Compaction

Compacting MoR rewrites its table without the rows its deletion vectors drop, so what MoR's reads
lose as snapshots are added comes back after each compaction. What it costs is the compactions'
writes, and storage while older snapshots are kept.

```{code-cell} ipython3
:label: workloads-updates-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMU, ["window_s", "full_s"], STU, "RMoW-append")
```

MoR's simulated read latency after every 5 writes, never compacted or compacted every 10 or 3
writes (a read at a compaction's write reads the compacted table), beside RMoW-append's (dashed,
never compacted); and what MoR's table stores while every snapshot is kept, over its live bytes.
The table below has every 12 and 6 writes too.

```{code-cell} ipython3
:tags: [remove-input]

table(TU, caption="MoR at each compaction interval, simulated latency (s): a write on average without and with "
      "its compactions spread over the writes, a compaction on average, each read on average and after 30 "
      "writes; and what the table stores after 30 writes, every snapshot kept, over its live bytes")
```

```{code-cell} ipython3
:tags: [remove-cell]

rw_ref = reads_of(CMU, "RMoW-append", READ["window_s"])
rmow_stored = q("""SELECT s.arm, s.stored_bytes / s.live_bytes AS stored,
                          (s.stored_bytes - s.reclaimable_bytes) / s.live_bytes AS kept
                   FROM steps s JOIN canonical_runs r USING (run_id)
                   WHERE r.host = 'havoc' AND r.label = 'wl-upd-base' AND s.phase = 'state' AND s.step = 30""").set_index("arm")
```

- **Compaction brings MoR's range read back down.** Never compacted, it grows from
  {eval}`f2(at(CMU, "never", READ["window_s"], 5))` s after 5 writes to
  {eval}`f2(at(CMU, "never", READ["window_s"], 30))` after 30 on S3 Express. Compacted every 10
  writes it stays between {eval}`f2(reads_of(CMU, "every 10 writes", READ["window_s"]).min())` and
  {eval}`f2(reads_of(CMU, "every 10 writes", READ["window_s"]).max())` s, and every 3 writes between
  {eval}`f2(reads_of(CMU, "every 3 writes", READ["window_s"]).min())` and
  {eval}`f2(reads_of(CMU, "every 3 writes", READ["window_s"]).max())`, against RMoW-append's
  {eval}`span(rw_ref)` s with no compaction. A full scan moves less: from
  {eval}`f2(cu(S3X, "never", "full scan"))` s on average never compacted to
  {eval}`f2(cu(S3X, "every 3 writes", "full scan"))` every 3 writes.
- **The writes barely pay for it.** With its compactions spread over the writes, MoR's write takes
  {eval}`f2(cu(S3X, "every 10 writes", "write with compactions"))` s compacted every 10 writes and
  {eval}`f2(cu(S3X, "every 3 writes", "write with compactions"))` every 3, against
  {eval}`f2(cu(S3X, "never", "write"))` never compacted, on S3 Express: a compacted table is
  cheaper to update, which pays for most of the compactions
  ({eval}`span([cu(S3X, n, "a compaction") for n in fig.WL_INTERVALS[1:]])` s each on average).
- **Storage pays, while older snapshots are kept.** After 30 writes MoR's table stores
  {eval}`f2(cu(S3X, "every 10 writes", "stored ÷ live"))` times its live bytes compacted every 10
  writes and {eval}`f2(cu(S3X, "every 3 writes", "stored ÷ live"))` every 3, against
  {eval}`f2(cu(S3X, "never", "stored ÷ live"))` never compacted: each compaction writes the table
  again. Once older snapshots expire, every interval keeps only its live bytes. Never compacted,
  RMoW-append stores {eval}`f1(rmow_stored.loc["rmow-c", "stored"])` times its live bytes and keeps
  all of it after expiry ({eval}`f1(rmow_stored.loc["rmow-c", "kept"])` times), since its old row
  groups stay in the objects its current snapshot reads; RMoW-external and RMoR give theirs back
  on expiry ({eval}`f2(rmow_stored.loc["rmor-c-footer-kept", "kept"])` and
  {eval}`f2(rmow_stored.loc["rmor-c-kept", "kept"])` times).

### 4. The deferred merges' threshold

```{code-cell} ipython3
:tags: [remove-cell]

THETA = {"θ 1": 1.0, "0.5": 0.5, "0.25": 0.25, "0.12": 0.12, "0.06": 0.06}
TH = U[U.family == "deferred merges"].assign(theta=lambda d: d.knob.map(THETA), design="deferred merges")
THL = TH.assign(**{"a write": TH.write_s, READ["window_s"]: TH.window_s, READ["full_s"]: TH.full_s}).melt(
    id_vars=["Store", "design", "theta"], value_vars=["a write", READ["window_s"], READ["full_s"]],
    var_name="what", value_name="s")


def th(s, theta, what):
    return THL[(THL.Store == s) & (THL.theta == theta) & (THL.what == what)].s.iloc[0]
```

```{code-cell} ipython3
:label: workloads-theta
:tags: [remove-input]
:class: col-page-right

knob_chart(THL, "theta", "θ", [0.06, 0.12, 0.25, 0.5, 1], [0.05, 1.2])
```

The deferred merges on the updates at five thresholds θ, the share of a row group's rows a write's
changes must reach before the row group is merged, simulated: the latency of a write, a range read
and a full scan, on average over the 30 writes and the reads after 5 to 30. At θ 1 a row group is
never merged; θ 0.06 is the deferred merges of the sections above.

```{code-cell} ipython3
:tags: [remove-input]

table(TH[["Store", "theta", "write_s", "window_s", "full_s", "sorted_s"]].sort_values(
    ["Store", "theta"]).round(3), caption="The deferred merges at each θ, simulated latency (s) on average")
```

- **The threshold moves their write little:**
  {eval}`span([th(S3X, t, "a write") for t in THETA.values()], "{:.1f}")` s from θ 1 to 0.06 on S3
  Express.
- **They read a range faster than MoR even when they never merge:** at θ 1 in
  {eval}`f2(th(S3X, 1.0, READ["window_s"]))` s against MoR's
  {eval}`f2(rd(S3X, B_, "mor-v3", "window_s"))`, for {eval}`f1(th(S3X, 1.0, "a write"))` s a write.
  The gain comes from the new versions waiting in a delta row group per region, not from merging.

### 5. Row-group size, 4 to 32 MiB

```{code-cell} ipython3
:tags: [remove-cell]

RG = {"wlc-upd-rg4m": 4, "wlc-upd-rg8m": 8, "wl-upd-base": 16, "wlc-upd-rg32m": 32}
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

knob_chart(RL, "rg_mib", "row-group size (MiB)", [4, 8, 16, 32], [3.5, 36])
```

RMoW-append and the deferred merges (θ 0.06) on the updates at four row-group sizes, simulated:
the latency of a write, a range read and a full scan, on average.

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
  merge, rise less: {eval}`f1(rg(S3X, "deferred merges", 4, "a write"))` s a write at 4 MiB and
  {eval}`f1(rg(S3X, "deferred merges", 32, "a write"))` s at 32 MiB.
- **On Azure, full scans get much faster as row groups grow:** RMoW-append's takes
  {eval}`f1(rg(AZ, "RMoW-append", 4, READ["full_s"]))` s at 4 MiB and
  {eval}`f1(rg(AZ, "RMoW-append", 32, READ["full_s"]))` s at 32 MiB, as its requests get fewer and
  larger ({eval}`f1(rg(S3X, "RMoW-append", 4, READ["full_s"]))` and
  {eval}`f1(rg(S3X, "RMoW-append", 32, READ["full_s"]))` s on S3 Express, whose requests cost less).
- **Iceberg's default is 128 MiB, four times the largest size here.** A design that copies whole
  row groups pays for the whole row group whatever share of it a write changes; the deferred merges
  could copy only the part a write changed, but as built they cannot.

## Inserts

```{code-cell} ipython3
:tags: [remove-cell]

base = {a: {s: P[(P.label == "wl-ing-base") & (P.arm == a) & (P.Store == s)].iloc[0] for s in STORES}
        for a in ("mor-v3", "rmor-c-kept", "rmow-c")}


def spread(col, s=S3X):
    xs = [base[a][s][col] for a in base]
    return max(xs) / min(xs) - 1


iw = {s: v(I, s, "wl-ing-base", "mor-v3", "write_s") for s in STORES}
ir = {s: v(I, s, "wl-ing-base", "mor-v3", "window_s") for s in STORES}
rs = {s: I[(I.Store == s) & (I.label == "wl-ing-seal160") & (I.arm == "rmor-c")].iloc[0] for s in STORES}
ws = {s: I[(I.Store == s) & (I.label == "wl-ing-seal160") & (I.arm == "rmow-c")].iloc[0] for s in STORES}
fulls = I[(I.Store == S3X) & I.main & (I.name != "MoR compacted")].full_s
```

On the inserts the designs do not differ: each appends a write's rows as new row groups, and none
merges a late row into the row group its event time falls in. MoR, RMoR and RMoW-append come within
{eval}`f"{max(spread(c) for c in ('window_ms', 'full_ms', 'sorted_ms')):.0%}"` of one another on every
read on S3 Express, and their writes within {eval}`f"{spread('write_ms'):.0%}"`; "appending" below
is any of them. What changes the reads is whether late rows are merged afterwards: by sealing
RMoR's or RMoW-append's tail in larger units, or by compacting MoR (section 9).

### 6. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-inserts
:tags: [remove-input]
:class: col-page-right

snapshot_chart(SNI_M, ["window_s", "sorted_s"])
```

Each policy's simulated write latency at every write, and its read latency after every 5 writes,
on the inserts (99% within 5 writes late) with nothing compacted: appending, and RMoR's and
RMoW-append's tails sealed at 160 MiB.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

snapshot_table(SNI_M, "a write")
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNI_M, READ["window_s"])
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNI_M, READ["sorted_s"])
```

- **Unmerged, a range read slows with every write:** appending's takes
  {eval}`f2(at(SNI_M, "appending", READ["window_s"], 5))` s after 5 writes and
  {eval}`f2(at(SNI_M, "appending", READ["window_s"], 30))` after 30 on S3 Express, as late rows widen
  more row groups' event-time ranges. With RMoR's seal it goes from
  {eval}`f2(at(SNI_M, "RMoR sealed", READ["window_s"], 5))` to
  {eval}`f2(at(SNI_M, "RMoR sealed", READ["window_s"], 30))` s.
- **Sealing buys range reads for a fraction of a second a write.** Appending writes in
  {eval}`f2(iw[S3X])` s on average and reads a range in {eval}`f2(ir[S3X])` s on S3 Express; RMoR
  sealed at 160 MiB costs {eval}`f2(rs[S3X].write_s - iw[S3X])` s more a write and reads in
  {eval}`f2(rs[S3X].window_s)` s (on Azure {eval}`f2(ir[AZ])` s appending,
  {eval}`f2(rs[AZ].window_s)` sealed).
- **A seal makes one write in four slow:** RMoR's sealed tail writes in
  {eval}`f2(seal_writes("RMoR sealed")[0])` s, and in {eval}`f2(seal_writes("RMoR sealed")[1])` s at
  writes {eval}`", ".join(str(k) for k in seal_writes("RMoR sealed")[2])`, when four units merge.
- **RMoW-append gains little from its seal:** {eval}`f2(ws[S3X].window_s)` s a range read on S3
  Express and {eval}`f2(ws[AZ].window_s)` on Azure, against appending's {eval}`f2(ir[S3X])` and
  {eval}`f2(ir[AZ])`. Its merged unit is one row group, which stretches back to its oldest late row
  as a single write's did.
- **The sorted read slows in every policy**, from
  {eval}`span([at(SNI_M, n, READ["sorted_s"], 5) for n in IN_M], "{:.1f}")` s after 5 writes to
  {eval}`span([at(SNI_M, n, READ["sorted_s"], 30) for n in IN_M], "{:.1f}")` s after 30 on S3 Express,
  and **a full scan does not care:** {eval}`span(fulls, "{:.1f}")` s in every policy.

### 7. How late the rows arrive

```{code-cell} ipython3
:label: workloads-lateness
:tags: [remove-input]

LATE = {"win-ing-late99-1": 1, "win-ing-base": 5, "win-ing-late99-20": 20,
        "win-ing-seal160-late99-1": 1, "win-ing-seal160": 5, "win-ing-seal160-late99-20": 20}
POLICY = {("win-ing", "mor-v3"): "appending", ("win-ing-seal160", "rmor-c"): "RMoR sealed",
          ("win-ing-seal160", "rmow-c"): "RMoW-append sealed"}


def late_policy(label, arm):
    key = ("win-ing-seal160" if label.startswith("win-ing-seal160") else "win-ing", arm)
    return POLICY.get(key)


Lt = P[P.label.isin(LATE)].copy()
Lt["policy"] = [late_policy(l, a) for l, a in zip(Lt.label, Lt.arm)]
Lt = Lt[Lt.policy.notna()].assign(lateness=lambda d: d.label.map(LATE), range_read_s=lambda d: d.window_ms / 1e3,
                                  write_s=lambda d: (d.write_ms + d.compact_ms) / 1e3)
LP = ["appending", "RMoR sealed", "RMoW-append sealed"]
lc = alt.Color("policy:N", title=None, sort=LP,
               scale=alt.Scale(domain=LP, range=[COLOR["appending"], COLOR["RMoR"], COLOR["RMoW-append"]]))
ls = alt.Shape("policy:N", title=None, sort=LP,
               scale=alt.Scale(domain=LP, range=[SHAPE["appending"], SHAPE["RMoR"], SHAPE["RMoW-append"]]))
lx = alt.X("lateness:Q", title="lateness, p99 (writes)", scale=alt.Scale(type="log", domain=[0.8, 25]),
           axis=alt.Axis(values=[1, 5, 20], format="~g"))
ly = alt.Y("range_read_s:Q", title="range read latency (s)", scale=alt.Scale(zero=True))
lb = alt.Chart().encode(x=lx, y=ly, color=lc)
alt.layer(lb.mark_line(strokeWidth=1.5, opacity=0.6),
          lb.mark_point(size=80, filled=True, opacity=1).encode(
              shape=ls, tooltip=["Store", "policy", "lateness", alt.Tooltip("range_read_s:Q", format=".2f"),
                                 alt.Tooltip("write_s:Q", format=".2f")]),
          data=Lt[["Store", "policy", "lateness", "range_read_s", "write_s"]]).properties(
    width=260, height=180).facet(column=alt.Column("Store:N", sort=STORES, title=None))
```

A range read's simulated seconds, on average over the reads after 5 to 30 writes, against how late
the inserts arrive (the 99th percentile, in writes).

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
  appending reads a range in {eval}`f2(lt(S3X, "appending", 1))` s on S3 Express, about what RMoR
  sealed reads at 5 writes ({eval}`f2(lt(S3X, "RMoR sealed", 5))`). At 5 or 20 writes it takes
  {eval}`f2(lt(S3X, "appending", 5))` and {eval}`f2(lt(S3X, "appending", 20))` s: a few late rows
  widen every row group's event-time range.
- **RMoR's seal holds at every lateness:** {eval}`f2(lt(S3X, "RMoR sealed", 1))`,
  {eval}`f2(lt(S3X, "RMoR sealed", 5))` and {eval}`f2(lt(S3X, "RMoR sealed", 20))` s.

### 8. Where the time goes

```{code-cell} ipython3
:label: workloads-inserts-parts
:tags: [remove-input]
:class: col-page-right

parts_chart(IP[IP.name.isin(IN_M)], ["a write", READ["window_s"], READ["sorted_s"]], IN_M)
```

Each policy's simulated seconds by part (as in section 2), on the inserts at lateness p99 5.

```{code-cell} ipython3
:tags: [remove-input]

IPM = IP[IP.name.isin(IN_M)]
table(IPM.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
      .reindex(columns=[p for p in PARTS if p in set(IPM.part)]).reset_index().round(3))
```

- **An appended write is mostly CPU:** encoding
  ({eval}`f"{share_of(IP, S3X, 'appending', 'a write', 'encoding'):.0%}"`) and making the rows
  ({eval}`f"{share_of(IP, S3X, 'appending', 'a write', 'rows and planning'):.0%}"`), with the store
  {eval}`f"{share_of(IP, S3X, 'appending', 'a write', 'the store'):.0%}"` on S3 Express. RMoR's seal
  adds {eval}`f2(seal_add.sum())` s a write, spread over the same parts
  ({eval}`", ".join(f"{p} {s:.2f} s" for p, s in seal_add.sort_values(ascending=False).items() if s >= 0.01)`).
- **A range read is mostly the store** in every policy
  ({eval}`span([share_of(IP, S3X, n, READ['window_s'], 'the store') for n in IN_M], "{:.0%}")` on S3
  Express). Merging cuts the store's time and the decoding alike: RMoR sealed's range read takes
  {eval}`f2(part_s(IP, S3X, 'RMoR sealed', READ['window_s'], 'the store'))` s at the store and
  {eval}`f2(part_s(IP, S3X, 'RMoR sealed', READ['window_s'], 'decoding'))` s decoding, against
  {eval}`f2(part_s(IP, S3X, 'appending', READ['window_s'], 'the store'))` and
  {eval}`f2(part_s(IP, S3X, 'appending', READ['window_s'], 'decoding'))` s appending.

### 9. Compaction, and the seal's size

```{code-cell} ipython3
:tags: [remove-cell]

CMI = compaction(CI, ["window_s"], ("wl-ing-seal160", "rmor-c", "RMoR", "RMoR sealed"))
STI = stored_of(CI)
TI = costs(CI, CMI, STI)
```

Compacting MoR merges late rows after the fact, every N writes, as RMoR's seal does as they arrive.

```{code-cell} ipython3
:label: workloads-inserts-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMI, ["window_s"], STI, "RMoR sealed")
```

MoR's simulated range-read latency after every 5 writes on the inserts, appending (never compacted)
or compacted every 10 or 3 writes, beside RMoR's tail sealed at 160 MiB (dashed); and what MoR's
table stores while every snapshot is kept, over its live bytes. The table below has every 12 and 6
writes too.

```{code-cell} ipython3
:tags: [remove-input]

table(TI, caption="MoR at each compaction interval on the inserts, as in section 3")
```

```{code-cell} ipython3
:tags: [remove-cell]

def ci(store, name, col):
    return TI[(TI.Store == store) & (TI.compacted == name)][col].iloc[0]


SEAL = {"tune-ing-seal160-f2": "160 MiB, 2 units", "wl-ing-seal160": "160 MiB, 4 units",
        "tune-ing-seal160-f8": "160 MiB, 8 units", "tune-ing-seal256": "256 MiB, 4 units"}
SL = I[I.label.isin(SEAL) & (I.arm == "rmor-c")].assign(seal=lambda d: d.label.map(SEAL))
SL = SL[["Store", "seal", "write_s", "window_s", "full_s", "sorted_s"]].sort_values(
    ["Store", "seal"], key=lambda c: c.map(list(SEAL.values()).index) if c.name == "seal" else c)


def sl(store, seal, col):
    return SL[(SL.Store == store) & (SL.seal == seal)][col].iloc[0]
```

- **Compaction brings appending's range read back down, as the seal does.** Compacted every 10
  writes, MoR's range read drops back after each compaction, to
  {eval}`", ".join(f2(at(CMI, "every 10 writes", READ["window_s"], k)) for k in (10, 20, 30))` s
  after writes 10, 20 and 30 on S3 Express, against
  {eval}`f2(at(CMI, "never", READ["window_s"], 30))` s appending after 30 writes and
  {eval}`span(reads_of(CMI, "RMoR sealed", READ["window_s"]))` s with RMoR's seal. Its compactions
  take {eval}`span(reads_of(CMI, "every 10 writes", "a compaction"), "{:.1f}")` s each, or
  {eval}`f2(ci(S3X, "every 10 writes", "write with compactions") - ci(S3X, "never", "write"))` s a
  write spread over the writes, against {eval}`f2(rs[S3X].write_s - iw[S3X])` s for the seal.
- **Storage pays while older snapshots are kept:** after 30 writes the compacted table stores
  {eval}`f2(ci(S3X, "every 10 writes", "stored ÷ live"))` times its live bytes compacted every 10
  writes and {eval}`f2(ci(S3X, "every 3 writes", "stored ÷ live"))` every 3, against
  {eval}`f2(ci(S3X, "never", "stored ÷ live"))` appending.
- **A larger seal reads faster for a slower write.** RMoR's tail sealed at 256 MiB reads a range in
  {eval}`f2(sl(S3X, "256 MiB, 4 units", "window_s"))` s for
  {eval}`f2(sl(S3X, "256 MiB, 4 units", "write_s"))` s a write on S3 Express, against
  {eval}`f2(sl(S3X, "160 MiB, 4 units", "window_s"))` and
  {eval}`f2(sl(S3X, "160 MiB, 4 units", "write_s"))` at 160 MiB; merging 2 units at a time instead
  of 4 reads in {eval}`f2(sl(S3X, "160 MiB, 2 units", "window_s"))` s, 8 in
  {eval}`f2(sl(S3X, "160 MiB, 8 units", "window_s"))` (table below).

```{code-cell} ipython3
:tags: [remove-input]

table(SL.round(3), caption="RMoR's seal at each size and fanout, simulated latency (s) on average")
```

## Limits

- **One row-group size for the comparison, 16 MiB, on a narrow table.** The update regions are
  about one 16 MiB row group each, so copying a whole row group copies about the region a write
  changed. At Iceberg's default of 128 MiB a row group holds about six regions; that setting, and
  copying parts of row groups, are not tested (the next experiment).
- **The deferred merges copy whole row groups.** They choose when to copy, from the deletion
  vector's share of the row group, but not what: no part of a row group, no splitting or merging of
  row groups.
- **The inserts' compaction intervals (but every 10 writes), seal settings and lateness twins are
  earlier runs**, from the tune and windows campaigns,
  before the locate by key and the sorted read's skip. On the inserts' main points those changes
  moved no time by more than {eval}`f"{ing_moved:.1%}"`.
- **Simulated**, from one seed and 30 writes, CPU at the laptop's speed; reads one at a time on one
  thread.
- **RMoW-external did not run on the inserts.**
