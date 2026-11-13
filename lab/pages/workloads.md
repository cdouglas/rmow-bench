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
COLOR, SHAPE, NCOLOR, READ, WRITE = fig.WL_COLOR, fig.WL_SHAPE, fig.WL_NCOLOR, fig.WL_READ, fig.WL_WRITE

# (configuration, arm, family, curve it lies on, its knob's value, its name if a main point, order on
# the curve). A curve runs through one family's settings of one knob.
UPD = [
    ("wl-upd-base", "mor-v3", "MoR", "MoR", "never", "MoR", 0),
    ("wlc-upd-mor-c12", "mor-v3", "MoR", "MoR", "12", None, 1),
    ("wl-upd-compact", "mor-v3", "MoR", "MoR", "10", "MoR (compacted)", 2),
    ("wlc-upd-mor-c6", "mor-v3", "MoR", "MoR", "6", None, 3),
    ("wlc-upd-mor-c3", "mor-v3", "MoR", "MoR", "3", None, 4),
    ("wl-upd-base", "rmor-c-kept", "RMoR", "", "", "RMoR", 0),
    ("wl-upd-base", "rmor-c-footer-kept", "RMoW-external", "", "", "RMoW-external", 0),
    ("wl-upd-base", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
    ("wlc-upd-defer-p100", "rmor-density", "RMoR-density", "RMoR-density", "θ 1", None, 0),
    ("wlc-upd-defer-p50", "rmor-density", "RMoR-density", "RMoR-density", "0.5", None, 1),
    ("wlc-upd-defer-p25", "rmor-density", "RMoR-density", "RMoR-density", "0.25", None, 2),
    ("wlc-upd-defer-p12", "rmor-density", "RMoR-density", "RMoR-density", "0.12", None, 3),
    ("wl-upd-base", "rmor-density", "RMoR-density", "RMoR-density", "0.06", "RMoR-density", 4),
]
# the inserts' main points from workloads-ref, their curves' other points from the tune campaign
ING = [
    ("wl-ing-base", "mor-v3", "MoR", "MoR", "", "MoR", 0),
    ("tune-ing-mor-c12", "mor-v3", "MoR", "MoR", "12", None, 1),
    ("wl-ing-compact", "mor-v3", "MoR", "MoR", "10", "MoR (compacted)", 2),
    ("tune-ing-mor-c6", "mor-v3", "MoR", "MoR", "6", None, 3),
    ("tune-ing-mor-c3", "mor-v3", "MoR", "MoR", "3", None, 4),
    ("tune-ing-seal160-f8", "rmor-c", "RMoR", "", "160 ×8", None, 0),
    ("wl-ing-seal160", "rmor-c", "RMoR", "", "", "RMoR", 0),
    ("tune-ing-seal256", "rmor-c", "RMoR", "", "256 ×4", None, 0),
    ("tune-ing-seal160-f2", "rmor-c", "RMoR", "", "160 ×2", None, 0),
    ("wl-ing-seal160", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
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
# to each phase's work; I/O is the store client's CPU plus the priced requests.
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
WHAT = {"write": ("batch", WRITE), "window": ("read_window", READ["window_s"]),
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
                    ms = work * s + (total - work if part == "I/O" else 0)
                    rows.append(dict(Store=r.Store, name=name, what=what, part=part, s=ms / 1e3))
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


seal_add = (by_part(IP, S3X, "RMoR", WRITE) - by_part(IP, S3X, "MoR", WRITE)).fillna(0)


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
    w = d[(d.Store == store) & (d.name == name) & (d.what == WRITE) & d.step.between(first, last)]
    return w.s.mean()


def reads_of(d, name, what, store=S3X):
    return d[(d.Store == store) & (d.name == name) & (d.what == what)].sort_values("step").s


SNI = snapshots(ING, ["window_s", "sorted_s"])
IN = [s[5] for s in ING if s[5]]


def seal_writes(name, store=S3X):
    """A merging design's ordinary write (s), its write that merges (s), and those writes' steps."""
    w = SNI[(SNI.Store == store) & (SNI.name == name) & (SNI.what == WRITE)]
    slow = w.s > 2 * w.s.median()
    return w[~slow].s.mean(), w[slow].s.mean(), sorted(w[slow].step)


# The designs compared with nothing compacted: compaction has sections of its own (5 and 13)
UN = [s[5] for s in UPD if s[5]]
UN_M = [n for n in UN if n != "MoR (compacted)"]
IN_M = [n for n in IN if n != "MoR (compacted)"]
SNU_M = SNU[SNU.name.isin(UN_M)]
SNI_M = SNI[SNI.name.isin(IN_M)]
UM, IM = U[U.main & U.name.isin(UN_M)], I[I.main & I.name.isin(IN_M)]
REG_N = ["RMoR", "RMoW-external", "RMoW-append"]

# What each design has written after every write, MoR (compacted) beside them (the same on both
# stores)
WRU = fig.wl_written([(s[0], s[1], s[5]) for s in UPD if s[5]])
WRI = fig.wl_written([(s[0], s[1], s[5]) for s in ING if s[5]])
FAM = {s[5]: s[2] for s in UPD + ING if s[5]}


def written_chart(d, names):
    return fig.wl_written_chart(d, {n: NCOLOR.get(n, COLOR[FAM[n]]) for n in names},
                                {n: SHAPE[FAM[n]] for n in names}, dashed=["MoR (compacted)"])


def gbw(d, name, step=30):
    """What d's design has written by `step` writes (GB)."""
    return d[(d.name == name) & (d.step == step)].gb.iloc[0]


# a compaction's bytes on average (MB), per configuration
COMP_MB = q("""SELECT r.label, avg(s.bytes_written) / 1e6 AS mb FROM steps s JOIN canonical_runs r USING (run_id)
               WHERE r.host = 'havoc' AND s.phase = 'compact' AND s.arm = 'mor-v3' GROUP BY 1""").set_index("label").mb

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


def written_of(spec):
    """What MoR has written at each compaction interval of spec."""
    return fig.wl_written([(l, "mor-v3", n) for l, n in spec])


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
        column=alt.Column("what:N", sort=[WRITE, READ["window_s"], READ["full_s"]], title=None)
    ).resolve_scale(y="independent")


CMU = compaction(CU, ["window_s", "full_s"], ("wl-upd-base", "rmow-c", "RMoW-append", "RMoW-append"))
WRCU = written_of(CU)
TU = costs(CU, CMU, WRCU)


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
- **The region designs write far more:** over the 30 writes RMoR, RMoW-external and RMoW-append
  write {eval}`span([gbw(WRU, n) for n in REG_N], "{:.1f}")` GB,
  {eval}`f"{gbw(WRU, 'RMoW-append') / gbw(WRU, 'MoR'):.0f}"` times MoR's
  {eval}`f2(gbw(WRU, "MoR"))` and {eval}`f"{gbw(WRU, 'RMoW-append') / gbw(WRU, 'MoR (compacted)'):.0f}"`
  times MoR's compacted every 10 writes (section 3).
- **Compaction brings MoR's reads back in line, for little data written:** compacted every 10
  writes, its range read after 30 writes takes
  {eval}`f2(at(CMU, "every 10 writes", READ["window_s"], 30))` s against
  {eval}`f2(at(CMU, "never", READ["window_s"], 30))` never compacted, for no slower writes and
  {eval}`f2(cu(S3X, "every 10 writes", "GB written") - cu(S3X, "never", "GB written"))` GB more
  written over the 30 writes (section 5).
- **Inserts: what matters is whether late rows are merged.** RMoR, which merges the files of its
  four newest writes every 4th write, reads a range
  {eval}`f"{i_('wl-ing-base', 'mor-v3', 'window_s') / i_('wl-ing-seal160', 'rmor-c', 'window_s'):.1f}"`
  times as fast as MoR for
  {eval}`f"{i_('wl-ing-seal160', 'rmor-c', 'write_s') - i_('wl-ing-base', 'mor-v3', 'write_s'):.2f}"`
  s more a write, as MoR (compacted every 10 writes) does; it pays only once rows arrive more than
  a write late.
- **Whole-row-group copies get dearer as row groups grow:** RMoW-append's write rises
  {eval}`f"{v(P.assign(write_s=P.write_ms), S3X, 'wlc-upd-rg32m', 'rmow-c', 'write_s') / v(P.assign(write_s=P.write_ms), S3X, 'wlc-upd-rg4m', 'rmow-c', 'write_s'):.1f}"`
  times from 4 to 32 MiB. Iceberg's default, 128 MiB, and copying parts of row groups are not
  tested here.
- **Where the time goes:** on S3 Express the largest part of a region design's write is encode,
  for the row groups it copies, and MoR's is mostly I/O and decode, which finds the rows to delete;
  reads are mostly I/O.

## Setup

**The workloads**, on one table (11 columns, about 1 GB, 25.7 million rows), 30 writes each:
- **updates:** 150,000 changes to existing rows a write, five updates to each delete, clustered
  in regions of 513,700 rows (about one 16 MiB row group each), some regions far more often than
  others;
- **inserts:** 350,000 new rows a write, sorted by event time; most arrive on time, and 99% within
  5 writes of their event time (with twins at 1 and 20 writes).

**The designs:** MoR (deletion vectors, new rows in new files), RMoR (each changed row group
copied into the write's new file, a deletion vector over the old one), RMoW-external (the same,
the old row group dropped by a new footer appended to its file instead of a deletion vector),
RMoW-append (each changed row group appended to its own file with a new footer), and RMoR-density
(RMoR that defers each merge: deletion vectors until a row group's changed rows reach a share θ of
it, then the whole row group copied; the new versions wait in a delta row group per region). Each copies **whole row groups**: none copies part of one. Each
writer finds the rows a write changes by key, reading only the row groups whose statistics may hold
a changed key ([our changes to readers and writers](./reader-changes.md)).

**The designs are compared with nothing compacted,** as snapshots are added. The knobs each have
a section of their own: MoR compacted every 12, 10, 6 or 3 writes (sections 5 and 13);
RMoR-density at θ of 1 (never merging), 0.5, 0.25, 0.12 and 0.06 (section 6); the row-group
size (section 7); on the inserts, how many of its newest writes RMoR merges at a time (section 13).

**The reads**, one at a time:
- **a range read:** 1% of the table, by id on the updates and by event time on the inserts, timed
  as the mean of five at each probe;
- **a full scan**, skipping row groups whose rows are all dead;
- **a sorted read:** the whole table in key order, skipping them too.

Where a section gives one time, a read's is its mean over the reads after 5 to 30 writes, and a
write's over the 30 writes.

**Data written** is every byte a design's writes and compactions put in the store after the
initial load, the same on both stores. No run deletes anything (every snapshot stays readable), so
it is also what the store holds beyond the initial load.

**How the times are simulated.** Each run's requests and bytes were counted on havoc and its CPU
on the laptop, at {eval}`", ".join(sorted(runs.build.unique()))`. A phase's time is its CPU at the
laptop's speed, plus its requests priced by size from a latency test of the store (S3 Express on
a `c7a.4xlarge` in usw2-az3, Azure Blob on a `D8as_v6` in westus). [The measured page](./workloads-cloud.md) has the main points run in the cloud on 2026-11-11,
and [the simulation against the cloud](./simulation-cloud.md) (calibration) compares the two.

```{code-cell} ipython3
:tags: [remove-input]

table(runs)
```

## Updates

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

### 1. Write latency against read latency

```{code-cell} ipython3
:label: workloads-updates-tradeoff
:tags: [remove-input]
:class: col-page-right

fig.wl_tradeoff(UM, ["window_s", "full_s", "sorted_s"])
```

Each design's simulated write latency against its read latency on the updates, with nothing
compacted (section 5 compacts MoR): a write's mean over the 30 writes, its whiskers from the 10th
to the 90th percentile, and a read's mean over the reads after 5 to 30 writes, its whiskers from
the fastest to the slowest. Section 2 shows how they move as snapshots are added.

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(UM, ["window_s", "full_s", "sorted_s"]), caption="Simulated latency (s) on the updates")
```

- **The region designs take {eval}`span(reg_write[S3X], "{:.1f}")` times MoR's time to write on S3
  Express and {eval}`span(reg_write[AZ], "{:.1f}")` on Azure**, where the store's slower requests
  weigh more on MoR's write, which is mostly I/O (section 4). On S3 Express RMoW-append takes
  {eval}`f1(w[S3X]["rmow-c"])` s a write on average, RMoW-external {eval}`f1(w[S3X]["rmor-c-footer-kept"])`
  and RMoR {eval}`f1(w[S3X]["rmor-c-kept"])`, against MoR's {eval}`f1(w[S3X]["mor-v3"])` (on Azure,
  {eval}`f1(w[AZ]["rmow-c"])`, {eval}`f1(w[AZ]["rmor-c-footer-kept"])` and
  {eval}`f1(w[AZ]["rmor-c-kept"])` against {eval}`f1(w[AZ]["mor-v3"])`).
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

### 2. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-updates
:tags: [remove-input]
:class: col-page-right

snapshot_chart(SNU_M, ["window_s", "full_s", "sorted_s"])
```

Each design's simulated write latency at every write, and its read latency after every 5 writes,
on the updates with nothing compacted (section 5 compacts MoR). Each graph has one latency axis for
both stores.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

snapshot_table(SNU_M, WRITE)
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

- **MoR slows as its deletes pile up, in its writes and its reads.** On S3 Express its write takes
  {eval}`f1(avg_write(SNU_M, "MoR", 1, 10))` s on average over the first 10 writes and
  {eval}`f1(avg_write(SNU_M, "MoR", 21, 30))` over the last 10, and its range read
  {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 5))` s after 5 writes and
  {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 30))` after 30 (on Azure
  {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 5, AZ))` and {eval}`f2(at(SNU_M, "MoR", READ["window_s"], 30, AZ))`).
- **The region designs stay level:** RMoW-append's write takes
  {eval}`f1(avg_write(SNU_M, "RMoW-append", 1, 10))` and {eval}`f1(avg_write(SNU_M, "RMoW-append", 21, 30))` s
  over the first and last 10 writes, and its range read
  {eval}`span(reads_of(SNU_M, "RMoW-append", READ["window_s"]))` s throughout. RMoR-density climbs
  as MoR does ({eval}`f1(avg_write(SNU_M, "RMoR-density", 1, 10))` to
  {eval}`f1(avg_write(SNU_M, "RMoR-density", 21, 30))` s a write, a range read from
  {eval}`f2(at(SNU_M, "RMoR-density", READ["window_s"], 5))` to
  {eval}`f2(at(SNU_M, "RMoR-density", READ["window_s"], 30))` s), as more of its changes wait in
  deletion vectors and delta row groups.

### 3. Data written as snapshots are added

```{code-cell} ipython3
:label: workloads-updates-written
:tags: [remove-input]
:class: col-page-right

written_chart(WRU, UN_M + ["MoR (compacted)"])
```

What each design has written after every write on the updates, in GB, by its writes and
compactions, with MoR (compacted every 10 writes) dashed. RMoR, RMoW-external and RMoW-append
write the same, on one line.

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_written_table(WRU, UN_M + ["MoR (compacted)"])
```

- **The region designs write about {eval}`f"{gbw(WRU, 'RMoW-append') / gbw(WRU, 'MoR'):.0f}"` times
  MoR's data.** Over the 30 writes RMoR, RMoW-external and RMoW-append write
  {eval}`span([gbw(WRU, n) for n in REG_N], "{:.1f}")` GB, against MoR's {eval}`f2(gbw(WRU, "MoR"))`
  and RMoR-density's {eval}`f2(gbw(WRU, "RMoR-density"))`. A region design writes each row group a
  write changes again, whole
  ({eval}`f"{gbw(WRU, 'RMoW-append') * 1e3 / 30:.0f}"` MB a write on average), where MoR writes the
  changed rows and their deletion vectors ({eval}`f"{gbw(WRU, 'MoR') * 1e3 / 30:.1f}"` MB).
- **Compaction writes in bursts, and far less:** MoR (compacted every 10 writes) writes
  {eval}`f2(gbw(WRU, "MoR (compacted)"))` GB, about {eval}`f"{COMP_MB['wl-upd-compact']:.0f}"` MB a
  compaction. Iceberg's rule rewrites the small files and the files with at least 30% of their rows
  deleted, which here are mostly the files the writes since the last compaction added, not the
  table (section 5).

### 4. Where the time goes

```{code-cell} ipython3
:label: workloads-updates-parts
:tags: [remove-input]
:class: col-page-right

parts_chart(UP[UP.name.isin(UN_M)], [WRITE, READ["window_s"], READ["full_s"]], UN_M)
```

Each design's simulated seconds by part, on average over the 30 writes and the reads after 5 to
30: Parquet's decode and encode; DVs and footers (deletion vectors built, encoded, loaded and
decoded, and RMoW's footers); other CPU (the rest of the CPU: making the rows a write writes,
planning scans, Iceberg's manifests and commit); and I/O (the store client's CPU and the requests,
priced by size).

```{code-cell} ipython3
:tags: [remove-input]

UPM = UP[UP.name.isin(UN_M)]
table(UPM.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
      .reindex(columns=[p for p in PARTS if p in set(UPM.part)]).reset_index().round(3))
```

- **On S3 Express the largest part of a region design's write is encode:** the row groups it
  copies are encoded again, which is
  {eval}`f"{share_of(UP, S3X, 'RMoW-append', WRITE, 'encode'):.0%}"` of RMoW-append's write
  and {eval}`f"{share_of(UP, S3X, 'RMoR', WRITE, 'encode'):.0%}"` of RMoR's, against I/O's {eval}`f"{share_of(UP, S3X, 'RMoW-append', WRITE, 'I/O'):.0%}"` and
  {eval}`f"{share_of(UP, S3X, 'RMoR', WRITE, 'I/O'):.0%}"` (on Azure I/O is the larger
  part, {eval}`span([share_of(UP, AZ, n, WRITE, 'I/O') for n in ('RMoR', 'RMoW-external', 'RMoW-append')], "{:.0%}")`).
  MoR's write is mostly I/O and decode, which finds the rows to delete
  ({eval}`f"{share_of(UP, S3X, 'MoR', WRITE, 'I/O'):.0%}"` and
  {eval}`f"{share_of(UP, S3X, 'MoR', WRITE, 'decode'):.0%}"`).
- **A read is mostly I/O:**
  {eval}`span([share_of(UP, S3X, n, READ['full_s'], 'I/O') for n in UN_M], "{:.0%}")` of a full
  scan on S3 Express and
  {eval}`span([share_of(UP, AZ, n, READ['full_s'], 'I/O') for n in UN_M], "{:.0%}")` on Azure;
  {eval}`span([share_of(UP, S3X, n, READ['window_s'], 'I/O') for n in UN_M], "{:.0%}")` of a range
  read on S3 Express.

### 5. Compaction

Compacting MoR rewrites its table without the rows its deletion vectors drop, so what MoR's reads
lose as snapshots are added comes back after each compaction. What it costs is the compactions'
writes, and storage while older snapshots are kept.

```{code-cell} ipython3
:label: workloads-updates-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMU, ["window_s", "full_s"], "RMoW-append")
```

MoR's simulated read latency after every 5 writes, never compacted or compacted every 10 or 3
writes (a read at a compaction's write reads the compacted table), beside RMoW-append's (dashed,
never compacted). The table below has every 12 and 6 writes too.

```{code-cell} ipython3
:label: workloads-updates-compaction-written
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_written_chart(WRCU)
```

What MoR has written after every write, in GB, never compacted or compacted every 10 or 3 writes.

```{code-cell} ipython3
:tags: [remove-input]

table(TU, caption="MoR at each compaction interval, simulated latency (s): a write on average without and with "
      "its compactions spread over the writes, a compaction on average, each read on average and after 30 "
      "writes; and the data written over the 30 writes (GB)")
```

```{code-cell} ipython3
:tags: [remove-cell]

rw_ref = reads_of(CMU, "RMoW-append", READ["window_s"])
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
- **Compaction writes little.** Over the 30 writes MoR writes
  {eval}`f2(cu(S3X, "every 10 writes", "GB written"))` GB compacted every 10 writes and
  {eval}`f2(cu(S3X, "every 3 writes", "GB written"))` every 3, against
  {eval}`f2(cu(S3X, "never", "GB written"))` never compacted: each compaction rewrites mostly the
  small files the writes since the last one added. RMoR-density writes
  {eval}`f2(gbw(WRU, "RMoR-density"))` GB and the region designs
  {eval}`span([gbw(WRU, n) for n in REG_N], "{:.1f}")` (section 3).

### 6. RMoR-density's threshold

```{code-cell} ipython3
:tags: [remove-cell]

THETA = {"θ 1": 1.0, "0.5": 0.5, "0.25": 0.25, "0.12": 0.12, "0.06": 0.06}
TH = U[U.family == "RMoR-density"].assign(theta=lambda d: d.knob.map(THETA), design="RMoR-density")
THL = TH.assign(**{WRITE: TH.write_s, READ["window_s"]: TH.window_s, READ["full_s"]: TH.full_s}).melt(
    id_vars=["Store", "design", "theta"], value_vars=[WRITE, READ["window_s"], READ["full_s"]],
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

RMoR-density on the updates at five thresholds θ, the share of a row group's rows a write's
changes must reach before the row group is merged, simulated: the latency of a write, a range read
and a full scan, on average over the 30 writes and the reads after 5 to 30. At θ 1 a row group is
never merged; θ 0.06 is RMoR-density as in the sections above.

```{code-cell} ipython3
:tags: [remove-input]

table(TH[["Store", "theta", "write_s", "window_s", "full_s", "sorted_s"]].sort_values(
    ["Store", "theta"]).round(3), caption="RMoR-density at each θ, simulated latency (s) on average")
```

- **The threshold moves its write little:**
  {eval}`span([th(S3X, t, WRITE) for t in THETA.values()], "{:.1f}")` s from θ 1 to 0.06 on S3
  Express.
- **It reads a range faster than MoR even when it never merges:** at θ 1 in
  {eval}`f2(th(S3X, 1.0, READ["window_s"]))` s against MoR's
  {eval}`f2(rd(S3X, B_, "mor-v3", "window_s"))`, for {eval}`f1(th(S3X, 1.0, WRITE))` s a write.
  The gain comes from the new versions waiting in a delta row group per region, not from merging.

### 7. Row-group size, 4 to 32 MiB

```{code-cell} ipython3
:tags: [remove-cell]

RG = {"wlc-upd-rg4m": 4, "wlc-upd-rg8m": 8, "wl-upd-base": 16, "wlc-upd-rg32m": 32}
R = P[P.label.isin(RG) & P.arm.isin(["rmow-c", "rmor-density"])].copy()
R["design"] = R.arm.map({"rmow-c": "RMoW-append", "rmor-density": "RMoR-density"})
R["rg_mib"] = R.label.map(RG)
R = R.assign(**{WRITE: R.write_ms / 1e3, READ["window_s"]: R.window_ms / 1e3, READ["full_s"]: R.full_ms / 1e3})
RL = R.melt(id_vars=["Store", "design", "rg_mib"], value_vars=[WRITE, READ["window_s"], READ["full_s"]],
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

RMoW-append and RMoR-density (θ 0.06) on the updates at four row-group sizes, simulated:
the latency of a write, a range read and a full scan, on average.

```{code-cell} ipython3
:tags: [remove-input]

table(R[["Store", "design", "rg_mib", WRITE, READ["window_s"], READ["full_s"]]].sort_values(
    ["Store", "design", "rg_mib"]).round(3))
```

- **Copying whole row groups gets dearer as they grow.** RMoW-append's write takes
  {eval}`f1(rg(S3X, "RMoW-append", 4, WRITE))` s at 4 MiB and
  {eval}`f1(rg(S3X, "RMoW-append", 32, WRITE))` s at 32 MiB on S3 Express, and its range read
  {eval}`f2(rg(S3X, "RMoW-append", 4, READ["window_s"]))` and
  {eval}`f2(rg(S3X, "RMoW-append", 32, READ["window_s"]))` s. RMoR-density, which seldom
  merges, rises less: {eval}`f1(rg(S3X, "RMoR-density", 4, WRITE))` s a write at 4 MiB and
  {eval}`f1(rg(S3X, "RMoR-density", 32, WRITE))` s at 32 MiB.
- **On Azure, full scans get much faster as row groups grow:** RMoW-append's takes
  {eval}`f1(rg(AZ, "RMoW-append", 4, READ["full_s"]))` s at 4 MiB and
  {eval}`f1(rg(AZ, "RMoW-append", 32, READ["full_s"]))` s at 32 MiB, as its requests get fewer and
  larger ({eval}`f1(rg(S3X, "RMoW-append", 4, READ["full_s"]))` and
  {eval}`f1(rg(S3X, "RMoW-append", 32, READ["full_s"]))` s on S3 Express, whose requests cost less).
- **Iceberg's default is 128 MiB, four times the largest size here.** A design that copies whole
  row groups pays for the whole row group whatever share of it a write changes; RMoR-density
  could copy only the part a write changed, but as built it cannot.

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
fulls = I[(I.Store == S3X) & I.main & (I.name != "MoR (compacted)")].full_s
```

Every design writes a write's rows (350,000, about 12 MB) as a file of its own, and none merges a
late row into the row group its event time falls in. Left at that, MoR, RMoR and RMoW-append write
the same files: they come within
{eval}`f"{max(spread(c) for c in ('window_ms', 'full_ms', 'sorted_ms')):.0%}"` of one another on every
read on S3 Express, and their writes within {eval}`f"{spread('write_ms'):.0%}"`. What changes the
reads is whether the newest writes' rows are merged, sorted by event time, so that late rows stop
widening every file's range of event times. **Here RMoR and RMoW-append merge their newest
writes:** every 4th write merges the files of the four newest writes into one of about 47 MB
(`tierFanout` 4; a merged file is not merged again, since four of them would pass the limit of
160 MiB, `tailSealBytes`). RMoR cuts the merged file into 16 MiB row groups, and RMoW-append
appends it to its object as one row group. MoR does not merge; MoR (compacted) merges the same
files every 10 writes, in a compaction of its own (section 13). RMoW-external and RMoR-density
did not run on the inserts.

### 8. Write latency against read latency

```{code-cell} ipython3
:label: workloads-inserts-tradeoff
:tags: [remove-input]
:class: col-page-right

fig.wl_tradeoff(IM, ["window_s", "full_s", "sorted_s"])
```

Each design's simulated write latency against its read latency on the inserts (99% within 5
writes late), with nothing compacted (section 13 compacts MoR). The means and whiskers are as in
section 1.

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(IM, ["window_s", "full_s", "sorted_s"]), caption="Simulated latency (s) on the inserts")
```

- **Merging the newest writes buys range reads for a fraction of a second a write.** MoR writes in
  {eval}`f2(iw[S3X])` s on average and reads a range in {eval}`f2(ir[S3X])` s on S3 Express; RMoR
  takes {eval}`f2(rs[S3X].write_s - iw[S3X])` s more a write and reads in
  {eval}`f2(rs[S3X].window_s)` s (on Azure {eval}`f2(ir[AZ])` s for MoR,
  {eval}`f2(rs[AZ].window_s)` for RMoR).
- **RMoW-append gains little from its merge:** {eval}`f2(ws[S3X].window_s)` s a range read on S3
  Express and {eval}`f2(ws[AZ].window_s)` on Azure, against MoR's {eval}`f2(ir[S3X])` and
  {eval}`f2(ir[AZ])`. Its merged file is one row group, which stretches back to its oldest late row
  as a single write's did.
- **A full scan does not care:** {eval}`span(fulls, "{:.1f}")` s in every design on S3 Express.

### 9. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-inserts
:tags: [remove-input]
:class: col-page-right

snapshot_chart(SNI_M, ["window_s", "sorted_s"])
```

Each design's simulated write latency at every write, and its read latency after every 5 writes,
on the inserts (99% within 5 writes late) with nothing compacted.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

snapshot_table(SNI_M, WRITE)
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNI_M, READ["window_s"])
```

```{code-cell} ipython3
:tags: [remove-input]

snapshot_table(SNI_M, READ["sorted_s"])
```

- **Unmerged, a range read slows with every write:** MoR's takes
  {eval}`f2(at(SNI_M, "MoR", READ["window_s"], 5))` s after 5 writes and
  {eval}`f2(at(SNI_M, "MoR", READ["window_s"], 30))` after 30 on S3 Express, as late rows widen
  more row groups' event-time ranges. RMoR's goes from
  {eval}`f2(at(SNI_M, "RMoR", READ["window_s"], 5))` to
  {eval}`f2(at(SNI_M, "RMoR", READ["window_s"], 30))` s.
- **The merge makes one write in four slow:** RMoR writes in
  {eval}`f2(seal_writes("RMoR")[0])` s, and in {eval}`f2(seal_writes("RMoR")[1])` s at
  writes {eval}`", ".join(str(k) for k in seal_writes("RMoR")[2])`, when it merges the four newest
  writes' files.
- **The sorted read slows in every design**, from
  {eval}`span([at(SNI_M, n, READ["sorted_s"], 5) for n in IN_M], "{:.1f}")` s after 5 writes to
  {eval}`span([at(SNI_M, n, READ["sorted_s"], 30) for n in IN_M], "{:.1f}")` s after 30 on S3 Express.

### 10. Data written as snapshots are added

```{code-cell} ipython3
:label: workloads-inserts-written
:tags: [remove-input]
:class: col-page-right

written_chart(WRI, IN_M + ["MoR (compacted)"])
```

What each design has written after every write on the inserts, in GB, as in section 3, with MoR
(compacted every 10 writes) dashed.

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_written_table(WRI, IN_M + ["MoR (compacted)"])
```

- **Merging writes the merged rows again, and compaction does the same:** over the 30 writes MoR
  writes {eval}`f2(gbw(WRI, "MoR"))` GB, RMoR {eval}`f2(gbw(WRI, "RMoR"))` and RMoW-append
  {eval}`f2(gbw(WRI, "RMoW-append"))`, and MoR (compacted)
  {eval}`f2(gbw(WRI, "MoR (compacted)"))`, its compactions rewriting the files the writes added.

### 11. How late the rows arrive

```{code-cell} ipython3
:label: workloads-lateness
:tags: [remove-input]

LATE = {"win-ing-late99-1": 1, "win-ing-base": 5, "win-ing-late99-20": 20,
        "win-ing-seal160-late99-1": 1, "win-ing-seal160": 5, "win-ing-seal160-late99-20": 20}
POLICY = {("win-ing", "mor-v3"): "MoR", ("win-ing-seal160", "rmor-c"): "RMoR",
          ("win-ing-seal160", "rmow-c"): "RMoW-append"}


def late_policy(label, arm):
    key = ("win-ing-seal160" if label.startswith("win-ing-seal160") else "win-ing", arm)
    return POLICY.get(key)


Lt = P[P.label.isin(LATE)].copy()
Lt["design"] = [late_policy(l, a) for l, a in zip(Lt.label, Lt.arm)]
Lt = Lt[Lt.design.notna()].assign(lateness=lambda d: d.label.map(LATE), range_read_s=lambda d: d.window_ms / 1e3,
                                  write_s=lambda d: (d.write_ms + d.compact_ms) / 1e3)
LP = ["MoR", "RMoR", "RMoW-append"]
lc = alt.Color("design:N", title=None, sort=LP, scale=alt.Scale(domain=LP, range=[COLOR[n] for n in LP]))
ls = alt.Shape("design:N", title=None, sort=LP, scale=alt.Scale(domain=LP, range=[SHAPE[n] for n in LP]))
lx = alt.X("lateness:Q", title="lateness, p99 (writes)", scale=alt.Scale(type="log", domain=[0.8, 25]),
           axis=alt.Axis(values=[1, 5, 20], format="~g"))
ly = alt.Y("range_read_s:Q", title="1% range read latency (s)", scale=alt.Scale(zero=True))
lb = alt.Chart().encode(x=lx, y=ly, color=lc)
alt.layer(lb.mark_line(strokeWidth=1.5, opacity=0.6),
          lb.mark_point(size=80, filled=True, opacity=1).encode(
              shape=ls, tooltip=["Store", "design", "lateness", alt.Tooltip("range_read_s:Q", format=".2f"),
                                 alt.Tooltip("write_s:Q", format=".2f")]),
          data=Lt[["Store", "design", "lateness", "range_read_s", "write_s"]]).properties(
    width=260, height=180).facet(column=alt.Column("Store:N", sort=STORES, title=None))
```

A range read's simulated seconds, on average over the reads after 5 to 30 writes, against how late
the inserts arrive (the 99th percentile, in writes).

```{code-cell} ipython3
:tags: [remove-input]

table(Lt[["Store", "design", "lateness", "write_s", "range_read_s"]].sort_values(["Store", "design", "lateness"]).round(3))
```

```{code-cell} ipython3
:tags: [remove-cell]

def lt(s, design, late):
    return Lt[(Lt.Store == s) & (Lt.design == design) & (Lt.lateness == late)].range_read_s.iloc[0]
```

- **How late the rows are decides whether merging is worth it.** When 99% arrive within a write,
  MoR reads a range in {eval}`f2(lt(S3X, "MoR", 1))` s on S3 Express, about what RMoR reads at 5
  writes ({eval}`f2(lt(S3X, "RMoR", 5))`). At 5 or 20 writes it takes
  {eval}`f2(lt(S3X, "MoR", 5))` and {eval}`f2(lt(S3X, "MoR", 20))` s: a few late rows widen every
  row group's event-time range.
- **RMoR's merge holds at every lateness:** {eval}`f2(lt(S3X, "RMoR", 1))`,
  {eval}`f2(lt(S3X, "RMoR", 5))` and {eval}`f2(lt(S3X, "RMoR", 20))` s.

### 12. Where the time goes

```{code-cell} ipython3
:label: workloads-inserts-parts
:tags: [remove-input]
:class: col-page-right

parts_chart(IP[IP.name.isin(IN_M)], [WRITE, READ["window_s"], READ["sorted_s"]], IN_M)
```

Each design's simulated seconds by part (as in section 4), on the inserts at lateness p99 5.

```{code-cell} ipython3
:tags: [remove-input]

IPM = IP[IP.name.isin(IN_M)]
table(IPM.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
      .reindex(columns=[p for p in PARTS if p in set(IPM.part)]).reset_index().round(3))
```

- **A write is mostly CPU:** MoR's is encode
  ({eval}`f"{share_of(IP, S3X, 'MoR', WRITE, 'encode'):.0%}"`) and other CPU, mostly making the
  rows ({eval}`f"{share_of(IP, S3X, 'MoR', WRITE, 'other CPU'):.0%}"`), with I/O
  {eval}`f"{share_of(IP, S3X, 'MoR', WRITE, 'I/O'):.0%}"` on S3 Express. RMoR's merge adds
  {eval}`f2(seal_add.sum())` s a write, spread over the same parts
  ({eval}`", ".join(f"{p} {s:.2f} s" for p, s in seal_add.sort_values(ascending=False).items() if s >= 0.01)`).
- **A range read is mostly I/O** in every design
  ({eval}`span([share_of(IP, S3X, n, READ['window_s'], 'I/O') for n in IN_M], "{:.0%}")` on S3
  Express). Merging cuts I/O and decode alike: RMoR's range read spends
  {eval}`f2(part_s(IP, S3X, 'RMoR', READ['window_s'], 'I/O'))` s on I/O and
  {eval}`f2(part_s(IP, S3X, 'RMoR', READ['window_s'], 'decode'))` s on decode, against MoR's
  {eval}`f2(part_s(IP, S3X, 'MoR', READ['window_s'], 'I/O'))` and
  {eval}`f2(part_s(IP, S3X, 'MoR', READ['window_s'], 'decode'))` s.

### 13. Compaction, and how much RMoR merges

```{code-cell} ipython3
:tags: [remove-cell]

CMI = compaction(CI, ["window_s"], ("wl-ing-seal160", "rmor-c", "RMoR", "RMoR"))
WRCI = written_of(CI)
TI = costs(CI, CMI, WRCI)
```

Compacting MoR merges the files the writes added after the fact, every N writes, as RMoR's merge
does every 4th write.

```{code-cell} ipython3
:label: workloads-inserts-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMI, ["window_s"], "RMoR")
```

MoR's simulated range-read latency after every 5 writes on the inserts, never compacted or
compacted every 10 or 3 writes, beside RMoR's (dashed). The table below has every 12 and 6 writes
too.

```{code-cell} ipython3
:label: workloads-inserts-compaction-written
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_written_chart(WRCI)
```

What MoR has written after every write on the inserts, in GB, at each interval, as in section 5.

```{code-cell} ipython3
:tags: [remove-input]

table(TI, caption="MoR at each compaction interval on the inserts, as in section 5")
```

```{code-cell} ipython3
:tags: [remove-cell]

def ci(store, name, col):
    return TI[(TI.Store == store) & (TI.compacted == name)][col].iloc[0]


SEAL = {"tune-ing-seal160-f2": "2 at a time, up to 160 MiB", "wl-ing-seal160": "4 at a time, up to 160 MiB",
        "tune-ing-seal160-f8": "8 at a time, up to 160 MiB", "tune-ing-seal256": "4 at a time, up to 256 MiB"}
SL = I[I.label.isin(SEAL) & (I.arm == "rmor-c")].assign(setting=lambda d: d.label.map(SEAL))
SL = SL[["Store", "setting", "write_s", "window_s", "full_s", "sorted_s"]].sort_values(
    ["Store", "setting"], key=lambda c: c.map(list(SEAL.values()).index) if c.name == "setting" else c)


def sl(store, setting, col):
    return SL[(SL.Store == store) & (SL.setting == setting)][col].iloc[0]
```

- **Compaction brings MoR's range read back down, as RMoR's merge does.** Compacted every 10
  writes, MoR's range read drops back after each compaction, to
  {eval}`", ".join(f2(at(CMI, "every 10 writes", READ["window_s"], k)) for k in (10, 20, 30))` s
  after writes 10, 20 and 30 on S3 Express, against
  {eval}`f2(at(CMI, "never", READ["window_s"], 30))` s never compacted after 30 writes and
  {eval}`span(reads_of(CMI, "RMoR", READ["window_s"]))` s for RMoR. Its compactions
  take {eval}`span(reads_of(CMI, "every 10 writes", "a compaction"), "{:.1f}")` s each, or
  {eval}`f2(ci(S3X, "every 10 writes", "write with compactions") - ci(S3X, "never", "write"))` s a
  write spread over the writes, against {eval}`f2(rs[S3X].write_s - iw[S3X])` s for RMoR's merge.
- **Compaction rewrites what the writes added:** over the 30 writes MoR writes
  {eval}`f2(ci(S3X, "every 10 writes", "GB written"))` GB compacted every 10 writes and
  {eval}`f2(ci(S3X, "every 3 writes", "GB written"))` every 3, against
  {eval}`f2(ci(S3X, "never", "GB written"))` never compacted and {eval}`f2(gbw(WRI, "RMoR"))` for
  RMoR.
- **Merging more at once reads faster for a slower write.** RMoR merging 4 at a time up to 256 MiB
  (so every 16th write merges again, the four merged files into one) reads a range in
  {eval}`f2(sl(S3X, "4 at a time, up to 256 MiB", "window_s"))` s for
  {eval}`f2(sl(S3X, "4 at a time, up to 256 MiB", "write_s"))` s a write on S3 Express, against
  {eval}`f2(sl(S3X, "4 at a time, up to 160 MiB", "window_s"))` and
  {eval}`f2(sl(S3X, "4 at a time, up to 160 MiB", "write_s"))` up to 160 MiB; merging 2 at a time
  reads in {eval}`f2(sl(S3X, "2 at a time, up to 160 MiB", "window_s"))` s, 8 at a time in
  {eval}`f2(sl(S3X, "8 at a time, up to 160 MiB", "window_s"))` (table below).

```{code-cell} ipython3
:tags: [remove-input]

table(SL.round(3), caption="RMoR's merge at each setting, simulated latency (s) on average")
```

## Limits

- **One row-group size for the comparison, 16 MiB, on a narrow table.** The update regions are
  about one 16 MiB row group each, so copying a whole row group copies about the region a write
  changed. At Iceberg's default of 128 MiB a row group holds about six regions; that setting, and
  copying parts of row groups, are not tested here ([the cut](./region-cut.md) tests them).
- **RMoR-density copies whole row groups.** It chooses when to copy, from the deletion
  vector's share of the row group, but not what: no part of a row group, no splitting or merging of
  row groups.
- **RMoR is another arm on the inserts:** `rmor-c`, whose merged files are cut into
  row groups; `rmor-c-kept`, RMoR on the updates, would keep a merged file as one row group, as
  RMoW-append does. RMoW-external and RMoR-density did not run on the inserts.
- **What expiring old snapshots frees is not shown.** No run expires a snapshot, so the store
  holds the initial load and every byte written. Expiry would free MoR's replaced files, but not
  RMoW-append's old row groups, which stay inside the objects its newest snapshot reads, nor RMoR's
  and RMoW-external's inside files still in use.
- **The inserts' compaction intervals (but every 10 writes), merge settings and lateness twins
  are earlier runs**, from the tune and windows campaigns, before the locate by key and the sorted
  read's skip. On the inserts' main points those changes moved no time by more than
  {eval}`f"{ing_moved:.1%}"`.
- **Simulated**, from one seed and 30 writes, CPU at the laptop's speed; reads one at a time on one
  thread.
