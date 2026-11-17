---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Updates and inserts at 128 MiB: writes against reads'
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

STORES = fig.WL_STORES
S3X, AZ = STORES
COLOR, SHAPE, NCOLOR, READ, WRITE = fig.WL_COLOR, fig.WL_SHAPE, fig.WL_NCOLOR, fig.WL_READ, fig.WL_WRITE
X = fig.W128_SNAPSHOT_X
WIDTH = 400  # a store's panel: two side by side fill the page
READS_U = ["window_s", "full_s", "sorted_s"]

# (configuration, arm, design): campaign w128, one design a configuration
UPD = [("w128-upd-mor", "mor-v3", "MoR"), ("w128-upd-compact", "mor-v3", "MoR (compacted)"),
       ("w128-upd-rmor", "rmor-c-kept", "RMoR"), ("w128-upd-external", "rmor-c-footer-kept", "RMoW-external"),
       ("w128-upd-append", "rmow-c", "RMoW-append"), ("w128-upd-density", "rmor-density", "RMoR-density")]
ING = [("w128-ing-mor", "mor-v3", "MoR"), ("w128-ing-compact", "mor-v3", "MoR (compacted)"),
       ("w128-ing-density", "rmor-density", "RMoR-density")]
# the designs compared with nothing compacted (compaction has sections of its own, 5 and 10)
UN = ["MoR", "RMoR", "RMoW-external", "RMoW-append", "RMoR-density"]
IN = ["MoR", "RMoR-density"]


def family(name):
    return "MoR" if name.startswith("MoR") else name


def colors(names):
    return {n: NCOLOR.get(n, COLOR[family(n)]) for n in names}


def shapes(names):
    return {n: SHAPE[family(n)] for n in names}


def spec(runs, names=None):
    """fig.wl_*'s form, (configuration, arm, family, curve, knob, name, order), for the runs whose
    design is in `names` (all by default)."""
    return [(l, a, family(n), "", "", n, 0) for l, a, n in runs if names is None or n in names]


# Simulated time per phase: havoc's requests priced by size from each store's latency test, plus the
# phase's work as its laptop twin measured it (campaign w128-twins: user-mode instructions at the
# arm's pace on the laptop, views/35_work.sql). Nothing stands in for a twin that has not run.
class TwinsOnly:
    def predict(self, d):
        return np.full(len(d), np.nan)


GRID = "(list_contains(campaigns, 'w128') OR list_contains(campaigns, 'w128-twins'))"
tune.READS["sorted"] = ("read_sorted", "cached-skip")
tune.WORKLOADS["w128_upd"] = "regexp_matches(label, '^w128-upd-')"
tune.WORKLOADS["w128_ing"] = "regexp_matches(label, '^w128-ing-')"


def simulated(workload):
    d = tune.steps(con, workload, TwinsOnly(), {}, True, GRID)
    d = d[d.source == "measured"]
    return d.assign(Store=d.store.map(fig.WL_STORE), s=d.ms / 1e3)


S = pd.concat([simulated("w128_upd"), simulated("w128_ing")], ignore_index=True)


def means(d):
    """Each run's mean write (its compactions spread over its writes) and reads, per store."""
    p = tune.policies(con, "w128", None, None, True, d)
    return p.assign(Store=p.store.map(fig.WL_STORE))


P, W = means(S), fig.wl_whiskers(S)
# the same over the first 30 writes, the span RMoW-append ran
S30 = S[S.step <= 30]
P30, W30 = means(S30), fig.wl_whiskers(S30)


def frame(runs, p, w, names=None):
    """fig.wl_frame() over the runs that have times (a run has them once its twin has run)."""
    has = set(zip(p.label, p.arm))
    sp = [s for s in spec(runs, names) if (s[0], s[1]) in has]
    return fig.wl_frame(sp, p, w) if sp else pd.DataFrame(columns=["Store", "label", "arm", "name"])


U, I = frame(UPD, P, W), frame(ING, P, W)
U30 = frame(UPD, P30, W30, UN)


def v(d, store, name, col):
    return d[(d.Store == store) & (d.name == name)][col].iloc[0]


def f0(x):
    return f"{x:,.0f}"


def f1(x):
    return f"{x:.1f}"


def f2(x):
    return f"{x:.2f}"


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


def snapshots(runs, names, reads):
    """Each design's simulated time at every write, compaction and read."""
    return fig.wl_snapshots(spec(runs, names), S, reads)


def snapshot_chart(d, reads):
    return fig.wl_snapshot_chart(d, reads, width=WIDTH, x=X, point=(0, 0))


def at(d, name, what, step, store=S3X):
    return d[(d.Store == store) & (d.name == name) & (d.what == what) & (d.step == step)].s.iloc[0]


def mean_over(d, name, what, first, last, store=S3X):
    g = d[(d.Store == store) & (d.name == name) & (d.what == what) & d.step.between(first, last)]
    return g.s.mean()


def series(d, name, what, store=S3X):
    return d[(d.Store == store) & (d.name == name) & (d.what == what)].set_index("step").s


def at_step(label, phase, step):
    """Havoc's run's phase after a write: requests, MB read and written, and rows (a read's the mean
    over its probes, by the reader every design reads with here)."""
    return q(f"""SELECT avg(s.requests) AS requests, avg(s.bytes_read) / 1e6 AS mb,
                        avg(s.bytes_written) / 1e6 AS mb_written, avg(s.rows) AS rows
                 FROM steps s JOIN canonical_runs r USING (run_id)
                 WHERE list_contains(r.campaigns, 'w128') AND s.label = '{label}' AND s.phase = '{phase}'
                   AND (s.phase = 'batch' OR s.reader = 'cached-skip') AND s.step = {step}""").iloc[0]


# What each design has written after every write, and what the store holds: with every snapshot
# kept (nothing is ever deleted) and with the snapshots before the newest expired (what the newest
# snapshot's files hold, live rows or not). Havoc's runs; the same on both stores.
# Dashed: MoR (compacted), and RMoW-external, which writes and keeps what RMoR does (drawn last, on
# top of RMoR's line).
DASHED = ["MoR (compacted)", "RMoW-external"]


def on_top(d):
    """d's rows with RMoW-external's last, so its dashed line lies over RMoR's."""
    return pd.concat([d[d.name != "RMoW-external"], d[d.name == "RMoW-external"]], ignore_index=True)


def written(runs):
    return on_top(fig.wl_written([(l, a, n) for l, a, n in runs]))


def written_chart(d, names):
    return fig.wl_written_chart(d, colors(names), shapes(names), dashed=DASHED, width=2 * WIDTH + 30, x=X, point=0)


STORAGE = {"kept": "every snapshot kept", "expired": "older snapshots expired"}
ST = q("""SELECT r.label, s.arm, s.step, s.stored_bytes / 1e9 AS kept, (s.stored_bytes - s.reclaimable_bytes) / 1e9 AS expired,
                 s.row_groups
          FROM steps s JOIN canonical_runs r USING (run_id)
          WHERE list_contains(r.campaigns, 'w128') AND s.phase = 'state' ORDER BY 1, 2, 3""")
ST["name"] = [{(l, a): n for l, a, n in UPD + ING}[k] for k in zip(ST.label, ST.arm)]


def stored(runs):
    return on_top(ST[ST.label.isin([l for l, _, _ in runs])])


def storage_chart(d, names):
    L = d.melt(id_vars=["name", "step"], value_vars=list(STORAGE), var_name="basis", value_name="gb")
    L["basis"] = L.basis.map(STORAGE)
    names = [n for n in names if n in set(L.name)]
    legend = alt.Legend(orient="bottom", direction="horizontal", title=None, symbolStrokeWidth=2, labelLimit=320)
    return alt.Chart(L).mark_line(strokeWidth=2).encode(
        x=X, y=alt.Y("gb:Q", title="storage (GB)", scale=alt.Scale(zero=True)),
        color=alt.Color("name:N", sort=names, legend=legend,
                        scale=alt.Scale(domain=names, range=[colors(names)[n] for n in names])),
        strokeDash=alt.StrokeDash("name:N", sort=names, legend=None, scale=alt.Scale(
            domain=names, range=[[5, 3] if n in DASHED else [1, 0] for n in names])),
        tooltip=["name", "basis", "step", alt.Tooltip("gb:Q", format=".2f")],
    ).properties(width=WIDTH, height=180).facet(
        column=alt.Column("basis:N", title=None, sort=list(STORAGE.values())))


def storage_table(d, names, every=40):
    t = d[d.step % every == 0].pivot_table(index="step", columns="name", values=list(STORAGE))
    t = t.reindex(columns=[(b, n) for b in STORAGE for n in names if (b, n) in t.columns])
    t.columns = [f"{n}, {STORAGE[b]}" for b, n in t.columns]
    return table(t.reset_index().rename(columns={"step": "snapshot"}).round(2),
                 caption=f"Storage (GB) at every {every}th snapshot")


def gb(d, name, step=240, col="gb"):
    return d[(d.name == name) & (d.step == step)][col].iloc[0]


# Where the time goes: the twin's CPU split by kind (phase_breakdown, the phase's own thread) applies
# to each phase's work; I/O is the store client's CPU plus the priced requests.
B = q("""
SELECT r.label, b.arm, b.phase, b.component, sum(b.ms) AS ms
FROM phase_breakdown b JOIN canonical_runs r USING (run_id)
WHERE list_contains(r.campaigns, 'w128-twins') AND b.thread = 'phase' AND b.component LIKE '%CPU'
  AND ((b.phase = 'batch' AND b.step >= 1)
       OR (b.step >= 5 AND b.phase IN ('read_full', 'read_window', 'read_sorted') AND b.reader = 'cached-skip'))
GROUP BY ALL""")
CPU_PART = {k: v for k, v in fig.WL_CPU_PART.items() if k.endswith("CPU")}
B["part"] = B.component.map(CPU_PART)
SH = B.groupby(["label", "arm", "phase", "part"]).ms.sum()
SH = SH / SH.groupby(level=["label", "arm", "phase"]).transform("sum")
PARTS = [p for p in fig.WL_PARTS if p != "paused (GC, JIT)"]
WHAT = {"write": ("batch", WRITE), "window": ("read_window", READ["window_s"]),
        "full": ("read_full", READ["full_s"]), "sorted": ("read_sorted", READ["sorted_s"])}


def parts(runs, names, p=P):
    rows = []
    for label, arm, name in runs:
        if name not in names:
            continue
        for _, r in p[(p.label == label) & (p.arm == arm)].iterrows():
            for kind, (phase, what) in WHAT.items():
                work, total = r[f"{kind}_work_ms"], r[f"{kind}_ms"]
                for part, s in SH.loc[(label, arm, phase)].items():
                    ms = work * s + (total - work if part == "I/O" else 0)
                    rows.append(dict(Store=r.Store, name=name, what=what, part=part, s=ms / 1e3))
    d = pd.DataFrame(rows)
    d["pord"] = d.part.map(PARTS.index)
    return d


def share_of(d, store, name, what, part):
    g = d[(d.Store == store) & (d.name == name) & (d.what == what)]
    return g[g.part == part].s.sum() / g.s.sum()


def part_s(d, name, what, part=None, store=S3X):
    """A design's simulated seconds in one part of an operation (all of it with no part)."""
    g = d[(d.Store == store) & (d.name == name) & (d.what == what)]
    return (g[g.part == part] if part else g).s.sum()


runs = q(f"""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
             WHERE {GRID} ORDER BY label, host""")
```

What each design costs a writer and its readers at Iceberg's 128 MiB row groups, on two workloads
apart, **updates** and **inserts**, over 240 writes on S3 Express and Azure Blob. The times are
simulated from local runs.

```{code-cell} ipython3
:tags: [remove-cell]

# what the summary quotes and the sections below show: each design's times at every snapshot, MoR
# against MoR (compacted), data written and storage, and MoR's first and last write
SNU, SNI = snapshots(UPD, UN, READS_U), snapshots(ING, IN, READS_U)
CMP = ["MoR", "MoR (compacted)"]
CMU, CMI = snapshots(UPD, CMP, READS_U), snapshots(ING, CMP, READS_U)
WRU, STU = written(UPD), stored(UPD)
WRI, STI = written(ING), stored(ING)
WR_MOR = [at_step("w128-upd-mor", "batch", n) for n in (1, 240)]


def locate(label, first, last, spike=tune.SPIKES["s3x"]):
    """The scan that finds the rows a write changes (phase locate, a write's first requests) over
    writes first to last, a write's mean: its requests by what they read (each file's footer, in two;
    its deletion vector; the key column), their price by the store's latency test, and the twin's
    work (s)."""
    run = q(f"""SELECT run_id FROM canonical_runs WHERE list_contains(campaigns, 'w128')
                AND label = '{label}'""").run_id.iloc[0]
    n = last - first + 1
    d = q(f"""
        WITH loc AS (SELECT step, requests AS n FROM steps
                     WHERE run_id = '{run}' AND phase = 'locate' AND step BETWEEN {first} AND {last}),
        o AS (SELECT step, seq, row_number() OVER (PARTITION BY step ORDER BY seq) AS i FROM request_ops
              WHERE run_id = '{run}' AND phase = 'batch' AND step BETWEEN {first} AND {last}),
        rq AS (SELECT step, seq, kind, object FROM requests
               WHERE run_id = '{run}' AND phase = 'batch' AND step BETWEEN {first} AND {last}),
        pr AS (SELECT step, seq, price_ms FROM sized_requests('{run}', '{spike}')
               WHERE phase = 'batch' AND step BETWEEN {first} AND {last})
        SELECT CASE WHEN rq.object LIKE '%.puffin' THEN 'dvs' WHEN rq.kind = 'get_stream' THEN 'footers'
                    ELSE 'keys' END AS what, count(*) / {n} AS requests, sum(pr.price_ms) / {n} / 1e3 AS s
        FROM o JOIN loc USING (step) JOIN rq USING (step, seq) JOIN pr USING (step, seq)
        WHERE o.i <= loc.n GROUP BY 1""").set_index("what")
    work = q(f"""SELECT avg(w.work_ms) / 1e3 AS s FROM phase_work w JOIN canonical_runs r USING (run_id)
                 WHERE list_contains(r.campaigns, 'w128-twins') AND r.label = '{label}' AND w.phase = 'locate'
                   AND w.step BETWEEN {first} AND {last}""").s.iloc[0]
    out = {k: float(d.requests.get(w, 0.0)) for k, w in [("footers", "footers"), ("dvs", "dvs"), ("key_column", "keys")]}
    return pd.Series({**out, "requests": sum(out.values()), "s": float(d.s.sum()) + work})


LOC = {(n, b): locate(l, *span) for l, n in [("w128-upd-density", "RMoR-density"), ("w128-upd-rmor", "RMoR"),
                                            ("w128-upd-mor", "MoR"), ("w128-upd-external", "RMoW-external")]
       for b, span in [("first", (1, 10)), ("last", (231, 240))]}


def written_mb(label, first, last):
    """Havoc's run's writes over writes first to last: MB written, a write's mean."""
    return q(f"""SELECT avg(s.bytes_written) / 1e6 AS mb FROM steps s JOIN canonical_runs r USING (run_id)
                 WHERE list_contains(r.campaigns, 'w128') AND r.label = '{label}' AND s.phase = 'batch'
                   AND s.step BETWEEN {first} AND {last}""").mb.iloc[0]


RG = {n: [gb(ST[ST.label == l], n, step, col="row_groups") for step in (0, 240)]
      for l, n in [("w128-upd-density", "RMoR-density"), ("w128-upd-rmor", "RMoR"), ("w128-upd-mor", "MoR"),
                   ("w128-upd-external", "RMoW-external")]}
COPY_RMOR = [written_mb("w128-upd-rmor", 1, 10), written_mb("w128-upd-rmor", 231, 240)]
DEN_RISE = mean_over(SNU, "RMoR-density", WRITE, 231, 240) - mean_over(SNU, "RMoR-density", WRITE, 1, 10)
```

## What it shows

- **Updates: RMoR writes faster than MoR and reads a range
  {eval}`f"{v(U, S3X, 'MoR', 'window_s') / v(U, S3X, 'RMoR', 'window_s'):.1f}"` times as fast;
  RMoW-external is faster still at both.** On S3 Express RMoR's writes take
  {eval}`f1(v(U, S3X, "RMoR", "write_s"))` s on average against MoR's
  {eval}`f1(v(U, S3X, "MoR", "write_s"))`, and its 1% range reads
  {eval}`f1(v(U, S3X, "RMoR", "window_s"))` s against {eval}`f1(v(U, S3X, "MoR", "window_s"))`.
  RMoW-external takes {eval}`f1(v(U, S3X, "RMoW-external", "write_s"))` s to write and
  {eval}`f1(v(U, S3X, "RMoW-external", "window_s"))` s to read a range: the footers it appends
  drop the rows it replaced, so it reads no deletion vectors, and fewer files. RMoR-density takes
  {eval}`f1(v(U, S3X, "RMoR-density", "write_s"))` s and {eval}`f1(v(U, S3X, "RMoR-density", "window_s"))`
  s (sections 1 and 2).
- **MoR slows as snapshots are added; RMoR does not.** Each MoR write reads more than the last to
  find the rows it changes, from {eval}`f0(WR_MOR[0].mb)` MB at the first to
  {eval}`f0(WR_MOR[1].mb)` MB at the 240th, and its range read takes
  {eval}`f"{at(SNU, 'MoR', READ['window_s'], 240) / at(SNU, 'MoR', READ['window_s'], 1):.1f}"` times as
  long after 240 writes as after the first on S3 Express (section 2).
- **RMoR-density's writes slow as its row groups multiply.** It cuts what it copies into pieces,
  each a row group of its own ({eval}`f0(RG["RMoR-density"][1])` after 240 writes, from
  {eval}`f0(RG["RMoR-density"][0])`), and a write reads the key column of every row group in its
  range of keys to find the rows it changes. That scan takes
  {eval}`f1(LOC["RMoR-density", "first"].s)` s a write over the first 10 writes and
  {eval}`f1(LOC["RMoR-density", "last"].s)` s over the last 10 on S3 Express, most of the
  {eval}`f1(DEN_RISE)` s its write slows by (section 2).
- **RMoR's full scans slow on Azure Blob:** it reads the table in pieces of 1 to 16 MB, which Azure
  Blob serves far more slowly than whole row groups, so after 240 writes a scan takes
  {eval}`f0(at(SNU, "RMoR", READ["full_s"], 240, AZ))` s against MoR's
  {eval}`f0(at(SNU, "MoR", READ["full_s"], 240, AZ))` (section 2).
- **The region designs write far more:** over the 240 writes RMoR writes
  {eval}`f0(gb(WRU, "RMoR") / gb(WRU, "MoR"))` times MoR's {eval}`f1(gb(WRU, "MoR"))` GB, and
  RMoR-density {eval}`f1(gb(WRU, "RMoR-density"))` GB. RMoW-append, copying whole row groups,
  writes {eval}`f1(gb(WRU, "RMoW-append", 30))` GB in its 30 writes (section 3).
- **Inserts: RMoR-density keeps range reads fast for dearer writes.** On S3 Express its range reads
  take {eval}`f1(v(I, S3X, "RMoR-density", "window_s"))` s on average against MoR's
  {eval}`f1(v(I, S3X, "MoR", "window_s"))`, and its writes {eval}`f1(v(I, S3X, "RMoR-density", "write_s"))`
  s against {eval}`f2(v(I, S3X, "MoR", "write_s"))`. Its full scans are slower,
  {eval}`f0(v(I, S3X, "RMoR-density", "full_s"))` s against {eval}`f0(v(I, S3X, "MoR", "full_s"))`,
  over its {eval}`f0(gb(STI, "RMoR-density", col="row_groups"))` row groups (sections 6 to 8).
- **Compaction roughly halves MoR's range reads, at a price in writes.** After 240 writes, Iceberg's
  rule brings MoR's range read on S3 Express from
  {eval}`f1(at(CMU, "MoR", READ["window_s"], 240))` s to
  {eval}`f1(at(CMU, "MoR (compacted)", READ["window_s"], 240))` on the updates and from
  {eval}`f1(at(CMI, "MoR", READ["window_s"], 240))` s to
  {eval}`f1(at(CMI, "MoR (compacted)", READ["window_s"], 240))` on the inserts. With its compactions
  spread over the writes, a write costs {eval}`f1(v(U, S3X, "MoR (compacted)", "write_s"))` s against
  {eval}`f1(v(U, S3X, "MoR", "write_s"))` on the updates and
  {eval}`f1(v(I, S3X, "MoR (compacted)", "write_s"))` s against
  {eval}`f2(v(I, S3X, "MoR", "write_s"))` on the inserts (sections 5 and 10).

## Setup

```{code-cell} ipython3
:tags: [remove-cell]

T0 = ST[ST.step == 0].iloc[0]
init_rows = q("""SELECT DISTINCT value_num FROM run_params JOIN canonical_runs USING (run_id)
                 WHERE list_contains(campaigns, 'w128') AND key = 'workload.initialRows'""").value_num.iloc[0]
rg_rows = init_rows / T0.row_groups
region_rows = q("""SELECT DISTINCT value_num FROM run_params JOIN canonical_runs USING (run_id)
                   WHERE list_contains(campaigns, 'w128') AND key = 'workload.regionRows'""").value_num.iloc[0]
```

**The table:** {eval}`f0(init_rows)` rows (11 columns, about {eval}`f1(T0.kept)` GB of Parquet) in
event-time order, in {eval}`f0(T0.row_groups)` row groups of 128 MiB (about
{eval}`f1(rg_rows / 1e6)` million rows each) in 512 MiB files.

**The workloads**, 240 writes each:
- **updates:** 150,000 changes to existing rows a write, five updates to each delete, clustered
  in regions of {eval}`f0(region_rows)` rows (about 1/{eval}`f0(rg_rows / region_rows)` of a row
  group), some regions far more often than others;
- **inserts:** 350,000 new rows a write, by event time; most arrive on time, and 99% within 5
  writes of their event time.

**The designs:** MoR (deletion vectors, new rows in new files), RMoR (each changed part of a row
group copied into the write's new file, a deletion vector over the old rows), RMoW-external (the
same, the old rows dropped by a new footer appended to their file instead of a deletion vector),
RMoW-append (each changed row group appended whole to its own file with a new footer), and
RMoR-density (RMoR that defers each copy: deletion vectors until a row group's changed rows reach
6% of it, the new versions waiting in a delta row group per region). RMoR, RMoW-external and
RMoR-density copy a row group as pieces, cut where the write changes it, and copy only the pieces
it changes. RMoW-append copies whole 128 MiB row groups, so it ran the first 30 writes. On the
inserts RMoR-density places each late row in the row group whose event-time range holds it, when
that row group's waiting rows reach its threshold. RMoR, RMoW-external and RMoW-append, which
would merge every late row at once, rewriting whole row groups on almost every write, did not run
there ([why](./inserts-128.md)). Each writer finds the rows a write changes by scanning the key
column, as a MERGE would: it reads only the files whose key range may hold a changed key, and in
each of them every row group between the write's smallest and largest changed key (RMoW-append
finds its rows from its footers' statistics alone). The pruning of row groups by each changed key
in [our changes to readers and writers](./reader-changes.md) was not used here.

**The designs are compared with nothing compacted,** as snapshots are added. MoR compacted as
Iceberg plans it (its bin-pack rule, checked after every write: a group of at least 5 files) has
sections of its own (5 and 10).

**The reads**, one at a time, each skipping row groups whose rows are all dead:
- **a range read:** 1% of the table, by id on the updates and by event time on the inserts, timed
  as the mean of five (centred at 0.1, 0.3, 0.5, 0.7 and 0.9 of the span), after every write;
- **a full scan**, every 10 writes;
- **a sorted read:** the whole table in key order, every 10 writes.

Where a section gives one time, a write's is its mean over the 240 writes and a read's over the
reads after 5 to 240 writes.

**Data written** is every byte a design's writes and compactions put in the store after the
initial load, the same on both stores. **Storage** is what the store holds: with every snapshot
kept (no run deletes anything) and with the snapshots before the newest expired.

**How the times are simulated.** Each run's requests and bytes were counted on havoc, and its CPU
by its twin on the laptop, which replays the same writes and counts instructions. A phase's time
is its CPU at the laptop's speed, plus its requests priced by size from a latency test of the
store (S3 Express on a `c7a.4xlarge` in usw2-az3, Azure Blob on a `D8as_v6` in westus). No run at
128 MiB has been measured in the cloud.

```{code-cell} ipython3
:tags: [remove-input]

table(runs)
```

## Updates

### 1. Write latency against read latency

On the updates RMoR costs less than MoR for writes and for range reads. Its writes take
{eval}`f1(v(U, S3X, "RMoR", "write_s"))` s on average on S3 Express against MoR's
{eval}`f1(v(U, S3X, "MoR", "write_s"))`, and its 1% range reads
{eval}`f1(v(U, S3X, "RMoR", "window_s"))` s against {eval}`f1(v(U, S3X, "MoR", "window_s"))`.
RMoW-external, which drops the old rows with a new footer instead of a deletion vector, is the
fastest at both: it writes in {eval}`f1(v(U, S3X, "RMoW-external", "write_s"))` s and reads a range
in {eval}`f1(v(U, S3X, "RMoW-external", "window_s"))` s, reading no deletion vectors, and fewer files
(section 2). RMoR-density writes in {eval}`f1(v(U, S3X, "RMoR-density", "write_s"))` s and reads a
range in {eval}`f1(v(U, S3X, "RMoR-density", "window_s"))` s. MoR's writes cost more because each reads more than the last (section 2). On Azure Blob RMoR's
full scans are slower than MoR's, {eval}`f0(v(U, AZ, "RMoR", "full_s"))` s against
{eval}`f0(v(U, AZ, "MoR", "full_s"))`, since they read the table in smaller pieces, which Azure Blob
serves more slowly (section 2).

```{code-cell} ipython3
:label: workloads-128-updates-tradeoff
:tags: [remove-input]
:class: col-page-right

UM = U[U.name.isin([n for n in UN if n != "RMoW-append"])]
fig.wl_tradeoff(UM, READS_U)
```

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(UM, READS_U), caption="Each design's mean write and reads over the 240 writes, simulated (s), "
      "with the writes' 10th and 90th percentiles and the fastest and slowest read")
```

RMoW-append ran the first 30 writes. Over those, the designs compare as follows:

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(U30, READS_U), caption="Each design's mean write and reads over the first 30 writes, simulated (s)")
```

RMoW-append's writes cost the most, {eval}`f1(v(U30, S3X, "RMoW-append", "write_s"))` s on S3
Express against RMoR's {eval}`f1(v(U30, S3X, "RMoR", "write_s"))`, as each copies whole 128 MiB
row groups. Its range reads are the fastest, {eval}`f2(v(U30, S3X, "RMoW-append", "window_s"))` s.

### 2. Latency as snapshots are added

```{code-cell} ipython3
:tags: [remove-cell]

WR_RMOR = at_step("w128-upd-rmor", "batch", 240)
WR_DEN = [at_step("w128-upd-density", "batch", n) for n in (1, 240)]
FULL_U = {n: at_step(l, "read_full", 240) for l, _, n in UPD}
WIN_RANGE = {n: series(SNU, n, READ["window_s"]) for n in ["RMoR", "RMoW-external", "RMoR-density"]}


def scan_sizes(label, step=240):
    """Havoc's run's full scan after a write: its reads of 1 to 16 MB and of 64 MB or more (count, GB)."""
    return q(f"""SELECT count(*) FILTER (WHERE q.length >= 1e6 AND q.length < 16e6) AS mid_n,
                        sum(q.length) FILTER (WHERE q.length >= 1e6 AND q.length < 16e6) / 1e9 AS mid_gb,
                        count(*) FILTER (WHERE q.length >= 64e6) AS big_n,
                        sum(q.length) FILTER (WHERE q.length >= 64e6) / 1e9 AS big_gb
                 FROM requests q JOIN canonical_runs r USING (run_id)
                 WHERE list_contains(r.campaigns, 'w128') AND r.label = '{label}' AND q.phase = 'read_full'
                   AND q.step = {step} AND q.reader = 'cached-skip' AND q.kind = 'get_range'""").iloc[0]


SCAN_RMOR, SCAN_MOR = scan_sizes("w128-upd-rmor"), scan_sizes("w128-upd-mor")
# how fast a read streams at a size, by the latency test the times are priced with (views/65_sized.sql)
RATE = q(f"""SELECT profile, bytes, bytes / 1e6 / (ms / 1e3) AS mb_s FROM sized_points
             WHERE op = 'get' AND profile IN ({", ".join(f"'{s}'" for s in tune.SPIKES.values())})""")


def rate(store, mib):
    p = tune.SPIKES[{S3X: "s3x", AZ: "azure"}[store]]
    return RATE[(RATE.profile == p) & (RATE.bytes == mib << 20)].mb_s.iloc[0]


def files_read(label, phase, step=240):
    """Havoc's run's write (phase batch) or reads after it, at a write: its reads of deletion
    vectors, and the Parquet files it read from."""
    return q(f"""SELECT count(*) FILTER (WHERE q.object LIKE '%.puffin') AS dvs,
                        count(DISTINCT q.object) FILTER (WHERE q.object LIKE '%.parquet') AS files
                 FROM requests q JOIN canonical_runs r USING (run_id)
                 WHERE list_contains(r.campaigns, 'w128') AND r.label = '{label}' AND q.phase = '{phase}'
                   AND q.step = {step} AND (q.phase = 'batch' OR q.reader = 'cached-skip')
                   AND q.kind = 'get_range'""").iloc[0]


FR = {(n, what): files_read(l, phase) for l, n in [("w128-upd-rmor", "RMoR"), ("w128-upd-external", "RMoW-external")]
      for what, phase in [("write", "batch"), ("window", "read_window")]}

```

MoR's writes slow as writes are added: on S3 Express a write takes
{eval}`f1(mean_over(SNU, "MoR", WRITE, 1, 10))` s on average over the first 10 writes and
{eval}`f1(mean_over(SNU, "MoR", WRITE, 231, 240))` s over the last 10. To find the rows a write
changes, MoR reads every file whose keys may hold one, and every earlier write left a file of new
versions spread over the keys it changed: its first write reads {eval}`f0(WR_MOR[0].mb)` MB, its
240th {eval}`f0(WR_MOR[1].mb)` MB. RMoR's 240th write reads {eval}`f0(WR_RMOR.mb)` MB, and its writes
take {eval}`f1(mean_over(SNU, "RMoR", WRITE, 1, 10))` s over the first 10 and
{eval}`f1(mean_over(SNU, "RMoR", WRITE, 231, 240))` s over the last 10; RMoW-external's
{eval}`f1(mean_over(SNU, "RMoW-external", WRITE, 1, 10))` s and
{eval}`f1(mean_over(SNU, "RMoW-external", WRITE, 231, 240))` s. RMoR-density's writes slow, from
{eval}`f1(mean_over(SNU, "RMoR-density", WRITE, 1, 10))` s over the first 10 to
{eval}`f1(mean_over(SNU, "RMoR-density", WRITE, 231, 240))` s over the last 10, and the requests a
write sends grow from {eval}`f0(WR_DEN[0].requests)` at its first to
{eval}`f0(WR_DEN[1].requests)` at its 240th.

Most of RMoR-density's slowing is the scan that finds a write's rows. For each file whose keys may
hold a changed key, it reads the file's footer (two requests) and its deletion vector, then the key
column of every row group between the write's smallest and largest changed key (setup), one request
each. RMoR-density cuts what it copies into pieces, each a row group of its own, so its table goes
from {eval}`f0(RG["RMoR-density"][0])` row groups to {eval}`f0(RG["RMoR-density"][1])` over the 240
writes, and the scan with it: over the first 10 writes it sends {eval}`f0(LOC["RMoR-density", "first"].requests)`
requests a write and takes {eval}`f1(LOC["RMoR-density", "first"].s)` s on S3 Express, over the last
10 {eval}`f0(LOC["RMoR-density", "last"].requests)` requests ({eval}`f0(LOC["RMoR-density", "last"].footers)`
for footers, {eval}`f0(LOC["RMoR-density", "last"].dvs)` for deletion vectors and
{eval}`f0(LOC["RMoR-density", "last"].key_column)` for the key column) and
{eval}`f1(LOC["RMoR-density", "last"].s)` s, {eval}`f1(LOC["RMoR-density", "last"].s - LOC["RMoR-density", "first"].s)`
of the {eval}`f1(DEN_RISE)` s its write slows by. RMoR's scan grows too, from
{eval}`f1(LOC["RMoR", "first"].s)` to {eval}`f1(LOC["RMoR", "last"].s)` s, over a table cut into
{eval}`f0(RG["RMoR"][1])` row groups (dead ones included), but RMoR copies less as its pieces get
smaller ({eval}`f0(COPY_RMOR[0])` MB a write over the first 10 writes, {eval}`f0(COPY_RMOR[1])` over
the last 10), which offsets it. RMoR-density copies little from the start, since it defers. MoR's
scan grows from {eval}`f1(LOC["MoR", "first"].s)` to {eval}`f1(LOC["MoR", "last"].s)` s, a footer
and a deletion vector for each file its writes added, and RMoW-external's least, from
{eval}`f1(LOC["RMoW-external", "first"].s)` to {eval}`f1(LOC["RMoW-external", "last"].s)` s, over files
whose statistics its footers narrow (below).

MoR's 1% range read slows the same way, from {eval}`f1(at(SNU, "MoR", READ["window_s"], 1))` s after
the first write to {eval}`f1(at(SNU, "MoR", READ["window_s"], 240))` s after the 240th on S3
Express, and to {eval}`f1(at(SNU, "MoR", READ["window_s"], 240, AZ))` s on Azure Blob. RMoR's
stays between {eval}`f1(WIN_RANGE["RMoR"].min())` and {eval}`f1(WIN_RANGE["RMoR"].max())` s,
RMoW-external's between {eval}`f1(WIN_RANGE["RMoW-external"].min())` and
{eval}`f1(WIN_RANGE["RMoW-external"].max())` s, and RMoR-density's between
{eval}`f1(WIN_RANGE["RMoR-density"].min())` and {eval}`f1(WIN_RANGE["RMoR-density"].max())` s.
RMoW-append's, over its 30 writes, stays at {eval}`f2(at(SNU, "RMoW-append", READ["window_s"], 30))`
s: its writes leave the table as many live row groups as before, and no deletes to apply.

RMoW-external reads less than RMoR, for writes and for range reads. A footer it appends to a file
lists only the row groups the file still holds, and the file's statistics are taken from that
footer, so a read can pass over a file whose rows in its range were all replaced. RMoR's files keep
the statistics of every row they held, with a deletion vector over those replaced. The five range
reads after the 240th write read from {eval}`f0(FR["RMoW-external", "window"].files)` files and no
deletion vectors on RMoW-external's table, from {eval}`f0(FR["RMoR", "window"].files)` files and
{eval}`f0(FR["RMoR", "window"].dvs)` deletion vectors on RMoR's. The 240th write itself reads from
{eval}`f0(FR["RMoW-external", "write"].files)` files on RMoW-external's table, and from
{eval}`f0(FR["RMoR", "write"].files)` files and {eval}`f0(FR["RMoR", "write"].dvs)` deletion vectors
on RMoR's.

RMoR's full scans hold steady on S3 Express and slow on Azure Blob, from
{eval}`f0(at(SNU, "RMoR", READ["full_s"], 10, AZ))` s after 10 writes to
{eval}`f0(at(SNU, "RMoR", READ["full_s"], 240, AZ))` s after 240 (MoR's:
{eval}`f0(at(SNU, "MoR", READ["full_s"], 240, AZ))` s). After 240 writes RMoR's table is
{eval}`f0(gb(ST[ST.label == "w128-upd-rmor"], "RMoR", col="row_groups"))` row groups to MoR's
{eval}`f0(gb(ST[ST.label == "w128-upd-mor"], "MoR", col="row_groups"))`, so a full scan reads most
of it in pieces: {eval}`f1(SCAN_RMOR.mid_gb)` GB in {eval}`f0(SCAN_RMOR.mid_n)` reads of 1 to 16
MB, where MoR's reads {eval}`f1(SCAN_MOR.big_gb)` GB in {eval}`f0(SCAN_MOR.big_n)` reads of whole row
groups. By the stores' latency tests, a read streams about as fast at either size on S3 Express
({eval}`f0(rate(S3X, 8))` MB/s at 8 MiB, {eval}`f0(rate(S3X, 128))` at 128 MiB), but not on Azure
Blob ({eval}`f0(rate(AZ, 8))` and {eval}`f0(rate(AZ, 128))` MB/s).

```{code-cell} ipython3
:label: workloads-128-updates
:tags: [remove-input]
:class: col-page-right

snapshot_chart(SNU, READS_U)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNU, WRITE, every=10)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNU, READ["window_s"], every=10)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNU, READ["full_s"], every=10)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNU, READ["sorted_s"], every=10)
```

### 3. Data written and storage as snapshots are added

```{code-cell} ipython3
:tags: [remove-cell]

UNAMES = [n for _, _, n in UPD]
REG = ["RMoR", "RMoW-external"]
```

Over the 240 writes MoR writes {eval}`f2(gb(WRU, "MoR"))` GB. RMoR and RMoW-external write
{eval}`span([gb(WRU, n) for n in REG], "{:.1f}")` GB,
{eval}`f0(gb(WRU, "RMoR") / gb(WRU, "MoR"))` times as much, and RMoR-density
{eval}`f1(gb(WRU, "RMoR-density"))` GB. RMoW-append, copying whole row groups, writes
{eval}`f1(gb(WRU, "RMoW-append", 30))` GB in its 30 writes, where RMoR writes
{eval}`f1(gb(WRU, "RMoR", 30))`. MoR (compacted) writes {eval}`f1(gb(WRU, "MoR (compacted)"))` GB, most
of it in its compactions (section 5).

```{code-cell} ipython3
:label: workloads-128-updates-written
:tags: [remove-input]

written_chart(WRU, UNAMES)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_written_table(WRU, UNAMES, every=40)
```

Expiring old snapshots frees what a design replaced whole: MoR (compacted)'s compacted files,
from {eval}`f1(gb(STU, "MoR (compacted)", col="kept"))` GB to
{eval}`f1(gb(STU, "MoR (compacted)", col="expired"))` after 240 writes. Of what RMoR holds it frees
{eval}`f"{1 - gb(STU, 'RMoR', col='expired') / gb(STU, 'RMoR', col='kept'):.0%}"`,
{eval}`f1(gb(STU, "RMoR", col="kept"))` GB to {eval}`f1(gb(STU, "RMoR", col="expired"))` (MoR:
{eval}`f1(gb(STU, "MoR", col="expired"))`), and as much of RMoW-external's: the rows they copied out
of a row group stay in its file as long as any row of the file is live.

```{code-cell} ipython3
:label: workloads-128-updates-storage
:tags: [remove-input]

storage_chart(STU, UNAMES)
```

```{code-cell} ipython3
:tags: [remove-input]

storage_table(STU, UNAMES)
```

### 4. Where the time goes

```{code-cell} ipython3
:tags: [remove-cell]

UP = parts(UPD, [n for n in UN if n != "RMoW-append"])


def twin_phase(label, phase, first=1, last=240):
    """The laptop twin's part of a write (locate: the scan for the rows to change; merge-read: the
    read of what it copies) over writes first to last: decode CPU a write (s, on the wall clock), MB
    read a write, and decode ms a MB."""
    return q(f"""SELECT sum(s.decode_cpu_ns / c.cpu_per_wall) / 1e9 / count(*) AS decode_s,
                        sum(s.bytes_read) / 1e6 / count(*) AS mb,
                        sum(s.decode_cpu_ns / c.cpu_per_wall) / 1e6 / (sum(s.bytes_read) / 1e6) AS ms_mb
                 FROM steps s JOIN canonical_runs r USING (run_id) JOIN run_clock c USING (run_id)
                 WHERE list_contains(r.campaigns, 'w128-twins') AND r.label = '{label}' AND s.phase = '{phase}'
                   AND s.step BETWEEN {first} AND {last}""").iloc[0]


DEC = {(n, ph): twin_phase(l, ph) for l, n in [("w128-upd-rmor", "RMoR"), ("w128-upd-external", "RMoW-external")]
       for ph in ("locate", "merge-read")}
DEC_MB = {(n, b): twin_phase(l, "locate", *span).ms_mb
          for l, n in [("w128-upd-mor", "MoR"), ("w128-upd-density", "RMoR-density"), ("w128-upd-rmor", "RMoR"),
                       ("w128-upd-external", "RMoW-external")]
          for b, span in [("first", (1, 10)), ("last", (231, 240))]}
```

On S3 Express MoR's write spends {eval}`f1(part_s(UP, "MoR", WRITE, "decode"))` s decoding the
files it reads to find the changed rows, and {eval}`f1(part_s(UP, "MoR", WRITE, "I/O"))` s on I/O.
RMoR's spends {eval}`f1(part_s(UP, "RMoR", WRITE, "decode"))` s decoding and
{eval}`f1(part_s(UP, "RMoR", WRITE, "I/O"))` s on I/O, and {eval}`f1(part_s(UP, "RMoR", WRITE, "encode"))`
s encoding the pieces of row groups it copies. RMoW-external's spends
{eval}`f1(part_s(UP, "RMoW-external", WRITE, "decode"))` s decoding and
{eval}`f1(part_s(UP, "RMoW-external", WRITE, "I/O"))` s on I/O, and its range read
{eval}`f2(part_s(UP, "RMoW-external", READ["window_s"], "I/O"))` s on I/O against RMoR's
{eval}`f2(part_s(UP, "RMoR", READ["window_s"], "I/O"))`: it opens fewer files and reads no deletion
vectors (section 2).

RMoW-external's write decodes less than RMoR's in its scan for the rows to change, not in what it
copies. By the laptop twins' CPU, RMoR's scan decodes for {eval}`f1(DEC["RMoR", "locate"].decode_s)` s
a write and RMoW-external's for {eval}`f1(DEC["RMoW-external", "locate"].decode_s)`, while their reads
of the rows they copy decode alike ({eval}`f2(DEC["RMoR", "merge-read"].decode_s)` and
{eval}`f2(DEC["RMoW-external", "merge-read"].decode_s)` s). RMoW-external's scan reads less of the key
column, {eval}`f0(DEC["RMoW-external", "locate"].mb)` MB a write against
{eval}`f0(DEC["RMoR", "locate"].mb)`, as fewer of its files pass on their statistics, and decodes each
MB faster, {eval}`f0(DEC["RMoW-external", "locate"].ms_mb)` ms against
{eval}`f0(DEC["RMoR", "locate"].ms_mb)`. It is the one design whose scan applies no deletion vectors
to the rows it decodes, and it stays as fast as its table ages
({eval}`f0(DEC_MB["RMoW-external", "first"])` ms a MB over the first 10 writes,
{eval}`f0(DEC_MB["RMoW-external", "last"])` over the last 10), where the designs with deletion
vectors decode more slowly as they accumulate: MoR from {eval}`f0(DEC_MB["MoR", "first"])` to
{eval}`f0(DEC_MB["MoR", "last"])` ms a MB, RMoR-density from {eval}`f0(DEC_MB["RMoR-density", "first"])`
to {eval}`f0(DEC_MB["RMoR-density", "last"])`, and RMoR, with deletion vectors over its copied
pieces from its first writes, {eval}`f0(DEC_MB["RMoR", "first"])` and
{eval}`f0(DEC_MB["RMoR", "last"])`. MoR's range read is
{eval}`f"{share_of(UP, S3X, 'MoR', READ['window_s'], 'I/O'):.0%}"` I/O. On Azure Blob RMoR's full
scan spends {eval}`f0(part_s(UP, "RMoR", READ["full_s"], "I/O", AZ))` s on I/O against MoR's
{eval}`f0(part_s(UP, "MoR", READ["full_s"], "I/O", AZ))`, for its smaller reads (section 2).

```{code-cell} ipython3
:label: workloads-128-updates-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(UP, [WRITE, READ["window_s"], READ["full_s"]], [n for n in UN if n != "RMoW-append"], PARTS)
```

```{code-cell} ipython3
:tags: [remove-input]

table(UP.pivot_table(index=["Store", "what", "name"], columns="part", values="s", sort=False)
      .reindex(columns=[p for p in PARTS if p in set(UP.part)]).reset_index().round(2),
      caption="Where the time goes, simulated (s), over the 240 writes")
```

### 5. Compaction

```{code-cell} ipython3
:tags: [remove-cell]

# each compaction that did anything (havoc's runs): how many, from which write, the bytes the first
# read, the bytes each read (least, most) and all they wrote
COMP = q("""SELECT s.label, count(*) AS n, min(s.step) AS first, arg_min(s.bytes_read, s.step) / 1e6 AS first_mb,
                   min(s.bytes_read) / 1e6 AS lo_mb, max(s.bytes_read) / 1e6 AS hi_mb,
                   sum(s.bytes_written) / 1e9 AS gb
            FROM steps s JOIN canonical_runs r USING (run_id)
            WHERE list_contains(r.campaigns, 'w128') AND s.phase = 'compact' AND s.requests > 0
            GROUP BY 1""").set_index("label")


def compaction_s(d, store=S3X):
    """The simulated time of each compaction (MoR (compacted)'s compaction phases that did anything)."""
    return d[(d.Store == store) & (d.what == "a compaction")].s


def c(label, col):
    return COMP.loc[label, col]
```

Iceberg's rule compacted after {eval}`f0(c("w128-upd-compact", "n"))` of the 240 writes, the
first after write {eval}`f0(c("w128-upd-compact", "first"))` (the crosses). Each compaction
rewrites the small files written since the last together with the part-full file the previous
compaction wrote, so it reads more each time, up to {eval}`f0(c("w128-upd-compact", "hi_mb"))` MB,
until that file is full and the next one starts another from
{eval}`f0(c("w128-upd-compact", "lo_mb"))` MB. The first took in the last file of the table as
loaded, which was part full, so it read {eval}`f0(c("w128-upd-compact", "first_mb"))` MB, and its
climb starts high. Its time climbs with it, from {eval}`f1(compaction_s(CMU).min())` s to
{eval}`f1(compaction_s(CMU).max())` s on S3 Express. In all they write
{eval}`f1(c("w128-upd-compact", "gb"))` GB. Spread over the writes, a write costs
{eval}`f1(v(U, S3X, "MoR (compacted)", "write_s"))` s on S3 Express against MoR's
{eval}`f1(v(U, S3X, "MoR", "write_s"))`.

They keep the reads from slowing as MoR's do. After 240 writes a 1% range read takes
{eval}`f1(at(CMU, "MoR (compacted)", READ["window_s"], 240))` s on S3 Express against MoR's
{eval}`f1(at(CMU, "MoR", READ["window_s"], 240))`, and
{eval}`f1(at(CMU, "MoR (compacted)", READ["window_s"], 240, AZ))` s against
{eval}`f1(at(CMU, "MoR", READ["window_s"], 240, AZ))` on Azure Blob; a sorted read
{eval}`f0(at(CMU, "MoR (compacted)", READ["sorted_s"], 240))` s against
{eval}`f0(at(CMU, "MoR", READ["sorted_s"], 240))` on S3 Express. Over the first writes its range
reads are slower than MoR's: {eval}`f1(at(CMU, "MoR (compacted)", READ["window_s"], 10))` s against
{eval}`f1(at(CMU, "MoR", READ["window_s"], 10))` after 10 writes.

```{code-cell} ipython3
:label: workloads-128-updates-compaction
:tags: [remove-input]
:class: col-page-right

snapshot_chart(CMU, READS_U)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(CMU, READ["window_s"], every=10)
```

## Inserts

### 6. Write latency against read latency

On the inserts MoR's writes cost least by far. Each one adds a file of the new rows, taking
{eval}`f2(v(I, S3X, "MoR", "write_s"))` s on S3 Express. RMoR-density's writes take
{eval}`f1(v(I, S3X, "RMoR-density", "write_s"))` s on average (from
{eval}`f1(v(I, S3X, "RMoR-density", "write_lo"))` to {eval}`f1(v(I, S3X, "RMoR-density", "write_hi"))`
s, the 10th to the 90th percentile), as each rewrites the newest rows waiting to fill a row group
(section 7). In return its 1% range reads take {eval}`f1(v(I, S3X, "RMoR-density", "window_s"))` s
against MoR's {eval}`f1(v(I, S3X, "MoR", "window_s"))`. Its full scans are slower,
{eval}`f0(v(I, S3X, "RMoR-density", "full_s"))` s against {eval}`f0(v(I, S3X, "MoR", "full_s"))`.
Its sorted reads take {eval}`f0(v(I, S3X, "RMoR-density", "sorted_s"))` s against
{eval}`f0(v(I, S3X, "MoR", "sorted_s"))` on S3 Express, and
{eval}`f0(v(I, AZ, "RMoR-density", "sorted_s"))` against {eval}`f0(v(I, AZ, "MoR", "sorted_s"))` on
Azure Blob.

```{code-cell} ipython3
:label: workloads-128-inserts-tradeoff
:tags: [remove-input]
:class: col-page-right

IM = I[I.name.isin(IN)]
fig.wl_tradeoff(IM, READS_U)
```

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(IM, READS_U), caption="Each design's mean write and reads over the 240 writes, simulated (s), "
      "with the writes' 10th and 90th percentiles and the fastest and slowest read")
```

### 7. Latency as snapshots are added

```{code-cell} ipython3
:tags: [remove-cell]

WIN_MOR, WIN_DEN = at_step("w128-ing-mor", "read_window", 240), at_step("w128-ing-density", "read_window", 240)
FULL_MOR, FULL_DEN = at_step("w128-ing-mor", "read_full", 240), at_step("w128-ing-density", "read_full", 240)
# what each of RMoR-density's writes wrote (MB, havoc's run): the newest rows' delta row group grows
# by a write's rows, then starts again once they fill a row group
DW = q("""SELECT s.step, s.bytes_written / 1e6 AS mb FROM steps s JOIN canonical_runs r USING (run_id)
          WHERE list_contains(r.campaigns, 'w128') AND s.label = 'w128-ing-density' AND s.phase = 'batch'
            AND s.step >= 1 ORDER BY 1""")
DW_GROWTH = DW.mb.diff()[DW.mb.diff() > 0].median()
DW_CYCLE = len(DW) / (DW.mb.diff() < 0).sum()


def passes(d, a, b, what, store=S3X):
    """The first write after which a's time stays above b's."""
    x = (series(d, a, what, store) - series(d, b, what, store)).dropna()
    below = x[x <= 0]
    return x.index[x.index > below.index.max()].min() if len(below) else x.index.min()


DEN_W = series(SNI, "RMoR-density", WRITE)
```

MoR's 1% range read slows as writes are added, from
{eval}`f1(at(SNI, "MoR", READ["window_s"], 10))` s after 10 writes to
{eval}`f1(at(SNI, "MoR", READ["window_s"], 240))` s after 240 on S3 Express, and to
{eval}`f1(at(SNI, "MoR", READ["window_s"], 240, AZ))` s on Azure Blob. Each write's file holds a few
rows that arrived late, so its event times reach back over older ranges, and a range read over them
must read it. After 240 writes MoR's range read reads {eval}`f0(WIN_MOR.mb)` MB for
{eval}`f1(WIN_MOR.rows / 1e6)` million rows. RMoR-density's reads {eval}`f0(WIN_DEN.mb)` MB, since
it places each late row in the row group that holds its event time; its range read never takes
more than {eval}`f1(series(SNI, "RMoR-density", READ["window_s"]).max())` s on S3 Express.

RMoR-density's writes rise and fall. The newest rows wait in a delta row group, which each write
reads and writes again with its own rows added, about {eval}`f0(DW_GROWTH)` MB more each time.
Every {eval}`f0(DW_CYCLE)` writes they fill a row group, and the next write starts a new delta. Its
writes take from {eval}`f1(DEN_W.min())` s to {eval}`f1(DEN_W.max())` s on S3 Express.

Its full scans and sorted reads slow faster than MoR's. They read the same bytes in many more,
smaller row groups (section 8 counts them): after 240 writes a full scan reads
{eval}`f1(FULL_DEN.mb / 1e3)` GB in {eval}`f0(FULL_DEN.requests)` requests, MoR's
{eval}`f1(FULL_MOR.mb / 1e3)` GB in {eval}`f0(FULL_MOR.requests)`. Its sorted read is slower than
MoR's after {eval}`f0(passes(SNI, "RMoR-density", "MoR", READ["sorted_s"]))` writes on S3 Express.

```{code-cell} ipython3
:label: workloads-128-inserts
:tags: [remove-input]
:class: col-page-right

snapshot_chart(SNI, READS_U)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNI, WRITE, every=10)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNI, READ["window_s"], every=10)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNI, READ["sorted_s"], every=10)
```

### 8. Data written and storage as snapshots are added

```{code-cell} ipython3
:tags: [remove-cell]

INAMES = [n for _, _, n in ING]
```

Over the 240 writes MoR writes {eval}`f1(gb(WRI, "MoR"))` GB, RMoR-density
{eval}`f1(gb(WRI, "RMoR-density"))` GB and MoR (compacted) {eval}`f1(gb(WRI, "MoR (compacted)"))` GB.

```{code-cell} ipython3
:label: workloads-128-inserts-written
:tags: [remove-input]

written_chart(WRI, INAMES)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_written_table(WRI, INAMES, every=40)
```

Expiring old snapshots frees nothing of RMoR-density's: after 240 writes it holds
{eval}`f1(gb(STI, "RMoR-density", col="expired"))` GB either way, against MoR's
{eval}`f1(gb(STI, "MoR", col="expired"))` and MoR (compacted)'s
{eval}`f1(gb(STI, "MoR (compacted)", col="expired"))` once its compacted files are expired (its line
lies on MoR's). Each row
group it copies stays in its file, under a deletion vector, as long as any row of the file is
live. It holds them in {eval}`f0(gb(STI, "RMoR-density", col="row_groups"))` row groups, where MoR has
{eval}`f0(gb(STI, "MoR", col="row_groups"))`.

```{code-cell} ipython3
:label: workloads-128-inserts-storage
:tags: [remove-input]

storage_chart(STI, INAMES)
```

```{code-cell} ipython3
:tags: [remove-input]

storage_table(STI, INAMES)
```

### 9. Where the time goes

```{code-cell} ipython3
:tags: [remove-cell]

IP = parts(ING, IN)
```

A MoR write takes {eval}`f2(part_s(IP, "MoR", WRITE))` s: encoding the new rows, and one upload.
Most of RMoR-density's write is the delta it writes again (section 7):
{eval}`f1(part_s(IP, "RMoR-density", WRITE, "encode"))` s encoding and
{eval}`f1(part_s(IP, "RMoR-density", WRITE, "I/O"))` s of I/O on S3 Express. MoR's range read is
{eval}`f"{share_of(IP, S3X, 'MoR', READ['window_s'], 'I/O'):.0%}"` I/O, the bytes of section 7. A
full scan spends about as long decoding in either design; RMoR-density's spends more on I/O,
{eval}`f0(part_s(IP, "RMoR-density", READ["full_s"], "I/O"))` s against MoR's
{eval}`f0(part_s(IP, "MoR", READ["full_s"], "I/O"))` on S3 Express, for its many more requests.

```{code-cell} ipython3
:label: workloads-128-inserts-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(IP, [WRITE, READ["window_s"], READ["sorted_s"]], IN, PARTS)
```

```{code-cell} ipython3
:tags: [remove-input]

table(IP.pivot_table(index=["Store", "what", "name"], columns="part", values="s", sort=False)
      .reindex(columns=[p for p in PARTS if p in set(IP.part)]).reset_index().round(2),
      caption="Where the time goes, simulated (s), over the 240 writes")
```

### 10. Compaction

On the inserts the rule compacted after {eval}`f0(c("w128-ing-compact", "n"))` writes, the first
after write {eval}`f0(c("w128-ing-compact", "first"))`. Again each compaction reads the part-full
file the previous one wrote, from {eval}`f0(c("w128-ing-compact", "lo_mb"))` MB to
{eval}`f0(c("w128-ing-compact", "hi_mb"))` MB (the first, with the part-full last file of the table
as loaded, {eval}`f0(c("w128-ing-compact", "first_mb"))` MB), taking from {eval}`f1(compaction_s(CMI).min())` s to
{eval}`f1(compaction_s(CMI).max())` s on S3 Express; in all they write
{eval}`f1(c("w128-ing-compact", "gb"))` GB. Spread over the writes, a write costs
{eval}`f1(v(I, S3X, "MoR (compacted)", "write_s"))` s against MoR's
{eval}`f2(v(I, S3X, "MoR", "write_s"))`.

After 240 writes a 1% range read takes {eval}`f1(at(CMI, "MoR (compacted)", READ["window_s"], 240))`
s against MoR's {eval}`f1(at(CMI, "MoR", READ["window_s"], 240))` on S3 Express, and
{eval}`f1(at(CMI, "MoR (compacted)", READ["window_s"], 240, AZ))` s against
{eval}`f1(at(CMI, "MoR", READ["window_s"], 240, AZ))` on Azure Blob; a sorted read
{eval}`f0(at(CMI, "MoR (compacted)", READ["sorted_s"], 240))` s against
{eval}`f0(at(CMI, "MoR", READ["sorted_s"], 240))` on S3 Express. A full scan, which reads every row
either way, takes {eval}`f0(at(CMI, "MoR (compacted)", READ["full_s"], 240))` s against
{eval}`f0(at(CMI, "MoR", READ["full_s"], 240))` on S3 Express, and
{eval}`f0(at(CMI, "MoR (compacted)", READ["full_s"], 240, AZ))` s against
{eval}`f0(at(CMI, "MoR", READ["full_s"], 240, AZ))` on Azure Blob.

```{code-cell} ipython3
:label: workloads-128-inserts-compaction
:tags: [remove-input]
:class: col-page-right

snapshot_chart(CMI, READS_U)
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(CMI, READ["window_s"], every=10)
```

## Limits

- **Simulated, not measured.** No run at 128 MiB has run in the cloud; the times price havoc's
  requests by size and add the laptop's CPU.
- **One table, one seed.** The narrow 11-column table of the 16 MiB runs at 8 times the rows.
- **Writers prune row groups only to a write's range of keys,** not by each changed key (`locate`
  `keys`, [Appendix A](./reader-changes.md)), as the 16 MiB runs did. The scan for a write's rows
  (section 2) reads the key column of every row group in that range.
- **RMoR, RMoW-external and RMoW-append did not run on the inserts,** and RMoW-append ran 30 of
  the updates' 240 writes.
