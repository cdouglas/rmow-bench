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


runs = q(f"""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
             WHERE {GRID} ORDER BY label, host""")
```

What each design costs a writer and its readers at Iceberg's 128 MiB row groups, on two workloads
apart, **updates** and **inserts**, over 240 writes on S3 Express and Azure Blob. The times are
simulated from local runs.

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
there ([why](./inserts-128.md)). Each writer finds the rows a write changes by key, reading only
the row groups whose statistics may hold a changed key ([our changes to readers and
writers](./reader-changes.md)).

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

### 2. Latency as snapshots are added

```{code-cell} ipython3
:tags: [remove-cell]

SNU = snapshots(UPD, UN, READS_U)
```

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

WRU, STU = written(UPD), stored(UPD)
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
```

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

CMP = ["MoR", "MoR (compacted)"]
CMU = snapshots(UPD, CMP, READS_U)
```

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

SNI = snapshots(ING, IN, READS_U)
```

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

WRI, STI = written(ING), stored(ING)
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

```{code-cell} ipython3
:tags: [remove-cell]

CMI = snapshots(ING, CMP, READS_U)
```

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
- **One table, one seed.** The narrow 11-column table of the 16 MiB pages at 8 times the rows.
- **RMoR, RMoW-external and RMoW-append did not run on the inserts,** and RMoW-append ran 30 of
  the updates' 240 writes.
