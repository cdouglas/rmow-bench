---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Copies cut where a write changes
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import q, table, theme
from labbook.db import connect
theme()
con = connect()

CAMPAIGNS = ["density128", "density128-cut"]
# (configuration, arm, design, short name)
DESIGNS = [
    ("d128-upd-mor", "mor-v3", "MoR", "MoR"),
    ("d128-upd-compact", "mor-v3", "MoR, compacted every 10 writes", "MoR compacted"),
    ("d128-upd-density", "rmor-density", "RMoR, deferred merges as built (θ 0.06)", "deferred merges"),
    ("d128-upd-copies", "rmor-c-kept", "RMoR, row groups copied whole", "RMoR whole"),
    ("d128-upd-copies", "rmow-c", "RMoW-append, row groups copied whole", "RMoW-append"),
    ("d128-upd-cut", "rmor-c-kept", "RMoR, copies cut (θ 0)", "cut θ 0"),
    ("d128-upd-density-cut", "rmor-density", "RMoR, deferred merges cut (θ 0.06)", "cut θ 0.06"),
]
NAMES = [d[3] for d in DESIGNS]
COLOR = alt.Scale(domain=NAMES, range=["#2a78d6", "#7fb2ea", "#b8478f", "#eda100", "#1baf7a",
                                       "#e8590c", "#5b3f8c"])
VALUES = ", ".join(f"('{l}', '{a}', '{n}', {i})" for i, (l, a, _, n) in enumerate(DESIGNS))
IN = "list_has_any(r.campaigns, ['density128', 'density128-cut'])"

runs = q(f"""
    WITH d(label, arm, name, ord) AS (VALUES {VALUES})
    SELECT d.name, d.label, d.arm, r.run_id, r.status, r.git_sha[:7] AS build,
           json_extract_string(r.manifest, '$.oplog_sha256')[:8] AS oplog, r.cpus,
           round(r.elapsed_s / 60) AS minutes
    FROM d JOIN runs r USING (label) WHERE {IN} ORDER BY d.ord""")
present = set(runs.name)
D = [d for d in DESIGNS if d[3] in present]

# The steps of the designs' arms, from the runs the campaigns hold
STEPS = f"""
    WITH d(label, arm, name, ord) AS (VALUES {VALUES}),
    s AS (
        SELECT d.name, d.ord, s.step, s.phase, s.reader, s.rows, s.bytes_read, s.bytes_written,
               s.requests, s.row_groups, s.data_files
        FROM steps s JOIN canonical_runs r USING (run_id)
        JOIN d ON d.label = r.label AND d.arm = s.arm
        WHERE {IN})"""

# A write's bytes (the rules of D1-D4): its writes are what it puts (write, delete-write), its reads
# its update and merge reads; the locate and MoR's compactions apart
per_write = q(STEPS + """
    SELECT name, ord, step,
           sum(bytes_written) FILTER (WHERE phase IN ('write', 'delete-write')) / 1e6 AS written,
           sum(bytes_read) FILTER (WHERE phase IN ('update-read', 'merge-read')) / 1e6 AS "read",
           coalesce(sum(bytes_read) FILTER (WHERE phase = 'locate'), 0) / 1e6 AS locate,
           coalesce(sum(bytes_written) FILTER (WHERE phase = 'compact'), 0) / 1e6 AS compacted
    FROM s WHERE step >= 1 GROUP BY ALL ORDER BY ord, step""")

# Each read (every 40 writes; every 10 for the whole copies): the five 1% windows' mean over live
# bytes, a live row's bytes being those of the same arm's full read then; the full read over the
# live rows at the initial load's bytes a row (its clean layout, the same in every arm)
reads = q(STEPS + """,
    f AS (SELECT name, ord, step, bytes_read, rows, requests FROM s
          WHERE phase = 'read_full' AND reader = 'cached-skip'),
    clean AS (SELECT name, bytes_read / rows AS b FROM f WHERE step = 0),
    w AS (SELECT name, step, avg(bytes_read) AS mb, avg(requests) AS req,
                 avg(bytes_read / rows) AS b_row, avg(rows) AS rows,
                 list(bytes_read / rows) AS per_row
          FROM s WHERE phase = 'read_window' AND reader = 'cached-skip' GROUP BY ALL),
    st AS (SELECT name, step, row_groups, data_files FROM s WHERE phase = 'state')
    SELECT f.name, f.ord, f.step, w.mb / 1e6 AS window_mb, w.req AS window_req,
           w.mb / (w.rows * f.bytes_read / f.rows) AS window_amp_approx,
           f.bytes_read / 1e6 AS full_mb, f.requests AS full_req,
           f.bytes_read / (f.rows * c.b) AS full_amp, st.row_groups, st.data_files,
           f.bytes_read / f.rows AS full_b_row
    FROM f JOIN w USING (name, step) JOIN clean c USING (name) JOIN st USING (name, step)
    ORDER BY f.ord, f.step""")
# the windows' amplification as the rules state it: the mean of each window's ratio
wamp = q(STEPS + """,
    f AS (SELECT name, step, bytes_read / rows AS b FROM s
          WHERE phase = 'read_full' AND reader = 'cached-skip')
    SELECT name, step, avg(s.bytes_read / (s.rows * f.b)) AS window_amp
    FROM s JOIN f USING (name, step)
    WHERE s.phase = 'read_window' AND s.reader = 'cached-skip' GROUP BY ALL""")
reads = reads.merge(wamp, on=["name", "step"]).drop(columns=["window_amp_approx"])

last = reads.groupby("name").step.max()
end = reads[reads.step == reads.name.map(last)].set_index("name")


def summary(name):
    w = per_write[per_write.name == name]
    return dict(written=w.written.mean(), written_first40=w.written.head(40).mean(),
                written_last40=w.written.tail(40).mean(), read=w.read.mean(),
                read_last40=w.read.tail(40).mean(), locate=w.locate.mean(),
                compacted=w.compacted.mean(), writes=len(w))


S = pd.DataFrame({n: summary(n) for n in NAMES if n in present}).T
S = S.join(end[["window_amp", "full_amp", "window_mb", "window_req", "full_mb", "full_req",
                "row_groups", "data_files"]])
# the deferred merges over the whole copies' 30 writes, for D2
dm30 = per_write[(per_write.name == "deferred merges") & (per_write.step <= 30)].written.mean()

# The model's forecasts (docs/HYPOTHESES.md, 2026-11-09): MB written a write (mean, first 40, last
# 40), MB read a write (mean, last 40), windows and full read over live bytes at the last write
MODEL = {
    "MoR": (3.6, None, None, 1594, 1939, 16.7, 1.18),
    "MoR compacted": (3.6, None, None, 1640, 2069, 7.0, 1.15),
    "deferred merges": (55, 15, 74, 1304, 1413, 3.3, 1.03),
    "RMoR whole": (861, None, None, None, None, 3.3, None),
    "RMoW-append": (861, None, None, None, None, 3.3, None),
    "cut θ 0": (218, 369, 162, 231, 173, 1.01, 1.00),
    "cut θ 0.06": (52, 13, 57, 763, 477, 1.69, 1.03),
}
M = pd.DataFrame(MODEL, index=["written", "written_first40", "written_last40", "read",
                               "read_last40", "window_amp", "full_amp"]).T


def s(name, col):
    return float(S.loc[name, col])


def has(*names):
    return all(n in present for n in names)
```

Does copying a row group as several, cut where a write changes it, let RMoR choose what a write
copies at a finer grain than a row group, without a new reader? This page judges D1–D4
(docs/HYPOTHESES.md, 2026-11-09) on the runs of campaigns `density128` and `density128-cut`,
on havoc: the designs as built at Iceberg's 128 MiB row groups, and the cut
(docs/drafts/region-cut-2026-11-09.md).

## Setup

**The workload** is `win-upd-base`'s updates with 8 times the rows: 205,480,000 rows at the
start (about 6.2 GB of Parquet), 150,000 changes a write, clustered by key region (513,700 rows a
region, about an eighth of a row group), every update reading the rows it changes. Each run
replays the layout model's op log (the SHA-256 below), over 240 writes; the whole copies over the
first 30 (a 30-write op log, the same writes).

**The layout:** 128 MiB row groups in 512 MiB files. **The reads**, every 40 writes (every 10 for
the whole copies): a full read and five 1% windows by id, under `cached-skip`, the reader that skips
row groups and files a deletion vector covers whole. **The cut** (`layout.cut`: changed strides,
runs of at least 4 strides, 20,000 rows a stride) copies a row group as one row group a run of
strides that the write changes or that holds dead rows, and one a run between them.

```{code-cell} ipython3
:tags: [remove-input]

table(runs.drop(columns=["status"]).rename(columns={"name": "design", "minutes": "min"}),
      caption="The runs: one job each on havoc (3 cores)")
```

## What each design writes and reads

```{code-cell} ipython3
:tags: [remove-cell]

def judged(n):
    r = S.loc[n]
    m = M.loc[n]
    return dict(design=n, writes=int(r.writes),
                written=r.written, written_model=m.written,
                first40=r.written_first40, last40=r.written_last40,
                read=r.read, read_model=m.read, read_last40=r.read_last40,
                window_amp=r.window_amp, window_model=m.window_amp,
                full_amp=r.full_amp, full_model=m.full_amp)


J = pd.DataFrame([judged(n) for n in NAMES if n in present])
```

```{code-cell} ipython3
:tags: [remove-input]

table(J.round(2), caption="MB written and read per write (mean over the run's writes, its first 40 "
      "and last 40), and the windows and the full read over live bytes at the last write; "
      "`_model` the layout model's forecast")
```

```{code-cell} ipython3
:tags: [remove-cell]

pw = per_write.copy()
pw["read (10-write mean)"] = pw.groupby("name")["read"].transform(lambda x: x.rolling(10, 1).mean())
pw["written (10-write mean)"] = pw.groupby("name")["written"].transform(lambda x: x.rolling(10, 1).mean())
long = pw.melt(id_vars=["name", "step"], value_vars=["written (10-write mean)", "read (10-write mean)"],
               var_name="what", value_name="mb")
```

```{code-cell} ipython3
:label: cut-per-write
:tags: [remove-input]
:class: col-page-right

alt.Chart(long).mark_line().encode(
    x=alt.X("step:Q", title="write"),
    y=alt.Y("mb:Q", scale=alt.Scale(type="log"), title="MB per write"),
    color=alt.Color("name:N", scale=COLOR, title="design"),
).properties(width=300, height=220).facet(column=alt.Column("what:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(pw.groupby("name", sort=False).apply(
    lambda g: pd.Series({f"writes {a}–{b}: written": g[(g.step >= a) & (g.step <= b)].written.mean()
                         for a, b in [(1, 40), (81, 120), (201, 240)]}
                        | {f"writes {a}–{b}: read": g[(g.step >= a) & (g.step <= b)]["read"].mean()
                           for a, b in [(1, 40), (81, 120), (201, 240)]}),
    include_groups=False).round(1).reset_index().rename(columns={"name": "design"}),
      caption="MB per write, by span of writes (the whole copies ran 30 writes)")
```

```{code-cell} ipython3
:label: cut-windows
:tags: [remove-input]
:class: col-page-right

alt.Chart(reads).mark_line(point=True).encode(
    x=alt.X("step:Q", title="after write"),
    y=alt.Y("window_amp:Q", scale=alt.Scale(type="log"), title="1% window / live bytes"),
    color=alt.Color("name:N", scale=COLOR, title="design"),
).properties(width=420, height=240)
```

```{code-cell} ipython3
:tags: [remove-input]

table(reads[["name", "step", "window_amp", "window_mb", "window_req", "full_amp", "full_mb",
             "full_req", "row_groups", "data_files"]].round(2).rename(columns={"name": "design"}),
      caption="Each read: the windows' mean (MB, requests) and their bytes over live bytes; the "
      "full read; the row groups of the live data files (with those a deletion vector covers "
      "whole) and the live data files")
```

## Verdicts

```{code-cell} ipython3
:tags: [remove-cell]

V = {}
if has("MoR", "MoR compacted", "deferred merges"):
    d1 = {n: (s(n, "read"), s(n, "read") / s(n, "written"), s(n, "window_amp"))
          for n in ["MoR", "MoR compacted", "deferred merges"]}
    V["D1"] = all(r >= 1000 and x >= 15 for r, x, _ in d1.values()) and \
        d1["MoR"][2] >= 8 and d1["MoR compacted"][2] >= 4 and d1["deferred merges"][2] >= 2.5
if has("RMoR whole", "RMoW-append", "deferred merges"):
    d2 = {n: (s(n, "written") / dm30, s(n, "window_amp")) for n in ["RMoR whole", "RMoW-append"]}
    V["D2"] = all(x >= 30 and a >= 2.5 for x, a in d2.values())
if has("cut θ 0", "deferred merges"):
    c = "cut θ 0"
    d3 = dict(read=s(c, "read_last40") / s("deferred merges", "read_last40"),
              written=s(c, "written_last40") / s("deferred merges", "written_last40"),
              warm=s(c, "written_first40") / s(c, "written_last40"),
              window=s(c, "window_amp"), full=s(c, "full_amp"))
    d3_parts = {"its reads": d3["read"] <= 0.2, "its writes": d3["written"] <= 3,
                "its warm-up": d3["warm"] >= 1.8, "its windows": d3["window"] <= 1.3,
                "its full read": d3["full"] <= 1.05}
    V["D3"] = all(d3_parts.values())
if has("cut θ 0.06", "deferred merges"):
    c = "cut θ 0.06"
    d4 = dict(read=s(c, "read_last40") / s("deferred merges", "read_last40"),
              written=s(c, "written_last40") / s("deferred merges", "written_last40"),
              window=s(c, "window_amp"))
    V["D4"] = d4["read"] <= 0.5 and 0.5 <= d4["written"] <= 1.2 and d4["window"] <= 2.2


def verdict(k, parts=None):
    v = {True: "held", False: "contradicted", None: "not run yet"}[V.get(k)]
    failed = [p for p, ok in (parts or {}).items() if not ok]
    if V.get(k) is False and failed and len(failed) < len(parts):
        v += " by " + " and ".join(failed) + ", the rest held"
    return v


def f0(x):
    return f"{x:,.0f}"


def f1(x):
    return f"{x:.1f}"


def f2(x):
    return f"{x:.2f}"


def pct(x):
    return f"{100 * x:.0f}%"
```

**D1 (whole row groups read, as built): {eval}`verdict("D1")`.** Over the 240 writes MoR reads
{eval}`f0(d1["MoR"][0])` MB a write, compacted MoR {eval}`f0(d1["MoR compacted"][0])` and the
deferred merges {eval}`f0(d1["deferred merges"][0])` (predicted at least 1,000), which is
{eval}`f0(d1["MoR"][1])`, {eval}`f0(d1["MoR compacted"][1])` and
{eval}`f0(d1["deferred merges"][1])` times what each writes (at least 15). After the 240th write
their windows read {eval}`f1(d1["MoR"][2])`, {eval}`f1(d1["MoR compacted"][2])` and
{eval}`f1(d1["deferred merges"][2])` times their live bytes (at least 8, 4 and 2.5). MoR writes
{eval}`f1(s("MoR", "written"))` MB a write where the model forecast 3.6: the model leaves out the
deletion vectors' files.

**D2 (whole row groups copied, 30 writes): {eval}`verdict("D2")`.** RMoR with row groups kept
writes {eval}`f0(s("RMoR whole", "written"))` MB a write and RMoW-append
{eval}`f0(s("RMoW-append", "written"))`, {eval}`f0(d2["RMoR whole"][0])` and
{eval}`f0(d2["RMoW-append"][0])` times the deferred merges' {eval}`f1(dm30)` over the same 30
writes (at least 30). Their windows read {eval}`f2(d2["RMoR whole"][1])` and
{eval}`f2(d2["RMoW-append"][1])` times their live bytes (at least 2.5).

**D3 (the cut, θ 0): {eval}`verdict("D3", d3_parts)`.** Over writes 201–240 a write reads
{eval}`f0(s("cut θ 0", "read_last40"))` MB, {eval}`f2(d3["read"])` of the deferred merges'
{eval}`f0(s("deferred merges", "read_last40"))` (at most 0.2), and writes
{eval}`f0(s("cut θ 0", "written_last40"))` MB, {eval}`f2(d3["written"])` times theirs
{eval}`f1(s("deferred merges", "written_last40"))` (at most 3; the model forecast 162 against 74).
The cut wrote {eval}`pct(s("cut θ 0", "written_last40") / 162 - 1)` more than forecast there, the
deferred merges {eval}`pct(1 - s("deferred merges", "written_last40") / 74)` less.
Its first 40 writes write {eval}`f1(d3["warm"])` times its last 40 (at least 1.8). After the 240th
write its windows read {eval}`f2(d3["window"])` times their live bytes (at most 1.3) and its full
read {eval}`f2(d3["full"])` times (at most 1.05).

**D4 (the cut, θ 0.06): {eval}`verdict("D4")`.** Over writes 201–240 a write reads
{eval}`f0(s("cut θ 0.06", "read_last40"))` MB, {eval}`f2(d4["read"])` of the deferred merges'
(at most 0.5), and writes {eval}`f0(s("cut θ 0.06", "written_last40"))` MB,
{eval}`f2(d4["written"])` times theirs (0.5 to 1.2). After the 240th write its windows read
{eval}`f2(d4["window"])` times their live bytes (at most 2.2).

**How it was judged:** as registered, under `cached-skip`, a window being the mean of the five and
a live row's bytes those of the same arm's full read. The rules give the full read no basis of its
own; it is taken over the live rows at the initial load's bytes a row (its layout before any write,
the same in every arm). The op logs are the model's: every 240-write run's `oplog.bin` has SHA-256
`22ced251…`, the whole copies' `b2629314…` (table above).

## Simulated time

```{code-cell} ipython3
:tags: [remove-cell]

# Each phase's logged requests priced by size from a store's latency test (lab view sized_latency,
# the spikes analysis/tune.py uses), one after another, plus CPU at the laptop's pace per MB, as the
# forecasts were priced: a full read of win-upd-base (f5d480efc231, 2026-11-08) took 2.5 ms of work
# a MB read, a write 24 ms a MB written and 3 ms a MB read. havoc counted no instructions.
SPIKES = {"S3 Express": "spike/s3x-spike-20261031T163533Z",
          "Azure Blob": "spike/azure-spike-20261031T234510Z"}
READ_MS_MB, WRITE_MS_MB, WREAD_MS_MB = 2.5, 24.0, 3.0
ph = q(STEPS + """
    SELECT name, step, phase, sum(bytes_read) / 1e6 AS mb_read, sum(bytes_written) / 1e6 AS mb_written,
           count(*) AS records
    FROM s WHERE phase IN ('batch', 'compact', 'read_full', 'read_window') GROUP BY ALL""")
arms = {(l, a): n for l, a, _, n in DESIGNS}
priced = []
for _, r in runs.iterrows():
    for store, spike in SPIKES.items():
        p = con.sql(f"""SELECT arm, step, phase, request_ms, requests
                        FROM sized_latency('{r.run_id}', '{spike}')
                        WHERE arm = '{r.arm}' AND phase IN ('batch', 'compact', 'read_full', 'read_window')
                          AND coalesce(reader, 'cached-skip') = 'cached-skip'""").df()
        priced.append(p.assign(name=r["name"], store=store))
priced = pd.concat(priced).merge(ph, on=["name", "step", "phase"])
cpu = np.where(priced.phase == "batch", WRITE_MS_MB * priced.mb_written + WREAD_MS_MB * priced.mb_read,
               np.where(priced.phase == "compact",
                        WRITE_MS_MB * priced.mb_written + READ_MS_MB * priced.mb_read,
                        READ_MS_MB * priced.mb_read / priced.records))
priced["s"] = (priced.request_ms + cpu) / 1e3
priced["store_s"] = priced.request_ms / 1e3


def window_of(n):
    """The writes a design is judged over: 201-240, or the whole copies' 1-30."""
    return (1, 30) if n in ("RMoR whole", "RMoW-append") else (201, 240)


rows = []
for (n, store), g in priced.groupby(["name", "store"]):
    a, b = window_of(n)
    wr = g[g.phase.isin(["batch", "compact"]) & (g.step >= a) & (g.step <= b)]
    at = g[g.step == last[n]]
    rows.append(dict(design=n, store=store, writes=f"{a}–{b}",
                     write_s=wr.s.sum() / (b - a + 1), write_store_s=wr.store_s.sum() / (b - a + 1),
                     window_s=at[at.phase == "read_window"].s.mean(),
                     full_s=at[at.phase == "read_full"].s.mean()))
T = pd.DataFrame(rows)
T["ord"] = T.design.map({n: i for i, n in enumerate(NAMES)})
T = T.sort_values(["store", "ord"]).drop(columns="ord")
```

Measured, not judged. A write's time is its batch (locate, reads and writes) and, for compacted
MoR, its share of the compactions, over writes 201–240; the reads' are at the last write. The
whole copies ran 30 writes, so theirs are writes 1–30 and the reads after the 30th: they are the
cost of copying whole row groups, not a steady state.

```{code-cell} ipython3
:label: cut-tradeoff
:tags: [remove-input]
:class: col-page-right

alt.Chart(T).mark_point(filled=True, size=90).encode(
    x=alt.X("write_s:Q", scale=alt.Scale(type="log"), title="write latency (s)"),
    y=alt.Y("window_s:Q", scale=alt.Scale(type="log"), title="1% window read latency (s)"),
    color=alt.Color("design:N", scale=COLOR),
    tooltip=["design", "store", "writes", alt.Tooltip("write_s:Q", format=".2f"),
             alt.Tooltip("window_s:Q", format=".2f"), alt.Tooltip("full_s:Q", format=".1f")],
).properties(width=260, height=220).facet(column=alt.Column("store:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(T.round(2), caption="Simulated latency (s): a write over the writes named (of which its "
      "requests, `write_store_s`), and a window and a full read at the last write")
```

```{code-cell} ipython3
:tags: [remove-cell]

loc = q(STEPS + """
    SELECT name, ord, phase, avg(requests) AS requests, avg(bytes_read) / 1e6 AS mb_read,
           avg(bytes_written) / 1e6 AS mb_written
    FROM s WHERE phase IN ('locate', 'update-read', 'merge-read', 'write', 'delete-write', 'compact')
      AND step BETWEEN 201 AND 240
    GROUP BY ALL ORDER BY ord, phase""")
L = loc.pivot_table(index=["ord", "name"], columns="phase", values="requests").fillna(0)
L.columns = [f"{c} requests" for c in L.columns]
L = L.join(loc[loc.phase == "locate"].set_index(["ord", "name"])[["mb_read"]]
           .rename(columns={"mb_read": "locate MB"})).reset_index().drop(columns="ord")


def lreq(n):
    return float(L.set_index("name").loc[n, "locate requests"])


def lmb(n):
    return float(L.set_index("name").loc[n, "locate MB"])


s3 = T[T.store == "S3 Express"].set_index("design")
az = T[T.store == "Azure Blob"].set_index("design")
```

**Where a write's time goes.** Every design's write first finds the rows its statements change
(the locate, `layout.locate` `scan`): it reads the key column of each live row group between
the write's smallest and largest changed key, which here spans most of the table, about
{eval}`f0(lmb("deferred merges"))` MB a write for the deferred merges and
{eval}`f0(lmb("cut θ 0"))` for the cut, in a request a row group. The cut keeps more row groups
live, so its locate sends {eval}`f0(lreq("cut θ 0"))` requests a write over writes 201–240,
against the deferred merges' {eval}`f0(lreq("deferred merges"))` and compacted MoR's
{eval}`f0(lreq("MoR compacted"))`. The model left the locate out. On S3 Express the cut's write
takes {eval}`f1(s3.loc["cut θ 0", "write_s"])` s against the deferred merges'
{eval}`f1(s3.loc["deferred merges", "write_s"])` (the forecast: 7.0 against 16.4). On Azure
Blob, where a small GET costs more, it takes {eval}`f1(az.loc["cut θ 0", "write_s"])` s against
{eval}`f1(az.loc["deferred merges", "write_s"])`, of which
{eval}`f1(az.loc["cut θ 0", "write_store_s"])` s are requests. Its windows send
{eval}`f0(s("cut θ 0", "window_req"))` requests (the model counted 14): the footers of the files
whose metrics overlap a window, as the region designs' windows did at 16 MiB.

```{code-cell} ipython3
:tags: [remove-input]

table(L.round(1), caption="Requests per write by phase, and the MB the locate reads, over writes "
      "201–240 (the whole copies: none)")
```

## Limits

- **One seed, one workload.** A statement changes 3% of a region's rows, spread evenly, so every
  stride of a region changes and the cut follows regions. The deletion vectors' density has
  nothing finer to find here; a workload of sparse changes would.
- **A narrow table.** A 128 MiB row group is about 4.3 million of these 11-column rows. A wide
  table reaches 128 MiB in fewer rows and pays for small row groups in a request a column.
- **Cold pieces never merge back.** The cut only splits.
- **No page-skipping reader.** Planned separately (docs/drafts/region-cut-2026-11-09.md).
- **No instruction counts on havoc.** CPU in the simulated times is the laptop's pace per MB, as in
  the forecasts.
