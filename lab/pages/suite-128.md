---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Every design at 128 MiB: the hypotheses'
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# (configuration, arm, design, workload)
DESIGNS = [
    ("w128-upd-mor", "mor-v3", "MoR", "updates"),
    ("w128-upd-compact", "mor-v3", "MoR (compacted)", "updates"),
    ("w128-upd-rmor", "rmor-c-kept", "RMoR", "updates"),
    ("w128-upd-external", "rmor-c-footer-kept", "RMoW-external", "updates"),
    ("w128-upd-append", "rmow-c", "RMoW-append", "updates"),
    ("w128-upd-density", "rmor-density", "RMoR-density", "updates"),
    ("w128-ing-mor", "mor-v3", "MoR", "inserts"),
    ("w128-ing-compact", "mor-v3", "MoR (compacted)", "inserts"),
    ("w128-ing-density", "rmor-density", "RMoR-density", "inserts"),
]
NAMES = ["MoR", "MoR (compacted)", "RMoR", "RMoW-external", "RMoW-append", "RMoR-density"]
COLOR = alt.Scale(domain=NAMES, range=[fig.WL_NCOLOR.get(n) or fig.WL_COLOR[n] for n in NAMES])
VALUES = ", ".join(f"('{l}', '{a}', '{n}', '{w}', {i})" for i, (l, a, n, w) in enumerate(DESIGNS))
D = f"d(label, arm, name, workload, ord) AS (VALUES {VALUES})"

runs = q(f"""
    WITH {D}
    SELECT d.workload, d.name, d.label, r.run_id, r.git_sha[:7] AS build,
           json_extract_string(r.manifest, '$.oplog_sha256')[:8] AS oplog, r.batches AS writes,
           round(r.elapsed_s / 3600, 1) AS hours, round(c.foreign_mean_cpus, 2) AS foreign_cpus
    FROM d JOIN canonical_runs r USING (label)
    LEFT JOIN container_jobs c ON c.run_id = r.run_id
    WHERE list_contains(r.campaigns, 'w128') ORDER BY d.ord""")
present = set(zip(runs.workload, runs.name))

# The designs' steps, from the campaign's runs
STEPS = f"""
    WITH {D},
    s AS (
        SELECT d.workload, d.name, d.ord, s.step, s.phase, s.reader, s.rows, s.bytes_read,
               s.bytes_written, s.requests, s.row_groups, s.data_files, s.delete_files,
               s.held_bytes, s.stored_bytes, s.window_at
        FROM steps s JOIN canonical_runs r USING (run_id)
        JOIN d ON d.label = r.label AND d.arm = s.arm
        WHERE list_contains(r.campaigns, 'w128'))"""

# A write's bytes (W1-W5's rules): what it puts (write, delete-write) and, for MoR (compacted), its
# compactions; what it reads to change rows (update-read, merge-read), the locate and compaction's
# reads apart; its requests, every phase of the write
per_write = q(STEPS + """
    SELECT workload, name, ord, step,
           sum(bytes_written) FILTER (WHERE phase IN ('write', 'delete-write', 'compact')) / 1e6
               AS written,
           coalesce(sum(bytes_written) FILTER (WHERE phase = 'compact'), 0) / 1e6 AS compacted,
           coalesce(sum(bytes_read) FILTER (WHERE phase IN ('update-read', 'merge-read')), 0) / 1e6
               AS "read",
           coalesce(sum(bytes_read) FILTER (WHERE phase = 'locate'), 0) / 1e6 AS locate,
           coalesce(sum(bytes_read) FILTER (WHERE phase = 'compact'), 0) / 1e6 AS compact_read,
           sum(requests) FILTER (WHERE phase IN ('locate', 'update-read', 'merge-read', 'write',
                                                 'delete-write', 'compact')) AS requests
    FROM s WHERE step >= 1 GROUP BY ALL ORDER BY ord, step""")

# Each full read (every 10 writes): the windows (each over its rows at the same arm's full-read
# bytes a row, then their mean), the newest 1% likewise, the full and sorted reads, the layout
reads = q(STEPS + """,
    f AS (SELECT workload, name, ord, step, bytes_read, rows, requests FROM s
          WHERE phase = 'read_full' AND reader = 'cached-skip'),
    w AS (SELECT s.workload, s.name, s.step,
                 avg(s.bytes_read / (s.rows * f.bytes_read / f.rows)) AS window_amp,
                 avg(s.bytes_read) / 1e6 AS window_mb, sum(s.bytes_read) / 1e6 AS windows_mb,
                 avg(s.requests) AS window_req,
                 any_value(s.bytes_read / (s.rows * f.bytes_read / f.rows))
                     FILTER (WHERE s.window_at = 0.9) AS window09_amp,
                 any_value(s.bytes_read) FILTER (WHERE s.window_at = 0.9) / 1e6 AS window09_mb
          FROM s JOIN f USING (workload, name, step)
          WHERE s.phase = 'read_window' AND s.reader = 'cached-skip' GROUP BY ALL),
    n AS (SELECT s.workload, s.name, s.step, s.bytes_read / (s.rows * f.bytes_read / f.rows) AS recent_amp,
                 s.requests AS recent_req
          FROM s JOIN f USING (workload, name, step)
          WHERE s.phase = 'read_recent' AND s.reader = 'cached-skip'),
    o AS (SELECT workload, name, step, held_bytes / 1e9 AS sorted_held_gb FROM s
          WHERE phase = 'read_sorted' AND reader = 'cached-skip'),
    st AS (SELECT workload, name, step, row_groups, data_files, delete_files,
                  stored_bytes / 1e9 AS stored_gb FROM s WHERE phase = 'state')
    SELECT f.workload, f.name, f.ord, f.step, w.window_amp, w.window_mb, w.windows_mb, w.window_req,
           w.window09_amp, w.window09_mb, n.recent_amp, n.recent_req,
           f.bytes_read / 1e9 AS full_gb, f.requests AS full_req, o.sorted_held_gb,
           st.row_groups, st.data_files, st.delete_files, st.stored_gb
    FROM f JOIN w USING (workload, name, step) JOIN n USING (workload, name, step)
    LEFT JOIN o USING (workload, name, step) JOIN st USING (workload, name, step)
    ORDER BY f.ord, f.step""")
last = reads.groupby(["workload", "name"]).step.max()
end = reads[reads.step == [last[(w, n)] for w, n in zip(reads.workload, reads.name)]] \
    .set_index(["workload", "name"])


def summary(g):
    return pd.Series(dict(writes=len(g), written=g.written.mean(), first40=g.written.head(40).mean(),
                          last40=g.written.tail(40).mean(), compacted=g.compacted.mean(),
                          read=g["read"].mean(), locate=g.locate.mean(),
                          requests=g.requests.mean(),
                          compactions=int((g.compacted > 0).sum())))


S = per_write.groupby(["workload", "name"], sort=False).apply(summary, include_groups=False)
S = S.join(end[["window_amp", "window_mb", "windows_mb", "window_req", "window09_amp", "window09_mb",
                "recent_amp", "full_gb", "full_req", "sorted_held_gb", "row_groups", "data_files",
                "delete_files", "stored_gb"]])


def has(w, *names):
    return all((w, n) in present for n in names)


def s(w, n, col):
    return float(S.loc[(w, n), col])


def f0(x):
    return f"{x:,.0f}"


def f1(x):
    return f"{x:.1f}"


def f2(x):
    return f"{x:.2f}"


def pct(x):
    return f"{100 * x:.1f}%"


def show(w, n, col, fmt=f2):
    """A measure for prose: '(not run yet)' while the design's run is missing."""
    return fmt(s(w, n, col)) if has(w, n) else "(not run yet)"
```

Do the designs' mechanisms, defined once and run on both workloads at Iceberg's 128 MiB row
groups, spare readers what they cost writers? This page judges W1–W5 (docs/HYPOTHESES.md,
2026-11-12, and its revision for the inserts) on campaign `w128`, on havoc, and K1–K3
(2026-11-16: writers that prune row groups by each changed key) on campaign `w128-keys`, on the
laptop. What a paper would
show from these runs, by snapshot and in simulated time, belongs on the Results pages; this page
holds the verdicts and the controls.

## Setup

**The table** of D1–D4 ([](region-cut.md)): 205,480,000 initial rows in event-time order (about
6.2 GB of Parquet), 128 MiB row groups (about 3.5 million rows each) in 512 MiB files, seed 1.
**The updates:** 150,000 changes per write, clustered by key region. **The inserts:** 350,000 rows
per write, lateness median 0.05 writes and p99 5.

**The designs:** MoR, MoR (compacted) by Iceberg's bin-pack rule after every write, RMoR,
RMoW-external, RMoW-append and RMoR-density (θ 0.06, a delta row group per region). RMoR,
RMoW-external and RMoR-density copy a row group as pieces cut where a write changes it (the cut,
[](region-cut.md)); RMoW-append copies whole row groups, so it runs the first 30 writes. On the
inserts, the region designs merge each late row into the row group whose event-time range holds it;
RMoR, RMoW-external and RMoW-append do not run there ([](inserts-128.md)).

**The reads,** under `cached-skip` with the sorted read: the newest 1% and five 1% windows (by id
on the updates, by event time on the inserts, centred at 0.1, 0.3, 0.5, 0.7 and 0.9 of the span)
after every write; the full read and the sorted read every 10 writes.

**Where:** havoc, one job per configuration on 3 whole cores, four at a time. Other users' work ran
on the jobs' cores for most of each run (`foreign_cpus`, its mean in CPUs), and the container
counts no instructions, so these runs decide nothing in CPU or time. The claims are bytes.

```{code-cell} ipython3
:tags: [remove-input]

table(runs.rename(columns={"name": "design"}),
      caption="The runs: one job each on havoc (3 cores); foreign_cpus is other users' mean load "
      "on the job's cores")
```

## The updates

```{code-cell} ipython3
:tags: [remove-input]

U = S.loc["updates"].reindex([n for n in NAMES if has("updates", n)])
table(U[["writes", "written", "first40", "last40", "compacted", "read", "locate", "requests",
         "window_amp", "recent_amp", "full_gb", "sorted_held_gb", "row_groups", "stored_gb"]]
      .round(2).reset_index().rename(columns={"name": "design"}),
      caption="Per write, the mean over the run's writes (its first and last 40): MB written "
      "(of which compactions), MB read to change rows, MB the locate reads, requests. At the last "
      "write: the windows' and the newest 1%'s bytes over live bytes, the full read (GB), the "
      "sorted read's peak held memory (GB), live row groups and stored GB (every snapshot kept)")
```

```{code-cell} ipython3
:label: suite-upd-windows
:tags: [remove-input]
:class: col-page-right

ur = reads[reads.workload == "updates"]
# RMoW-append (30 writes) drawn last and dashed: early on it lies on RMoR-density's line
order = ur.assign(top=ur.name == "RMoW-append").sort_values(["top", "ord", "step"])
alt.Chart(order).mark_line(point=True).encode(
    x=alt.X("step:Q", title="snapshot"),
    y=alt.Y("window_amp:Q", scale=alt.Scale(type="log"), title="1% range read / live bytes"),
    color=alt.Color("name:N", scale=COLOR, title="design"),
    strokeDash=alt.StrokeDash("top:N", legend=None,
                              scale=alt.Scale(domain=[False, True], range=[[1, 0], [5, 3]])),
    detail="name:N",
).properties(width=460, height=260)
```

```{code-cell} ipython3
:tags: [remove-input]

table(ur.pivot_table(index="step", columns="name", values="window_amp")
      .reindex(columns=[n for n in NAMES if n in set(ur.name)]).round(2).reset_index(),
      caption="The updates: the five 1% windows' bytes over their live bytes (mean), after each "
      "tenth write")
```

A 1% window holds about 2 million rows, under two-thirds of a row group, so on the initial load
every design reads about {eval}`f1(float(ur[ur.step == 0].window_amp.mean()))` times what a window
holds. The cut designs fall from there as their writes cut the row groups a window crosses; MoR
climbs as its deletes accumulate, and compaction slows the climb.

```{code-cell} ipython3
:tags: [remove-cell]

w3 = {}
if has("updates", "RMoR"):
    w3["RMoR's windows"] = s("updates", "RMoR", "window_amp") <= 1.3
if has("updates", "RMoW-external"):
    w3["RMoW-external's windows"] = s("updates", "RMoW-external", "window_amp") <= 1.3
if has("updates", "RMoR-density"):
    w3["RMoR-density's windows"] = s("updates", "RMoR-density", "window_amp") <= 2.2
if has("updates", "MoR"):
    w3["MoR's windows"] = s("updates", "MoR", "window_amp") >= 8
if has("updates", "RMoR", "RMoW-external"):
    ext_over = s("updates", "RMoW-external", "written") / s("updates", "RMoR", "written")
    w3["RMoW-external's bytes"] = abs(ext_over - 1) <= 0.05
W3_PARTS = 5
if has("updates", "RMoR-density"):
    ud = reads[(reads.workload == "updates") & (reads.name == "RMoR-density")].set_index("step").window_amp
    ud_last_over = int(ud[ud > 2.2].index.max())
    ud_at_last_over = float(ud.loc[ud_last_over])


def verdict(parts, total):
    if not parts:
        return "not run yet"
    failed = [p for p, ok in parts.items() if not ok]
    if failed:
        return "contradicted by " + " and ".join(failed) + \
            (", the rest held" if len(failed) < len(parts) else "")
    if len(parts) < total:
        return f"{len(parts)} of its {total} parts held; the rest not run yet"
    return "held"
```

**W3 (updates, D1–D4 again, now with RMoW-external): {eval}`verdict(w3, W3_PARTS)`.** After the
240th write RMoR's windows read {eval}`show("updates", "RMoR", "window_amp")` times their live bytes,
RMoW-external's {eval}`show("updates", "RMoW-external", "window_amp")` and RMoR-density's
{eval}`show("updates", "RMoR-density", "window_amp")` (at most 1.3, 1.3 and 2.2), and MoR's
{eval}`show("updates", "MoR", "window_amp", f1)` (at least 8). RMoR-density's came under its bound
late: they read {eval}`f2(ud_at_last_over)` times live at write {eval}`ud_last_over`. RMoW-external writes
{eval}`show("updates", "RMoW-external", "written", f1)` MB per write against RMoR's
{eval}`show("updates", "RMoR", "written", f1)`, {eval}`pct(ext_over - 1)` more (within 5%): the
same copies, with a footer appended where RMoR writes a deletion vector.

**The control:** these runs replay D1–D4's op log, and every record they share with D1–D4's runs
is the same, request for request and byte for byte (below). The new runs add only the reads after
every write, and change no write.

```{code-cell} ipython3
:tags: [remove-cell]

# Each w128 run against the D1-D4 run of the same design and op log
TWINS = [("MoR", "d128-upd-mor", "w128-upd-mor"), ("RMoR", "d128-upd-cut", "w128-upd-rmor"),
         ("RMoW-append", "d128-upd-copies", "w128-upd-append"),
         ("RMoR-density", "d128-upd-density-cut", "w128-upd-density")]
rid = q("SELECT label, run_id FROM canonical_runs WHERE label LIKE 'd128-upd-%' "
        "OR list_contains(campaigns, 'w128')").set_index("label").run_id
twins = []
for n, a, b in TWINS:
    if a not in rid or b not in rid:
        continue
    t = q(f"""SELECT count(*) FILTER (WHERE NOT one_run_only AND len(differs) = 0) AS same,
                     count(*) FILTER (WHERE NOT one_run_only AND len(differs) > 0) AS differ,
                     count(*) FILTER (WHERE one_run_only AND step >= 1
                                      AND phase NOT IN ('read_full', 'read_recent', 'read_window',
                                                        'read_sorted')) AS writes_one_only,
                     count(*) FILTER (WHERE one_run_only) AS one_run_only
              FROM steps_diff('{rid[a]}', '{rid[b]}')""")
    twins.append(dict(design=n, d1_d4_run=a, w128_run=b) | t.iloc[0].to_dict())
twins = pd.DataFrame(twins)
```

```{code-cell} ipython3
:tags: [remove-input]

table(twins, caption="Records (arm, write, phase, reader, window) in both runs that match in every "
      "request and byte count, or differ; records in one run only (the new reads), of which "
      "writes")
```

**Measured, not judged.** MoR (compacted) under Iceberg's rule compacts
{eval}`show("updates", "MoR (compacted)", "compactions", f0)` times in 240 writes, writing
{eval}`show("updates", "MoR (compacted)", "compacted", f1)` MB per write in compactions, and its
windows read {eval}`show("updates", "MoR (compacted)", "window_amp", f1)` times their live bytes at
the end, against MoR's {eval}`show("updates", "MoR", "window_amp", f1)`. For its first 40 writes
they read more than MoR's: the rule takes the initial load's last file, part full and so "small",
into its group, and its first compaction (write 3) rewrites that file with three batch files into
one, after which a window reads about 100 MB more (below). Each later compaction rewrites that
file again, about 0.3 GB, to take in four batch files of 4 MB, until it outgrows the small-file
size. RMoW-append, copying whole row groups, writes
{eval}`show("updates", "RMoW-append", "written", f0)` MB per write over its 30, and its windows read
{eval}`show("updates", "RMoW-append", "window_amp", f1)` times live: a window still crosses whole
128 MiB row groups. RMoR's sorted read holds
{eval}`show("updates", "RMoR", "sorted_held_gb")` GB at most, MoR's
{eval}`show("updates", "MoR", "sorted_held_gb")`: each of MoR's batch files holds rows updated
across the whole key range, so a read in key order holds a part of every one at once.

```{code-cell} ipython3
:tags: [remove-input]

first = q(STEPS + """
    SELECT name, step,
           any_value(data_files) FILTER (WHERE phase = 'state') AS data_files,
           any_value(delete_files) FILTER (WHERE phase = 'state') AS delete_files,
           sum(bytes_written) FILTER (WHERE phase = 'compact') / 1e6 AS compacted_mb,
           avg(bytes_read) FILTER (WHERE phase = 'read_window') / 1e6 AS window_mb,
           avg(requests) FILTER (WHERE phase = 'read_window') AS window_req
    FROM s WHERE workload = 'updates' AND name IN ('MoR', 'MoR (compacted)') AND step BETWEEN 1 AND 12
    GROUP BY ALL ORDER BY name, step""")
table(first.round(1).rename(columns={"name": "design"}),
      caption="The updates, MoR and MoR (compacted) over their first 12 writes: live data and delete "
      "files, MB compacted, and a window's mean MB and requests")
```

## The inserts

```{code-cell} ipython3
:tags: [remove-cell]

w1 = w2 = w4 = w5 = {}
if has("inserts", "MoR", "RMoR-density"):
    d_over_m = s("inserts", "RMoR-density", "written") / s("inserts", "MoR", "written")
    w1 = {"RMoR-density's bytes": d_over_m <= 10}
    w2 = {"its window's bytes over live": s("inserts", "RMoR-density", "window09_amp") <= 3,
          "its window against MoR's": s("inserts", "RMoR-density", "window09_mb")
          <= s("inserts", "MoR", "window09_mb") / 3,
          "its sorted read": s("inserts", "RMoR-density", "sorted_held_gb") <= 0.4,
          "MoR's sorted read": s("inserts", "MoR", "sorted_held_gb") >= 1}
if has("inserts", "MoR (compacted)", "RMoR-density"):
    d_over_c = s("inserts", "RMoR-density", "written") / s("inserts", "MoR (compacted)", "written")
    w4 = {"RMoR-density's bytes": 0.75 <= d_over_c <= 1.25}
    win_over_c = s("inserts", "RMoR-density", "windows_mb") / \
        s("inserts", "MoR (compacted)", "windows_mb")
    held_over_c = s("inserts", "RMoR-density", "sorted_held_gb") / \
        s("inserts", "MoR (compacted)", "sorted_held_gb")
    w5 = {"its windows": win_over_c <= 0.5, "its sorted read": held_over_c <= 1 / 3}

# W2's and W5's measures at every read point, not only the last
pts = None
if has("inserts", "MoR", "MoR (compacted)", "RMoR-density"):
    ip = reads[reads.workload == "inserts"].set_index(["name", "step"])
    dn, mo, co = (ip.loc[n] for n in ("RMoR-density", "MoR", "MoR (compacted)"))
    pts = pd.DataFrame({"window at 0.9 / live": dn.window09_amp,
                        "window at 0.9 / MoR's": dn.window09_mb / mo.window09_mb,
                        "windows / compacted's": dn.windows_mb / co.windows_mb,
                        "sorted read / compacted's": dn.sorted_held_gb / co.sorted_held_gb})
    pts = pts[pts.index > 0]
    earlier = pts[pts.index < pts.index.max()]
    bound = {"window at 0.9 / live": 3, "window at 0.9 / MoR's": 1 / 3,
             "windows / compacted's": 0.5, "sorted read / compacted's": 1 / 3}
    over = {k: int((earlier[k] > b).sum()) for k, b in bound.items()}
    w09 = earlier["window at 0.9 / live"]
    late = int(max(earlier.index[(earlier["windows / compacted's"] > 0.5)
                                 | (earlier["sorted read / compacted's"] > 1 / 3)], default=0))
```

```{code-cell} ipython3
:tags: [remove-input]

if any((w, n) in present for w, n in present if w == "inserts"):
    I = S.loc["inserts"].reindex([n for n in NAMES if has("inserts", n)])
    table(I[["writes", "written", "compacted", "locate", "requests", "window_amp", "window09_amp",
             "windows_mb", "recent_amp", "full_gb", "sorted_held_gb", "row_groups", "stored_gb"]]
          .round(2).reset_index().rename(columns={"name": "design"}),
          caption="The inserts, as the updates' table; window09 the window at 0.9 of the "
          "event-time span, among the inserted rows; windows_mb the five windows' MB in total")
else:
    print("The inserts' runs are not in yet.")
```

```{code-cell} ipython3
:label: suite-ing-windows
:tags: [remove-input]
:class: col-page-right

ir = reads[reads.workload == "inserts"]
ran = [n for n in NAMES if n in set(ir.name)]
alt.Chart(ir).mark_line(point=True).encode(
    x=alt.X("step:Q", title="snapshot"),
    y=alt.Y("window_amp:Q", scale=alt.Scale(type="log"), title="1% range read / live bytes"),
    color=alt.Color("name:N", title="design", sort=ran,
                    scale=alt.Scale(domain=ran, range=[COLOR.range[NAMES.index(n)] for n in ran])),
).properties(width=460, height=260)
```

```{code-cell} ipython3
:tags: [remove-input]

table(ir.pivot_table(index="step", columns="name", values="window_amp")
      .reindex(columns=[n for n in NAMES if n in set(ir.name)]).round(2).reset_index(),
      caption="The inserts: the five 1% windows by event time, their bytes over their live bytes "
      "(mean), after each tenth write")
```

**W1 (inserts, RMoR-density's half): {eval}`verdict(w1, 1)`.** Over its 240 writes RMoR-density
writes {eval}`show("inserts", "RMoR-density", "written", f1)` MB per write, against MoR's
{eval}`show("inserts", "MoR", "written", f1)` (at most 10 times). The half for RMoR, RMoW-external
and RMoW-append is not judged: they do not run on the inserts.

**W2 (inserts, what readers get): {eval}`verdict(w2, 4)`.** After the 240th write RMoR-density's
window at 0.9 of the event-time span reads {eval}`show("inserts", "RMoR-density", "window09_amp")`
times its live bytes (at most 3), {eval}`show("inserts", "RMoR-density", "window09_mb", f0)` MB
against MoR's {eval}`show("inserts", "MoR", "window09_mb", f0)` (at most a third), and its sorted
read holds {eval}`show("inserts", "RMoR-density", "sorted_held_gb")` GB (at most 0.4) where MoR's
holds {eval}`show("inserts", "MoR", "sorted_held_gb")` (at least 1).

**W4 (inserts, writes against compaction): {eval}`verdict(w4, 1)`.** RMoR-density writes
{eval}`show("inserts", "RMoR-density", "written", f1)` MB per write against MoR (compacted)'s
{eval}`show("inserts", "MoR (compacted)", "written", f1)` (0.75 to 1.25 times).

**W5 (inserts, reads against compaction): {eval}`verdict(w5, 2)`.** After the 240th write
RMoR-density's five windows read {eval}`show("inserts", "RMoR-density", "windows_mb", f0)` MB in
total against MoR (compacted)'s {eval}`show("inserts", "MoR (compacted)", "windows_mb", f0)` (at
most half), and its sorted read holds {eval}`show("inserts", "RMoR-density", "sorted_held_gb")` GB
against {eval}`show("inserts", "MoR (compacted)", "sorted_held_gb")` (at most a third).

**Judged at one write.** As registered, the verdicts read the 240th write, which is a low point
for RMoR-density. From one read point to the next its window at 0.9 swings between
{eval}`f1(w09.min())` and {eval}`f1(w09.max())` times live. Of the {eval}`len(earlier)` read points
before the last, it read more than 3 times live at {eval}`over["window at 0.9 / live"]` and more
than a third of MoR's bytes at {eval}`over["window at 0.9 / MoR's"]`. Its five windows read more
than half of MoR (compacted)'s at {eval}`over["windows / compacted's"]`, and its sorted read held
more than a third of theirs at {eval}`over["sorted read / compacted's"]`, none after write
{eval}`late`.

```{code-cell} ipython3
:tags: [remove-input]

if pts is not None:
    table(pts.round(3).reset_index().rename(columns={"step": "snapshot"}),
          caption="The inserts, W2's and W5's measures of RMoR-density at every read point: its "
          "window at 0.9 over its live bytes and over MoR's bytes, and its five windows' bytes and "
          "its sorted read's peak over MoR (compacted)'s")
```

**Measured, not judged.** RMoR-density's price on the inserts is requests, for its delta row
groups and the footers that list them. A write sends
{eval}`show("inserts", "RMoR-density", "requests", f0)` requests (MoR
{eval}`show("inserts", "MoR", "requests", f0)`, MoR (compacted)
{eval}`show("inserts", "MoR (compacted)", "requests", f0)`), most of them the locate's footer
reads. After the 240th write a window takes {eval}`show("inserts", "RMoR-density", "window_req", f0)`
requests (MoR (compacted) {eval}`show("inserts", "MoR (compacted)", "window_req", f0)`, MoR
{eval}`show("inserts", "MoR", "window_req", f0)`), and the full read
{eval}`show("inserts", "RMoR-density", "full_req", f0)`
({eval}`show("inserts", "MoR (compacted)", "full_req", f0)` and
{eval}`show("inserts", "MoR", "full_req", f0)`). That is over
{eval}`show("inserts", "RMoR-density", "row_groups", f0)` row groups, against
{eval}`show("inserts", "MoR (compacted)", "row_groups", f0)` and
{eval}`show("inserts", "MoR", "row_groups", f0)`. MoR (compacted) compacts
{eval}`show("inserts", "MoR (compacted)", "compactions", f0)` times in 240 writes,
{eval}`show("inserts", "MoR (compacted)", "compacted", f1)` MB per write, and its windows end at
{eval}`show("inserts", "MoR (compacted)", "window_amp", f1)` times their live bytes, against MoR's
{eval}`show("inserts", "MoR", "window_amp", f1)` and RMoR-density's
{eval}`show("inserts", "RMoR-density", "window_amp", f1)`.

The model that projects the inserts for the designs that did not run is checked against these
three runs on a calibration page, [](model-128.md).

**How it was judged:** as registered, under `cached-skip`, after the last write; a window's bytes
over live bytes being its bytes over its rows at the same design's full-read bytes per row then,
and the windows' measure their mean (W2 names the window at 0.9). A write's bytes are `write` and
`delete-write`, and `compact` for MoR (compacted), a mean over the run's writes.

## Writers that prune by key (K1–K3)

```{code-cell} ipython3
:tags: [remove-cell]

# K1-K3 (docs/HYPOTHESES.md, 2026-11-16): each w128-keys run (the laptop, one at a time, requests
# logged, instructions counted) against its configuration's w128 run (havoc) and that run's laptop
# twin (campaign w128-twins: instructions counted, no request log)
tune = fig.tuner()
KD = [("MoR", "mor", "mor-v3"), ("MoR (compacted)", "compact", "mor-v3"), ("RMoR", "rmor", "rmor-c-kept"),
      ("RMoW-external", "external", "rmor-c-footer-kept"), ("RMoR-density", "density", "rmor-density")]
REGION_K = ["RMoR", "RMoW-external", "RMoR-density"]
K_STORES = {"S3 Express": tune.SPIKES["s3x"], "Azure Blob": tune.SPIKES["azure"]}


def run_in(campaign, label):
    r = q(f"""SELECT run_id FROM canonical_runs
              WHERE list_contains(campaigns, '{campaign}') AND label = '{label}'""").run_id
    return r.iloc[0] if len(r) == 1 else None


KR = pd.DataFrame([dict(design=n, arm=a, keys_run=run_in("w128-keys", f"w128k-upd-{k}"),
                        scan_run=run_in("w128", f"w128-upd-{k}"),
                        twin_run=run_in("w128-twins", f"w128-upd-{k}"))
                   for n, k, a in KD]).set_index("design").dropna()
k_ran = set(KR.index)

# K1: the records against the scan run's. A write's totals (batch) and its locate may differ only in
# what they read; anything else differing, or a record in one run only, breaks the control
READ_COUNTS = "['requests', 'gets', 'get_range', 'get_stream', 'heads', 'bytes_read']"


def k1_records(n):
    r = KR.loc[n]
    return q(f"""
        SELECT count(*) AS records, count(*) FILTER (WHERE len(differs) > 0) AS differ,
               count(*) FILTER (WHERE one_run_only OR (len(differs) > 0
                                AND phase NOT IN ('locate', 'batch'))) AS differ_elsewhere,
               count(*) FILTER (WHERE len(list_filter(differs, x -> NOT list_contains({READ_COUNTS}, x)))
                                > 0) AS differ_beyond_reads
        FROM steps_diff('{r.keys_run}', '{r.scan_run}') WHERE arm = '{r.arm}'""").iloc[0]


def locate_reads(run):
    """Per write, the locate's requests (a write's first, as many as its locate record counts) by
    what they read: each file's footer (two streamed GETs), its deletion vector, the key column of
    the initial load's files or of the files writes added (MB); and the footers' and deletion
    vectors' objects."""
    return q(f"""
        WITH loc AS (SELECT step, requests AS n FROM steps WHERE run_id = '{run}' AND phase = 'locate'),
        o AS (SELECT step, kind, object, length, row_number() OVER (PARTITION BY step ORDER BY seq) AS i
              FROM requests WHERE run_id = '{run}' AND phase = 'batch'
                AND kind IN ('get_range', 'get_stream', 'head', 'put', 'append')),
        l AS (SELECT o.*, CASE WHEN object LIKE '%.puffin' THEN 'dv' WHEN kind = 'get_stream' THEN 'footer'
                               ELSE 'key' END AS what
              FROM o JOIN loc USING (step) WHERE o.i <= loc.n)
        SELECT step, count(*) FILTER (WHERE what = 'footer') AS footers,
               count(*) FILTER (WHERE what = 'dv') AS dvs, count(*) FILTER (WHERE what = 'key') AS key_column,
               coalesce(sum(length) FILTER (WHERE what = 'key' AND object LIKE '%/initial-%'), 0) / 1e6
                   AS initial_mb,
               coalesce(sum(length) FILTER (WHERE what = 'key' AND object NOT LIKE '%/initial-%'), 0) / 1e6
                   AS added_mb,
               coalesce(list(object ORDER BY object) FILTER (WHERE what = 'footer'), []) AS footer_objects,
               coalesce(list(object ORDER BY object) FILTER (WHERE what = 'dv'), []) AS dv_objects
        FROM l GROUP BY 1 ORDER BY 1""").set_index("step")


LR = {(n, w): locate_reads(KR.loc[n, f"{w}_run"]) for n in KR.index for w in ("keys", "scan")}


def same_objects(n, col):
    a, b = LR[(n, "keys")][col], LR[(n, "scan")][col]
    return int(sum(list(x) != list(y) for x, y in zip(a, b.reindex(a.index))))


def read_instructions(n):
    """Each read's user-mode instructions (thread and read pool), the run's over its twin's: their
    sums over the run, and the worst single read."""
    r = KR.loc[n]
    return q(f"""
        WITH j AS (
            SELECT a.phase, a.step, (a.instructions + coalesce(a.pool_instructions, 0)) AS ka,
                   (b.instructions + coalesce(b.pool_instructions, 0)) AS kb
            FROM steps a JOIN steps b ON a.arm = b.arm AND a.step = b.step AND a.phase = b.phase
              AND a.reader IS NOT DISTINCT FROM b.reader AND a.window_at IS NOT DISTINCT FROM b.window_at
            WHERE a.run_id = '{r.keys_run}' AND b.run_id = '{r.twin_run}' AND a.arm = '{r.arm}'
              AND a.phase IN ('read_full', 'read_recent', 'read_window', 'read_sorted')
              AND a.reader = 'cached-skip' AND b.instructions > 0)
        SELECT phase, sum(ka) / sum(kb) AS ratio, max(abs(ka / kb - 1)) AS worst, count(*) AS reads
        FROM j GROUP BY 1""").assign(design=n)


K1 = pd.DataFrame([dict(design=n, **k1_records(n).to_dict(),
                        footers_differ=same_objects(n, "footer_objects"),
                        dvs_differ=same_objects(n, "dv_objects")) for n in KR.index])
RI = pd.concat([read_instructions(n) for n in KR.index], ignore_index=True)
RI["off"] = (RI.ratio - 1).abs()
k1 = {"the records": bool((K1.differ_elsewhere == 0).all() and (K1.differ_beyond_reads == 0).all()),
      "the footers and deletion vectors": bool((K1.footers_differ == 0).all() and (K1.dvs_differ == 0).all())}
for n in KR.index:
    k1[f"{n}'s reads' instructions"] = bool((RI[RI.design == n].off <= 0.02).all())
ri_ok = RI[~RI.design.isin([n for n in KR.index if not k1[f"{n}'s reads' instructions"]])]


def ri(n, phase):
    return float(RI[(RI.design == n) & (RI.phase == phase)].ratio.iloc[0])


# MoR's full read against its twin's, read by read (G instructions)
mor_full = None
if "MoR" in k_ran:
    r = KR.loc["MoR"]
    mor_full = q(f"""
        SELECT a.step, ((a.instructions + coalesce(a.pool_instructions, 0))
                        - (b.instructions + coalesce(b.pool_instructions, 0))) / 1e9 AS more_g
        FROM steps a JOIN steps b ON a.arm = b.arm AND a.step = b.step AND a.phase = b.phase
          AND a.reader IS NOT DISTINCT FROM b.reader
        WHERE a.run_id = '{r.keys_run}' AND b.run_id = '{r.twin_run}' AND a.arm = '{r.arm}'
          AND a.phase = 'read_full' AND a.reader = 'cached-skip' ORDER BY 1""").set_index("step").more_g


# K2: the locate's requests a write over writes first to last
def locate_requests(run, arm, first=231, last=240):
    return float(q(f"""SELECT sum(requests) / {last - first + 1} AS r FROM steps
                       WHERE run_id = '{run}' AND arm = '{arm}' AND phase = 'locate'
                         AND step BETWEEN {first} AND {last}""").r.iloc[0])


K2 = pd.DataFrame([dict(design=n, scan=locate_requests(KR.loc[n, "scan_run"], KR.loc[n, "arm"]),
                        keys=locate_requests(KR.loc[n, "keys_run"], KR.loc[n, "arm"]),
                        footers=LR[(n, "keys")].footers.loc[231:240].mean(),
                        dvs=LR[(n, "keys")].dvs.loc[231:240].mean(),
                        key_column=LR[(n, "keys")].key_column.loc[231:240].mean()) for n in KR.index])
K2["of_scan"] = K2["keys"] / K2["scan"]
K2 = K2.set_index("design")
K2_BOUND = {"RMoR": ("<=", 0.55), "RMoR-density": ("<=", 0.55), "RMoW-external": ("<=", 0.35),
            "MoR": (">=", 0.85)}
k2 = {f"{n}'s locate": (K2.of_scan[n] <= b if op == "<=" else K2.of_scan[n] >= b)
      for n, (op, b) in K2_BOUND.items() if n in k_ran}


# K3: a write's simulated time at every write: its requests priced by size, plus its work (under
# scan, the twin's: havoc's runs count no instructions)
def write_s(run, arm, spike, work_run=None):
    d = q(f"""SELECT step, request_ms FROM sized_latency('{run}', '{spike}')
              WHERE arm = '{arm}' AND phase = 'batch' AND step >= 1""")
    w = q(f"""SELECT step, work_ms FROM sized_latency('{work_run or run}', '{spike}')
              WHERE arm = '{arm}' AND phase = 'batch' AND step >= 1""")
    d = d.merge(w, on="step")
    return d.assign(s=(d.request_ms + d.work_ms) / 1e3).set_index("step").s.sort_index()


def f3(x):
    return f"{x:.3f}"


K3 = []
for store, spike in K_STORES.items():
    for n in KR.index:
        r = KR.loc[n]
        sc = write_s(r.scan_run, r.arm, spike, r.twin_run)
        ke = write_s(r.keys_run, r.arm, spike)
        K3.append(dict(store=store, design=n, scan=sc.mean(), keys=ke.mean(),
                       scan_last_first=sc.loc[231:240].mean() / sc.loc[1:10].mean(),
                       keys_last_first=ke.loc[231:240].mean() / ke.loc[1:10].mean(), writes=len(ke)))
K3 = pd.DataFrame(K3)
K3["less"] = 1 - K3["keys"] / K3["scan"]
K3["of_mor"] = K3["keys"] / K3.store.map(K3[K3.design == "MoR"].set_index("store")["keys"])
K3S = K3[K3.store == "S3 Express"].set_index("design")
K3A = K3[K3.store == "Azure Blob"].set_index("design")
k3 = {}
for n in REGION_K:
    if n in k_ran:
        k3[f"{n}'s writes"] = K3S.less[n] >= 0.20
if "MoR" in k_ran:
    k3["MoR's writes"] = K3S.less["MoR"] <= 0.05
    if all(n in k_ran for n in REGION_K):
        k3["the region designs against MoR"] = bool((K3S.of_mor[REGION_K] <= 0.6).all())
if "RMoR-density" in k_ran:
    k3["RMoR-density's last 10 writes"] = K3S.keys_last_first["RMoR-density"] <= 2


# Why: the locate's requests and bytes against the scan run's, and its instructions against the
# twin's, over every write
def locate_totals(n):
    r = KR.loc[n]
    return q(f"""
        SELECT sum(requests) FILTER (WHERE run_id = '{r.keys_run}')
                 / sum(requests) FILTER (WHERE run_id = '{r.scan_run}') AS requests,
               sum(bytes_read) FILTER (WHERE run_id = '{r.keys_run}')
                 / sum(bytes_read) FILTER (WHERE run_id = '{r.scan_run}') AS bytes,
               sum(instructions) FILTER (WHERE run_id = '{r.keys_run}')
                 / sum(instructions) FILTER (WHERE run_id = '{r.twin_run}') AS instructions,
               avg(bytes_read) FILTER (WHERE run_id = '{r.scan_run}') / 1e6 AS scan_mb,
               avg(bytes_read) FILTER (WHERE run_id = '{r.keys_run}') / 1e6 AS keys_mb
        FROM steps WHERE run_id IN ('{r.keys_run}', '{r.scan_run}', '{r.twin_run}') AND arm = '{r.arm}'
          AND phase = 'locate' AND step >= 1""").iloc[0]


LT = pd.DataFrame([dict(design=n, **locate_totals(n).to_dict(),
                        initial_scan_mb=LR[(n, "scan")].initial_mb.mean(),
                        initial_keys_mb=LR[(n, "keys")].initial_mb.mean(),
                        added_scan_mb=LR[(n, "scan")].added_mb.mean(),
                        added_keys_mb=LR[(n, "keys")].added_mb.mean()) for n in KR.index]).set_index("design")
```

The updates' writers find the rows a write changes by reading the table (`layout.locate` `scan`):
in each file that may hold a changed key, the key column of every row group between the write's
smallest and largest changed key. Campaign `w128-keys` reran the five configurations that do so with
one change, `locate` `keys`, which reads the key column only of the row groups whose key statistics
may hold a changed key (docs/HYPOTHESES.md, 2026-11-16). RMoW-append finds its rows from its
footers' statistics, so it did not run again. The runs ran on the laptop, one at a time from one
build snapshot, with every request logged, and they count instructions, so they decide time. A
write's simulated time is its requests priced by size plus its work, as on [](workloads-128.md):
under `keys` the run's own; under `scan` havoc's requests and its laptop twin's work.

```{code-cell} ipython3
:tags: [remove-input]

table(KR.drop(columns="arm").reset_index(),
      caption="The runs: each configuration under keys (the laptop), under scan (havoc), and the "
      "scan run's laptop twin, which counts its work")
```

**K1 (a control): {eval}`verdict(k1, 2 + len(KD))`.** Only the locate's records differ from the
`scan` runs', with the writes' totals that include them, and only in what they read: every read,
compaction and table-state record, and every other part of every write, sends the same requests
and reads and writes the same bytes, over the {eval}`f0(K1.records.sum())` records of the five
runs. Every write reads the footers and deletion vectors of the same files. The reads'
instructions come within {eval}`pct(ri_ok.off.max())` of their twins' in every design but MoR (at
most 2%). MoR's full read runs {eval}`pct(ri("MoR", "read_full") - 1)` more instructions than its
twin's, and its read of the newest 1% {eval}`pct(ri("MoR", "read_recent") - 1)` more, with the same
requests and bytes. From write 10 on, each of MoR's full reads runs between
{eval}`f1(mor_full.loc[10:].min())` and {eval}`f1(mor_full.loc[10:].max())` billion instructions more
than its twin's; the read before the first write, {eval}`f1(mor_full.loc[0])` billion more. These
runs do not say why.

```{code-cell} ipython3
:tags: [remove-input]

table(K1, caption="K1: records (write, phase, reader, window) compared with the scan run; those that "
      "differ; those that differ beyond what the write reads, or outside its locate and totals, or "
      "are in one run only; writes whose locate reads other files' footers or deletion vectors")
```

```{code-cell} ipython3
:tags: [remove-input]

table(RI.pivot_table(index="design", columns="phase", values="ratio").reindex(list(KR.index))
      .round(3).reset_index(),
      caption="K1: each read's user-mode instructions over the run, under keys over the twin's "
      "(within 0.98 and 1.02 is within 2%)")
```

**K2: {eval}`verdict(k2, 4)`.** Over the last 10 writes the locate sends
{eval}`f2(K2.of_scan["RMoR"])` of its `scan` requests for RMoR and
{eval}`f2(K2.of_scan["RMoR-density"])` for RMoR-density (at most 0.55),
{eval}`f2(K2.of_scan["RMoW-external"])` for RMoW-external (at most 0.35) and
{eval}`f2(K2.of_scan["MoR"])` for MoR (at least 0.85). What `keys` leaves is a file's: its footer,
in two requests, and its deletion vector, which every design but RMoW-external loads before
pruning.

```{code-cell} ipython3
:tags: [remove-input]

table(K2.round(2).reset_index(),
      caption="K2: the locate's requests a write over the last 10 writes, under scan and keys, and "
      "under keys its footer, deletion-vector and key-column requests")
```

**K3: {eval}`verdict(k3, 6)`.** On S3 Express, RMoR's writes take
{eval}`pct(K3S.less["RMoR"])` less on average than under `scan`, RMoW-external's
{eval}`pct(K3S.less["RMoW-external"])` and RMoR-density's {eval}`pct(K3S.less["RMoR-density"])` (at
least 20% each), and MoR's {eval}`pct(K3S.less["MoR"])` less (at most 5%). Under `keys` the region
designs write in {eval}`f3(K3S.of_mor["RMoR"])`, {eval}`f3(K3S.of_mor["RMoW-external"])` and
{eval}`f3(K3S.of_mor["RMoR-density"])` of MoR's time (at most 0.6), and RMoR-density's last 10
writes take {eval}`f2(K3S.keys_last_first["RMoR-density"])` times its first 10 (at most 2; under
`scan`, {eval}`f2(K3S.scan_last_first["RMoR-density"])`).

**Why MoR's writes take less:** a locate's work follows the bytes it reads, not its requests.
Under `keys` MoR's locate sends {eval}`f2(LT.requests["MoR"])` of its `scan` requests but reads
{eval}`f2(LT.bytes["MoR"])` of the bytes, with {eval}`f2(LT.instructions["MoR"])` of its twin's
instructions. The key columns it skips are those of the initial load's 128 MiB row groups: it
reads {eval}`f1(LT.initial_keys_mb["MoR"])` MB of them a write against
{eval}`f1(LT.initial_scan_mb["MoR"])` under `scan`. The files MoR's writes add, a small row group
each, keep their reads: {eval}`f1(LT.added_keys_mb["MoR"])` MB of key columns a write, against
{eval}`f1(LT.added_scan_mb["MoR"])` under `scan`. The
forecast cut the locate's work in proportion to its key-column requests.

```{code-cell} ipython3
:tags: [remove-input]

table(LT.round(2).reset_index(),
      caption="The locate over every write, under keys: its requests and bytes over the scan run's, "
      "its instructions over the twin's; and a write's MB of key columns from the initial load's "
      "files and from the files writes added, under scan and keys")
```

**Measured, not judged.** MoR (compacted)'s writes alone take
{eval}`f1(K3S["keys"]["MoR (compacted)"])` s on S3 Express under `keys` against
{eval}`f1(K3S.scan["MoR (compacted)"])` under `scan`. On Azure Blob, `keys` takes RMoR's write from
{eval}`f1(K3A.scan["RMoR"])` to {eval}`f1(K3A["keys"]["RMoR"])` s, RMoW-external's from
{eval}`f1(K3A.scan["RMoW-external"])` to {eval}`f1(K3A["keys"]["RMoW-external"])`, RMoR-density's
from {eval}`f1(K3A.scan["RMoR-density"])` to {eval}`f1(K3A["keys"]["RMoR-density"])`, MoR's from
{eval}`f1(K3A.scan["MoR"])` to {eval}`f1(K3A["keys"]["MoR"])` and MoR (compacted)'s from
{eval}`f1(K3A.scan["MoR (compacted)"])` to {eval}`f1(K3A["keys"]["MoR (compacted)"])`.

```{code-cell} ipython3
:tags: [remove-input]

table(K3[["store", "design", "writes", "scan", "keys", "less", "of_mor", "scan_last_first",
          "keys_last_first"]].round(3),
      caption="K3: a write's simulated latency (s), its mean over the 240 writes, under scan and keys; "
      "the share less; under keys over MoR's; and the last 10 writes over the first 10")
```

**How it was judged:** as registered. A write's simulated latency is its `batch` phase: its
requests priced by size from each store's latency test (lab/views/65_sized.sql) plus its work at
its arm's pace, a mean over the 240 writes. Under `keys` both come from the run; under `scan` the
requests from havoc's run and the work from its twin.

## Limits

- **One seed, one lateness,** as registered; no watermark drops late rows.
- **30 writes for RMoW-append,** so no steady state.
- **No time from the `w128` runs.** Other users' work shared every job's cores, and the container
  counted no instructions: requests and bytes only. K3's times come from the `w128-keys` runs and
  the `w128` runs' laptop twins, which count instructions.
- **A run's work is one measurement.** MoR's reads under `keys` run up to
  {eval}`pct(RI[RI.design == "MoR"].ratio.max() - 1)` more instructions than its twin's with the same
  requests and bytes (K1), so a few percent of a phase's work can differ between two runs of the
  same reads.
