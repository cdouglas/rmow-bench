---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The inserts model against the runs
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# (configuration, arm, the model's key, design)
RUNS = [("w128-ing-mor", "mor-v3", "mor", "MoR"),
        ("w128-ing-compact", "mor-v3", "compact", "MoR (compacted)"),
        ("w128-ing-density", "rmor-density", "density", "RMoR-density")]
NAMES = [n for *_, n in RUNS]
COLOR = alt.Scale(domain=NAMES, range=[fig.ET_COLOR[n] for n in NAMES])
VALUES = ", ".join(f"('{l}', '{a}', '{n}', {i})" for i, (l, a, _, n) in enumerate(RUNS))
D = f"d(label, arm, name, ord) AS (VALUES {VALUES})"
S = f"""
    WITH {D},
    s AS (SELECT d.name, d.ord, r.run_id, s.step, s.phase, s.reader, s.rows, s.bytes_read,
                 s.bytes_written, s.held_bytes, s.row_groups, s.window_at
          FROM steps s JOIN canonical_runs r USING (run_id)
          JOIN d ON d.label = r.label AND d.arm = s.arm
          WHERE list_contains(r.campaigns, 'w128'))"""

runs = q(S + """
    SELECT DISTINCT name, ord, run_id FROM s ORDER BY ord""")

# A write's bytes as W1-W5 count them: write, delete-write and compact
measured_w = q(S + """
    SELECT name, step,
           sum(bytes_written) FILTER (WHERE phase IN ('write', 'delete-write', 'compact')) AS b
    FROM s WHERE step >= 1 GROUP BY ALL ORDER BY name, step""")

proj = fig.et_projection()
args = proj["args"]
WRITES = args["writes"]
model_w = pd.DataFrame([(fig.ET_NAMES[k], step, b) for k, per in proj["written"].items()
                        if fig.ET_NAMES[k] in NAMES for step, b in enumerate(per, start=1)],
                       columns=["name", "step", "b"])

# Data written as snapshots are added, cumulative GB, both
cum = pd.concat([measured_w.assign(source="measured"), model_w.assign(source="model")])
cum = cum.sort_values(["source", "name", "step"])
cum["gb"] = cum.groupby(["source", "name"]).b.cumsum() / 1e9

# What RMoR-density's merges read of the row groups already stored, a write (merge-read)
merge_read = float(q(S + """
    SELECT avg(bytes_read) / 1e6 AS mb FROM s
    WHERE name = 'RMoR-density' AND phase = 'merge-read' AND step >= 1""").mb.iloc[0])

# Bytes a write over the 240 writes
mw = measured_w.groupby("name").b.mean() / 1e6
pw = model_w.groupby("name").b.mean() / 1e6
writes = pd.DataFrame({"model": pw, "measured": mw}).reindex(NAMES)
writes["model / measured"] = writes.model / writes.measured
writes["within 20%"] = (writes["model / measured"] - 1).abs() <= 0.2

# The reads at the model's read points (every 40 writes): each window's bytes over its rows at
# the same run's full-read bytes a row, and the sorted read's peak held
mr = q(S + """,
    f AS (SELECT name, step, bytes_read / rows AS bpr FROM s
          WHERE phase = 'read_full' AND reader = 'cached-skip')
    SELECT s.name, s.step, s.window_at AS "window", s.bytes_read / (s.rows * f.bpr) AS ratio
    FROM s JOIN f USING (name, step)
    WHERE s.phase = 'read_window' AND s.reader = 'cached-skip' AND s.step % 40 = 0""")
mh = q(S + """
    SELECT name, step, held_bytes / 1e9 AS held_gb FROM s
    WHERE phase = 'read_sorted' AND reader = 'cached-skip' AND step % 40 = 0""")
mrg = q(S + """
    SELECT name, row_groups FROM s WHERE phase = 'state'
    QUALIFY step = max(step) OVER (PARTITION BY name)""").set_index("name").row_groups
pr = fig.et_reads(proj)
pr = pr[pr.name.isin(NAMES)]

LAST = WRITES
win = pd.concat([mr.assign(source="measured"), pr[["name", "step", "window", "ratio"]]
                 .assign(source="model")])
held = pd.concat([mh[["name", "step", "held_gb"]].assign(source="measured"),
                  pr.groupby(["name", "step"]).held_gb.first().reset_index().assign(source="model")])

at = win[win.step == LAST].pivot_table(index=["name", "window"], columns="source", values="ratio")
at["model / measured"] = at.model / at.measured
mean_at = win[win.step == LAST].groupby(["name", "source"]).ratio.mean().unstack()
h_at = held[held.step == LAST].pivot_table(index="name", columns="source", values="held_gb")
reads = pd.DataFrame({
    "windows, model": mean_at.model, "windows, measured": mean_at.measured,
    "sorted GB, model": h_at.model, "sorted GB, measured": h_at.measured}).reindex(NAMES)
reads["windows: model / measured"] = reads["windows, model"] / reads["windows, measured"]
reads["sorted: model / measured"] = reads["sorted GB, model"] / reads["sorted GB, measured"]


def within(x, k=1.5):
    return 1 / k <= x <= k


checks = {}
for n in NAMES:
    checks[f"{n}'s bytes a write"] = bool(writes.loc[n, "within 20%"])
    checks[f"{n}'s windows"] = within(reads.loc[n, "windows: model / measured"])
    checks[f"{n}'s sorted read"] = within(reads.loc[n, "sorted: model / measured"])
missed = [c for c, ok in checks.items() if not ok]


def f1(x):
    return f"{x:.1f}"


def f2(x):
    return f"{x:.2f}"

```

Does `analysis/event_time_model.py`, which projects the inserts at 128 MiB row groups for the
designs that cannot run ([](inserts-128.md)), match the three designs that ran? Campaign `w128` on
havoc ran MoR, MoR (compacted) and RMoR-density on the inserts for 240 writes ([](suite-128.md)).
The revision of 2026-11-12 in docs/HYPOTHESES.md set the test: the projection stands beside the
runs only if, for each of the three, the model's bytes a write come within 20% of the run's, and
its windows' bytes over live bytes and its sorted read's peak come within a factor of 1.5 at the
240th write.

**The model:** {eval}`fig.ET_NAMES['mor']`, {eval}`fig.ET_NAMES['compact']` and
{eval}`fig.ET_NAMES['density']` as the projection page draws them, at its defaults
({eval}`f"{args['initial']:,}"` initial rows, {eval}`f"{args['per_write']:,}"` inserts a write,
{eval}`args['bpr']` bytes a row, θ {eval}`args['theta']`, seed {eval}`args['seed']` of numpy's
generator, not the op log's). It reads at every 40th write; the runs' full and sorted reads come
every 10th and their windows after every write, so the reads below are the runs' at the model's
points.

```{code-cell} ipython3
:tags: [remove-input]

table(runs[["name", "run_id"]].rename(columns={"name": "design"}), caption="The runs (campaign w128)")
```

## The verdict

```{code-cell} ipython3
:tags: [remove-cell]

stands = "stands" if not missed else "does not stand: " + ", ".join(missed) + " missed"
DEN = "RMoR-density"
new_rows = args["per_write"] * args["bpr"] / 1e6  # the model's bytes for a write's own rows
```

**The projection {eval}`stands`.** Of the {eval}`len(checks)` checks,
{eval}`len(checks) - len(missed)` held. The model's windows come within
{eval}`f2(reads["windows: model / measured"].min())` to
{eval}`f2(reads["windows: model / measured"].max())` of the runs' and its sorted reads within
{eval}`f2(reads["sorted: model / measured"].min())` to
{eval}`f2(reads["sorted: model / measured"].max())`. It writes
{eval}`f2(writes.loc["MoR", "model / measured"])` and
{eval}`f2(writes.loc["MoR (compacted)", "model / measured"])` of what MoR and MoR (compacted)
wrote, and {eval}`f2(writes.loc[DEN, "model / measured"])` of RMoR-density's
{eval}`f1(writes.loc[DEN, "measured"])` MB a write.

**Where it misses:** RMoR-density's merges. Of the run's
{eval}`f1(writes.loc[DEN, "measured"])` MB a write, {eval}`f1(merge_read)` MB rewrite row groups
already stored (what its merges read, phase `merge-read`) and
{eval}`f1(writes.loc[DEN, "measured"] - merge_read)` MB are the write's own rows. The model rewrites
{eval}`f1(writes.loc[DEN, "model"] - new_rows)` MB of stored rows a write, besides the write's own
{eval}`f1(new_rows)`. As registered, the model is revised in a section of its own before its
projection of RMoR, RMoW-external and RMoW-append stands beside the runs.

## Bytes written

```{code-cell} ipython3
:label: model-128-written
:tags: [remove-input]
:class: col-page-right

alt.Chart(cum[cum.step % 4 == 0]).mark_line(strokeWidth=2).encode(
    x=alt.X("step:Q", title="snapshot", scale=alt.Scale(domain=[0, WRITES], nice=False)),
    y=alt.Y("gb:Q", title="data written (GB, log scale)", scale=alt.Scale(type="log")),
    color=alt.Color("name:N", sort=NAMES, scale=COLOR, title="design"),
    strokeDash=alt.StrokeDash("source:N", sort=["measured", "model"], title=None),
    detail="name:N",
    tooltip=["name", "source", "step", alt.Tooltip("gb:Q", format=".2f")],
).properties(width=460, height=220)
```

```{code-cell} ipython3
:tags: [remove-input]

table(writes.round(2).reset_index().rename(columns={"name": "design"}),
      caption="MB written a write, the mean over the 240 writes: the model's and the run's")
```

## The reads after the {eval}`LAST`th write

```{code-cell} ipython3
:label: model-128-windows
:tags: [remove-input]
:class: col-page-right

alt.Chart(win[win.step == LAST]).mark_line(point=True, strokeWidth=2).encode(
    x=alt.X("window:Q", title="where the 1% range falls in the event-time span",
            scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(values=[0.1, 0.3, 0.5, 0.7, 0.9], format=".1f")),
    y=alt.Y("ratio:Q", title="bytes read over live bytes", scale=alt.Scale(zero=True)),
    color=alt.Color("name:N", sort=NAMES, scale=COLOR, title="design"),
    strokeDash=alt.StrokeDash("source:N", sort=["measured", "model"], title=None),
    detail="name:N",
    tooltip=["name", "source", "window", alt.Tooltip("ratio:Q", format=".2f")],
).properties(width=460, height=220)
```

```{code-cell} ipython3
:tags: [remove-input]

table(at.round(2).reset_index().rename(columns={"name": "design"}),
      caption=f"Each 1% range by event time after write {LAST}: bytes read over live bytes")
```

```{code-cell} ipython3
:label: model-128-sorted
:tags: [remove-input]
:class: col-page-right

alt.Chart(held).mark_line(point=True, strokeWidth=2).encode(
    x=alt.X("step:Q", title="snapshot", scale=alt.Scale(domain=[0, WRITES], nice=False)),
    y=alt.Y("held_gb:Q", title="sorted read: bytes held at once (GB)", scale=alt.Scale(zero=True)),
    color=alt.Color("name:N", sort=NAMES, scale=COLOR, title="design"),
    strokeDash=alt.StrokeDash("source:N", sort=["measured", "model"], title=None),
    detail="name:N",
    tooltip=["name", "source", "step", alt.Tooltip("held_gb:Q", format=".2f")],
).properties(width=460, height=200)
```

```{code-cell} ipython3
:tags: [remove-input]

t = reads.copy()
t["row groups, model"] = [proj["row_groups"][k] for _, _, k, _ in RUNS]
t["row groups, measured"] = mrg.reindex(NAMES)
table(t.round(2).reset_index().rename(columns={"name": "design"}),
      caption=f"After write {LAST}: the five windows' mean bytes over live bytes, the sorted read's "
      "peak held (GB), and live row groups, the model's and the run's")
```

## Limits

- **The model counts whole row groups.** A run's reader skips row groups by their statistics and
  reads only the columns it asks for, and the runs count footers and pages; the model counts none
  of these.
- **Its rows are 30.1 bytes,** the initial load's; the inserts took 33.7 a row at 16 MiB.
- **Its draws are numpy's,** not the op log's: the same distribution of lateness, not the same
  rows.
- **Requests are not modelled.** The runs' requests are on [](suite-128.md).
