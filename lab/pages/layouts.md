---
title: Appendable and split RMoW files
kernelspec:
  name: labbook
  display_name: Python (labbook)
---

```{code-cell} python
:tags: [remove-cell]
import altair as alt
from labbook import figures_module, q, table, theme
theme()
COLS = ["write_mb_per_batch", "compact_mb_per_batch", "requests_per_batch", "read_full_gets",
        "read_full_mb", "read_old_gets", "stored_over_live", "kept_over_live", "objects",
        "live_objects", "footer_kb"]
ARMS = ["mor-v3", "cow", "tailcow-c", "rmow-b", "rmow-b-split", "rmow-c", "rmow-c-split"]
color, shape = figures_module().arm_encoding(ARMS)
t = q(f"""
    SELECT r.label, m.arm_label, m.ord, s.arm, {", ".join("s." + c for c in COLS)}
    FROM arm_summary s JOIN canonical_runs r USING (run_id) JOIN arm_meta m USING (arm)
    WHERE list_contains(r.campaigns, 'split-vs-append') AND list_contains($arms, s.arm)
    ORDER BY r.label, m.ord""", arms=ARMS)
```

The `split-vs-append` campaign, what H5 is decided by (`docs/HYPOTHESES.md`): per arm at the last
step, bytes and requests per batch (and compaction's), the last probe's full read under the
one-footer reader, stored and kept bytes against live bytes, and objects stored and in use.

```{code-cell} python
:label: split-vs-append
:tags: [remove-input]
:class: col-page-right
metrics = {"write_mb_per_batch": "MB written per batch", "requests_per_batch": "requests per batch",
           "read_full_gets": "full read GETs", "kept_over_live": "kept / live bytes"}
long = t.melt(id_vars=["label", "arm_label", "ord"], value_vars=list(metrics), var_name="metric")
long["metric"] = long.metric.map(metrics)
long["run"] = long.label.str.removeprefix("local-split-")
(alt.Chart(long).mark_point(filled=True, size=70)
 .encode(x=alt.X("value:Q", title=None, axis=alt.Axis(tickCount=4)),
         y=alt.Y("arm_label:N", title=None, sort=alt.EncodingSortField("ord")),
         color=color, shape=shape, tooltip=["label", "arm_label", alt.Tooltip("value:Q", format=".3g")])
 .properties(width=130, height=130)
 .facet(row=alt.Row("run:N", title=None, header=alt.Header(labelAngle=-90, labelFontSize=11)),
        column=alt.Column("metric:N", title=None, sort=list(metrics.values())))
 .resolve_scale(x="independent"))
```

The tables `analysis/layouts.py` writes to `layouts.md`, one per run:

```{code-cell} python
:label: split-vs-append-tables
:tags: [remove-input]
:class: col-page-right
from IPython.display import display
for label, d in t.groupby("label", sort=True):
    display(table(d[["arm"] + COLS], caption=label))
```
