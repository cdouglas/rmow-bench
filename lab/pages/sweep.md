---
title: Row-group size sweep
kernelspec:
  name: labbook
  display_name: Python (labbook)
---

```{code-cell} python
:tags: [remove-cell]
import altair as alt
from labbook import figures_module, q, table, theme
theme()
fm = figures_module()
color, shape = fm.arm_encoding(fm.SWEEP_ARMS)
t = fm.sweep_summary()
```

The `rg-sweep` campaign: the `local-m1` recency workload with compaction off and
`rowGroupBytes` and `tailSealBytes` both 1, 2, 4 and 8 MiB
(`docs/measurements/rg-sweep-recency-2026-10-21.md`). Reads are the last probe's, under the
one-footer reader. The x axis is log 2.

```{code-cell} python
:label: rg-sweep
:tags: [remove-input]
:class: col-page-right
panels = [title for _, title, _ in fm.SWEEP_PANELS]
long = t.melt(id_vars=["arm_label", "rg_mib"], value_vars=[m for m, _, _ in fm.SWEEP_PANELS],
              var_name="metric", value_name="value").dropna(subset=["value"])
long["panel"] = long.metric.map({m: f"{title} ({unit})" for m, title, unit in fm.SWEEP_PANELS})
(alt.Chart(long).mark_line(point=True)
 .encode(x=alt.X("rg_mib:Q", title="row-group and tail-seal size (MiB)",
                 scale=alt.Scale(type="log", base=2), axis=alt.Axis(values=[1, 2, 4, 8])),
         y=alt.Y("value:Q", title=None), color=color, shape=shape,
         tooltip=["arm_label", "rg_mib", alt.Tooltip("value:Q", format=".3g")])
 .properties(width=220, height=150)
 .facet(facet=alt.Facet("panel:N", title=None,
                        sort=[f"{t} ({u})" for _, t, u in fm.SWEEP_PANELS]), columns=3)
 .resolve_scale(y="independent"))
```

The same numbers, as `analysis/sweep.py` writes them to `sweep.md`; `rg_mb_median` and
`new_rg_mb_median` are the median compressed row group at the end, all of them and those written
after the initial load:

```{code-cell} python
:label: rg-sweep-table
:tags: [remove-input]
:class: col-page-right
table(t[["arm", "rg_mib"] + [m for m, _, _ in fm.SWEEP_PANELS]
        + ["read_old_gets", "row_groups", "rg_mb_median", "new_rg_mb_median"]])
```
