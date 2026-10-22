---
title: Footer micro-benchmarks
kernelspec:
  name: labbook
  display_name: Python (labbook)
---

```{code-cell} python
:tags: [remove-cell]
import altair as alt
from labbook import q, table, theme
theme()
df = q("""
    SELECT replace(run_id, 'micro/', '') AS file, method,
           params['rowGroups']::INTEGER AS row_groups, params['columns'] AS columns,
           score, ci_lo, ci_hi, unit,
           CASE WHEN ci_lo > 0 THEN ci_lo END AS ci_lo_drawn
    FROM micro WHERE metric = 'score'
    ORDER BY file, method, columns, row_groups""")
```

JMH average time per operation (µs) of the Parquet footer operations against the number of row
groups, for 11 and 50 columns, with the 99.9% confidence interval. `footer-merge-shallow` is a
later `mergeAndSerialize` variant. Both axes are log 10, so intervals reaching below zero (too few
samples for the spread) are not drawn; the table below lists them.

```{code-cell} python
:label: footer-micro
:tags: [remove-input]
:class: col-page-right
base = alt.Chart(df).encode(
    x=alt.X("row_groups:Q", title="row groups", scale=alt.Scale(type="log")),
    color=alt.Color("columns:N", title="columns"),
    strokeDash=alt.StrokeDash("file:N", title="result file"))
lines = base.mark_line(point=True).encode(
    y=alt.Y("score:Q", title="µs/op", scale=alt.Scale(type="log")),
    tooltip=["file", "method", "row_groups", "columns", alt.Tooltip("score:Q", format=".3g"),
             alt.Tooltip("ci_lo:Q", format=".3g"), alt.Tooltip("ci_hi:Q", format=".3g")])
bars = base.mark_rule().encode(y="ci_lo_drawn:Q", y2="ci_hi:Q").transform_filter(
    alt.datum.ci_lo_drawn != None)  # noqa: E711 (a Vega expression, not Python)
(lines + bars).properties(width=230, height=200).facet(column=alt.Column("method:N", title=None))
```

```{code-cell} python
:label: footer-micro-table
:tags: [remove-input]
table(df.drop(columns="ci_lo_drawn"))
```
