---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Repetitions
---

```{code-cell} ipython3
:tags: [remove-cell]

from labbook import q, table
```

Requests and bytes are a deterministic function of the op log, the configuration and the code,
so two runs of one configuration must match exactly in them (wall and CPU time may not). This
page checks every repetition: runs sharing a label and configuration hash. For any two runs,
`lab sql "FROM steps_diff('<run a>', '<run b>') WHERE len(differs) > 0 OR one_run_only"` lists
the records that differ (`analysis/compare_runs.py`).

```{code-cell} ipython3
:label: repetitions
:tags: [remove-input]

reps = q("FROM repetitions ORDER BY label")
rows = []
for rep in reps.itertuples():
    first = rep.run_ids[0]
    for other in rep.run_ids[1:]:
        d = q("""SELECT arm, count(*) AS records,
                        count(*) FILTER (WHERE len(differs) > 0) AS differ,
                        count(*) FILTER (WHERE one_run_only) AS one_run_only
                 FROM steps_diff($a, $b) GROUP BY arm ORDER BY arm""", a=first, b=other)
        d.insert(0, "runs", f"{first} vs {other}")
        rows.append(d)
if rows:
    import pandas as pd
    out = table(pd.concat(rows))
else:
    print("No repetitions yet: every finished run has its own label and configuration hash.")
    out = None
out
```

```{code-cell} ipython3

```
