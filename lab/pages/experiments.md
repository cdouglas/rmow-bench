---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Experiments: what each one varies'
---

```{code-cell} ipython3
:tags: [remove-cell]

import json

import pandas as pd
from IPython.display import display

from labbook import q, table

# every parameter of every canonical run, by campaign (a run in several campaigns appears in
# each); parameters that only repeat the run's label are left out
p = q("""
    WITH r AS (
        SELECT run_id, label, started_at,
               unnest(CASE WHEN len(campaigns) = 0 THEN ['(no campaign)'] ELSE campaigns END)
                   AS campaign
        FROM canonical_runs)
    SELECT r.campaign, r.label, r.run_id, r.started_at, p.key, p.value
    FROM r JOIN run_params p USING (run_id)
    WHERE p.value IS DISTINCT FROM r.label
    ORDER BY r.campaign, r.started_at, p.key""")
titles = dict(q("SELECT campaign, title FROM campaigns").values.tolist())


def shown(key, value):
    """Lists as their items (or their items' names), power-of-two byte sizes in KiB/MiB/GiB."""
    try:
        v = json.loads(value)
    except (TypeError, ValueError):
        v = value
    if isinstance(v, list):
        return ", ".join(str(e.get("name", e)) if isinstance(e, dict) else str(e) for e in v)
    if key.lower().endswith("bytes") and isinstance(v, int) and v > 0:
        for unit, size in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
            if v % size == 0:
                return f"{v // size} {unit}"
    return str(value)


p["shown"] = [shown(k, v) for k, v in zip(p.key, p.value, strict=True)]
everywhere = p.drop_duplicates("run_id").shape[0]
overall = p.drop_duplicates(["run_id", "key"]).groupby("key").value.nunique()
```

Which settings each experiment (campaign) varies, generated from every canonical run's
parameters. The first table summarizes; then one table per campaign lists its runs and the
settings that differ between them; the last lists what every experiment shares.

```{code-cell} ipython3
:label: experiments-summary
:tags: [remove-input]
:class: col-page-right

rows = []
for campaign, c in p.groupby("campaign", sort=False):
    within = c.groupby("key").value.nunique()
    fixed = [k for k in within.index if within[k] == 1 and overall[k] > 1]
    rows.append({
        "campaign": campaign,
        "question": titles.get(campaign, ""),
        "runs": c.run_id.nunique(),
        "varies": ", ".join(k for k in within.index if within[k] > 1),
        "fixed here, varies elsewhere": "; ".join(
            f"{k} = {c[c.key == k].shown.iloc[0]}" for k in fixed),
    })
table(pd.DataFrame(rows))
```

```{code-cell} ipython3
:label: experiments-by-campaign
:tags: [remove-input]
:class: col-page-right

for campaign, c in p.groupby("campaign", sort=False):
    within = c.groupby("key").value.nunique()
    keys = [k for k in within.index if within[k] > 1]
    if not keys:
        continue
    wide = (c[c.key.isin(keys)]
            .pivot_table(index=["started_at", "label"], columns="key", values="shown",
                         aggfunc="first")
            .reset_index().drop(columns="started_at"))
    display(table(wide[["label", *keys]], caption=f"{campaign}: {titles.get(campaign, '')}"))
```

What every canonical run shares:

```{code-cell} ipython3
:label: experiments-baseline
:tags: [remove-input]

base = (p[p.key.isin(overall[overall == 1].index)].drop_duplicates("key")[["key", "shown"]]
        .rename(columns={"shown": "value"}))
table(base, caption=f"the same in all {everywhere} runs")
```
