---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: One run, step by step
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()
alt.data_transformers.disable_max_rows()  # every canonical run's steps, filtered in the page
# The designs by their results names (figures.py WL_COLOR, WL_SHAPE): RMoR is rmor-c-kept on the
# updates and rmor-c (sealed) on the inserts; no run holds both
NAMES = {"mor-v3": "MoR", "rmor-c-kept": "RMoR", "rmor-c": "RMoR", "rmor-c-footer-kept": "RMoW-external",
         "rmow-c": "RMoW-append", "rmor-density": "deferred merges"}
ARMS = ", ".join(f"('{a}', '{n}')" for a, n in NAMES.items())
DOMAIN = ["MoR", "RMoR", "RMoW-external", "RMoW-append", "deferred merges"]
color = alt.Color("design:N", title="design", sort=DOMAIN,
                  scale=alt.Scale(domain=DOMAIN, range=[fig.WL_COLOR[d] for d in DOMAIN]))
shape = alt.Shape("design:N", title="design", sort=DOMAIN,
                  scale=alt.Scale(domain=DOMAIN, range=[fig.WL_SHAPE[d] for d in DOMAIN]))
# havoc's runs (requests, bytes and held memory), the two workloads' main points first
runs = q("""SELECT run_id FROM canonical_runs
            WHERE source = 'bench' AND host = 'havoc'
            ORDER BY list_contains(campaigns, 'workloads-ref') DESC, label LIKE '%-upd-%' DESC, label,
                     started_at""").run_id.tolist()
readers = q("""SELECT DISTINCT reader FROM steps JOIN canonical_runs USING (run_id)
               WHERE source = 'bench' AND host = 'havoc' AND reader IS NOT NULL
               ORDER BY reader = 'cached-skip' DESC, reader""").reader
run = alt.param(name="run", value=runs[0],
                bind=alt.binding_select(options=runs, name="run "))
reader = alt.param(name="reader", value=readers[0],
                   bind=alt.binding_select(options=list(readers), name="reader "))
```

What each design spends at every step of one run, for each of havoc's runs (requests, bytes and
held memory are exact there; the laptop's twins count the same and add instructions): pick the run,
and for the reads the reader, below the charts. Every run has a reader that skips dead row groups
(`cached-skip`), which the results pages read through; the windows campaign's runs have the others
too. Wall time is not shown: these are local runs.

```{code-cell} ipython3
:label: run-steps
:tags: [remove-input]
:class: col-page-right

base = f"""
    WITH d(arm, design) AS (VALUES {ARMS})
    SELECT s.run_id, s.step, d.design, s.reader, s.bytes_written / 1e6 AS mb_written,
           s.bytes_read / 1e6 AS mb_read, s.requests, s.gets,
           s.stored_bytes / s.live_bytes AS space_amp,
           (s.stored_bytes - s.reclaimable_bytes) / s.live_bytes AS kept_amp
    FROM steps s JOIN canonical_runs r USING (run_id) JOIN d USING (arm)
    WHERE r.source = 'bench' AND r.host = 'havoc' AND s.phase = $phase"""
write, full, window, state = (q(base, phase=p) for p in
                              ("batch", "read_full", "read_window", "state"))
# a window read: the mean of the step's five 1% windows
window = window.groupby(["run_id", "step", "design", "reader"], as_index=False)[["mb_read", "gets"]].mean()


def panel(df, y, title, unit, log=False, by_reader=False):
    keep = alt.datum.run_id == run
    if log:
        keep = keep & (alt.datum[y] > 0)
    if by_reader:
        keep = keep & (alt.datum.reader == reader)
    return (alt.Chart(df, title=title).mark_line(point=True)
            .encode(x=alt.X("step:Q", title="write"),
                    y=alt.Y(f"{y}:Q", title=unit,
                            scale=alt.Scale(type="log") if log else alt.Undefined),
                    color=color, shape=shape,
                    tooltip=["design", "step", alt.Tooltip(f"{y}:Q", format=".3g")])
            .transform_filter(keep)
            .properties(width=250, height=170))


alt.vconcat(
    alt.hconcat(panel(write, "mb_written", "Bytes written a write", "MB", log=True),
                panel(write, "requests", "Requests a write", "requests", log=True)),
    alt.hconcat(panel(full, "gets", "Full scan: GETs", "GETs", log=True, by_reader=True),
                panel(full, "mb_read", "Full scan: bytes", "MB", by_reader=True),
                panel(window, "mb_read", "A 1% range read: bytes", "MB", by_reader=True)),
    alt.hconcat(panel(state, "space_amp", "Stored / live bytes (every version kept)", "ratio"),
                panel(state, "kept_amp", "Kept / live bytes (older versions expired)", "ratio")),
).add_params(run, reader)
```

The per-design summary of the two workloads' main points (havoc's runs of campaign
`workloads-ref`; `arm_summary`):

```{code-cell} ipython3
:label: wl-summary
:tags: [remove-input]
:class: col-page-right

table(q(f"""
    WITH d(arm, design) AS (VALUES {ARMS})
    SELECT r.label, d.design, s.write_mb_per_batch AS write_mb, s.compact_mb_per_batch AS compact_mb,
           s.requests_per_batch AS requests, s.stored_over_live, s.kept_over_live,
           s.objects
    FROM arm_summary s JOIN canonical_runs r USING (run_id) JOIN d USING (arm)
    WHERE list_contains(r.campaigns, 'workloads-ref') AND r.host = 'havoc'
    ORDER BY r.label, list_position(['MoR', 'RMoR', 'RMoW-external', 'RMoW-append', 'deferred merges'],
                                    d.design)"""))
```
