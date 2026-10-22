---
title: One run, step by step
kernelspec:
  name: labbook
  display_name: Python (labbook)
---

```{code-cell} python
:tags: [remove-cell]
import altair as alt
from labbook import figures_module, q, table, theme
theme()
alt.data_transformers.disable_max_rows()  # every canonical run's steps, filtered in the page
color, shape = figures_module().arm_encoding(
    q("SELECT arm FROM arm_meta WHERE headline").arm.tolist())
runs = q("""SELECT run_id FROM canonical_runs WHERE source = 'bench'
            ORDER BY list_contains(campaigns, 'm1') DESC, started_at""").run_id.tolist()
readers = q("SELECT DISTINCT reader FROM steps WHERE reader IS NOT NULL ORDER BY reader").reader
run = alt.param(name="run", value=runs[0],
                bind=alt.binding_select(options=runs, name="run "))
reader = alt.param(name="reader", value="one-footer",
                   bind=alt.binding_select(options=list(readers), name="reader "))
```

What `analysis/plots.py` draws for one run, for every canonical run: pick the run (and, for the
reads, the reader model) below the charts. Headline arms only: MoR v3, TailCoW-C, RMoW-B and
RMoW-C, appendable and split. Local and in-memory runs: requests and bytes are exact and
reproducible; wall time is not shown.

```{code-cell} python
:label: run-steps
:tags: [remove-input]
:class: col-page-right
base = """
    SELECT s.run_id, s.step, m.arm_label, s.reader, s.bytes_written / 1e6 AS mb_written,
           s.bytes_read / 1e6 AS mb_read, s.requests, s.gets,
           s.stored_bytes / s.live_bytes AS space_amp,
           (s.stored_bytes - s.reclaimable_bytes) / s.live_bytes AS kept_amp
    FROM steps s JOIN canonical_runs USING (run_id) JOIN arm_meta m USING (arm)
    WHERE m.headline AND s.phase = $phase"""
batch, full, recent, state = (q(base, phase=p) for p in
                              ("batch", "read_full", "read_recent", "state"))


def panel(df, y, title, unit, log=False, by_reader=False):
    keep = alt.datum.run_id == run
    if log:
        keep = keep & (alt.datum[y] > 0)
    if by_reader:
        keep = keep & (alt.datum.reader == reader)
    return (alt.Chart(df, title=title).mark_line(point=True)
            .encode(x=alt.X("step:Q", title="batch"),
                    y=alt.Y(f"{y}:Q", title=unit,
                            scale=alt.Scale(type="log") if log else alt.Undefined),
                    color=color, shape=shape,
                    tooltip=["arm_label", "step", alt.Tooltip(f"{y}:Q", format=".3g")])
            .transform_filter(keep)
            .properties(width=250, height=170))


alt.vconcat(
    alt.hconcat(panel(batch, "mb_written", "Bytes written per batch", "MB", log=True),
                panel(batch, "requests", "Requests per batch", "requests", log=True)),
    alt.hconcat(panel(full, "gets", "Full read: GETs", "GETs", log=True, by_reader=True),
                panel(full, "mb_read", "Full read: bytes", "MB", by_reader=True),
                panel(recent, "gets", "Newest 1% of event time: GETs", "GETs", by_reader=True)),
    alt.hconcat(panel(state, "space_amp", "Stored / live bytes (every version kept)", "ratio"),
                panel(state, "kept_amp", "Kept / live bytes (older versions expired)", "ratio")),
).add_params(run, reader)
```

The per-arm summary of every M1 run (all arms; `arm_summary`, the numbers `analysis/sweep.py`
and `layouts.py` tabulate):

```{code-cell} python
:label: m1-summary
:tags: [remove-input]
:class: col-page-right
table(q("""
    SELECT r.label, m.arm_label AS arm, s.write_mb_per_batch, s.compact_mb_per_batch,
           s.requests_per_batch, s.read_full_gets, s.read_recent_gets, s.read_old_gets,
           s.stored_over_live, s.kept_over_live, s.objects
    FROM arm_summary s JOIN canonical_runs r USING (run_id) JOIN arm_meta m USING (arm)
    WHERE list_contains(r.campaigns, 'm1')
    ORDER BY r.label, m.ord"""))
```
