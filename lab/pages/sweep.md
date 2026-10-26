---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Row-group size sweep
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import figures_module, q, table, theme
theme()
fm = figures_module()
color, shape = fm.arm_encoding(fm.SWEEP_ARMS)
t = fm.sweep_summary()
T = t.set_index(["arm", "rg_mib"])


def v(arm, rg, col):
    return T.loc[(arm, rg), col]


bc = t[t.arm.isin(["rmow-b", "rmow-c"])].merge(t[t.arm == "mor-v3"][["rg_mib", "write_mb_per_batch", "requests_per_batch", "read_full_gets"]],
                                             on="rg_mib", suffixes=("", "_mor"))
a = t[t.arm == "rmow-a"].merge(t[t.arm == "mor-v3"][["rg_mib", "write_mb_per_batch", "requests_per_batch", "read_full_gets"]],
                               on="rg_mib", suffixes=("", "_mor"))
N = {
    "b4": f"{v('rmow-b', 4, 'write_mb_per_batch'):.1f}", "b1": f"{v('rmow-b', 1, 'write_mb_per_batch'):.1f}",
    "b_cut": f"{100 * (v('rmow-b', 1, 'write_mb_per_batch') / v('rmow-b', 4, 'write_mb_per_batch') - 1):.0f}%",
    "mor_mb": f"{v('mor-v3', 4, 'write_mb_per_batch'):.2f}",
    "g4": f"{v('rmow-b', 4, 'read_full_gets'):.0f}", "g1": f"{v('rmow-b', 1, 'read_full_gets'):.0f}",
    "g_x": f"{v('rmow-b', 1, 'read_full_gets') / v('rmow-b', 4, 'read_full_gets'):.1f}",
    "f4": f"{v('rmow-b', 4, 'footer_kb'):.0f}", "f1": f"{v('rmow-b', 1, 'footer_kb'):.0f}",
    "a_mb_lo": f"{t[t.arm == 'rmow-a'].write_mb_per_batch.min():.1f}",
    "a_mb_hi": f"{t[t.arm == 'rmow-a'].write_mb_per_batch.max():.1f}",
    "b8": f"{v('rmow-b', 8, 'write_mb_per_batch'):.1f}", "s8": f"{v('rmow-b', 8, 'stored_over_live'):.1f}",
    "g8": f"{v('rmow-b', 8, 'read_full_gets'):.0f}", "m8": f"{v('mor-v3', 8, 'read_full_gets'):.0f}",
    "w_lo": f"{(bc.write_mb_per_batch / bc.write_mb_per_batch_mor).min():.0f}",
    "w_hi": f"{(bc.write_mb_per_batch / bc.write_mb_per_batch_mor).max():.0f}",
    "r_lo": f"{100 * (bc.requests_per_batch / bc.requests_per_batch_mor).min():.0f}",
    "r_hi": f"{100 * (bc.requests_per_batch / bc.requests_per_batch_mor).max():.0f}",
    "ra_lo": f"{100 * (a.requests_per_batch / a.requests_per_batch_mor).min():.0f}",
    "ra_hi": f"{100 * (a.requests_per_batch / a.requests_per_batch_mor).max():.0f}",
    "gr_lo": f"{100 * (bc.read_full_gets / bc.read_full_gets_mor).min():.0f}",
    "gr_hi": f"{100 * (bc.read_full_gets / bc.read_full_gets_mor).max():.0f}",
    "ga_lo": f"{100 * (a.read_full_gets / a.read_full_gets_mor).min():.0f}",
    "ga_hi": f"{100 * (a.read_full_gets / a.read_full_gets_mor).max():.0f}",
    "tc_is_mor": all(abs(v("tailcow-c", 1, c) - v("mor-v3", 1, c)) < 1e-9
                     for c in ["write_mb_per_batch", "requests_per_batch", "read_full_gets"]),
}
```

How does the row-group size trade RMoW's writes against its reads? The `rg-sweep` campaign
runs the `local-m1` recency workload with compaction off and `rowGroupBytes` and
`tailSealBytes` both 1, 2, 4 and 8 MiB. Its runs are the rerun of 2026-10-25 on the current
build (docs/HYPOTHESES.md), which replaced those of the static report
(`docs/measurements/rg-sweep-recency-2026-10-21.md`). Reads are the last probe's, under the
one-footer reader. The x axis is log 2.

```{code-cell} ipython3
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

```{code-cell} ipython3
:label: rg-sweep-table
:tags: [remove-input]
:class: col-page-right

table(t[["arm", "rg_mib"] + [m for m, _, _ in fm.SWEEP_PANELS]
        + ["read_old_gets", "row_groups", "rg_mb_median", "new_rg_mb_median"]])
```

## Findings

- **Smaller row groups barely cut RMoW's writes.**
  - From 4 to 1 MiB, RMoW-B writes {eval}`N["b4"]` → {eval}`N["b1"]` MB per batch
    ({eval}`N["b_cut"]`), against MoR-v3's {eval}`N["mor_mb"]` at every size.
  - Its full read meanwhile rises from {eval}`N["g4"]` to {eval}`N["g1"]` GETs
    ({eval}`N["g_x"]`×), and its footer from {eval}`N["f4"]` to {eval}`N["f1"]` KB.
  - Updates fall on the last few batches' rows, so each batch rewrites that window whatever the
    row-group size; smaller row groups only trim the rounding at its edges.
  - RMoW-A never merges its tail, so it writes the least ({eval}`N["a_mb_lo"]`–
    {eval}`N["a_mb_hi"]` MB) and reads the most.
- **Larger row groups cost RMoW writes and space.** At 8 MiB, RMoW-B writes {eval}`N["b8"]` MB
  per batch and stores {eval}`N["s8"]`× its live bytes. Its full read falls to {eval}`N["g8"]`
  GETs, against MoR-v3's {eval}`N["m8"]`.
- **The balance is near 2–4 MiB for this workload.** Below it, reads and footers grow much faster
  than writes shrink; above it, writes and dead bytes grow. No setting changes the order
  against MoR-v3:
  - RMoW-B and -C write {eval}`N["w_lo"]`–{eval}`N["w_hi"]`× its bytes per batch;
  - in {eval}`N["r_lo"]`–{eval}`N["r_hi"]`% of its requests, or {eval}`N["ra_lo"]`–
    {eval}`N["ra_hi"]`% for RMoW-A;
  - and read the table in {eval}`N["gr_lo"]`–{eval}`N["gr_hi"]`% of its GETs, or
    {eval}`N["ga_lo"]`–{eval}`N["ga_hi"]`% for RMoW-A.
- **TailCoW-C degenerates to MoR-v3 at 1 MiB.** Its units are whole batch files (inserts plus
  updated versions), four of which pass a 1 MiB seal, so every unit is sealed on arrival and
  never merges. RMoW's units hold inserts only.

## Limits

- **One workload, uncompacted, one seed.** Recency-skewed updates at 1M rows; compaction is the
  compaction sweep's question.
- **The static report's request ratios are superseded.** Its runs left RMoW's writer holding
  footers across batches, so RMoW's batches took 3–4 requests fewer than these (README, I/O parity
  rule 6).
