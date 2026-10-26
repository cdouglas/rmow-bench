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
S = t.set_index(["label", "arm"])
# requests per batch by class, bytes, and compaction, per run and arm (batches, the 60 of each run)
w = q("""
    SELECT r.label, s.arm,
           avg(s.puts + s.appends) FILTER (WHERE s.phase = 'batch') AS put_class,
           avg(s.appends) FILTER (WHERE s.phase = 'batch') AS appends,
           avg(s.gets + s.heads) FILTER (WHERE s.phase = 'batch') AS gets,
           avg(s.requests) FILTER (WHERE s.phase = 'batch') AS requests,
           avg(s.bytes_written) FILTER (WHERE s.phase = 'batch') AS bytes,
           count(*) FILTER (WHERE s.phase = 'compact' AND s.bytes_written > 0) AS compactions,
           sum(s.bytes_written) FILTER (WHERE s.phase = 'batch' AND s.step <= 9) AS bytes_1_9
    FROM steps s JOIN canonical_runs r USING (run_id)
    WHERE list_contains(r.campaigns, 'split-vs-append') GROUP BY ALL""").set_index(["label", "arm"])
NC, C10, UNI = ("local-split-recency-exp-nocompact", "local-split-recency-exp-compact10",
                "local-split-uniform-compact10")


def pair(run, arm, col, frame=w):
    """(split, appendable) of a column."""
    return frame.loc[(run, arm + "-split"), col], frame.loc[(run, arm), col]


def rng(xs, fmt="{:.2f}"):
    lo, hi = min(xs), max(xs)
    return fmt.format(lo) if fmt.format(lo) == fmt.format(hi) else f"{fmt.format(lo)}–{fmt.format(hi)}"


BC = ["rmow-b", "rmow-c"]
d_put = [pair(NC, a, "put_class")[0] - pair(NC, a, "put_class")[1] for a in BC]
d_get = [pair(NC, a, "gets")[1] - pair(NC, a, "gets")[0] for a in BC]
d_req = [pair(NC, a, "requests")[1] - pair(NC, a, "requests")[0] for a in BC]
changed = [w.loc[(NC, a), "appends"] for a in BC]
bytes_r = [pair(NC, a, "bytes")[0] / pair(NC, a, "bytes")[1] - 1 for a in BC]
reads_same = all(pair(NC, a, c, S)[0] == pair(NC, a, c, S)[1] for a in BC for c in ["read_full_gets", "read_old_gets"])
read_mb = [pair(NC, a, "read_full_mb", S)[0] / pair(NC, a, "read_full_mb", S)[1] - 1 for a in BC]
cow19 = w.loc[(UNI, "cow"), "bytes_1_9"]
N = {
    "d_put": rng(d_put), "changed": rng(changed), "d_get": rng(d_get), "d_req": rng(d_req),
    "bytes_pct": rng([100 * x for x in bytes_r], "{:.1f}%"),
    "reads_same": "the same" if reads_same else "different",
    "read_mb_pct": rng([100 * abs(x) for x in read_mb], "{:.1f}%"),
    "kept_app": rng([S.loc[(NC, a), "kept_over_live"] for a in BC], "{:.1f}"),
    "kept_split": rng([S.loc[(NC, a + "-split"), "kept_over_live"] for a in BC]),
    "kept_split_b": f"{S.loc[(NC, 'rmow-b-split'), 'kept_over_live']:.2f}",
    "kept_split_c": f"{S.loc[(NC, 'rmow-c-split'), 'kept_over_live']:.2f}",
    "objects": rng([S.loc[(NC, a + "-split"), "objects"] for a in BC], "{:.0f}"),
    "live_objects": rng([S.loc[(NC, a + "-split"), "live_objects"] for a in BC], "{:.0f}"),
    "footer_split": rng([S.loc[(NC, a + "-split"), "footer_kb"] for a in BC], "{:.0f}"),
    "footer_app": rng([S.loc[(NC, a), "footer_kb"] for a in BC], "{:.0f}"),
    "c_app": rng([w.loc[(C10, a), "compactions"] for a in BC], "{:.0f}"),
    "c_split": rng([w.loc[(C10, a + "-split"), "compactions"] for a in BC], "{:.0f}"),
    "c_app_mb": rng([S.loc[(C10, a), "compact_mb_per_batch"] for a in BC]),
    "c_split_mb": rng([S.loc[(C10, a + "-split"), "compact_mb_per_batch"] for a in BC]),
    "c_kept_app": rng([S.loc[(C10, a), "kept_over_live"] for a in BC]),
    "c_kept_split": rng([S.loc[(C10, a + "-split"), "kept_over_live"] for a in BC]),
    "uni_cow": rng([w.loc[(UNI, a), "bytes_1_9"] / cow19 for a in BC + [x + "-split" for x in BC]], "{:.3f}"),
}
```

What does RMoW pay for putting each version's regions in new objects instead of appending to
one? The `split-vs-append` campaign compares the two layouts of RMoW-B and RMoW-C, and decides
H5 (`docs/HYPOTHESES.md`). It reads three runs: recency-skewed updates without compaction and
with compaction every 10 batches, and uniform updates with compaction. They are the rerun of
2026-10-25 on the current build, which replaced the static report's
(`docs/measurements/split-vs-append-2026-10-21.md`).

- **Appendable:** one object per file.
- **Split:** each version's regions in a new object, and its footer in a footer-only one
  (docs/DESIGN.md, Regions in separate objects).

Per arm at the last step: bytes and requests per batch (and compaction's), and the last probe's
full read under the one-footer reader. **Kept** is what remains once every older version
expires, against live bytes; objects are counted stored and in use.

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


## Findings

All are recency-skewed updates without compaction, RMoW-B and RMoW-C, unless they say otherwise.

- **Split files write the same bytes, in more PUTs and fewer GETs.**
  - They write within {eval}`N["bytes_pct"]` of the appendable bytes; each region object carries
    its own small footer.
  - They take {eval}`N["d_put"]` more PUT-class requests per batch, one per file a batch changes
    ({eval}`N["changed"]`): a region object and a footer object, instead of one append.
  - Their writer reads {eval}`N["d_get"]` fewer GETs per batch. It fetches a split file's small
    footer-only object whole, in one request, where it reads an appendable file's footer as
    parquet-java does, in two.
  - On balance they take {eval}`N["d_req"]` fewer requests per batch.
- **Reads cost {eval}`N["reads_same"]` GETs**, full and time travel, and bytes within
  {eval}`N["read_mb_pct"]`.
- **Expiry reclaims what append cannot.** Without compaction an appendable file keeps every
  replaced region, {eval}`N["kept_app"]`× its live bytes at batch 60. Once older versions expire,
  a split file keeps {eval}`N["kept_split"]`×. What remains is replaced row groups inside objects
  still in use, chiefly the large initial file.
- **With compaction, split files rewrite less and keep less.** Compacting every 10 batches, the
  appendable files compacted {eval}`N["c_app"]` times ({eval}`N["c_app_mb"]` MB per batch) to keep
  {eval}`N["c_kept_app"]`×. The split files moved row groups out of mostly dead objects
  {eval}`N["c_split"]` times ({eval}`N["c_split_mb"]` MB per batch), and keep
  {eval}`N["c_kept_split"]`×.
- **Objects.** A split file adds one or two objects per version: {eval}`N["objects"]` stored by
  batch 60, against 2 appendable objects. The current version reads from
  {eval}`N["live_objects"]` of them, against 2.
- **The footer grows by the object names.** Each column chunk of a row group in another object
  carries its `file_path`: {eval}`N["footer_split"]` KB against {eval}`N["footer_app"]` KB.
- **Uniform updates validate it.** Every batch touches every row group, so region merge on write
  must write what copy-on-write writes, in either layout. Over batches 1–9, before the first
  compaction, each RMoW arm wrote {eval}`N["uni_cow"]`× CoW's bytes.

## H5 (docs/HYPOTHESES.md)

- **Writes: held for PUTs and bytes; contradicted for requests in all.** One more PUT-class
  request per changed file and the same bytes, as predicted. But split files now take
  {eval}`N["d_req"]` fewer requests per batch in all. H5 was registered when RMoW's writer kept its
  footers across batches. It reads them every batch now (README, I/O parity rule 6), and a
  footer-only object costs one GET where an appendable footer costs two.
- **Reads: held.** {eval}`N["reads_same"].capitalize()` GETs, full and time travel, and the same
  bytes within {eval}`N["read_mb_pct"]`.
- **Space: held for RMoW-B ({eval}`N["kept_split_b"]`×), just missed for RMoW-C
  ({eval}`N["kept_split_c"]`×)** against 1.5× without compaction. With compaction the split files
  need fewer rewrites, as predicted.
- **Objects: held.** The stored count grows with batches; the live count stays a small multiple
  of the appendable layout's.

## Limits

- **One seed, 4 MiB row groups, local.** Requests and bytes are what S3 would see
  (`S3StoreTest`).
- **Expiry is assumed immediate.** Kept bytes assume every older version expires at once. With a
  retention window, a split file's reclaimable objects stay stored until they leave it.
- **The writer's footer reads are parquet-java's,** two requests for a file over 1 MiB. With a
  footer prefetch (`footerPrefetchBytes`), an appendable file's would take one, and the split
  layout's GET advantage would go.
