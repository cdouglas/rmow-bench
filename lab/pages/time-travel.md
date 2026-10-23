---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Time travel and dead bytes (H4)
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import palette, q, table, theme
theme()

# The runs this page reads: the uncompacted configurations that read old versions, rerun on
# 2026-10-22/23 from cfe40b2, once RmowArm.readAfter planned old versions with their split offsets.
RUNS = [
    "local-m1-recency-exp-nocompact-4a77b88bc5f8/20261022T224506Z",
    "local-m1-uniform-nocompact-c7e50a35a7c5/20261023T042229Z",
    "local-rg-sweep-recency-exp-rg1m-ddb0f45ee310/20261023T042220Z",
    "local-rg-sweep-recency-exp-rg2m-9ff075ae2483/20261023T044613Z",
    "local-rg-sweep-recency-exp-rg4m-b2e059145e94/20261023T042224Z",
    "local-rg-sweep-recency-exp-rg8m-4920abe2147d/20261023T044422Z",
    "local-split-recency-exp-nocompact-309eabcc0dc5/20261023T045645Z",
]
GROUP = """CASE WHEN p.arm LIKE 'rmow-%-split' THEN 'RMoW split'
                WHEN p.arm LIKE 'rmow-%' THEN 'RMoW appendable'
                ELSE 'MoR, CoW, TailCoW' END"""
GROUPS = ["MoR, CoW, TailCoW", "RMoW appendable", "RMoW split"]

summary = q(f"""
    SELECT {GROUP} AS arms, p.reader, count(*) AS pairs,
           count(*) FILTER (WHERE old_gets = cur_gets AND old_streams = cur_streams
                              AND old_aborts = cur_aborts AND old_bytes = cur_bytes
                              AND old_skipped = cur_skipped) AS identical,
           sum(old_gets)::BIGINT AS old_gets, sum(cur_gets)::BIGINT AS cur_gets,
           100.0 * (sum(old_bytes) - sum(cur_bytes)) / sum(cur_bytes) AS bytes_pct,
           sum(old_aborts)::BIGINT AS old_aborts, sum(cur_aborts)::BIGINT AS cur_aborts,
           sum(old_streams)::BIGINT AS old_streams, sum(cur_streams)::BIGINT AS cur_streams,
           sum(old_skipped + cur_skipped)::BIGINT AS skipped,
           bool_and(same_rows) AS same_rows
    FROM h4_pairs p WHERE list_contains($runs, p.run_id)
    GROUP BY ALL ORDER BY reader""", runs=RUNS)
summary = summary.sort_values("arms", key=lambda s: s.map(GROUPS.index), kind="stable")

pairs = q(f"""
    SELECT {GROUP} AS arms, p.reader, p.arm, r.label, p.step, p.old_gets, p.cur_gets
    FROM h4_pairs p JOIN runs_raw r USING (run_id) WHERE list_contains($runs, p.run_id)""",
          runs=RUNS)

dead = q("""
    SELECT replace(r.label, 'local-', '') AS run, f.arm,
           f.bytes_read / st.live_bytes AS read_over_live,
           st.stored_bytes / st.live_bytes AS stored_over_live,
           CASE WHEN st.footer_bytes >= 0 THEN st.footer_bytes / 1e3 END AS footer_kb
    FROM steps f JOIN canonical_runs r USING (run_id)
    JOIN steps st ON st.run_id = f.run_id AND st.arm = f.arm AND st.step = f.step
                 AND st.phase = 'state'
    JOIN arm_meta m ON m.arm = f.arm
    WHERE f.phase = 'read_full' AND f.reader = 'one-footer' AND f.step = 60
      AND list_contains($runs, f.run_id)
      AND r.label IN ('local-rg-sweep-recency-exp-rg1m', 'local-rg-sweep-recency-exp-rg4m',
                      'local-rg-sweep-recency-exp-rg8m', 'local-m1-uniform-nocompact')
      AND f.arm IN ('rmow-b', 'rmow-c', 'mor-v3', 'tailcow-c')
    ORDER BY m.family = 'rmow' DESC, r.label, m.ord""", runs=RUNS)

skips = q("""
    SELECT count(DISTINCT run_id) AS runs, sum(bytes_skipped_through)::BIGINT AS skipped
    FROM steps JOIN canonical_runs USING (run_id) WHERE source = 'bench'""").iloc[0]


def row(arms, reader):
    return summary[(summary.arms == arms) & (summary.reader == reader)].iloc[0]


rmow_rows = summary[summary.arms != "MoR, CoW, TailCoW"]
app_s = row("RMoW appendable", "stock")
rmow = dead[dead.arm.str.startswith("rmow-")]
N = {
    "pairs": f"{summary.pairs.sum()}",
    "identical": f"{summary.identical.sum()}",
    "per_reader": f"{summary.groupby('reader').pairs.sum().iloc[0]}",
    "rmow_pairs": f"{rmow_rows.pairs.sum()}",
    "rmow_per_reader": f"{rmow_rows.groupby('reader').pairs.sum().iloc[0]}",
    "read_live": f"{rmow.read_over_live.min():.3f}–{rmow.read_over_live.max():.3f}",
    "stored_live": f"{rmow.stored_over_live.min():.1f}–{rmow.stored_over_live.max():.0f}",
    "stored_max": f"{rmow.stored_over_live.max():.0f}",
    "read_live_pct": f"{100 * (rmow.read_over_live.max() - 1):.1f}%",
    "skip_runs": f"{skips.runs}",
    "aborts_old": f"{app_s.old_aborts:,} of {app_s.old_streams:,}",
    "aborts_cur": f"{app_s.cur_aborts:,} of {app_s.cur_streams:,}",
    "pairs_per": f"{pairs.groupby(['label', 'arm', 'reader']).size().max()}",
}
```

H4 (docs/HYPOTHESES.md) predicts two things:

- **Reads.** With bounded ranged reads, an old version of an appendable file reads like a current
  version of the same size, and its dead regions add no read bytes.
- **Footers.** Without a footer cache, an old version's footer read on S3 costs one more aborted
  open-ended GET than a current one.

The seven uncompacted configurations that read old versions were rerun for this page, once time
travel planned old RMoW versions with their own split offsets (commit 150cc93). The earlier runs
did not, which made the stock reader's comparison measure the harness. They are deleted, and
[what they showed](#earlier-runs) is recorded below.

## Setup

**Runs.** These are the reruns, pinned by id (`RUNS`):
- `local-m1-recency-exp-nocompact` and `local-m1-uniform-nocompact`;
- `local-rg-sweep-recency-exp-rg{1,2,4,8}m`;
- `local-split-recency-exp-nocompact`.

`local-compact-none` has no compaction either, but it reads no old versions.

**Workload.** The runs share the m1 workload: 1M initial rows, then 60 batches of 10k operations,
70% inserts, 25% updates and 5% deletes.

**Builds.** All are commit cfe40b2:
- `m1-recency` came from `results/builds/20261022T224505Z-cfe40b2f65bf`.
- The others came from four snapshots marked dirty: `20261023T042219Z`, `…042224Z`, `…042228Z`
  and `…045645Z`. Their patch touches only `CLAUDE.md`, `docs/EXPERIMENTS.md` and
  `lab/annotations.toml`.
- The runs ran two or three at a time, which matters only for CPU, and CPU is not used here.

**Probes.** Every 10 batches, every run reads the table three times, each under both reader
models:
- a full read of the current table (`read_full`);
- a recent-window read (`read_recent`);
- a time-travel read of the table as it was 20 batches back (`read_old`).

The two readers are:
- **`one-footer`** reads each footer once, in one 64 KiB prefetch, which is a bounded ranged GET.
- **`stock`** behaves like stock Iceberg: one split per row group, and each split reads the
  footer with two stream requests.

**The comparison is paired.** `read_old` at step s reads exactly the table state that
`read_full` read at step s − 20, when that state was current. So each pair has the same files,
the same versions, the same footers and the same rows. That is stricter than "a current version
of the same size".

Each arm records its history after a batch and before that step's compaction
(`MorArm.java:109`, `RmowArm.java:309`). `read_full` runs after compaction (`Runner.java:165`).
In a run that compacts, a pair would therefore read different files, so compacted runs are left
out.

**Views.** The view `h4_pairs` (`lab/views/30_hypotheses.sql`) holds one row per run, arm,
reader and pair.

**Counting.** Requests and bytes are the tracing FileIO's model of Iceberg's object-store
streams (`TracingFileIO`). `S3StoreTest` matches its GETs, PUTs, HEADs and DELETEs against the
SDK on versitygw.

## Old reads against current reads of the same state

```{code-cell} ipython3
:label: h4-pairs
:tags: [remove-input]
:class: col-page-right

family = palette("family")
color = alt.Color("arms:N", title="arms", sort=GROUPS,
                  scale=alt.Scale(domain=GROUPS,
                                  range=[family["mor"], family["rmow"], family["rmowsplit"]]))
lo, hi = 1, max(pairs.old_gets.max(), pairs.cur_gets.max()) * 1.2
panels = []
for reader in ["one-footer", "stock"]:
    d = pairs[pairs.reader == reader]
    axis = alt.Scale(type="log", domain=[lo, hi])
    same = (alt.Chart(alt.Data(values=[{"g": lo}, {"g": hi}]))
            .mark_line(color="#9a9a9a", strokeDash=[4, 4], strokeWidth=1)
            .encode(x="g:Q", y="g:Q"))
    dots = (alt.Chart(d).mark_point(filled=True, size=60, opacity=0.8)
            .encode(x=alt.X("cur_gets:Q", scale=axis, title="GETs, current read of the state"),
                    y=alt.Y("old_gets:Q", scale=axis, title="GETs, time-travel read"),
                    color=color,
                    tooltip=["label", "arm", "reader", "step", "cur_gets", "old_gets"]))
    panels.append(alt.layer(same, dots).properties(width=330, height=300, title=f"{reader} reader"))
alt.hconcat(*panels)
```

Each point is one pair: a time-travel read against the full read of the same state 20 batches
earlier. Both axes are logarithmic, and the dashed line is "the same": every pair lies on it,
under both readers. The table gives the totals.

```{code-cell} ipython3
:tags: [remove-input]

t = summary[["arms", "reader", "pairs", "identical", "old_gets", "cur_gets", "bytes_pct",
             "old_aborts", "cur_aborts", "skipped"]].rename(columns={
    "identical": "identical in every column", "old_gets": "GETs, old",
    "cur_gets": "GETs, current", "bytes_pct": "bytes, old vs current (%)",
    "old_aborts": "aborts, old", "cur_aborts": "aborts, current",
    "skipped": "bytes skipped through"})
table(t.round({"bytes, old vs current (%)": 2}))
```

The table comes from the view `h4_pairs`, grouped by arms and reader. "Identical" compares GETs,
stream GETs, aborts, bytes read and bytes skipped through. Every pair returned the rows the
reference model holds (`same_rows` is true in all of them).

## Dead bytes at batch 60

These are full reads under the one-footer reader, against the footprint at the same step. "Read
/ live" is bytes read over the bytes the current version uses. "Stored / live" is the whole
store over the same.

```{code-cell} ipython3
:tags: [remove-input]

table(dead.assign(read_over_live=dead.read_over_live.map("{:.3f}".format))
      .round({"stored_over_live": 2, "footer_kb": 0}).rename(columns={
    "read_over_live": "read / live", "stored_over_live": "stored / live",
    "footer_kb": "footer KB"}))
```

The table comes from `steps`: `read_full` (one-footer reader) joined to `state` at step 60.

Across all {eval}`N["skip_runs"]` canonical runs, every read phase under both readers shows
`bytes_skipped_through = 0`. So does every write-side read.

(earlier-runs)=
## The earlier runs

The earlier runs of these configurations (2026-10-21) were deleted on 2026-10-23. Their numbers
below were computed from them before deletion, so this section is a record, not computed. See
also `docs/EXPERIMENTS.md`, Retired runs.

**What the missing split offsets did.** In those runs, `RmowArm.readAfter` built each old
version's `DataFile` from its path and length. Its comment read: "Old versions are planned
without split offsets (a snapshot's entry would carry them)".
- `FileScanTask.split` therefore cut the file into 128 MiB byte ranges (`Scans.java:169`). A
  32 MiB file became one split instead of one per row group.
- **Under the stock reader**, one split meant one footer read (two stream requests) per file,
  instead of one per row group. Old reads took 62% fewer GETs than current reads of the same
  state: 3,938 against 10,248 over 100 pairs on appendable files, and 199 against 239 over 10 on
  split files.
- **Under the one-footer reader**, the GETs were the same. A file read as one split got its
  footer from the 64 KiB prefetch, which also covers the end of its last row group. A current
  file, planned per row group, has its footer read once by `readTail`, which keeps only the
  footer, so its last row group is read whole. Old reads therefore read about 40 KB, or 0.11%,
  fewer bytes.
- **Stock Iceberg would not do this.** It plans an old snapshot from its manifest entry, whose
  split offsets are that version's. The non-RMoW arms keep each version's `DataFile`, offsets
  included, and matched exactly in all 190 pairs per reader.
- `RunnerSmokeTest.timeTravelReadsLikeTheCurrentRead` now checks every request and byte column of
  each pair, for every arm and both readers.
- Published time-travel numbers used the one-footer reader, and are unaffected: `read_old_gets`
  in the rg-sweep and split-vs-append write-ups.

**The reruns against them.** `steps_diff` compared each rerun with its earlier run, record by
record. docs/HYPOTHESES.md (2026-10-22) registered, before the reruns, that only two kinds of
record would differ:
- the RMoW arms' `read_old`;
- in the two m1 runs, the MoR and TailCoW arms, whose earlier builds predate counting Puffin's
  `create()` HEAD and run-relative paths.

| configuration | records | differing | of which RMoW `read_old` | outside the registered kinds |
|---|---|---|---|---|
| m1-recency-exp-nocompact | 3,118 | 1,102 | 28 | 0 |
| m1-uniform-nocompact | 3,121 | 1,107 | 25 | 0 |
| rg-sweep-recency-exp-rg1m | 3,096 | 24 | 24 | 0 |
| rg-sweep-recency-exp-rg2m | 3,119 | 30 | 30 | 0 |
| rg-sweep-recency-exp-rg4m | 3,118 | 28 | 28 | 0 |
| rg-sweep-recency-exp-rg8m | 3,098 | 27 | 27 | 0 |
| split-recency-exp-nocompact | 2,049 | 22 | 22 | 0 |

The m1 runs' other differences are their MoR and TailCoW arms' HEADs and bytes, as registered.

## Findings

- **An old version reads like the current one, under both readers.**
  - All {eval}`N["identical"]` of {eval}`N["pairs"]` pairs ({eval}`N["per_reader"]` per reader)
    are identical in every column. That includes all {eval}`N["rmow_pairs"]` over RMoW files,
    appendable and split.
  - Under the stock reader, the old version now costs the same footer reads as the current one:
    one per row group, two stream requests each.
- **The earlier runs measured the harness, not RMoW.** Under the stock reader, their time-travel
  reads took 62% fewer GETs, because old versions were planned without split offsets
  ([above](#earlier-runs)). Apart from that, the reruns match them record for record, as
  registered.
- **Dead regions cost no read bytes.**
  - No read anywhere read through a gap. Iceberg's vectored Parquet reads go through
    `RangeReadable` as one bounded GET per run of adjacent column chunks (I/O parity rule 1).
  - An appendable file read {eval}`N["read_live"]`× its live bytes while storing
    {eval}`N["stored_live"]`× them.
- **The abort prediction cannot be decided from the trace.**
  - **What the model counts.** The model records an abort when a stream seeks away from an open
    request (`TracingFileIO.java:162`). It records none on close, because it does not know the
    object's length (`TracingFileIO.java:196-198`).
  - **What the model records per footer.** Stock parquet-java reads a footer as an 8-byte probe
    at the version's end, then a backward seek to the footer. The model therefore records two
    stream GETs and one abort per footer read, for a current and an old version alike. In the
    pairs over appendable files, aborts are half the stream GETs on both sides:
    {eval}`N["aborts_old"]` for old reads, and {eval}`N["aborts_cur"]` for current ones.
  - **What S3 would do.** Iceberg 1.12's `S3InputStream.abortStream` aborts a request only if it
    still has bytes. It reads one byte first, and does this on a seek away and on close alike.
  - **On a current version:**
    - the probe request ends at the object's end, so it closes without an abort;
    - the footer request still holds the last 8 bytes when the reader closes it, so it is
      aborted.
  - **On an old version**, both requests are aborted, the probe's after it streamed into newer
    versions.
  - That is H4's one extra abort, which is exactly what the model leaves out: it counts one abort
    for each.
  - **What the SDK sees.** `S3Recorder` does not see aborts either ("aborts stay modelled only").
    It does record the bytes each GET response declares. For an old version's open-ended probe,
    that is the rest of the object, which an S3 run could compare.
  - **The one-footer reader is unaffected.** It issues no open-ended GET for footers under its
    64 KiB prefetch. Under it, no read aborted.

## H4 (docs/HYPOTHESES.md)

- **Old versions read like current ones: held, under both readers.** All
  {eval}`N["rmow_pairs"]` pairs over RMoW files cost exactly what the current read of the same
  state cost, in requests, bytes, aborts and skip-through.
- **Dead bytes add no read bytes: held.** No read read through a gap, and appendable files read
  their live bytes, within {eval}`N["read_live_pct"]`, at up to {eval}`N["stored_max"]`× stored.
- **One more aborted GET per old footer read on S3: open.**
  - The trace counts the same abort for both, and the SDK does not see aborts.
  - Deciding it needs S3 with a count of aborted connections, or the declared GET bytes against
    the trace's.

## Limits

- **Local model.** Every number here is the trace's model of what S3 receives. `S3StoreTest`
  checks the model's request counts against versitygw; it does not check read bytes or aborts.
- **No compaction.** Compacted runs pair different files, because history is recorded before
  compaction. The compaction sweep has no time travel at all (`travelBack` 1000 with 60 batches).
- **One distance.** Every pair is 20 batches back, and probes come every 10 batches. Each run has
  {eval}`N["pairs_per"]` pairs per arm and reader.
- **Next step.** Counting aborts needs the AWS SDK's HTTP-client metrics on the S3 runs (M3).
