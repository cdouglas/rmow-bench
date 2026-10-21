# Appendable Parquet files: design notes

What this proof of concept established about appending regions to a Parquet object, and the rules
its writer follows. These notes are the seed of a parquet-format proposal. Every claim below is
backed by a test or a measurement in this repository, named in parentheses.

## The layout

```
PAR1 | RG0 ... RGk | footer0 len PAR1 | RGk+1 ... | footer1 len PAR1 | ... | footerN len PAR1
                      ^ version 0 ends here           ^ version 1 ends here     ^ version N
```

A version is a prefix of the object, ending at its footer. A new version appends one or more
regions, each a row group with its page indexes and bloom filters, followed by a footer. That
footer lists the live row groups of the previous version, minus the ones being replaced, then the
new ones. Nothing else changes:

- **Offsets.** Every offset in `parquet.thrift` that is still in use is absolute: pages, column
  and offset indexes, bloom filters, and row-group starts. A row group therefore keeps its offsets
  in every later footer.
- **Gaps.** Bytes between row groups are already legal, since bloom filters may sit there
  (`BloomFilter.md`).
- **Finding a version.** Readers find the footer from the length they are given, at length − 8.
  No stock reader checked here requires the leading magic to be followed by data
  (`AppendableParquetTest`).

## Readers that read every version unchanged

All four read each version's prefix and return exactly the rows written, seeing only the live row
groups (`AppendableParquetTest`, `analysis/interop_check.py`):

- Iceberg 1.12.0, through `Parquet.read` with the generic reader, at the version's length;
- parquet-java 1.17.1, through `ParquetFileReader` and its example record reader;
- pyarrow 26.0.0;
- DuckDB 1.5.6.

Iceberg carries the length already. A manifest entry's `file_size_in_bytes` is what Iceberg's
FileIOs open a data file at, and the S3, GCS, ADLS and Hadoop FileIOs all keep it. So a snapshot
that records an older length reads the older version.

Four further points hold for these readers:

- `_pos` is the running row count in footer-list order.
- An Iceberg split selects exactly the row groups whose midpoints fall inside it.
- Row-group statistics skip the regions that don't match.
- Old row groups' page indexes stay valid, since they sit at absolute offsets.

## Writer rules

1. **Encode with the stock writer.** Write the region with a position stream that starts at the
   object's length and drops the leading magic. Every offset the writer records is then already
   absolute (`OffsetOutputFile`). The writer's own footer is cut off and replaced with the merged
   one (`RegionWriter`, `FooterCodec`).
2. **Keep row groups in offset order.** New row groups go at the end of the footer list, which
   keeps the list in ascending offset order. parquet-java's fallback for encrypted columns
   (PARQUET-2078) checks that order.
3. **Renumber ordinals.** `ordinal` is the list position, and the list holds at most 32767 entries.
   Encryption puts the ordinal into the AAD, so encrypted files would have to keep each row
   group's original ordinal. They are out of scope here.
4. **Keep the metadata.** The merged footer keeps the previous `created_by`, which readers use for
   statistics workarounds, and its key-value metadata (`iceberg.schema`). It adds
   `rmow.prev-length`, a back-pointer to the previous version, and `rmow.version`.
5. **Commit with one append.** Each version is written as a single append, regions first and
   footer last, conditioned on the object's length: S3 Express with `writeOffsetBytes`, ADLS with
   `appendPosition`. A reader trusts only the length a snapshot recorded, so a torn append is
   unreferenced tail garbage.
6. **Bound the footer.** Every version rewrites the whole footer and every open decodes it.
   - For the 11-column bench schema, the cost per row group listed is about 1.16 KB, 18 µs to
     parse, 13 µs for parquet-java's conversion, and 17 µs to merge and serialize. At 50 columns
     it is about 5.3 KB (`FooterBench`).
   - A file should therefore roll over at around 100 row groups, or merge its small row groups
     (tiered tails).

## Deletion vectors on an appendable file (an ablation)

This section is outside the design's goal, under which readers apply no deletes. The `-dv` arms
exist only to measure what region rewrites cost against deleting by DV.

A DV names positions in a data file. Positions here are the running row count in footer order,
so a DV stays valid across versions only while every row group before the deleted rows keeps its
place. The writer uses DVs only on a file's *stable prefix*: the leading row groups in footer
order that no tail merge will drop.

- Row groups in the stable prefix are never rewritten. Their deletes go to the DV, and updated
  versions join the batch's tail region.
- Open tail row groups, after the prefix, are still merged or rewritten. A row group sealed behind
  an open one joins the prefix only once the open ones before it have been merged away. Its
  position is fixed from then on.

(`RmowArm` with `-dv`; the oracle checks every version, time travel included, in
`RunnerSmokeTest`.)

One cost comes with this. Updated rows move to the tail, so row-group `id` ranges overlap, and
finding a row means scanning, as with MoR. Without DVs, updates rewrite rows in place, ranges stay
disjoint, and the footers' statistics place each row exactly.

## Costs a reader of the results should know

- **Per-split footer reads.** Stock Iceberg readers plan one split per row group: one split per
  offset, which the planner then bin-packs into tasks. They open every split separately, reading
  the file's footer each time (`GenericReader`, Spark's `BaseReader`).
  - An appendable file of N row groups therefore costs N footer reads per scan.
  - Iceberg prunes at file granularity, from manifest metrics. A selective read, such as the
    newest 1% of event time, opens every row-group split of a selected file only to skip most of
    them by their statistics.
  - Readers that read each footer once per query avoid both costs. That covers engines with a
    footer cache, or that read footers while planning, such as DuckDB.
  - This is a property of today's readers, not of the format: a version is summarized in exactly
    one footer.
  - The benchmark's primary reader model (`one-footer`) reads that footer once per file per read,
    then only the row groups it needs. With 15 live row groups in 2 objects, it takes 19 GETs
    against stock Iceberg's 45 (`ReaderModelTest` pins the counts).
  - Stock Iceberg is still measured, as the `stock` reader.

- **Footer reads.** Stock parquet-java reads a footer with two requests: the 8-byte tail, then the
  footer. Files up to 1 MiB are fetched whole in one request.
- **Old versions on S3.** The first of those two requests is an open-ended GET. When reading an
  old version on S3, it streams newer bytes until the reader seeks away, an aborted request the
  trace counts.
- **Prefetch.** A tail prefetch, as Arrow and DuckDB do it, makes the footer read one request. It
  is the `footerPrefetchBytes` knob, applied to every arm.
- **Dead bytes on S3.** Iceberg's stream reads through gaps of up to 1 MiB. Dead regions cost
  nothing only because Parquet's vectored reads go through `RangeReadable`, as bounded ranged
  GETs. A FileIO without `RangeReadable` would charge dead bytes to reads.
