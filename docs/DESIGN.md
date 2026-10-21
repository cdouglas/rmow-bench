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
footer lists the live row groups of the previous version, minus the ones being replaced, plus the
new ones. A row group that replaces another takes its place in the list; new rows are listed
last (see [Footer order](#footer-order)). So `<R1> … <RN> <F1>`, updated at `RK`, becomes
`<R1> … <RN> <F1> <RK'> <F2>`, where F2 lists `R1 … RK' … RN`. Nothing else changes:

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
groups, whether or not the footer lists them in offset order (`AppendableParquetTest`,
`VersionDumpTest`, `analysis/interop_check.py`):

- Iceberg 1.12.0, through `Parquet.read` with the generic reader, at the version's length;
- parquet-java 1.17.1, through `ParquetFileReader` and its example record reader;
- pyarrow 26.0.0;
- DuckDB 1.5.6.

Iceberg carries the length already. A manifest entry's `file_size_in_bytes` is what Iceberg's
FileIOs open a data file at, and the S3, GCS, ADLS and Hadoop FileIOs all keep it. So a snapshot
that records an older length reads the older version.

This holds for every footer the benchmark's arms write, not only for hand-built files. A run with
`--dump-versions` keeps every version of every appendable object, and checks the versions each
step held against the reference model. `analysis/interop_check.py` then reads each version with
pyarrow and DuckDB, and checks:

- the row groups listed, in order;
- every row: digests over (id, version) and (id, ts, version), and each payload column;
- each row's position, against Iceberg's `_pos`;
- an id-range filter, which skips row groups by their statistics.

Four further points hold for these readers:

- `_pos` is the running row count in footer-list order, in all four.
- An Iceberg split selects exactly the row groups whose midpoints fall inside it.
- Row-group statistics skip the regions that don't match.
- Old row groups' page indexes stay valid, since they sit at absolute offsets.

## Writer rules

1. **Encode with the stock writer.** Write the region with a position stream that starts at the
   object's length and drops the leading magic. Every offset the writer records is then already
   absolute (`OffsetOutputFile`). The writer's own footer is cut off and replaced with the merged
   one (`RegionWriter`, `FooterCodec`).
2. **Keep footer order the row order.** A rewritten row group takes the replaced one's place in
   the footer list, and new rows are listed last (`footerOrder` `logical`, the default; see
   [Footer order](#footer-order)).
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

## Footer order

A footer lists row groups in an order, and that order is the file's row order. A row's position
counts the rows of the row groups listed before it, in parquet-java (`generateRowGroupOffsets`,
which Iceberg's `_pos` reads), pyarrow and DuckDB (`file_row_number`) alike
(`interop_check.py` checks all three against each other). Writers that stream row groups list
them in offset order, so the two orders have always been the same. An appendable file separates
them: `RK'` sits at the end of the object, but its rows belong where `RK`'s were.

**The Parquet spec does not require offset order.** `parquet.thrift` says only "Row groups in this
file" of `FileMetaData.row_groups` and "Row group ordinal in the file" of `RowGroup.ordinal`.
Every offset is absolute, so a row group can be found wherever it is listed. Encryption.md lets
readers derive the AAD's row-group ordinal from "a local row group counter", which is a position
in the list, not in the file.

**parquet-java relies on offset order in three places.** None of them is on the read path Iceberg
and Spark take for unencrypted files:

- `ParquetMetadataConverter.toParquetMetadata`, which serializes parquet-java's own metadata
  objects (`ParquetFileWriter`, the rewrite and merge tools), rejects a row group that starts
  before the previous one ends ("Invalid block starting position"). A tool that rewrote a
  logical-order footer through those classes would fail. RMoW writes footers in Thrift directly.
- `ParquetInputFormat`'s client-side splits (Hadoop MapReduce, Hive) walk the row groups and the
  HDFS blocks forward only, and take the last listed row group as the end of the file.
- The PARQUET-2078 fallback in `filterFileMetaDataByMidpoint` and `filterFileMetaDataByStart`
  runs only when the first column chunk has no plaintext metadata (encrypted columns). It assumes
  ascending offsets, and also that the first listed row group starts at offset 4, which fails as
  soon as a version drops the first row group, in either order. Encrypted files are out of scope
  (the ordinal is part of the AAD).

Iceberg sorts a data file's split offsets (`ParquetUtil.getSplitOffsets`, and its spec requires
them ascending), and a split selects the row groups whose midpoints it holds, wherever they are
listed (`AppendableParquetTest.inPlaceRowGroupKeepsItsSlot`).

**Logical order is the default, because it keeps sorted files sorted.** With RK' listed last, the
first update to a sealed row group moves its rows to the end of the file, and every later row
after K moves up. A file written in `ts` or `id` order then no longer reads in that order, which
breaks whatever relies on it:

- an Iceberg `sort_order_id`, which claims the file's rows are sorted;
- engines that stream a sorted file in order, or stop early on a top-N;
- binary search over row-group statistics.

Logical order keeps every footer in id order, which `RmowArm` checks after each append; the
`row_groups_out_of_order` state metric counts the row groups listed after a larger id. The I/O is
the same either way: the same regions, footers of the same size, and the same requests.

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
- **Puffin writes.** Iceberg's `PuffinWriter` creates a DV file with `create()`, which on S3 and
  ADLS first checks that the object does not exist: a HEAD per DV file. Iceberg's data and
  position-delete writers skip the check (`overwrite`). The benchmark counts the HEAD, as stock
  Iceberg pays it (`S3StoreTest` matches it against the SDK's requests).
- **Old versions on S3.** The first of those two requests is an open-ended GET. When reading an
  old version on S3, it streams newer bytes until the reader seeks away, an aborted request the
  trace counts.
- **Prefetch.** A tail prefetch, as Arrow and DuckDB do it, makes the footer read one request. It
  is the `footerPrefetchBytes` knob, applied to every arm.
- **Dead bytes on S3.** Iceberg's stream reads through gaps of up to 1 MiB. Dead regions cost
  nothing only because Parquet's vectored reads go through `RangeReadable`, as bounded ranged
  GETs. A FileIO without `RangeReadable` would charge dead bytes to reads.
