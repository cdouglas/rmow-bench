---
title: 'Appendix A: Changes to the readers'
---

Every design is an Iceberg table, and stock Iceberg 1.12 reads each one correctly: an appending
design's file is a Parquet file whose newest footer, at the length the table records, lists its
live row groups (README, Compatibility). But stock Iceberg does work some layouts multiply, so
every design is read through Iceberg's vectorized reader with the changes below, which an engine
makes or could make. Each changes what a read fetches, never which rows it returns, and each
applies to every design that has what it changes.

## Readers

**A footer once per file.** Stock Iceberg plans a split per row group and reopens the file for
each, reading its footer every time, so a file of many row groups, as RMoW-append's grow to, pays
a footer read per row group in every scan. Our reader reads each file's footer once per read, as
engines with a footer cache, and DuckDB, do.

**A footer in one request.** parquet-java reads a footer in two requests: the file's last 8 bytes,
which give the footer's length, then the footer. Our reader fetches the file's last 64 KiB in one,
as Arrow and DuckDB do; the writers read footers as parquet-java does.

**Deletion vectors once per file.** Stock Iceberg loads a data file's deletion vector once per
split, so once per row group. Our reader loads each file's deletes once per read, as an engine with
a delete cache does, which helps the designs with deletion vectors, MoR and RMoR.

**Skipping the row groups a deletion vector covers whole.** RMoR leaves each region it rewrites in
its old file under a deletion vector, and stock Iceberg fetches and decodes that row group only to
drop every one of its rows. Our reader plans no split for a row group whose rows the file's
deletion vector all covers, nor for a file whose vector covers every row, and RMoR's writers skip
them the same way when they scan for the rows a write changes. The sorted read skips them too (since
2026-11-10; before, it fetched every row group of a file it did not skip whole, charging RMoR and
the deferred merges for the row groups they had copied out). Our check asks the vector's bitmap
whether each row group's range is deleted whole, at the cost of a few of its containers. The
results pages take RMoR's times from runs with this check; earlier runs tested each row position in
turn, which cost RMoR seconds of each write and read ([RMoR re-measured](./rmor-rerun.md)).

**Delete files opened with their length.** Stock Iceberg's delete filter opens a delete file
without the length the table records, so the store client asks the store for it: a HEAD per file
on S3 and Azure. Our reader passes the length.

**Azure reads of exactly their range.** Iceberg 1.12's Azure stream opens every file with a GET of
its first 4 MiB, and a ranged read fetches up to 4 MiB more than it needs, since it asks for its
end offset as its length. In our fork of Iceberg 1.12 (`atomicio`) the stream opens nothing until
it reads, and asks for exactly its range, as Iceberg's S3 stream does.

## Writers

**Finding a write's rows by key.** A MERGE finds the rows a write changes by scanning the table's
key column. Iceberg prunes the scan's files and row groups by its filter, but stops pruning by a
list of values past 200 of them, so a write of thousands of changes prunes at best to the range
between its smallest and largest key, which spans most of the table when the changes fall in
several regions. Our writers prune files by each changed key, and since 2026-11-10 row groups too:
they read the key column only of the row groups whose statistics, in the footer the writer already
holds, may hold one of the keys (`layout.locate` `keys`). It helps MoR, RMoR, RMoW-external and
the deferred merges alike; RMoW-append finds its rows from its footers' statistics alone.

**Conditional appends.** RMoW-append and RMoW-external extend an existing object, which Iceberg's
FileIO cannot do. Our fork's `SupportsAtomicOperations` appends at the object's length on the
condition that it is still the version the writer read, so of two writers appending, one lands: a
PutObject append on S3 Express, an Append Block on Azure (one per 100 MiB). Standard S3 has no
append.

**Appendable files on Azure.** Azure appends only to Append Blobs, and Iceberg writes block blobs.
The appending designs create each file as a temporary Append Blob that they rename onto its name,
three requests where Iceberg's own write is one.

**A data file that grows under its name.** Iceberg assumes a data file never changes, but an
appending design's file grows, and each append is a new version of it, which the write commits to
the table with the file's new length, split offsets and metrics. A reader of any snapshot opens the
object at the length that snapshot records and finds that version's footer at its end, as stock
readers already do.
