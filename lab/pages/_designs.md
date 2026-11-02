The four designs differ in what an update writes and in how the rows it replaces disappear:

| Design | An update writes | The rows it replaces are hidden by | Appends to objects |
|---|---|---|---|
| **MoR** | the changed rows, in the write's new data file | a deletion vector on their file, which every read applies | no |
| **RMoR** | each region it changes, whole, in the write's new data file | a deletion vector over the region's old row group, which a read skips | no |
| **RMoW-external** | each region it changes, whole, in the write's new data file | a footer appended to the old file that no longer lists the old row group | yes |
| **RMoW-append** | each region it changes, whole, appended to the region's own file | the footer appended after it, which lists it in the old row group's place | yes |

A region is a row group. MoR is Iceberg's merge on read (format v3). The other three merge a write's
inserts into a tail of files that grow and merge with each other (row groups of one file, in
RMoW-append), where MoR writes a new data file per write. Every table is an Iceberg table, and stock
Iceberg reads each one correctly; the two appending designs need a store that appends to an object,
as S3 Express and Azure Blob do.

**We changed Iceberg's reader, the same way for every design**, so that no design pays for work a
reader does needlessly and its layout multiplies. The largest change is RMoR's: stock Iceberg fetches
and decodes each old row group under its deletion vector only to drop all of its rows, and our
reader skips it. Every change is in [Appendix A: Changes to the readers](./reader-changes.md), and
RMoR read without skipping, with TailCoW (a control: MoR with RMoR's tail of inserts), is in this
page's Appendix B.
