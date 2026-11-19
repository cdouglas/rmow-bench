# Stores and appends

The stores a run can use, how each counts its requests, and how the appending designs (RMoW-external
and RMoW-append) append through the atomicio fork. Moved from the README on 2026-11-18; a citation
"README, The S3 store", "README, The Azure store" or "README, Appends" means the section of that
name here. Local runs use `store.type` `local` (`PreadFileIO`) or `mem` (`MemFileIO`); the runbooks
for the cloud are [S3X_RUNBOOK.md](S3X_RUNBOOK.md) and [AZURE_RUNBOOK.md](AZURE_RUNBOOK.md).

## The S3 store

`store.type` `s3` runs every arm on Iceberg's `S3FileIO`, over one AWS SDK client that this
project builds (`Store`), with credentials from the default chain, so none are in a configuration
or its manifest. Only directory buckets (S3 Express One Zone, `<name>--<az id>--x-s3`) append, so
the bucket decides:

- **a directory bucket:** appends go through the fork (`AtomicAppender`): one PutObject at the
  object's length (`writeOffsetBytes`), conditioned on its ETag, with no HEAD;
- **a standard bucket:** the runner refuses the appending arms; split files run;
- **an emulator** (`store.endpoint`): appends read the object and CAS it back longer, on an
  unmeasured client (`EmulatorAppenders.rewriting`). Creates are the fork's.

An interceptor on the client (`S3Recorder`) counts the requests the SDK sends. Each phase then
records them beside the trace's model, as `sdk_*` fields: requests by operation, retries, and
bytes. `S3StoreTest` runs every arm family against versitygw (in Docker; skipped without it), and
checks that in every phase the SDK's GETs, PUTs, HEADs and DELETEs equal the model's, with no
retries, and that the run deletes its objects. So local runs count the requests S3 receives.

What the emulator cannot check is the S3 Express append (versitygw ignores `writeOffsetBytes`),
S3 Express's session requests (`CreateSession`, counted as `sdk_sessions`: they count in cost,
not in the model), and latency. Two things check them, given a directory bucket:

- **`S3ExpressTest`** runs when `RMOW_S3X_ROOT` names a prefix in a directory bucket
  (`s3://<bucket>--<az id>--x-s3/<prefix>`; region from `RMOW_S3X_REGION` or `AWS_REGION`). It
  checks that:
  - an append at the object's length lands;
  - one anywhere else fails and changes nothing, and so does a create where an object is;
  - the recorder sees a create as one PutObject and an append as one PutObject with a write
    offset, neither with a HEAD;
  - a run of every arm family matches the model, appends and bytes written included.
- **`org.rmow.bench.micro.S3ExpressSpike <s3://bucket/prefix>`** measures latencies, interleaved:
  the store's own appends (a write offset alone) against the fork's (conditioned on the ETag too),
  PUTs against the fork's creates (conditioned on absence), from 64 KiB to 128 MiB; GETs by size,
  of the PUTs' objects whole and as ranges of a 128 MiB one, for the read rate per MiB; and GETs
  (whole, last 64 KiB, middle 64 KiB) of objects of 1 to 1000 appended parts. It writes
  `results/spike/s3x-spike-<UTC>/spike.jsonl`. The simulated results price requests from such a
  test (`tune.SPIKES`).

`configs/s3x-calibration.json` was the first cloud run (2026-10-28), and
`configs/s3x-calibration-local.json` its twin on the local store, whose requests and bytes the
cloud run's model and SDK counts must match exactly, apart from `CreateSession`.

## The Azure store

`store.type` `azure` runs every arm on Iceberg's `ADLSFileIO`, on a hierarchical-namespace account
(`store.root` `abfss://<container>@<account>.dfs.core.windows.net/<prefix>`). Credentials come from
the default chain (a VM's managed identity, the environment, the Azure CLI), so none are in a
configuration or its manifest. For an emulator, `store.endpoint` names it, and the account key is
in `AZURE_STORAGE_KEY`.

The fork's ADLS FileIO sends what the model counts. An output stream is one Put Blob up to
256 MiB, and `create()` adds its existence check, a HEAD. A stream opens nothing until it reads,
and a ranged read is one GET of exactly its range. Iceberg 1.12.0 as shipped reads more: every
stream opens with a GET of the file's first 4 MiB, and a ranged read at an offset fetches up to a
4 MiB chunk, since it asks for `position + length` bytes ([AZURE_RUNBOOK.md](AZURE_RUNBOOK.md)).
The fork's sequential reads still download the SDK's 4 MiB blocks whole, so they read more bytes
than the model counts, but no more requests.

`AppendBlock` extends only an Append Blob, and Iceberg writes block blobs. So the appending designs'
files are Append Blobs, created through the fork (`AtomicAppender`) as a temporary Append Blob, one
Append Block and a rename with `If-None-Match: *`: three requests, where the model counts one PUT.
Each append is one `AppendBlock`, conditioned on the blob's length and ETag. An Append Block takes
at most 100 MiB, so a larger append or create takes one per 100 MiB (`append_splits` counts the
extra ones).

The recorder (`AzureRecorder`) counts what the SDK sends through every client in the process,
Iceberg's included: azure-core adds its policies to every storage pipeline
(`META-INF/services`). Phases record the counts as `sdk_*` fields, and `sdk_latency` in the lab
prices them.

`AzureStoreTest` runs the store's contract (`AzureStoreContract`) against Azurite, in Docker
(skipped without it). Azurite has no Data Lake endpoint, so the store deletes an emulator's runs
blob by blob, and it cannot rename, so the appending arms create their files there through a
stand-in (`EmulatorAppenders.appendBlobs`): an empty Append Blob made in place, a HEAD, and an
Append Block. The contract checks that:
- Iceberg's write is a HEAD and one Put Blob;
- its reads open nothing and ask for exactly their range;
- Append Blobs append at their length only, a create fails where a blob is, and a file Iceberg
  wrote cannot be appended to (`InvalidBlobType`);
- every phase of every arm family sends what the model counts, request for request (a create as
  above), with nothing else and no retries.

`AzureTest` runs the same contract against Azure when `RMOW_AZURE_ROOT` names a prefix in a
container. `org.rmow.bench.micro.AzureSpike <abfss://...>` measures latencies, interleaved: Put
Blobs, the store's own Append Blocks (conditioned on the length alone) and the fork's (on the ETag
too), from 64 KiB to 128 MiB (a 128 MiB append is two Append Blocks); GETs by size, of the Put
Blobs' blobs whole and as ranges of a 128 MiB one; creates (an empty Append Blob, the store's own
appendable file, and the fork's atomic one) and HEADs; GETs of Append Blobs of 1 to 1000 blocks;
and Iceberg's ranged read of a 32 MiB file against the SDK's. `configs/azure-calibration.json` is
the calibration run (2026-10-28). Its Azurite twin (`-azurite.json`) gives the requests it must
send, but for its creates: each costs a HEAD there where Azure's costs a rename.

## Appends: the fork's atomic operations

The atomicio fork's `SupportsAtomicOperations` is the conditional write its catalog commits
through, and APPEND is the appending designs' primitive. Every store's appender is
`AtomicAppender` over it: S3FileIO and ADLSFileIO implement it on the fork, and the local stores
(`MemFileIO`, `PreadFileIO`) here, so local runs take the same path. Each write returns the next
snapshot, holding a length and an ETag, and the appender keeps it, so appends chain:
- **An append** is one request, at the snapshot's length and conditioned on its ETag: a PutObject
  on S3 Express, an Append Block on ADLS. Of several writers appending to one version, exactly one
  lands. Where the store caps an append (Azure's Append Block, 100 MiB), it is several, in order,
  each conditioned on the snapshot the last returned, so the footer lands last. Such an append is
  not all or nothing: a writer that stops between parts leaves bytes past the length a commit
  records, which no reader opens. S3 Express takes 5 GB per append.
- **A new appendable file** is a CAS from a snapshot that has seen nothing, which assumes the
  object is absent and sends no lookup (every appendable file has a name of its own):
  - on S3, one PutObject with `If-None-Match: *`;
  - on ADLS, a temporary Append Blob (created, then one Append Block, or one per 100 MiB) renamed
    onto the file with `If-None-Match: *`. An Append Blob is created empty, so only the rename
    makes the create all or nothing, at any size: one request more than the store's own way, an
    empty Append Blob and its blocks.
  - on the local filesystem, a temporary file renamed onto the file.

  The model counts it as one PUT.

`prepare()` reads the content once for its checksum (CRC32C on S3 and locally, MD5 on ADLS), so a
write's CPU includes it on every store. Emulators lack part of the API, and run the appending arms
on stand-ins (`EmulatorAppenders`, above); a store on a cloud endpoint never uses them.

The fork's own contract (`SupportsAtomicOperationsContractTest`, from its test jar) runs against
both local stores (`LocalAtomicContractTest`): creates, replaces and appends pinned to the
version a snapshot saw, serial and concurrent races with one winner, and reads of the version a
file saw. A file that looked an object up reads that version or fails with `StaleReadException`:
after an append, the local stores keep the older bytes as a prefix and read them; after a replace
(a CAS, or a rename onto a local file), they fail, as S3 and ADLS answer a read conditioned on a
version that is gone. `AtomicAppenderTest` checks the appender, and that it sends no lookup.
Against the services, `S3ExpressTest` and `AzureTest` check it; the spikes time it against the
stores' own requests.

`atomicio` has the fixes the appending designs need, beyond the API:
- ADLS reads open nothing until they read, and ask for exactly their range;
- S3 and ADLS appends to a snapshot send no lookup, and a create from a snapshot that has seen
  nothing sends none either;
- S3's `prepare()` no longer needs commons-io, which Iceberg does not ship.

The two read fixes are upstream performance bugs: the reads fetch more than they need, but return
the right bytes. So they are on `atomicio`, not `core-fixes`, which holds correctness fixes.
