-- What reached a cloud store, priced (docs/AZURE_RUNBOOK.md, docs/S3X_RUNBOOK.md). Runs on S3 or
-- Azure, and their emulator twins, record per phase the requests their SDK sent (sdk_*:
-- S3Recorder, AzureRecorder), beside what TracingFileIO modelled. sdk_latency prices what was sent
-- as phase_latency prices the model:
--   CPU + sum over requests sent of E[latency(kind)] + MiB moved x rate
-- GETs, HEADs and lists cost a GET; Put Blobs (S3 PUTs), Put Blocks, Put Block Lists, Append Blob
-- creates and DELETEs a PUT; Append Blocks (S3 Express appends) an append. Requests outside the
-- model (sdk_other, S3 Express's CreateSession) are left out.
--
-- GET bytes are what responses declare. That is what a bounded GET transfers, and every GET of
-- Iceberg's ADLS reads is bounded (store.adlsReads = iceberg). An open-ended GET that is abandoned
-- transfers less than it declares, so for the other runs read bytes are the model's.

-- Runs without a cloud store have no sdk_* columns: these stand in until a run has them.
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_requests BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_gets BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_heads BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_lists BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_puts BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_blocks BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_block_lists BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_blob_creates BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_appends BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_deletes BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_other BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_sessions BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_retries BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_get_bytes BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS sdk_put_bytes BIGINT;

-- One row per run, arm, step, phase, reader and profile, for phases on one thread (as
-- phase_latency): the simulated latency of what the SDK sent, in ms.
CREATE VIEW sdk_latency AS
WITH s AS (
    SELECT s.*, coalesce(rr.parallelism, 1) AS parallelism, w.work_ms,
           coalesce(p.value, '') AS adls_reads
    FROM steps s LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
    LEFT JOIN phase_work w ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step
                          AND w.phase = s.phase AND w.reader IS NOT DISTINCT FROM s.reader
    LEFT JOIN run_params p ON p.run_id = s.run_id AND p.key = 'store.adlsReads'
    WHERE s.sdk_requests IS NOT NULL
      AND s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old')
)
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, p.profile,
       s.cpu_ns / 1e6 AS cpu_ms, s.work_ms,
       (s.sdk_gets + s.sdk_heads + coalesce(s.sdk_lists, 0)) * p.get_ms * exp(p.get_sigma ^ 2 / 2)
         + (s.sdk_puts + coalesce(s.sdk_blocks, 0) + coalesce(s.sdk_block_lists, 0)
            + coalesce(s.sdk_blob_creates, 0) + s.sdk_deletes) * p.put_ms * exp(p.put_sigma ^ 2 / 2)
         + CASE WHEN s.sdk_appends = 0 THEN 0
                WHEN p.append_ms IS NULL THEN 'NaN'::DOUBLE
                ELSE s.sdk_appends * p.append_ms * exp(p.append_sigma ^ 2 / 2) END
         AS request_ms,
       (CASE WHEN s.adls_reads = 'iceberg' THEN s.sdk_get_bytes ELSE s.bytes_read END)
           / 1048576.0 * p.read_ms_per_mib
         + s.sdk_put_bytes / 1048576.0 * p.write_ms_per_mib AS transfer_ms
FROM s CROSS JOIN store_profiles p
WHERE s.parallelism = 1;
