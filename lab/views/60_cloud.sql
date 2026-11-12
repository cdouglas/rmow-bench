-- The cloud runs' charts (from the retired cloud calibration; docs/LESSONS.md): what each update
-- sent the store and stored, by the kind of file, and the store profile the spikes measured.

-- Each logged request (the outermost phases: batch, read_*) with the file it touched:
--   file_type     the format: parquet or puffin
--   object_class  base (the initial load's file), data (a batch's data file) or dv
--   appended      an APPEND, or a ranged GET at or past the object's first append offset (the
--                 object's length before any append): RMoW's regions and footers. Streams are not
--                 told apart
--   planning      a read phase's request outside its split tasks (task -1): the footer prefetches
--                 (64 KiB) and DV loads; in a batch every request is
-- Kinds that are not requests: stream_bytes (the bytes a stream delivered), abort (a stream
-- closed early) and task (a split task).
CREATE VIEW request_files AS
WITH first_append AS (
    SELECT run_id, arm, object, min("offset") AS append_offset
    FROM requests WHERE kind = 'append' GROUP BY ALL
)
SELECT r.run_id, r.arm, r.step, r.phase, r.reader, r.kind, r.object, r."offset", r.length,
       r.dur_ns, r.task,
       CASE WHEN r.object LIKE '%.parquet' THEN 'parquet'
            WHEN r.object LIKE '%.puffin' THEN 'puffin' ELSE 'other' END AS file_type,
       CASE WHEN regexp_matches(r.object, '(^|/)initial-[0-9]+\.parquet$') THEN 'base'
            WHEN r.object LIKE '%.puffin' THEN 'dv' ELSE 'data' END AS object_class,
       r.kind = 'append' OR (r.kind = 'get_range' AND r."offset" >= f.append_offset) AS appended,
       r.task = -1 AND r.phase LIKE 'read_%' AS planning
FROM requests r LEFT JOIN first_append f USING (run_id, arm, object)
WHERE r.object <> '';

-- The model's requests and bytes per outermost phase, file and op (get: ranged or streamed; head;
-- put; append; delete), as the steps count them: summed over a phase they are its gets + heads +
-- puts + appends + deletes, bytes_read and bytes_written. A stream's bytes are its stream_bytes
-- records.
CREATE VIEW file_requests AS
SELECT run_id, arm, step, phase, reader, file_type, object_class, appended, planning,
       CASE WHEN kind IN ('get_range', 'get_stream', 'stream_bytes') THEN 'get' ELSE kind END AS op,
       count(*) FILTER (WHERE kind <> 'stream_bytes') AS requests,
       coalesce(sum(length) FILTER (WHERE kind IN ('get_range', 'stream_bytes')), 0) AS bytes_read,
       coalesce(sum(length) FILTER (WHERE kind IN ('put', 'append')), 0) AS bytes_written
FROM request_files
WHERE kind IN ('get_range', 'get_stream', 'stream_bytes', 'head', 'put', 'append', 'delete')
GROUP BY ALL;

-- What each outermost phase sent the store, by file type and the SDK's kind of request, for runs
-- that logged their requests. Where the run counted what its SDK sent (sdk_*: S3 and Azure, and
-- their emulators), source is 'sdk':
--   * a DV is written by Iceberg's output stream (one PUT, a Put Blob up to 256 MiB) and read by
--     ranged GETs, which reach every store as the model counts them, so its requests are the
--     model's;
--   * the rest of what the SDK sent, kind by kind, went to Parquet files: the model's GETs, HEADs,
--     PUTs and APPENDs, and what a store's protocol adds. Azure creates an appendable file with a
--     staged Append Blob, an Append Block and a rename (other); Azurite, which cannot rename, with
--     an Append Blob, a HEAD and an Append Block; an append past 100 MiB is several Append Blocks;
--   * S3 Express's CreateSession calls touch no file (file_type 'session').
-- Retries are not requests (sdk_retries). Elsewhere source is 'model', and kinds are the model's.
-- A negative count would mean a DV sent more than the model counts; parquet_check is the least
-- Parquet count per run, which must be 0 or more.
CREATE VIEW sent_requests AS
WITH logged AS (SELECT DISTINCT run_id FROM requests),
sdk AS (
    SELECT run_id, arm, step, phase, reader,
           coalesce(sdk_gets, 0) AS gets, coalesce(sdk_heads, 0) AS heads,
           coalesce(sdk_puts, 0) AS puts, coalesce(sdk_blocks, 0) AS blocks,
           coalesce(sdk_block_lists, 0) AS block_lists,
           coalesce(sdk_blob_creates, 0) AS blob_creates, coalesce(sdk_appends, 0) AS appends,
           coalesce(sdk_deletes, 0) AS deletes, coalesce(sdk_lists, 0) AS lists,
           coalesce(sdk_sessions, 0) AS sessions, coalesce(sdk_other, 0) AS other
    FROM steps JOIN logged USING (run_id)
    WHERE phase IN ('batch', 'read_full', 'read_recent', 'read_old') AND sdk_requests IS NOT NULL
),
sdk_long AS (
    UNPIVOT sdk ON gets, heads, puts, blocks, block_lists, blob_creates, appends, deletes, lists,
                   sessions, other
    INTO NAME kind VALUE n
),
model AS (
    SELECT run_id, arm, step, phase, reader, file_type, op || 's' AS kind, sum(requests) AS n
    FROM file_requests GROUP BY ALL
),
puffin AS (SELECT * FROM model WHERE file_type = 'puffin'),
from_sdk AS (
    SELECT s.run_id, s.arm, s.step, s.phase, s.reader,
           CASE WHEN s.kind = 'sessions' THEN 'session' ELSE 'parquet' END AS file_type, s.kind,
           s.n - coalesce(p.n, 0) AS requests
    FROM sdk_long s
    LEFT JOIN puffin p ON p.run_id = s.run_id AND p.arm = s.arm AND p.step = s.step
                      AND p.phase = s.phase AND p.reader IS NOT DISTINCT FROM s.reader
                      AND p.kind = s.kind
    UNION ALL
    SELECT p.run_id, p.arm, p.step, p.phase, p.reader, 'puffin', p.kind, p.n
    FROM puffin p SEMI JOIN (SELECT DISTINCT run_id FROM sdk) USING (run_id)
)
SELECT *, 'sdk' AS source, min(requests) FILTER (WHERE file_type = 'parquet')
                               OVER (PARTITION BY run_id) AS parquet_check
FROM from_sdk WHERE requests <> 0
UNION ALL BY NAME
SELECT m.* EXCLUDE (n), m.n AS requests, 'model' AS source, NULL AS parquet_check
FROM model m ANTI JOIN (SELECT DISTINCT run_id FROM sdk) USING (run_id)
WHERE m.n <> 0;

-- What each arm stores after each step, by part: the initial load (the state record at step 0,
-- before any batch), then every byte the batches wrote, from the log, cumulated. No run here
-- deletes an object, so a state's stored_bytes is exactly the initial load plus what the batches
-- wrote since (true in every arm of the twins); stored_check is the difference, which must be 0.
--   part: base (the initial load), base_appended (regions and footers appended to it), data (new
--   data files), data_appended (appended to them), dv
CREATE VIEW stored_by_part AS
WITH w AS (
    SELECT run_id, arm, step,
           object_class || CASE WHEN appended AND object_class <> 'dv' THEN '_appended' ELSE '' END
             AS part,
           sum(bytes_written) AS b
    FROM file_requests WHERE phase = 'batch' GROUP BY ALL
),
st AS (SELECT run_id, arm, step, stored_bytes FROM steps WHERE phase = 'state'),
parts AS (SELECT DISTINCT run_id, arm, part FROM w),
grid AS (SELECT st.run_id, st.arm, st.step, parts.part FROM st JOIN parts USING (run_id, arm)),
cum AS (
    SELECT g.run_id, g.arm, g.step, g.part,
           sum(coalesce(w.b, 0)) OVER (PARTITION BY g.run_id, g.arm, g.part ORDER BY g.step)
             AS bytes
    FROM grid g LEFT JOIN w USING (run_id, arm, step, part)
),
base AS (
    SELECT run_id, arm, st.step, 'base' AS part, b0.stored_bytes AS bytes
    FROM st JOIN (SELECT run_id, arm, stored_bytes FROM st WHERE step = 0) b0 USING (run_id, arm)
),
allp AS (SELECT * FROM cum UNION ALL SELECT * FROM base)
SELECT a.run_id, a.arm, a.step, a.part, a.bytes,
       st.stored_bytes - sum(a.bytes) OVER (PARTITION BY a.run_id, a.arm, a.step) AS stored_check
FROM allp a JOIN st USING (run_id, arm, step);

-- The state's split of what each arm stores after each step: live bytes, dead bytes inside live
-- objects (RMoW's superseded regions and footers), and reclaimable objects (that no version
-- refers to, which a cleanup would delete).
CREATE VIEW stored_state AS
SELECT run_id, arm, step, stored_bytes, live_bytes,
       stored_bytes - live_bytes - reclaimable_bytes AS dead_bytes, reclaimable_bytes,
       objects, data_files, delete_files
FROM steps WHERE phase = 'state';

-- The spikes' requests; empty until a spike is ingested.
CREATE TABLE IF NOT EXISTS spike_requests (run_id VARCHAR, op VARCHAR, bytes BIGINT, parts BIGINT,
                                           rep BIGINT, ns BIGINT);

-- Per spike, op and size (and for the GETs of appended objects, parts): the requests' median and
-- 10th and 90th percentiles in ms, and their lognormal sigma (the standard deviation of ln ns).
-- An append's parts (the blob's after it) differ by rep, so writes are by size alone.
CREATE VIEW spike_ops AS
SELECT run_id, op, bytes,
       CASE WHEN op IN ('get_full', 'get_tail', 'get_mid') THEN parts END AS parts,
       count(*) AS n, median(ns) / 1e6 AS median_ms,
       quantile_cont(ns, 0.1) / 1e6 AS p10_ms, quantile_cont(ns, 0.9) / 1e6 AS p90_ms,
       stddev_samp(ln(ns)) AS sigma
FROM spike_requests GROUP BY ALL;

-- Per spike and op over sizes: latency = fixed + MiB x rate. fixed_ms and sigma are the median
-- and sigma at the smallest size, where the fixed part dominates; ms_per_mib is the least-squares
-- slope of the medians over sizes. For the GETs of appended objects (get_full, get_tail,
-- get_mid), per_part_ms is the slope over parts.
CREATE VIEW spike_fits AS
SELECT run_id, op, count(*) AS sizes,
       arg_min(median_ms, bytes) AS fixed_ms,
       regr_slope(median_ms, bytes / 1048576.0) AS ms_per_mib,
       arg_min(sigma, bytes) AS sigma,
       regr_slope(median_ms, parts) FILTER (WHERE op IN ('get_full', 'get_tail', 'get_mid'))
         AS per_part_ms
FROM spike_ops GROUP BY ALL;

-- A store profile per spike, in store_profiles' terms: GETs from get_size (whole objects by size),
-- PUTs from put, appends from append_sao (the fork's, which RMoW sends), the read rate from
-- get_size's slope and the write rate from put's. Azure caps an append at 100 MiB.
CREATE VIEW measured_profiles AS
SELECT run_id AS profile,
       max(fixed_ms) FILTER (WHERE op = 'get_size') AS get_ms,
       max(sigma) FILTER (WHERE op = 'get_size') AS get_sigma,
       max(fixed_ms) FILTER (WHERE op = 'put') AS put_ms,
       max(sigma) FILTER (WHERE op = 'put') AS put_sigma,
       max(fixed_ms) FILTER (WHERE op = 'append_sao') AS append_ms,
       max(sigma) FILTER (WHERE op = 'append_sao') AS append_sigma,
       max(ms_per_mib) FILTER (WHERE op = 'get_size') AS read_ms_per_mib,
       max(ms_per_mib) FILTER (WHERE op = 'put') AS write_ms_per_mib,
       CASE WHEN run_id LIKE 'spike/azure-%' THEN 100 END AS max_append_mib
FROM spike_fits GROUP BY ALL;

-- The model's check (internal, for the development model): each one-thread outermost phase of a
-- run, its measured wall time against the model's, priced with a measured profile. The model is
-- phase_latency's (40_latency.sql), on the run's own CPU: thread CPU on the wall clock
-- (run_clock), plus each request's expected latency (a lognormal's mean, median x
-- exp(sigma^2 / 2); HEADs cost a GET, DELETEs a PUT, appends past a cap one append per part), plus
-- the bytes moved at the profile's rates.
CREATE MACRO model_check(run_name, profile_name) AS TABLE
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, p.profile,
       s.wall_ns / 1e6 AS measured_ms,
       s.cpu_ns / c.cpu_per_wall / 1e6 AS cpu_ms,
       (s.gets + s.heads) * p.get_ms * exp(p.get_sigma ^ 2 / 2)
         + (s.puts + s.deletes) * p.put_ms * exp(p.put_sigma ^ 2 / 2)
         + (s.appends + CASE WHEN p.max_append_mib IS NULL THEN 0
                             ELSE coalesce(s.append_splits, 0) END)
           * p.append_ms * exp(p.append_sigma ^ 2 / 2) AS request_ms,
       s.bytes_read / 1048576.0 * p.read_ms_per_mib
         + s.bytes_written / 1048576.0 * p.write_ms_per_mib AS transfer_ms
FROM steps s
JOIN run_clock c USING (run_id)
LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
CROSS JOIN (SELECT profile, get_ms, get_sigma, put_ms, put_sigma, append_ms, append_sigma,
                   read_ms_per_mib, write_ms_per_mib, max_append_mib FROM measured_profiles
            UNION ALL BY NAME
            SELECT profile, get_ms, get_sigma, put_ms, put_sigma, append_ms, append_sigma,
                   read_ms_per_mib, write_ms_per_mib, max_append_mib FROM store_profiles) p
WHERE s.run_id = run_name AND p.profile = profile_name
  AND s.phase IN ('batch', 'read_full', 'read_recent', 'read_old')
  AND coalesce(rr.parallelism, 1) = 1;
