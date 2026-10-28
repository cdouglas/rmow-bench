-- Store latency, simulated over the recorded requests (lab/pages/store-latency.md). A phase that
-- issues one request at a time takes about
--   its thread CPU + sum over requests of E[latency(op)] + MiB read x read rate + MiB written x write rate
-- where E[latency] is a lognormal's mean, median x exp(sigma^2 / 2). Phases on several threads
-- (reader models with parallelism above 1) are left out: their critical path needs the
-- per-request event log.

-- Store profiles, from the endive simulator (~/work/catalog/endive, commit 2c4c0af):
-- endive/providers/<name>.toml for GET (read), PUT (write) and their per-MiB write rate, and
-- docs/analysis/distributions.json for appends by a single writer (threads = 1, median). HEADs
-- cost a GET and DELETEs a PUT (not measured). Reads were measured on small objects only, so the
-- read rate per MiB is assumed equal to the write rate; store_sweep varies it. A NULL append means
-- the store has none, so appendable RMoW cannot run there. max_append_mib is the most one append
-- may write: Azure's Append Block takes 100 MiB (service version 2022-11-02 and later), and the
-- fork splits a larger append, or a larger appendable create's staging, into several; S3 Express
-- takes 5 GB, which no write here reaches (NULL).
CREATE TABLE store_profiles AS
SELECT * FROM (VALUES
    ('cpu only', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, NULL),
    ('s3x', 2.5, 0.57, 6.5, 0.24, 13.0, 0.25, 10.0, 10.0, NULL),
    ('s3', 27.0, 0.62, 60.0, 0.29, NULL, NULL, 20.0, 20.0, NULL),
    ('azure premium', 35.0, 0.08, 41.0, 0.10, 70.9, 0.23, 15.0, 15.0, 100),
    ('azure', 38.0, 0.66, 45.0, 0.50, 87.6, 0.28, 25.0, 25.0, 100)
) t(profile, get_ms, get_sigma, put_ms, put_sigma, append_ms, append_sigma, read_ms_per_mib,
    write_ms_per_mib, max_append_mib);

-- The appends beyond the first that a capped store splits a phase's appends and appendable
-- creates into (append_splits, IoTrace.splits, counted at 100 MiB: the only cap a profile has).
-- The model counts neither as more than one request. Runs before 2026-10-27 lack the count: for an
-- outermost phase of a run that logged its requests it is taken from the log's appends (the log
-- does not tell an appendable create from a PUT, so their splits are missed), and otherwise taken
-- as none.
ALTER TABLE steps ADD COLUMN IF NOT EXISTS append_splits BIGINT;
CREATE VIEW phase_splits AS
SELECT s.run_id, s.arm, s.step, s.phase, s.reader,
       coalesce(s.append_splits, l.splits, 0) AS append_splits
FROM steps s
LEFT JOIN (
    SELECT run_id, arm, step, phase, reader,
           sum(CASE WHEN length > 100 * 1048576 THEN (length - 1) // (100 * 1048576) ELSE 0 END)
             AS splits
    FROM requests WHERE kind = 'append' GROUP BY ALL
) l ON l.run_id = s.run_id AND l.arm = s.arm AND l.step = s.step AND l.phase = s.phase
   AND l.reader IS NOT DISTINCT FROM s.reader;

-- The reader models each run used, with their parallelism (runs before reader models: one-footer
-- and stock, both on one thread).
CREATE VIEW run_readers AS
SELECT run_id, r ->> 'name' AS reader, coalesce(TRY_CAST(r ->> 'parallelism' AS INTEGER), 1) AS parallelism
FROM run_params, unnest(CAST(from_json(value, '["json"]') AS JSON[])) AS t(r)
WHERE key = 'reads.readers';

-- One row per run, arm, step, phase, reader and profile: the phase's simulated latency in ms,
-- split into CPU, requests and transfer. Only phases on one thread: batches, compactions, and
-- probes under a reader model with parallelism 1. cpu_ms is the phase's thread CPU; work_ms, for
-- runs with instruction counts, its work at its arm's pace (phase_work), the steadier measure. On
-- a store with a cap, each split costs an append more (phase_splits).
CREATE VIEW phase_latency AS
WITH s AS (
    SELECT s.* REPLACE (x.append_splits AS append_splits),
           coalesce(rr.parallelism, 1) AS parallelism, w.work_ms
    FROM steps s LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
    LEFT JOIN phase_work w ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step
                          AND w.phase = s.phase AND w.reader IS NOT DISTINCT FROM s.reader
    JOIN phase_splits x ON x.run_id = s.run_id AND x.arm = s.arm AND x.step = s.step
                       AND x.phase = s.phase AND x.reader IS NOT DISTINCT FROM s.reader
    WHERE s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old')
)
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, p.profile,
       s.cpu_ns / 1e6 AS cpu_ms, s.work_ms,
       (s.gets + s.heads) * p.get_ms * exp(p.get_sigma ^ 2 / 2)
         + (s.puts + s.deletes) * p.put_ms * exp(p.put_sigma ^ 2 / 2)
         + CASE WHEN s.appends = 0 THEN 0
                WHEN p.append_ms IS NULL THEN 'NaN'::DOUBLE  -- the store cannot append
                ELSE (s.appends
                      + CASE WHEN p.max_append_mib IS NULL THEN 0 ELSE s.append_splits END)
                     * p.append_ms * exp(p.append_sigma ^ 2 / 2) END
         AS request_ms,
       s.bytes_read / 1048576.0 * p.read_ms_per_mib
         + s.bytes_written / 1048576.0 * p.write_ms_per_mib AS transfer_ms,
       s.appends > 0 AND p.append_ms IS NULL AS needs_append
FROM s CROSS JOIN store_profiles p
WHERE s.parallelism = 1;

-- The same over a grid of generic stores: every request costs request_ms (whatever its kind) and
-- every MiB 1000 / mib_per_s ms, in either direction. For sweeps.
-- cpu_ms is the phase's modeled time where the run counts instructions, else its thread CPU.
CREATE MACRO store_sweep(request_ms_list, mib_per_s_list) AS TABLE
WITH s AS (
    SELECT s.*, coalesce(rr.parallelism, 1) AS parallelism, w.work_ms
    FROM steps s LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
    LEFT JOIN phase_work w ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step
                          AND w.phase = s.phase AND w.reader IS NOT DISTINCT FROM s.reader
    WHERE s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old')
), g AS (
    SELECT l AS request_ms, b AS mib_per_s
    FROM unnest(request_ms_list) AS t1(l), unnest(mib_per_s_list) AS t2(b)
)
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, g.request_ms, g.mib_per_s,
       coalesce(s.work_ms, s.cpu_ns / 1e6) AS cpu_ms,
       (s.gets + s.heads + s.puts + s.appends + s.deletes) * g.request_ms AS request_ms_total,
       (s.bytes_read + s.bytes_written) / 1048576.0 * 1000.0 / g.mib_per_s AS transfer_ms
FROM s CROSS JOIN g
WHERE s.parallelism = 1;
