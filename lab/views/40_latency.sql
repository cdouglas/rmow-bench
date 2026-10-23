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
-- the store has none, so appendable RMoW cannot run there.
CREATE TABLE store_profiles AS
SELECT * FROM (VALUES
    ('cpu only', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
    ('s3x', 2.5, 0.57, 6.5, 0.24, 13.0, 0.25, 10.0, 10.0),
    ('s3', 27.0, 0.62, 60.0, 0.29, NULL, NULL, 20.0, 20.0),
    ('azure premium', 35.0, 0.08, 41.0, 0.10, 70.9, 0.23, 15.0, 15.0),
    ('azure', 38.0, 0.66, 45.0, 0.50, 87.6, 0.28, 25.0, 25.0)
) t(profile, get_ms, get_sigma, put_ms, put_sigma, append_ms, append_sigma, read_ms_per_mib,
    write_ms_per_mib);

-- The reader models each run used, with their parallelism (runs before reader models: one-footer
-- and stock, both on one thread).
CREATE VIEW run_readers AS
SELECT run_id, r ->> 'name' AS reader, coalesce(TRY_CAST(r ->> 'parallelism' AS INTEGER), 1) AS parallelism
FROM run_params, unnest(CAST(from_json(value, '["json"]') AS JSON[])) AS t(r)
WHERE key = 'reads.readers';

-- One row per run, arm, step, phase, reader and profile: the phase's simulated latency in ms,
-- split into CPU, requests and transfer. Only phases on one thread: batches, compactions, and
-- probes under a reader model with parallelism 1.
CREATE VIEW phase_latency AS
WITH s AS (
    SELECT s.*, coalesce(rr.parallelism, 1) AS parallelism
    FROM steps s LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
    WHERE s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old')
)
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, p.profile,
       s.cpu_ns / 1e6 AS cpu_ms,
       (s.gets + s.heads) * p.get_ms * exp(p.get_sigma ^ 2 / 2)
         + (s.puts + s.deletes) * p.put_ms * exp(p.put_sigma ^ 2 / 2)
         + CASE WHEN s.appends = 0 THEN 0
                WHEN p.append_ms IS NULL THEN 'NaN'::DOUBLE  -- the store cannot append
                ELSE s.appends * p.append_ms * exp(p.append_sigma ^ 2 / 2) END
         AS request_ms,
       s.bytes_read / 1048576.0 * p.read_ms_per_mib
         + s.bytes_written / 1048576.0 * p.write_ms_per_mib AS transfer_ms,
       s.appends > 0 AND p.append_ms IS NULL AS needs_append
FROM s CROSS JOIN store_profiles p
WHERE s.parallelism = 1;

-- The same over a grid of generic stores: every request costs request_ms (whatever its kind) and
-- every MiB 1000 / mib_per_s ms, in either direction. For sweeps.
CREATE MACRO store_sweep(request_ms_list, mib_per_s_list) AS TABLE
WITH s AS (
    SELECT s.*, coalesce(rr.parallelism, 1) AS parallelism
    FROM steps s LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
    WHERE s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old')
), g AS (
    SELECT l AS request_ms, b AS mib_per_s
    FROM unnest(request_ms_list) AS t1(l), unnest(mib_per_s_list) AS t2(b)
)
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, g.request_ms, g.mib_per_s,
       s.cpu_ns / 1e6 AS cpu_ms,
       (s.gets + s.heads + s.puts + s.appends + s.deletes) * g.request_ms AS request_ms_total,
       (s.bytes_read + s.bytes_written) / 1048576.0 * 1000.0 / g.mib_per_s AS transfer_ms
FROM s CROSS JOIN g
WHERE s.parallelism = 1;
