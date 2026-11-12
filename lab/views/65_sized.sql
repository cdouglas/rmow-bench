-- A store profile priced by request size (ROADMAP.md, A profile priced by request size): each
-- logged request costs a spike's expected latency at the request's size, where measured_profiles
-- (60_cloud.sql) takes a latency at the smallest size plus a slope. Only runs that logged their
-- requests can be priced this way.

-- Per spike, op and size: the expected latency in ms, a lognormal's mean (median x exp(sigma^2/2)),
-- for the request ops the logs have:
--   get     get_range: a range from the middle of a large object, as a row group is read; a
--           streamed GET is priced by the bytes it delivered
--   head    head where the spike measured it (Azure), else a GET of the smallest size (S3 Express)
--   put     put
--   append  append_sao: the fork's appends, which the region arms send
--   create  an appendable create: an append of its size plus what a create costs over an append
--           at the size create_sao was measured (on S3 Express within 1 ms of create_sao itself at
--           every size; Azure measured only 64 KiB, where a create is a staged Append Blob, an
--           Append Block and a rename)
-- A smaller size is never priced above a larger one: each size takes the least expected latency at
-- it or above. The spikes' smallest get_range is each rep's first read of a freshly written
-- 128 MiB blob, which took 33-118 ms on Azure, where the calibration's 64 KiB GETs took 9 ms.
CREATE VIEW sized_points AS
WITH raw AS (
    SELECT run_id AS profile,
           CASE op WHEN 'get_range' THEN 'get' WHEN 'append_sao' THEN 'append' ELSE op END AS op,
           bytes, median_ms * exp(sigma ^ 2 / 2) AS ms
    FROM spike_ops WHERE op IN ('get_range', 'head', 'put', 'append_sao')
),
create_cost AS (
    SELECT c.run_id AS profile,
           arg_min(c.median_ms * exp(c.sigma ^ 2 / 2), c.bytes)
             - arg_min(a.median_ms * exp(a.sigma ^ 2 / 2), c.bytes) AS ms
    FROM spike_ops c JOIN spike_ops a ON a.run_id = c.run_id AND a.op = 'append_sao'
                                     AND a.bytes = c.bytes
    WHERE c.op = 'create_sao' GROUP BY ALL
),
with_creates AS (
    SELECT * FROM raw
    UNION ALL
    SELECT r.profile, 'create', r.bytes, r.ms + c.ms
    FROM raw r JOIN create_cost c USING (profile) WHERE r.op = 'append'
),
envelope AS (
    SELECT profile, op, bytes,
           min(ms) OVER (PARTITION BY profile, op ORDER BY bytes DESC
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ms
    FROM with_creates
)
SELECT * FROM envelope
UNION ALL
SELECT g.profile, 'head', 0, arg_min(g.ms, g.bytes)
FROM envelope g ANTI JOIN (SELECT DISTINCT profile FROM envelope WHERE op = 'head') USING (profile)
WHERE g.op = 'get' GROUP BY ALL;

-- The points as segments: a request of `bytes` in [lo, hi) costs ms + (bytes - at) x slope. Flat
-- below the smallest size; beyond the largest, the last two sizes' slope carries on.
CREATE VIEW sized_segments AS
WITH p AS (
    SELECT *, lead(bytes) OVER w AS next_bytes, lead(ms) OVER w AS next_ms,
           lag(bytes) OVER w AS prev_bytes, lag(ms) OVER w AS prev_ms,
           row_number() OVER w AS i
    FROM sized_points WINDOW w AS (PARTITION BY profile, op ORDER BY bytes)
)
SELECT profile, op, -9.0e18 AS lo, bytes AS hi, bytes AS at, ms, 0.0 AS slope
FROM p WHERE i = 1
UNION ALL
SELECT profile, op, bytes, coalesce(next_bytes, 9.0e18), bytes, ms,
       CASE WHEN next_bytes IS NOT NULL THEN (next_ms - ms) / (next_bytes - bytes)
            WHEN prev_bytes IS NOT NULL THEN (ms - prev_ms) / (bytes - prev_bytes)
            ELSE 0.0 END
FROM p;

-- The most one append may write, per profile: Azure's Append Block takes 100 MiB, and the fork
-- splits a larger append or appendable create into several; S3 Express has no cap here.
CREATE VIEW sized_caps AS
SELECT DISTINCT profile, CASE WHEN profile LIKE 'spike/azure-%' THEN 100 * 1048576 END AS cap
FROM sized_points;

-- Each logged request of the outermost phases (batch, read_*) as an op and a size, with its
-- measured time. A streamed GET's size is the bytes its stream delivered, and its time includes
-- the reads that waited on them. The log records an appendable create as a PUT (TracingFileIO),
-- so a Parquet PUT by an arm that appends in the run is taken as one: RMoW-C's new files and
-- RMoR-C with footers' data files (each later takes a footer, or may). No other arm appends. Not
-- requests: task, abort (a stream closed early) and stream_bytes. A table, made once per ingest.
-- Every join condition is an equality between the two sides, the request's kind folded into a key:
-- a LEFT JOIN that tests r.kind in its ON runs as a nested loop (labbook's troubleshooting, A slow
-- ingest), which took 332 s against 1.8 s on 2.4 million requests, with the same rows.
CREATE TABLE request_ops AS
WITH streams AS (
    SELECT run_id, arm, step, phase, reader, task, object, seq, kind, length, dur_ns,
           count(*) FILTER (WHERE kind = 'get_stream')
             OVER (PARTITION BY run_id, arm, step, phase, reader, task, object ORDER BY seq)
             AS stream
    FROM requests WHERE kind IN ('get_stream', 'stream_bytes')
),
stream_sizes AS (
    SELECT run_id, arm, step, phase, reader, task, object,
           min(seq) FILTER (WHERE kind = 'get_stream') AS seq,
           coalesce(sum(length) FILTER (WHERE kind = 'stream_bytes'), 0) AS bytes,
           sum(dur_ns) AS dur_ns
    FROM streams WHERE stream > 0 GROUP BY ALL
),
appending AS (SELECT DISTINCT run_id, arm, 'put' AS kind FROM requests WHERE kind = 'append')
SELECT r.run_id, r.arm, r.step, r.phase, r.reader, r.seq, r.kind,
       CASE r.kind WHEN 'get_range' THEN 'get' WHEN 'get_stream' THEN 'get'
                   WHEN 'put' THEN CASE WHEN a.arm IS NOT NULL AND r.object LIKE '%.parquet'
                                       THEN 'create' ELSE 'put' END
                   ELSE r.kind END AS op,
       CASE WHEN r.kind = 'get_stream' THEN ss.bytes ELSE greatest(r.length, 0) END AS bytes,
       CASE WHEN r.kind = 'get_stream' THEN ss.dur_ns ELSE r.dur_ns END AS dur_ns
FROM requests r
LEFT JOIN appending a ON a.run_id = r.run_id AND a.arm = r.arm AND a.kind = r.kind
LEFT JOIN stream_sizes ss ON ss.run_id = r.run_id AND ss.arm = r.arm
                         AND ss.seq = CASE WHEN r.kind = 'get_stream' THEN r.seq END
WHERE r.kind IN ('get_range', 'get_stream', 'head', 'put', 'append');

-- Each logged request of a run priced with a spike's sizes: an append or create past the cap is
-- that many appends of the cap, then one of the rest.
CREATE MACRO sized_requests(run_name, spike_name) AS TABLE
WITH q AS (
    SELECT o.*, c.cap,
           CASE WHEN o.op IN ('append', 'create') AND c.cap IS NOT NULL AND o.bytes > c.cap
                THEN (o.bytes - 1) // c.cap ELSE 0 END AS splits
    FROM request_ops o CROSS JOIN sized_caps c
    WHERE o.run_id = run_name AND c.profile = spike_name
),
r AS (SELECT *, bytes - splits * coalesce(cap, 0) AS rest FROM q),
cap_ms AS (
    SELECT s.ms + (c.cap - s.at) * s.slope AS ms
    FROM sized_segments s JOIN sized_caps c USING (profile)
    WHERE s.profile = spike_name AND s.op = 'append' AND c.cap >= s.lo AND c.cap < s.hi
)
SELECT r.run_id, r.arm, r.step, r.phase, r.reader, r.seq, r.kind, r.op, r.bytes,
       r.dur_ns / 1e6 AS measured_ms,
       s.ms + (r.rest - s.at) * s.slope
         + CASE WHEN r.splits = 0 THEN 0
                ELSE r.splits * coalesce(cap_ms.ms, 'NaN'::DOUBLE) END AS price_ms
FROM r JOIN sized_segments s
  ON s.profile = spike_name AND s.op = r.op AND r.rest >= s.lo AND r.rest < s.hi
LEFT JOIN cap_ms ON true;

-- The sized model per one-thread outermost phase of a run (batch, compact, read_*), priced with a
-- spike: its CPU plus its requests' prices. cpu_ms is thread CPU on the wall clock (run_clock),
-- work_ms the phase's work at its arm's pace where the run counted instructions (phase_work); a
-- cloud run's own check uses cpu_ms, a prediction from a local twin work_ms. measured_ms is the
-- phase's wall time, and logged_ms the time its logged requests took (a cloud run's; a local
-- run's are the local store's). A probe reads several windows (read_window, `records` of them),
-- whose records and requests the log does not tell apart: a window's row is their mean, so its
-- `requests` may be fractional.
CREATE MACRO sized_latency(run_name, spike_name) AS TABLE
WITH q AS (
    SELECT arm, step, phase, reader, count(*) AS requests, sum(price_ms) AS request_ms,
           sum(measured_ms) AS logged_ms
    FROM sized_requests(run_name, spike_name) GROUP BY ALL
),
s AS (
    SELECT s.run_id, s.arm, s.step, s.phase, s.reader, count(*) AS records,
           sum(s.wall_ns) AS wall_ns, sum(s.cpu_ns) AS cpu_ns
    FROM steps s
    WHERE s.run_id = run_name
      AND s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old', 'read_window',
                      'read_sorted')
    GROUP BY ALL
),
w AS (
    SELECT arm, step, phase, reader, sum(work_ms) AS work_ms
    FROM phase_work WHERE run_id = run_name GROUP BY ALL
)
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, spike_name AS profile,
       s.wall_ns / s.records / 1e6 AS measured_ms, s.cpu_ns / s.records / c.cpu_per_wall / 1e6 AS cpu_ms,
       w.work_ms / s.records AS work_ms,
       CASE WHEN s.records = 1 THEN coalesce(q.requests, 0)
            ELSE coalesce(q.requests, 0) / s.records END AS requests,
       coalesce(q.request_ms, 0) / s.records AS request_ms,
       coalesce(q.logged_ms, 0) / s.records AS logged_ms, s.records
FROM s
JOIN run_clock c USING (run_id)
LEFT JOIN run_readers rr ON rr.run_id = s.run_id AND rr.reader = s.reader
LEFT JOIN w ON w.arm = s.arm AND w.step = s.step AND w.phase = s.phase
           AND w.reader IS NOT DISTINCT FROM s.reader
LEFT JOIN q ON q.arm = s.arm AND q.step = s.step AND q.phase = s.phase
           AND q.reader IS NOT DISTINCT FROM s.reader
WHERE coalesce(rr.parallelism, 1) = 1;
