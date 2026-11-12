-- H3 and H4 (docs/HYPOTHESES.md at 1145f89) from the local runs of 2026-10-25, retired 2026-11-12
-- (docs/LESSONS.md); their pages, layout-vs-policy.md and time-travel.md, are in git at bfbdf33.

-- H4: a time-travel read at step s reads the table as it was after batch s - travelBack, the state
-- read_full read at step s - travelBack, when it was current. An arm records its history before a
-- step's compaction and read_full runs after it, so the pairs are exact only in runs without
-- compaction. One row per run, arm, reader and pair.
CREATE VIEW h4_pairs AS
SELECT o.run_id, o.arm, o.reader, o.step, c.step AS state_step,
       o.gets AS old_gets, c.gets AS cur_gets,
       o.get_stream AS old_streams, c.get_stream AS cur_streams,
       o.aborts AS old_aborts, c.aborts AS cur_aborts,
       o.bytes_read AS old_bytes, c.bytes_read AS cur_bytes,
       o.bytes_skipped_through AS old_skipped, c.bytes_skipped_through AS cur_skipped,
       o.rows = c.rows AS same_rows
FROM steps o
JOIN runs_raw r USING (run_id)
JOIN run_params t ON t.run_id = o.run_id AND t.key = 'reads.travelBack'
JOIN steps c ON c.run_id = o.run_id AND c.arm = o.arm AND c.reader = o.reader
            AND c.phase = 'read_full' AND c.step = o.step - t.value_num
WHERE o.phase = 'read_old' AND r.compact_every = 0;

-- H3: one row per run and arm. Per batch (means over the batches after the initial load): the
-- batch phase's requests by type and bytes, and its requests by sub-phase (locate, merge-read,
-- delete-write and write; sums over the run per batch, since an arm skips a sub-phase in some
-- batches). Reads: one full read under the one-footer reader, the mean over the probes after the
-- initial load, and at the last probe. Priced on S3 Express as analysis/compaction.py prices it
-- (PRICES, US East since 2025-04-10: PUT-class and GET-class requests, bytes written and read; no
-- storage or CPU), in µ$.
CREATE VIEW h3_arms AS
WITH b AS (
    SELECT run_id, arm, count(*) AS batches,
           avg(puts) AS puts, avg(appends) AS appends, avg(heads) AS heads,
           avg(get_range) AS get_range, avg(get_stream) AS get_stream, avg(deletes) AS deletes,
           avg(requests) AS requests,
           avg(bytes_written) / 1e6 AS mb_written, avg(bytes_read) / 1e6 AS mb_read,
           avg(1e6 * ((puts + appends) * 0.00113e-3 + (gets + heads) * 0.00003e-3
                      + bytes_written / 1e9 * 0.0032 + bytes_read / 1e9 * 0.0006)) AS write_usd
    FROM steps WHERE phase = 'batch' AND step > 0
    GROUP BY ALL
), sub AS (
    SELECT run_id, arm,
           sum(requests) FILTER (WHERE phase = 'locate') AS locate_req,
           sum(requests) FILTER (WHERE phase = 'merge-read') AS merge_read_req,
           sum(requests) FILTER (WHERE phase = 'delete-write') AS delete_write_req,
           sum(requests) FILTER (WHERE phase = 'write') AS write_req
    FROM steps WHERE step > 0 AND phase IN ('locate', 'merge-read', 'delete-write', 'write')
    GROUP BY ALL
), f AS (
    SELECT run_id, arm,
           avg(gets + heads) AS full_req, avg(bytes_read) / 1e6 AS full_mb,
           avg(1e6 * ((gets + heads) * 0.00003e-3 + bytes_read / 1e9 * 0.0006)) AS full_usd,
           arg_max(gets + heads, step) AS full_req_last, arg_max(bytes_read, step) / 1e6 AS full_mb_last
    FROM steps WHERE phase = 'read_full' AND reader = 'one-footer' AND step > 0
    GROUP BY ALL
), rr AS (
    SELECT run_id, arm, arg_max(gets + heads, step) AS recent_req_last
    FROM steps WHERE phase = 'read_recent' AND reader = 'one-footer' AND step > 0
    GROUP BY ALL
)
SELECT b.*,
       coalesce(sub.locate_req, 0) / b.batches AS locate_req,
       coalesce(sub.merge_read_req, 0) / b.batches AS merge_read_req,
       coalesce(sub.delete_write_req, 0) / b.batches AS delete_write_req,
       coalesce(sub.write_req, 0) / b.batches AS write_req,
       f.full_req, f.full_mb, f.full_usd, f.full_req_last, f.full_mb_last, rr.recent_req_last
FROM b
LEFT JOIN sub USING (run_id, arm)
LEFT JOIN f USING (run_id, arm)
LEFT JOIN rr USING (run_id, arm);

-- H3's differences, per tail policy and run, as hi_arm minus lo_arm:
--   layout    = RMoW-p minus TailCoW-p (the layout and the update handling together);
--   policy    = TailCoW-p minus MoR-v3 (merging the tail);
--   append    = RMoW-p minus RMoW-p-split (one appendable object against an object per version).
-- Writes per batch, one full read, and both priced at q full reads per batch (usd_q = write_usd
-- + q * full_usd).
CREATE VIEW h3_diffs AS
WITH pairs(diff, pol, hi, lo) AS (
    VALUES ('layout', 'b', 'rmow-b', 'tailcow-b'), ('layout', 'c', 'rmow-c', 'tailcow-c'),
           ('policy', 'b', 'tailcow-b', 'mor-v3'), ('policy', 'c', 'tailcow-c', 'mor-v3'),
           ('append', 'b', 'rmow-b', 'rmow-b-split'), ('append', 'c', 'rmow-c', 'rmow-c-split')
)
SELECT h.run_id, upper(pairs.pol) AS pol, pairs.diff, h.arm AS hi_arm, l.arm AS lo_arm,
       h.requests - l.requests AS d_requests,
       h.puts + h.appends - l.puts - l.appends AS d_put_class,
       h.heads - l.heads AS d_heads,
       h.get_range + h.get_stream - l.get_range - l.get_stream AS d_gets,
       h.mb_written - l.mb_written AS d_mb_written,
       h.mb_read - l.mb_read AS d_mb_read,
       h.full_req - l.full_req AS d_full_req,
       h.full_mb - l.full_mb AS d_full_mb,
       h.write_usd - l.write_usd AS d_write_usd,
       h.full_usd - l.full_usd AS d_full_usd
FROM pairs
JOIN h3_arms h ON h.arm = pairs.hi
JOIN h3_arms l ON l.run_id = h.run_id AND l.arm = pairs.lo;
