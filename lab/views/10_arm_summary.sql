-- One row per run and arm: analysis/sweep.py's summarize() (also what layouts.py tabulates).
-- Per batch: means over the batch phase after the initial load (step > 0). Reads: the last probe
-- under the one-footer reader. State: the last step. Row-group sizes: medians of the version
-- dump's compressed row groups (rg_meta), all of them and those written after the initial load.
CREATE VIEW arm_summary AS
WITH batch AS (
    SELECT run_id, arm,
           avg(bytes_written) / 1e6 AS write_mb_per_batch,
           avg(requests) AS requests_per_batch,
           count(DISTINCT step) AS batches
    FROM steps WHERE phase = 'batch' AND step > 0
    GROUP BY ALL
), compact AS (
    SELECT run_id, arm, sum(bytes_written) / 1e6 AS compact_mb
    FROM steps WHERE phase = 'compact'
    GROUP BY ALL
), probe AS (
    SELECT run_id, arm, phase, gets, bytes_read
    FROM steps
    WHERE phase LIKE 'read%' AND reader = 'one-footer'
    QUALIFY step = max(step) OVER (PARTITION BY run_id, arm)
), last_state AS (
    SELECT * FROM steps WHERE phase = 'state'
    QUALIFY row_number() OVER (PARTITION BY run_id, arm ORDER BY step DESC) = 1
), rg AS (
    SELECT run_id, arm, file_name, row_group_id,
           sum(total_compressed_size) / 1e6 AS mb,
           NOT starts_with(parse_filename(file_name), 'initial-') AS later
    FROM rg_meta GROUP BY ALL
), rg_median AS (
    SELECT run_id, arm, median(mb) AS rg_mb_median,
           median(mb) FILTER (WHERE later) AS new_rg_mb_median
    FROM rg GROUP BY ALL
)
SELECT b.run_id, b.arm,
       r.rg_bytes / 1048576.0 AS rg_mib,
       b.write_mb_per_batch,
       coalesce(c.compact_mb, 0) / greatest(1, b.batches) AS compact_mb_per_batch,
       b.requests_per_batch,
       f.gets AS read_full_gets,
       f.bytes_read / 1e6 AS read_full_mb,
       rr.gets AS read_recent_gets,
       o.gets AS read_old_gets,
       s.row_groups,
       CASE WHEN s.footer_bytes >= 0 THEN s.footer_bytes / 1e3 END AS footer_kb,
       s.stored_bytes / s.live_bytes AS stored_over_live,
       (s.stored_bytes - s.reclaimable_bytes) / s.live_bytes AS kept_over_live,
       s.objects,
       s.live_objects,
       m.rg_mb_median,
       m.new_rg_mb_median
FROM batch b
JOIN runs_raw r USING (run_id)
LEFT JOIN compact c USING (run_id, arm)
LEFT JOIN probe f ON f.run_id = b.run_id AND f.arm = b.arm AND f.phase = 'read_full'
LEFT JOIN probe rr ON rr.run_id = b.run_id AND rr.arm = b.arm AND rr.phase = 'read_recent'
LEFT JOIN probe o ON o.run_id = b.run_id AND o.arm = b.arm AND o.phase = 'read_old'
LEFT JOIN last_state s ON s.run_id = b.run_id AND s.arm = b.arm
LEFT JOIN rg_median m ON m.run_id = b.run_id AND m.arm = b.arm;
