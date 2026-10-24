-- One row per job of a container campaign (docker/README.md), with the run it made: where it ran
-- and its foreign load, the busy time of its cores (SMT siblings included) less its own CPU time,
-- in CPU-seconds, with its mean and its largest sample in CPUs (samples of 5 seconds in the first
-- scale campaign, 1 second since). Jobs since then also carry the samples (the load table) and
-- shield's `interfered` flag in their records.
CREATE VIEW container_jobs AS
SELECT regexp_extract(filename, 'campaigns/([^/]+)/[^/]+$', 1) AS campaign,
       regexp_extract(filename, '([^/]+)\.json$', 1) AS job,
       regexp_replace(run_dir, '^/results/', '') AS run_id,
       node, cpus, owned, CAST(started AS TIMESTAMP) AS started, CAST(ended AS TIMESTAMP) AS ended,
       "exit", foreign_cpu_s, foreign_max_cpus, watch_samples,
       foreign_cpu_s / nullif(epoch(CAST(ended AS TIMESTAMP)) - epoch(CAST(started AS TIMESTAMP)), 0)
           AS foreign_mean_cpus
FROM container_records
WHERE run_dir IS NOT NULL;

-- Runs before 2026-10-24 have neither phase start times (steps.start_ms) nor load samples, and an
-- ingest of only such runs creates no load table: these stand in until a run has both.
ALTER TABLE steps ADD COLUMN IF NOT EXISTS start_ms BIGINT;
CREATE TABLE IF NOT EXISTS load (run_id VARCHAR, t0 DOUBLE, t1 DOUBLE, own_cpus DOUBLE,
                                 foreign_cpus DOUBLE, sibling_busy_cpus DOUBLE);

-- The foreign load during each timed phase of a run with load samples: the samples whose
-- interval overlaps the phase's. A phase is clean when no overlapping sample reached 0.25 CPUs
-- of foreign load, which half a CPU taken from the job's own (0.5) or a busy sibling (1) exceed;
-- only a clean phase's CPU decides anything (docs/HYPOTHESES.md). Nested phases are matched on
-- their own intervals. The phase's own CPU, bytes and requests come along; to join another view
-- by phase, match reader with IS NOT DISTINCT FROM, since batches and compactions have none.
CREATE VIEW phase_load AS
SELECT s.run_id, s.arm, s.step, s.phase, s.reader, s.start_ms, s.wall_ns, s.cpu_ns,
       s.process_cpu_ns, s.requests, s.bytes_read, s.bytes_written,
       count(*) AS samples,
       max(l.foreign_cpus) AS foreign_max_cpus,
       max(l.sibling_busy_cpus) AS sibling_max_cpus,
       max(l.foreign_cpus) < 0.25 AS clean
FROM steps s
JOIN load l ON l.run_id = s.run_id
           AND l.t1 > s.start_ms / 1e3 AND l.t0 < s.start_ms / 1e3 + s.wall_ns / 1e9
WHERE s.start_ms IS NOT NULL
GROUP BY ALL;
