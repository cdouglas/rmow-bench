-- One row per job of a container campaign (docker/README.md), with the run it made: where it ran
-- and its foreign load, the busy time of its cores (SMT siblings included) less its own CPU time,
-- in CPU-seconds, with its mean and its largest 5-second rate in CPUs. A job with foreign load
-- decides nothing about CPU.
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

