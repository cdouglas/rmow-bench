-- The CPU measure (lab/pages/cpu-measure.md). Thread CPU time moves with the clock: the same read
-- repeated back to back took up to twice the time. A phase's user-mode instructions, its thread's
-- and its read pool's (ThreadCounters), do not. A phase's time for the latency model is its work
-- at its arm's own pace:
--   work_ms = instructions x cpi / clock
-- cpi is the median cycles per instruction of that arm's phases of that kind (and reader) in the
-- run; clock is the run's median user-mode cycles per user-mode nanosecond. Kernel time (the local
-- store's reads, page faults: 1-6% of thread CPU) is left out. Runs without counters have none.

-- Runs before 2026-10-24 have no counts: these columns stand in until a run has them.
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS user_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_user_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS instructions BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS cycles BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_instructions BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_cycles BIGINT;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS instructions BIGINT;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS cycles BIGINT;

CREATE VIEW phase_work AS
WITH s AS (
    SELECT run_id, arm, step, phase, reader,
           cpu_ns + coalesce(pool_cpu_ns, 0) AS cpu_ns,
           user_ns + coalesce(pool_user_ns, 0) AS user_ns,
           instructions + coalesce(pool_instructions, 0) AS instructions,
           cycles + coalesce(pool_cycles, 0) AS cycles
    FROM steps
    WHERE instructions > 0
), pace AS (
    SELECT run_id, arm, phase, reader, median(cycles / instructions) AS cpi
    FROM s GROUP BY ALL
), clock AS (
    SELECT run_id, median(cycles / user_ns) AS ghz FROM s WHERE user_ns > 0 GROUP BY ALL
)
SELECT s.*, p.cpi, k.ghz, s.instructions * p.cpi / k.ghz / 1e6 AS work_ms
FROM s
JOIN pace p ON p.run_id = s.run_id AND p.arm = s.arm AND p.phase = s.phase
           AND p.reader IS NOT DISTINCT FROM s.reader
JOIN clock k ON k.run_id = s.run_id;
