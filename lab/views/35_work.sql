-- The CPU measure (docs/LESSONS.md, Measuring work). Thread CPU time moves with the clock: the
-- same read repeated back to back took up to twice the time. A phase's user-mode instructions,
-- its thread's and its read pool's (ThreadCounters), do not. A phase's time for the latency model is its work
-- at its arm's own pace:
--   work_ms = instructions x cpi / clock
-- cpi is the median cycles per instruction of that arm's phases of that kind (and reader) in the
-- run; clock is the run's median user-mode cycles per user-mode nanosecond, on the wall clock
-- (run_clock). Kernel time (the local store's reads, page faults: 1-6% of thread CPU) is left out.
-- Runs without counters have none.

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

-- The thread CPU clock against the wall clock, per run: 1 where they agree. Under WSL2 on Hyper-V
-- (the laptop) the thread CPU clock ran 11.1% fast, so thread CPU times were 11% long, and so was
-- every phase's work_ms, whose clock divided cycles by them (decided 2026-10-27: CPU times go on
-- the wall clock). Runs record it from 2026-10-26 (manifest host.thread_cpu_per_wall, a 200 ms
-- spin). An earlier run takes the median of its host's (same name and CPU), which is right where
-- they share a boot: the laptop's runs with counters all ran on the boot of 2026-10-24, on which
-- three runs measured 1.1116. Other runs take 1: havoc's is not yet measured, and its runs have no
-- counters.
CREATE VIEW run_clock AS
WITH m AS (
    SELECT run_id, host, json_extract_string(manifest, '$.host.cpu_model') AS cpu,
           try_cast(json_extract(manifest, '$.host.thread_cpu_per_wall') AS DOUBLE) AS own
    FROM runs_raw
), h AS (
    SELECT host, cpu, median(own) AS rate FROM m WHERE own IS NOT NULL GROUP BY ALL
)
SELECT m.run_id, coalesce(m.own, h.rate, 1) AS cpu_per_wall, m.own IS NOT NULL AS measured
FROM m LEFT JOIN h ON h.host = m.host AND h.cpu = m.cpu;

-- cpu_ns and user_ns are on the wall clock (divided by run_clock's cpu_per_wall).
CREATE VIEW phase_work AS
WITH s AS (
    SELECT s.run_id, arm, step, phase, reader,
           (cpu_ns + coalesce(pool_cpu_ns, 0)) / c.cpu_per_wall AS cpu_ns,
           (user_ns + coalesce(pool_user_ns, 0)) / c.cpu_per_wall AS user_ns,
           instructions + coalesce(pool_instructions, 0) AS instructions,
           cycles + coalesce(pool_cycles, 0) AS cycles, c.cpu_per_wall
    FROM steps s JOIN run_clock c ON c.run_id = s.run_id
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
