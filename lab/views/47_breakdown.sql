-- Where a phase's time went (README, What is measured). Since 2026-10-26 each phase records, for
-- its thread, the wall and CPU time of each kind of work (Spans), exclusive of the others:
--   io       calls into the store: the client's work (checksums, HTTP, TLS) and the wait for it
--   encode   Parquet encoding of rows: data files and RMoW's regions
--   decode   Parquet decoding in scans: footers, pages and rows, with deletes applied
--   footer   RMoW's footers: Thrift decoding, encoding and merging
--   deletes  delete files: DVs and position deletes built, encoded, loaded and decoded
-- as <kind>_ns and <kind>_cpu_ns, and the same for its split tasks on a read pool (pool_*, with
-- pool_wall_ns and pool_cpu_ns). Nested phases are included in the phases around them.
--
-- phase_breakdown: one row per phase and component, in ms, for the phase's own thread (thread =
-- 'phase') and its pool's tasks together (thread = 'pool'). On the phase's thread the components
-- add up to its wall time:
--   <kind> CPU              the kind's CPU (encode, decode, footer, deletes)
--   other CPU               the rest of the thread's CPU: planning, bookkeeping, the oracle
--   store client CPU        io's CPU, on this thread; the SDK's own threads (Azure's event loop)
--                           are in process_cpu_ns, not here
--   waiting on the store    io's wall time less its CPU
--   other waiting           the rest of the wall time: off the CPU outside requests (garbage
--                           collection, the JIT, scheduling)
-- A pool's tasks run side by side, so theirs add up to their summed wall time (pool_wall_ns), not
-- the phase's.
--
-- CPU times are put on the wall clock first. They are thread CPU clock readings, and under WSL2 on
-- Hyper-V (the laptop) that clock ran 11% fast against the wall clock, so a busy phase's cpu_ns
-- exceeded its wall_ns and other waiting went negative. Each run records the ratio, spinning
-- before it starts (manifest host.thread_cpu_per_wall; 1 where the clocks agree), and CPU times
-- here are divided by it.

-- Runs before 2026-10-26 have none: these columns stand in until a run has them.
ALTER TABLE steps ADD COLUMN IF NOT EXISTS io_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS io_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS encode_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS encode_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS decode_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS decode_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS footer_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS footer_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS deletes_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS deletes_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_wall_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_io_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_io_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_encode_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_encode_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_decode_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_decode_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_footer_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_footer_cpu_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_deletes_ns BIGINT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS pool_deletes_cpu_ns BIGINT;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS start_ns BIGINT;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS dur_ns BIGINT;

CREATE VIEW phase_breakdown AS
WITH k AS (
    SELECT run_id,
           coalesce(try_cast(json_extract(manifest, '$.host.thread_cpu_per_wall') AS DOUBLE), 1)
             AS rate
    FROM runs_raw
), s AS (
    SELECT s.run_id, arm, step, phase, reader, wall_ns, cpu_ns / k.rate AS cpu_ns,
           io_ns, io_cpu_ns / k.rate AS io_cpu_ns, encode_ns, encode_cpu_ns / k.rate AS encode_cpu_ns,
           decode_ns, decode_cpu_ns / k.rate AS decode_cpu_ns,
           footer_ns, footer_cpu_ns / k.rate AS footer_cpu_ns,
           deletes_ns, deletes_cpu_ns / k.rate AS deletes_cpu_ns,
           coalesce(pool_wall_ns, 0) AS pool_wall_ns,
           coalesce(pool_cpu_ns, 0) / k.rate AS pool_cpu_ns,
           pool_io_ns, pool_io_cpu_ns / k.rate AS pool_io_cpu_ns,
           pool_encode_ns, pool_encode_cpu_ns / k.rate AS pool_encode_cpu_ns,
           pool_decode_ns, pool_decode_cpu_ns / k.rate AS pool_decode_cpu_ns,
           pool_footer_ns, pool_footer_cpu_ns / k.rate AS pool_footer_cpu_ns,
           pool_deletes_ns, pool_deletes_cpu_ns / k.rate AS pool_deletes_cpu_ns
    FROM steps s JOIN k ON k.run_id = s.run_id
    WHERE io_ns IS NOT NULL AND phase <> 'state'
), parts AS (
    SELECT run_id, arm, step, phase, reader, wall_ns / 1e6 AS wall_ms,
           unnest([
             {'thread': 'phase', 'ord': 1, 'component': 'encode CPU', 'ns': encode_cpu_ns},
             {'thread': 'phase', 'ord': 2, 'component': 'decode CPU', 'ns': decode_cpu_ns},
             {'thread': 'phase', 'ord': 3, 'component': 'footer CPU', 'ns': footer_cpu_ns},
             {'thread': 'phase', 'ord': 4, 'component': 'deletes CPU', 'ns': deletes_cpu_ns},
             {'thread': 'phase', 'ord': 5, 'component': 'other CPU',
              'ns': cpu_ns - encode_cpu_ns - decode_cpu_ns - footer_cpu_ns - deletes_cpu_ns
                    - io_cpu_ns},
             {'thread': 'phase', 'ord': 6, 'component': 'store client CPU', 'ns': io_cpu_ns},
             {'thread': 'phase', 'ord': 7, 'component': 'waiting on the store',
              'ns': io_ns - io_cpu_ns},
             -- off the CPU outside requests, inside the other kinds' spans or not
             {'thread': 'phase', 'ord': 8, 'component': 'other waiting',
              'ns': (wall_ns - cpu_ns) - (io_ns - io_cpu_ns)},
             {'thread': 'pool', 'ord': 1, 'component': 'encode CPU', 'ns': pool_encode_cpu_ns},
             {'thread': 'pool', 'ord': 2, 'component': 'decode CPU', 'ns': pool_decode_cpu_ns},
             {'thread': 'pool', 'ord': 3, 'component': 'footer CPU', 'ns': pool_footer_cpu_ns},
             {'thread': 'pool', 'ord': 4, 'component': 'deletes CPU', 'ns': pool_deletes_cpu_ns},
             {'thread': 'pool', 'ord': 5, 'component': 'other CPU',
              'ns': pool_cpu_ns - pool_encode_cpu_ns - pool_decode_cpu_ns - pool_footer_cpu_ns
                    - pool_deletes_cpu_ns - pool_io_cpu_ns},
             {'thread': 'pool', 'ord': 6, 'component': 'store client CPU', 'ns': pool_io_cpu_ns},
             {'thread': 'pool', 'ord': 7, 'component': 'waiting on the store',
              'ns': pool_io_ns - pool_io_cpu_ns},
             {'thread': 'pool', 'ord': 8, 'component': 'other waiting',
              'ns': (pool_wall_ns - pool_cpu_ns) - (pool_io_ns - pool_io_cpu_ns)}
           ], recursive := true)
    FROM s
)
SELECT run_id, arm, step, phase, reader, thread, ord, component, ns / 1e6 AS ms, wall_ms
FROM parts
WHERE thread = 'phase' OR ns <> 0;
