-- analysis/compare_runs.py: requests and bytes are a deterministic function of the op log, the
-- configuration and the code, so two runs of one configuration must match exactly in these
-- columns (wall and CPU time may not). A phase occurring twice for one arm and step would be
-- numbered n = 1, 2 in no particular order; no run has one today.
CREATE MACRO steps_diff(run_a, run_b) AS TABLE
WITH a AS (
    SELECT *, row_number() OVER (PARTITION BY arm, step, phase) AS n
    FROM steps WHERE run_id = run_a
), b AS (
    SELECT *, row_number() OVER (PARTITION BY arm, step, phase) AS n
    FROM steps WHERE run_id = run_b
), arms AS (
    SELECT DISTINCT arm FROM a INTERSECT SELECT DISTINCT arm FROM b
)
SELECT coalesce(a.arm, b.arm) AS arm, coalesce(a.step, b.step) AS step,
       coalesce(a.phase, b.phase) AS phase, coalesce(a.n, b.n) AS n,
       a.run_id IS NULL OR b.run_id IS NULL AS one_run_only,
       list_filter([
           CASE WHEN coalesce(a.requests, -1) <> coalesce(b.requests, -1) THEN 'requests' END,
           CASE WHEN coalesce(a.gets, -1) <> coalesce(b.gets, -1) THEN 'gets' END,
           CASE WHEN coalesce(a.get_range, -1) <> coalesce(b.get_range, -1) THEN 'get_range' END,
           CASE WHEN coalesce(a.get_stream, -1) <> coalesce(b.get_stream, -1) THEN 'get_stream' END,
           CASE WHEN coalesce(a.heads, -1) <> coalesce(b.heads, -1) THEN 'heads' END,
           CASE WHEN coalesce(a.puts, -1) <> coalesce(b.puts, -1) THEN 'puts' END,
           CASE WHEN coalesce(a.appends, -1) <> coalesce(b.appends, -1) THEN 'appends' END,
           CASE WHEN coalesce(a.deletes, -1) <> coalesce(b.deletes, -1) THEN 'deletes' END,
           CASE WHEN coalesce(a.aborts, -1) <> coalesce(b.aborts, -1) THEN 'aborts' END,
           CASE WHEN coalesce(a.bytes_read, -1) <> coalesce(b.bytes_read, -1) THEN 'bytes_read' END,
           CASE WHEN coalesce(a.bytes_skipped_through, -1) <> coalesce(b.bytes_skipped_through, -1)
                THEN 'bytes_skipped_through' END,
           CASE WHEN coalesce(a.bytes_written, -1) <> coalesce(b.bytes_written, -1)
                THEN 'bytes_written' END,
           CASE WHEN coalesce(a.rows, -1) <> coalesce(b.rows, -1) THEN 'rows' END,
           CASE WHEN coalesce(a.objects, -1) <> coalesce(b.objects, -1) THEN 'objects' END,
           CASE WHEN coalesce(a.stored_bytes, -1) <> coalesce(b.stored_bytes, -1)
                THEN 'stored_bytes' END,
           CASE WHEN coalesce(a.live_bytes, -1) <> coalesce(b.live_bytes, -1) THEN 'live_bytes' END,
           CASE WHEN coalesce(a.row_groups, -1) <> coalesce(b.row_groups, -1) THEN 'row_groups' END,
           CASE WHEN coalesce(a.data_files, -1) <> coalesce(b.data_files, -1) THEN 'data_files' END,
           CASE WHEN coalesce(a.delete_files, -1) <> coalesce(b.delete_files, -1)
                THEN 'delete_files' END
       ], x -> x IS NOT NULL) AS differs
FROM a FULL JOIN b USING (arm, step, phase, n)
WHERE coalesce(a.arm, b.arm) IN (FROM arms);

-- Repetitions: runs of one label and configuration hash (a rerun of the same config file).
CREATE VIEW repetitions AS
SELECT label, config_hash, list(run_id ORDER BY started_at) AS run_ids, count(*) AS runs
FROM runs WHERE source = 'bench' AND status = 'finished'
GROUP BY ALL HAVING count(*) > 1;
