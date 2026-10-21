#!/usr/bin/env python3
"""Reads every version of the appendable Parquet objects in a dump with pyarrow and DuckDB.

A dump is a directory holding the objects and versions.csv, one row per version: the object, the
version's length (its prefix ends at that version's footer), and what Iceberg's reader decoded
from that prefix (VersionDigest): the row count, wrapping sums over (id, version), (id, ts,
version) and (id, _pos), and the row counts of the row groups its footer lists. The runner writes
one per run with --dump-versions (<run dir>/versions); AppendableParquetTest writes
target/interop.

Each reader, on each version, must
  - list the same row groups, with the same row counts, in the same order;
  - decode the same rows: the digests match, and every payload column is the one Rows.record
    derives from (id, ts, version);
  - number the rows as Iceberg does, in footer order: the (id, position) digest matches;
  - skip row groups by their statistics without losing rows: an id-range filter returns exactly
    the rows in that range.

Usage: analysis/interop_check.py <dump dir> [-j N] [--every K] [--tmp DIR] [-v]
  -j N       check N versions at a time (default: the CPU count, at most 8)
  --every K  check every K-th version of each object, and its last (default: every version)
  --tmp DIR  where prefix copies go (default: /dev/shm when writable)
"""

import argparse
import csv
import os
import sys
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

U = np.uint64
GOLDEN = U(0x9E3779B97F4A7C15)
VMUL = U(0xD1B54A32D192ED03)
S1 = np.array([f"category-{i:04d}" for i in range(64)], dtype=object)
S2 = np.array([f"region-{i:04d}" for i in range(1024)], dtype=object)


def mix64(z):
    """Rows.mix64, the splitmix64 finalizer."""
    z = (z ^ (z >> U(30))) * U(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> U(27))) * U(0x94D049BB133111EB)
    return z ^ (z >> U(31))


def wsum(h):
    """A wrapping 64-bit sum, as Java's long arithmetic computes it."""
    return int(np.sum(h, dtype=np.uint64))


def digests(ids, ts, versions, pos):
    """VersionDigest's sums: over (id, version), (id, ts, version) and (id, position)."""
    ids = np.asarray(ids, dtype=np.int64).view(U)
    ts = np.asarray(ts, dtype=np.int64).view(U)
    v = np.asarray(versions, dtype=np.int64).view(U)
    pos = np.asarray(pos, dtype=np.int64).view(U)
    row = mix64((ids * GOLDEN) ^ (v + U(0x632BE59BD9B4E019)))
    content = mix64(row + ts * U(0xC2B2AE3D27D4EB4F))
    at = mix64((ids * GOLDEN) ^ (pos * VMUL + U(1)))
    return wsum(row), wsum(content), wsum(at)


def payload_errors(t):
    """Columns that differ from Rows.record(id, ts, version); empty if every row matches."""
    ids = t.column("id").to_numpy()
    v = t.column("version").to_numpy().astype(np.int64)
    uid, uv = ids.view(U), v.view(U)

    def h(field):
        return mix64(uid * GOLDEN + uv * VMUL + U(field)).view(np.int64)

    want = {
        "l1": np.mod(h(3), 1000),
        "l2": ids * 7 + (h(4).view(U) & U(0xFF)).astype(np.int64),
        "l3": h(5),
        "l4": v * 1_000_000 + np.mod(h(6), 1_000_000),
        "d1": np.mod(h(7), 100_000) / 100.0,
        "d2": (h(8).view(U) >> U(11)).astype(np.float64) * 2.0**-53,
        "s1": S1[np.mod(h(9), len(S1))],
        "s2": S2[np.mod(h(10), len(S2))],
    }
    bad = []
    for name, expected in want.items():
        col = t.column(name)
        if pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
            got = col.to_numpy()
        else:
            got = np.asarray(col.cast(pa.string()).to_numpy(zero_copy_only=False), dtype=object)
        if len(got) != len(expected) or not np.array_equal(got, expected):
            bad.append(name)
    return bad


def check_table(t, pos, want):
    """Problems with one reader's decoded rows, given the positions it numbered them with."""
    if t.num_rows != want["rows"]:
        return [f"{t.num_rows} rows, Iceberg read {want['rows']}"]
    problems = []
    s, c, p = digests(t.column("id").to_numpy(), t.column("ts").to_numpy(),
                      t.column("version").to_numpy(), pos)
    if s != want["sum"]:
        problems.append("(id, version) digest differs")
    if c != want["content_sum"]:
        problems.append("(id, ts, version) digest differs")
    if p != want["pos_sum"]:
        problems.append("(id, position) digest differs: rows numbered out of footer order")
    bad = payload_errors(t)
    if bad:
        problems.append("payload differs in " + ", ".join(bad))
    return problems


def id_range(t):
    """A filter range around the median id, and the (count, sum) of the ids in it."""
    ids = t.column("id").to_numpy()
    if len(ids) == 0:
        return 0, -1, (0, 0)
    lo, hi = (int(x) for x in np.quantile(ids, [0.45, 0.55], method="nearest"))
    inside = ids[(ids >= lo) & (ids <= hi)]
    return lo, hi, (len(inside), int(inside.sum()))


def check_pyarrow(path, want, rg_rows):
    pf = pq.ParquetFile(path)
    md = pf.metadata
    got_rgs = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
    if got_rgs != rg_rows:
        return [f"row groups {got_rgs}, footer lists {rg_rows}"]
    t = pf.read()  # row groups in footer order: row i is at position i
    problems = check_table(t, np.arange(t.num_rows), want)
    lo, hi, expected = id_range(t)
    f = pq.read_table(path, columns=["id"], filters=[("id", ">=", lo), ("id", "<=", hi)])
    ids = f.column("id").to_numpy()
    if (len(ids), int(ids.sum())) != expected:
        problems.append(f"id filter [{lo}, {hi}] read {len(ids)} rows, want {expected[0]}")
    return problems


def check_duckdb(path, want, rg_rows):
    con = duckdb.connect(config={"threads": 1})
    p = path.replace("'", "''")
    got_rgs = [n for _, n in con.execute(
        f"SELECT row_group_id, any_value(row_group_num_rows) FROM parquet_metadata('{p}') "
        "GROUP BY ALL ORDER BY row_group_id").fetchall()]
    if got_rgs != rg_rows:
        return [f"row groups {got_rgs}, footer lists {rg_rows}"]
    t = con.execute(
        f"SELECT * FROM read_parquet('{p}', file_row_number = true)").to_arrow_table()
    problems = check_table(t, t.column("file_row_number").to_numpy(), want)
    lo, hi, expected = id_range(t)
    n, total = con.execute(
        f"SELECT count(*), coalesce(sum(id), 0) FROM read_parquet('{p}') "
        f"WHERE id BETWEEN {lo} AND {hi}").fetchone()
    if (n, int(total)) != expected:
        problems.append(f"id filter [{lo}, {hi}] read {n} rows, want {expected[0]}")
    return problems


def prefix_copy(src, length, tmp):
    """The version's prefix as a file of its own; the object itself for its last version."""
    if os.path.getsize(src) == length:
        return src, False
    fd, path = tempfile.mkstemp(suffix=".parquet", dir=tmp)
    with open(src, "rb") as s, os.fdopen(fd, "wb") as d:
        left = length
        while left > 0:
            chunk = s.read(min(left, 16 << 20))
            if not chunk:
                raise IOError(f"{src} is shorter than {length}")
            d.write(chunk)
            left -= len(chunk)
    return path, True


def check_version(job):
    dump, row, tmp = job
    want = {k: int(row[k]) for k in ("rows", "sum", "content_sum", "pos_sum")}
    rg_rows = [int(x) for x in row["rg_rows"].split(";") if x]
    path, temporary = prefix_copy(str(Path(dump) / row["object"]), int(row["length"]), tmp)
    results = {}
    try:
        for name, check in (("pyarrow", check_pyarrow), ("duckdb", check_duckdb)):
            try:
                results[name] = check(path, want, rg_rows)
            except Exception as e:  # report every reader's failure, not just the first
                results[name] = [f"error: {type(e).__name__}: {e}"]
    finally:
        if temporary:
            os.unlink(path)
    return row, results


def init_worker():
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("dump")
    ap.add_argument("-j", type=int, default=min(os.cpu_count() or 1, 8))
    ap.add_argument("--every", type=int, default=1)
    ap.add_argument("--tmp", default="/dev/shm" if os.access("/dev/shm", os.W_OK) else None)
    ap.add_argument("-v", action="store_true", help="print every version, not only failures")
    a = ap.parse_args(argv[1:])

    with open(Path(a.dump) / "versions.csv") as f:
        rows = list(csv.DictReader(f))
    by_object = defaultdict(list)
    for r in rows:
        by_object[r["object"]].append(r)
    todo = []
    for versions in by_object.values():
        for i, r in enumerate(versions):
            if i % a.every == 0 or i == len(versions) - 1:
                todo.append((a.dump, r, a.tmp))

    failures = 0
    per_object = defaultdict(lambda: {"versions": 0, "unordered": 0, "failed": 0, "rgs": []})
    with ProcessPoolExecutor(max_workers=a.j, initializer=init_worker) as pool:
        for row, results in pool.map(check_version, todo):
            o = per_object[row["object"]]
            o["versions"] += 1
            o["unordered"] += row["offset_order"] != "true"
            o["rgs"].append(int(row["row_groups"]))
            bad = {k: v for k, v in results.items() if v}
            if bad:
                failures += 1
                o["failed"] += 1
            if bad or a.v:
                status = "FAIL" if bad else "OK  "
                print(f"{status} {row['object']} v{row['version']} (length {row['length']}, "
                      f"{row['row_groups']} row groups, offset order {row['offset_order']})")
                for reader, problems in bad.items():
                    for p in problems:
                        print(f"       {reader}: {p}")

    unordered = 0
    for name, o in sorted(per_object.items()):
        unordered += o["unordered"]
        status = "FAIL" if o["failed"] else "OK  "
        print(f"{status} {name}: {o['versions']} versions, {min(o['rgs'])}-{max(o['rgs'])} row "
              f"groups, {o['unordered']} out of offset order"
              + (f", {o['failed']} failed" if o["failed"] else ""))
    print(f"{len(todo)} versions of {len(per_object)} objects read by pyarrow {pa.__version__} and "
          f"DuckDB {duckdb.__version__}; {unordered} list row groups out of offset order; "
          f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
