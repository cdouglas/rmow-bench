#!/usr/bin/env python3
"""Reads each version of an appendable Parquet file with pyarrow and DuckDB.

Each version is a prefix copy of the object (the bytes up to that version's footer), listed with
its expected row count and table hash in <dir>/expected.csv (written by AppendableParquetTest and
by the runner). The table hash is the wrapping 64-bit sum of Rows.rowHash(id, version), the same
digest the Java oracle uses, so a match means both readers returned exactly the rows Java wrote.

Usage: analysis/interop_check.py <dir>
"""

import csv
import sys
from pathlib import Path

import duckdb
import numpy as np
import pyarrow.parquet as pq

M64 = np.uint64(0xFFFFFFFFFFFFFFFF)


def mix64(z):
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def table_hash(ids, versions):
    """Rows.rowHash: mix64(id * golden ^ (version + c)), summed with 64-bit wraparound."""
    with np.errstate(over="ignore"):
        ids = np.asarray(ids, dtype=np.int64).view(np.uint64)
        v = np.asarray(versions, dtype=np.int64).view(np.uint64)
        h = mix64((ids * np.uint64(0x9E3779B97F4A7C15)) ^ (v + np.uint64(0x632BE59BD9B4E019)))
        return len(ids), int(np.sum(h, dtype=np.uint64))


def check_pyarrow(path):
    meta = pq.ParquetFile(path).metadata
    t = pq.read_table(path, columns=["id", "version"])
    rows, digest = table_hash(t.column("id").to_numpy(), t.column("version").to_numpy())
    assert rows == meta.num_rows, f"pyarrow read {rows} rows, footer says {meta.num_rows}"
    return rows, digest, meta.num_row_groups


def check_duckdb(path):
    con = duckdb.connect()
    ids, versions = con.execute(
        f"SELECT id, version FROM read_parquet('{path}')"
    ).fetchnumpy().values()
    groups = con.execute(
        f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{path}')"
    ).fetchone()[0]
    rows, digest = table_hash(ids, versions)
    return rows, digest, groups


def main(argv):
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    d = Path(argv[1])
    failures = 0
    with open(d / "expected.csv") as f:
        for row in csv.DictReader(f):
            path = d / row["file"]
            want = (int(row["rows"]), int(row["sum"]))
            for name, check in (("pyarrow", check_pyarrow), ("duckdb", check_duckdb)):
                try:
                    rows, digest, groups = check(str(path))
                    ok = (rows, digest) == want
                    detail = f"rows={rows} row_groups={groups}"
                except Exception as e:  # report every reader's failure, not just the first
                    ok, detail = False, f"error: {e}"
                failures += 0 if ok else 1
                print(f"{'OK  ' if ok else 'FAIL'} {name:8s} {row['file']}: {detail}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
