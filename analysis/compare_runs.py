#!/usr/bin/env python3
"""Checks that two runs of the same op log issued the same requests.

Requests and bytes are a deterministic function of the op log, the configuration and the code,
so repetitions must match exactly; wall and CPU time may not. Prints, per arm, the records that
differ in any deterministic column, and exits non-zero if any do.

Usage: analysis/compare_runs.py <run dir A> <run dir B> [arm ...]
"""

import sys
from pathlib import Path

import pandas as pd

KEYS = ["arm", "step", "phase", "reader"]
EXACT = ["requests", "gets", "get_range", "get_stream", "heads", "puts", "appends", "append_splits",
         "deletes", "aborts", "bytes_read", "bytes_skipped_through", "bytes_written", "rows",
         "objects", "stored_bytes", "live_bytes", "row_groups", "data_files", "delete_files"]


def load(run):
    df = pd.read_json(Path(run) / "steps.jsonl", lines=True)
    # probes run every read phase once per reader model; runs before reader models have none
    df["reader"] = df["reader"].fillna("") if "reader" in df else ""
    # a key could occur more than once (none does today); number them in file order
    df["n"] = df.groupby(KEYS).cumcount()
    return df.set_index(KEYS + ["n"])


def main(argv):
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    a, b = load(argv[1]), load(argv[2])
    arms = argv[3:] or sorted(set(a.index.get_level_values("arm")) & set(b.index.get_level_values("arm")))
    bad = 0
    for arm in arms:
        x = a.xs(arm, level="arm")
        y = b.xs(arm, level="arm")
        common = x.index.intersection(y.index)
        cols = [c for c in EXACT if c in x.columns and c in y.columns]
        xs, ys = x.loc[common, cols].fillna(-1), y.loc[common, cols].fillna(-1)
        diff = (xs != ys).any(axis=1)
        missing = len(x.index.symmetric_difference(y.index))
        status = "same" if not diff.any() and missing == 0 else "DIFFERENT"
        print(f"{arm:18s} {status}: {len(common)} records compared, {int(diff.sum())} differ, "
              f"{missing} in one run only")
        if diff.any() or missing:
            bad += 1
        if diff.any():
            first = diff[diff].index[0]
            print("   first difference at (step, phase, reader, n) =", first)
            print(pd.DataFrame({"A": xs.loc[first], "B": ys.loc[first]})[lambda d: d.A != d.B])
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
