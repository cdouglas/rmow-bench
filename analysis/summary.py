#!/usr/bin/env python3
"""Prints a per-arm summary of one run's steps.jsonl.

Write phases are averaged over all batches; reads are shown at the first and last probe, so the
growth as updates accumulate is visible; state is shown at the last step. Local and in-memory
runs: compare requests, bytes and CPU, not wall time (see the I/O parity rules).

Usage: analysis/summary.py <run dir>
"""

import sys
from pathlib import Path

import pandas as pd

IO = ["requests", "gets", "puts", "appends", "heads", "bytes_read", "bytes_written"]


def main(argv):
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    df = pd.read_json(Path(argv[1]) / "steps.jsonl", lines=True)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 30)

    writes = df[df.phase.isin(["batch", "merge-read", "write", "delete-write"])].copy()
    writes["cpu_ms"] = writes.cpu_ns / 1e6
    w = writes.groupby(["arm", "phase"])[IO + ["cpu_ms"]].mean().round(1)
    print("Per batch (mean over batches):")
    print(w.to_string())

    reads = df[df.phase.str.startswith("read_")].copy()
    reads["cpu_ms"] = reads.cpu_ns / 1e6
    first, last = reads.step[reads.step > 0].min(), reads.step.max()
    r = reads[reads.step.isin([first, last])]
    print(f"\nReads at steps {first} and {last}:")
    print(
        r.set_index(["phase", "arm", "step"])[["rows"] + IO[:2] + ["bytes_read", "cpu_ms"]]
        .sort_index()
        .round(1)
        .to_string()
    )

    state = df[(df.phase == "state") & (df.step == df.step.max())]
    cols = ["objects", "stored_bytes", "live_bytes", "reclaimable_bytes", "row_groups",
            "data_files", "delete_files", "footer_bytes"]
    print(f"\nState after step {df.step.max()}:")
    print(state.set_index("arm")[cols].to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
