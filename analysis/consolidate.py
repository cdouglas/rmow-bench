#!/usr/bin/env python3
"""Gathers every finished run under a results directory into one Parquet file.

Each row of <results>/consolidated.parquet is one steps.jsonl record plus the run's identity:
experiment (label and config hash), run start time, op-log SHA-256, the rmow-bench commit, and
the config fields that vary across experiments (flattened, e.g. layout.compactEvery). Runs whose
manifest has no "finished" time (interrupted) and directories named superseded-* are skipped.

Usage: analysis/consolidate.py [results dir]
"""

import json
import sys
from pathlib import Path

import pandas as pd


def flatten(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        elif isinstance(v, list):
            out[key] = ",".join(map(str, v))
        else:
            out[key] = v
    return out


def main(argv):
    root = Path(argv[1] if len(argv) > 1 else "results")
    frames = []
    for manifest in sorted(root.glob("*/*/manifest.json")):
        run = manifest.parent
        if run.parent.name.startswith("superseded-"):
            continue
        m = json.loads(manifest.read_text())
        if "finished" not in m:
            print(f"skip (unfinished) {run}", file=sys.stderr)
            continue
        df = pd.read_json(run / "steps.jsonl", lines=True)
        df["experiment"] = f"{m['label']}-{m['config_hash']}"
        df["started"] = m["started"]
        df["oplog_sha256"] = m["oplog_sha256"]
        df["git"] = m["versions"].get("rmow_bench_git", "")
        for k, v in flatten(m["config"]).items():
            if k not in ("arms", "label"):
                df[f"cfg.{k}"] = v
        frames.append(df)
        print(f"{run}: {len(df)} records")
    if not frames:
        print("no finished runs", file=sys.stderr)
        return 1
    out = pd.concat(frames, ignore_index=True)
    out.to_parquet(root / "consolidated.parquet", index=False)
    print(f"wrote {root / 'consolidated.parquet'}: {len(out)} records from {len(frames)} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
