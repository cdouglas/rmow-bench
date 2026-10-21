#!/usr/bin/env python3
"""Compares arms across runs, one table per run: what H5 is decided by (docs/HYPOTHESES.md).

Per arm, at the last step: bytes and requests per batch (and compaction's, per batch), the last
probe's full read under the primary reader, stored and kept bytes (kept: what remains once every
older version expires) against live bytes, and objects stored and in use. Writes layouts.md to
the output directory and prints it.

Usage: analysis/layouts.py <out dir> <run dir>...
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import sweep  # noqa: E402  (per-run summary)

COLS = ["write_mb_per_batch", "compact_mb_per_batch", "requests_per_batch", "read_full_gets",
        "read_full_mb", "read_old_gets", "stored_over_live", "kept_over_live", "objects",
        "live_objects", "footer_kb"]


def main(argv):
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    sweep.ARMS = ["mor-v3", "cow", "tailcow-c", "rmow-b", "rmow-b-split", "rmow-c",
                  "rmow-c-split"]
    md = ["# Appendable and split RMoW files", ""]
    for run in argv[2:]:
        t = pd.DataFrame(sweep.summarize(run)).set_index("arm")[COLS]
        md += [f"## `{Path(run).parent.name}/{Path(run).name}`", "",
               "| arm | " + " | ".join(COLS) + " |", "|---|" + "---|" * len(COLS)]
        for arm, r in t.iterrows():
            md.append(f"| {arm} | " + " | ".join(
                "" if pd.isna(v) else f"{v:.3g}" for v in r) + " |")
        md.append("")
    text = "\n".join(md)
    (out / "layouts.md").write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
