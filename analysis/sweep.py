#!/usr/bin/env python3
"""Compares runs of one workload that differ in the row-group size (a sweep).

For each run (its layout's rowGroupBytes) and arm: bytes and requests per batch, the GETs of the
last probe's reads under the primary reader model, and the footprint at the last step. Writes
sweep.png (small multiples, one metric each, against the row-group size) and sweep.md (the same
numbers as a table) to the output directory.

rowGroupBytes is Parquet's target, which counts buffered bytes (mostly uncompressed page
buffers), while the tail seal (tailSealBytes) counts a region's compressed bytes, so the two
yield row groups of different sizes. For runs with a version dump (--dump-versions), the table
also gives each RMoW arm's compressed row-group sizes at the end: the median over its live row
groups, and over those written after the initial load.

Usage: analysis/sweep.py <out dir> <run dir>...
"""

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
import plots  # noqa: E402  (palette, arm encoding, direct labels)

ARMS = ["mor-v3", "tailcow-c", "rmow-a", "rmow-b", "rmow-c"]
READER = "one-footer"


def row_group_mb(run, arm):
    """Median compressed size (MB) of the arm's live row groups at the end, overall and of those
    in objects written after the initial load; None without a version dump."""
    objects = sorted((Path(run) / "versions" / arm).rglob("*.parquet"))
    if not objects:
        return None, None
    # the current objects: the dump holds compacted-away ones too, which only time travel reads
    sizes, later = [], []
    for f in objects:
        md = pq.ParquetFile(f).metadata
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            mb = sum(rg.column(j).total_compressed_size for j in range(rg.num_columns)) / 1e6
            sizes.append(mb)
            if not f.name.startswith("initial-"):
                later.append(mb)
    return (float(np.median(sizes)) if sizes else None,
            float(np.median(later)) if later else None)


def summarize(run):
    run = Path(run)
    manifest = json.loads((run / "manifest.json").read_text())
    rg_mib = manifest["config"]["layout"]["rowGroupBytes"] / (1 << 20)
    df = pd.read_json(run / "steps.jsonl", lines=True)
    rows = []
    for arm in [a for a in ARMS if a in set(df.arm)]:
        d = df[df.arm == arm]
        batch = d[(d.phase == "batch") & (d.step > 0)]
        probes = d[d.phase.str.startswith("read") & d["reader"].eq(READER)]
        last = probes[probes.step == probes.step.max()].set_index("phase")
        state = d[d.phase == "state"]
        end = state[state.step == state.step.max()].iloc[0]
        rg_all, rg_new = row_group_mb(run, arm)
        rows.append({
            "rg_mib": rg_mib,
            "arm": arm,
            "write_mb_per_batch": batch.bytes_written.mean() / 1e6,
            "requests_per_batch": batch.requests.mean(),
            "read_full_gets": last.loc["read_full", "gets"],
            "read_recent_gets": last.loc["read_recent", "gets"],
            "read_old_gets": last.loc["read_old", "gets"] if "read_old" in last.index else None,
            "row_groups": end.row_groups,
            "footer_kb": end.footer_bytes / 1e3 if end.footer_bytes >= 0 else None,
            "stored_over_live": end.stored_bytes / end.live_bytes,
            "rg_mb_median": rg_all,
            "new_rg_mb_median": rg_new,
        })
    return rows


PANELS = [
    ("write_mb_per_batch", "Bytes written per batch", "MB"),
    ("requests_per_batch", "Requests per batch", "requests"),
    ("read_full_gets", "Full read, last probe", "GETs"),
    ("read_recent_gets", "Newest 1% of event time, last probe", "GETs"),
    ("stored_over_live", "Stored / live bytes, last step", "ratio"),
    ("footer_kb", "Footer bytes, last step (RMoW)", "KB"),
]


def main(argv):
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    t = pd.DataFrame([r for run in argv[2:] for r in summarize(run)]).sort_values(["arm", "rg_mib"])

    fig, axes = plt.subplots(2, 3, figsize=(12, 6.6), constrained_layout=True)
    sizes = sorted(t.rg_mib.unique())
    for ax, (metric, title, unit) in zip(axes.flat, PANELS):
        ends = []
        for arm in ARMS:
            d = t[(t.arm == arm) & t[metric].notna()]
            if d.empty:
                continue
            fam, marker, label = plots.ARMS[arm]
            ax.plot(d.rg_mib, d[metric], color=plots.FAMILY[fam], lw=2, marker=marker, ms=6,
                    markeredgecolor=plots.SURFACE, markeredgewidth=1, label=label)
            ends.append((d.rg_mib.iloc[-1], d[metric].iloc[-1], label))
        ax.set_xscale("log", base=2)
        ax.set_xticks(sizes, [f"{s:g}" for s in sizes])
        ax.set_xlabel("row-group size and tail seal size (MiB)")
        ax.set_ylabel(unit)
        ax.set_title(title, loc="left")
        ax.margins(x=0.25)
        plots.label_ends(ax, ends)
    axes.flat[0].legend(loc="upper left", fontsize=7)
    fig.suptitle("Row-group size sweep (" + READER + " reader)", x=0.01, ha="left",
                 color=plots.INK, fontsize=11)
    fig.savefig(out / "sweep.png", dpi=150)

    cols = (["arm", "rg_mib"] + [m for m, _, _ in PANELS]
            + ["read_old_gets", "row_groups", "rg_mb_median", "new_rg_mb_median"])
    md = ["# Row-group size sweep", "",
          "Runs: " + ", ".join(f"`{Path(r).parent.name}/{Path(r).name}`" for r in argv[2:]), "",
          "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, r in t[cols].iterrows():
        md.append("| " + " | ".join(
            "" if pd.isna(v) else (f"{v:.3g}" if isinstance(v, float) else str(v)) for v in r)
            + " |")
    (out / "sweep.md").write_text("\n".join(md) + "\n")
    print(out / "sweep.png")
    print(out / "sweep.md")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
