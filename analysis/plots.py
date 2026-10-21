#!/usr/bin/env python3
"""Plots one run's steps.jsonl and writes a table twin of every figure.

Figures (PNG, light surface) and summary.md go to <run dir>/plots/:
  write.png, space.png              the headline arms that ran (MoR-v3, TailCoW-C, RMoW-B and
                                    RMoW-C, appendable and split); space shows stored bytes and
                                    the bytes kept once older versions expire
  reads-<reader>.png                the same arms' reads, per reader model (one-footer, the
                                    primary one, and stock)
  detail-write.png, detail-reads-<reader>.png  every arm
  summary.md the same numbers as tables, every arm (the accessible twin of the figures)

Encoding: hue is the arm's family (MoR, Iceberg copy-on-write, RMoW, the RMoW-dv ablation, and
RMoW on split files, those that ran), marker shape the variant within it; every line is direct-labeled at its end and
listed in the legend. Local and in-memory runs: requests and bytes are exact and reproducible;
wall time is not reported.

Usage: analysis/plots.py <run dir>
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
import pandas as pd  # noqa: E402

# Reference palette: the first five categorical slots, in order (validated adjacent pairs,
# light surface; aqua, yellow and magenta are under 3:1, so lines are direct-labeled and tabled)
FAMILY = {"mor": "#2a78d6", "cow": "#eb6834", "rmow": "#1baf7a", "rmowdv": "#eda100",
          "rmowsplit": "#e87ba4"}
ARMS = {  # arm -> (family, marker, label)
    "mor-v2-file": ("mor", "o", "MoR v2 file"),
    "mor-v2-partition": ("mor", "s", "MoR v2 partition"),
    "mor-v3": ("mor", "^", "MoR v3 DV"),
    "cow": ("cow", "o", "CoW"),
    "tailcow-b": ("cow", "s", "TailCoW-B"),
    "tailcow-c": ("cow", "^", "TailCoW-C"),
    "rmow-a": ("rmow", "o", "RMoW-A"),
    "rmow-b": ("rmow", "s", "RMoW-B"),
    "rmow-c": ("rmow", "^", "RMoW-C"),
    "rmow-a-dv": ("rmowdv", "o", "RMoW-A-dv"),
    "rmow-b-dv": ("rmowdv", "s", "RMoW-B-dv"),
    "rmow-c-dv": ("rmowdv", "^", "RMoW-C-dv"),
    "rmow-a-split": ("rmowsplit", "o", "RMoW-A-split"),
    "rmow-b-split": ("rmowsplit", "s", "RMoW-B-split"),
    "rmow-c-split": ("rmowsplit", "^", "RMoW-C-split"),
}
HEADLINE = ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
SURFACE, GRID, AXIS = "#fcfcfb", "#e1e0d9", "#c3c2b7"

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.size": 9,
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "axes.edgecolor": AXIS,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "axes.titlesize": 10,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "grid.linestyle": "-",
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "legend.frameon": False,
        "legend.labelcolor": INK2,
    }
)


def arms_in(df, only=None):
    return [a for a in ARMS if a in set(df.arm) and (only is None or a in only)]


def line_panel(ax, df, metric, title, ylabel, log=False, only=None):
    """One metric against step, one line per arm, labeled at its end (labels kept apart)."""
    ends = []
    for arm in arms_in(df, only):
        fam, marker, label = ARMS[arm]
        d = df[df.arm == arm].sort_values("step")
        if d.empty:
            continue
        every = max(1, len(d) // 12)
        ax.plot(d.step, d[metric], color=FAMILY[fam], lw=2, marker=marker, ms=5,
                markevery=every, markeredgecolor=SURFACE, markeredgewidth=1, label=label)
        ends.append((d.step.iloc[-1], d[metric].iloc[-1], label))
    ax.set_title(title, loc="left")
    ax.set_xlabel("batch")
    ax.set_ylabel(ylabel)
    if log:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(subs=(1, 2, 5)))
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.margins(x=0.12)
    label_ends(ax, ends)


def label_ends(ax, ends, gap_pt=9):
    """Direct labels at line ends, nudged vertically so that no two overlap."""
    if not ends:
        return
    ax.figure.canvas.draw()  # fix the axis limits and transforms
    to_px = ax.transData.transform
    pts = sorted(((to_px((x, y))[1], x, y, label) for x, y, label in ends), key=lambda e: e[0])
    gap = gap_pt * ax.figure.dpi / 72
    placed = []
    for py, x, y, label in pts:
        if placed and py - placed[-1][0] < gap:
            py = placed[-1][0] + gap
        placed.append((py, x, y, label))
    for py, x, y, label in placed:
        dy = (py - to_px((x, y))[1]) * 72 / ax.figure.dpi
        ax.annotate(label, (x, y), xytext=(4, dy), textcoords="offset points", va="center",
                    fontsize=7, color=INK2)


def markdown(table):
    def cell(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return f"{v:.2f}" if isinstance(v, float) else str(v)

    head = "| " + " | ".join(table.columns) + " |\n"
    rule = "|" + "|".join("---" for _ in table.columns) + "|\n"
    body = "".join("| " + " | ".join(cell(v) for v in row) + " |\n" for row in table.itertuples(index=False))
    return head + rule + body


def save(fig, path):
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main(argv):
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    run = Path(argv[1])
    out = run / "plots"
    out.mkdir(exist_ok=True)
    df = pd.read_json(run / "steps.jsonl", lines=True)
    df["mb_written"] = df.bytes_written / 1e6
    df["mb_read"] = df.bytes_read / 1e6
    if "reader" not in df.columns:
        df["reader"] = "stock"  # runs before reader models had one, stock Iceberg reader
    df["reader"] = df.reader.fillna("")
    readers = [r for r in df.reader.unique() if r]
    label = df.label.iloc[0]
    arms = arms_in(df)

    batch = df[df.phase == "batch"]
    full = df[df.phase == "read_full"]
    recent = df[df.phase == "read_recent"]
    state = df[df.phase == "state"].copy()
    state["space_amp"] = state.stored_bytes / state.live_bytes
    # what remains once every older version expires: reclaimable bytes are deletable then
    state["kept_amp"] = (state.stored_bytes - state.reclaimable_bytes) / state.live_bytes

    for prefix, only in (("", HEADLINE), ("detail-", None)):
        fig, axes = plt.subplots(1, 2, figsize=(11, 4))
        line_panel(axes[0], batch, "mb_written", "Bytes written per batch", "MB", True, only)
        line_panel(axes[1], batch, "requests", "Requests per batch", "requests", True, only)
        axes[1].legend(loc="upper left", bbox_to_anchor=(1.18, 1), fontsize=8)
        fig.suptitle(f"{label}: write path", x=0.01, ha="left", color=INK, fontsize=11)
        save(fig, out / f"{prefix}write.png")

        for reader in readers:
            f = full[full.reader == reader]
            r = recent[recent.reader == reader]
            fig, axes = plt.subplots(1, 3, figsize=(15, 4))
            line_panel(axes[0], f, "gets", "read_full: GET requests", "GETs", True, only)
            line_panel(axes[1], f, "mb_read", "read_full: bytes read", "MB", False, only)
            line_panel(axes[2], r, "gets", "read_recent: GET requests", "GETs", False, only)
            axes[2].legend(loc="upper left", bbox_to_anchor=(1.18, 1), fontsize=8)
            fig.suptitle(
                f"{label}: reads of the current table, {reader} reader",
                x=0.01, ha="left", color=INK, fontsize=11)
            save(fig, out / f"{prefix}reads-{reader}.png")

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    line_panel(axes[0], state, "space_amp", "Stored bytes / live bytes (every version kept)",
               "ratio", False, HEADLINE)
    line_panel(axes[1], state, "kept_amp", "Kept bytes / live bytes (older versions expired)",
               "ratio", False, HEADLINE)
    axes[1].legend(loc="upper left", bbox_to_anchor=(1.2, 1), fontsize=8)
    save(fig, out / "space.png")

    # the table twin
    rows = []
    last = state.step.max()
    probes = sorted(full.step.unique())
    first_probe, last_probe = probes[1] if len(probes) > 1 else probes[0], probes[-1]
    batches = batch.step.nunique()
    for arm in arms:
        b = batch[batch.arm == arm]
        loc = df[(df.phase == "locate") & (df.arm == arm)]
        comp = df[(df.phase == "compact") & (df.arm == arm)]
        s = state[(state.arm == arm) & (state.step == last)].iloc[0]
        row = {
            "arm": ARMS[arm][2],
            "write MB/batch": b.bytes_written.mean() / 1e6,
            "+compaction MB/batch": comp.bytes_written.sum() / 1e6 / batches,
            "read MB/batch (locate+merge)": b.bytes_read.mean() / 1e6,
            "locate GETs/batch": loc.gets.sum() / batches,
            "requests/batch": b.requests.mean(),
        }
        for reader in readers:
            f = full[(full.arm == arm) & (full.reader == reader)].set_index("step")
            r = recent[(recent.arm == arm) & (recent.reader == reader)].set_index("step")
            old = df[(df.phase == "read_old") & (df.arm == arm) & (df.reader == reader)]
            row[f"{reader}: full GETs @{first_probe}"] = f.gets.get(first_probe)
            row[f"{reader}: full GETs @{last_probe}"] = f.gets.get(last_probe)
            row[f"{reader}: recent GETs @{last_probe}"] = r.gets.get(last_probe)
            row[f"{reader}: old GETs (last)"] = old.gets.iloc[-1] if not old.empty else None
        first = full[(full.arm == arm) & (full.step == last_probe) & (full.reader == readers[0])]
        row[f"{readers[0]}: full MB @{last_probe}"] = first.bytes_read.iloc[0] / 1e6
        row[f"objects @{last}"] = s.objects
        row[f"stored/live @{last}"] = s.stored_bytes / s.live_bytes
        row[f"kept/live @{last}"] = (s.stored_bytes - s.reclaimable_bytes) / s.live_bytes
        if "live_objects" in s and not pd.isna(s.live_objects):
            row[f"live objects @{last}"] = s.live_objects
        rows.append(row)
    table = pd.DataFrame(rows)
    with open(out / "summary.md", "w") as fh:
        fh.write(
            f"# {label}\n\nRun `{run}`. Per-batch values are means over all {batches} batches"
            " (the batch phase includes locate and merge reads; compaction is amortized"
            " separately); reads at the probes shown.\n\n"
        )
        fh.write(markdown(table))
    print(table.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
