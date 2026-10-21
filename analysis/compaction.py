#!/usr/bin/env python3
"""Decides when to compact, per arm, from runs of one workload that differ only in the compaction
policy (configs/local-compact-*.json; H6 in docs/HYPOTHESES.md).

A policy is the check period P (layout.compactEvery; 0: never) and the thresholds. For each run
and arm, per batch over the whole run: bytes, PUT-class and GET-class requests, bytes read and
CPU of the batch and compaction phases; the full and recent reads averaged over every probe after
the initial load (probes every 3 batches sample every phase of a 1-, 5-, 10- or 20-batch cycle);
and kept bytes (stored minus reclaimable: what remains once every older version expires) averaged
over every step.

Each arm's cheapest policy then follows from a cost per batch:

  writes   PUTs x put + GETs x get + GB written x upload + GB read x retrieval + CPU x cpu
  reads    q x (the same terms for one full read)
  storage  kept GB x storage per GB-month x the batch interval

under each price model (PRICES), read rate q (full reads per batch) and batch interval. The
interval only weighs storage against the rest, so a longer one stands in for a larger table: at
this table size (about 55 MB) a day's interval weighs storage as a 5-minute one would for a table
288 times as large, other costs unchanged.

Writes compaction.md (every policy's numbers per arm, and the cheapest policy per scenario),
compaction-frontier.png (writes against reads and space, per arm) and compaction-cost.png (the
cost components of every policy, for one scenario) to the output directory.

Usage: analysis/compaction.py <out dir> <run dir>...
"""

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))
import plots  # noqa: E402  (palette, arm encoding)

READER = "one-footer"
ARMS = ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]

# US East (N. Virginia) list prices, per request and per GB (storage per GB-month): S3 Standard
# (in-region transfer is free) and S3 Express One Zone since its 2025-04-10 price cut
# (aws.amazon.com/blogs/aws/up-to-85-price-reductions-for-amazon-s3-express-one-zone/). DELETEs
# are free. Compute is an assumption: about the on-demand price of a general-purpose vCPU.
# Appendable RMoW files need an append, which S3 Standard lacks.
CPU_PER_VCPU_HOUR = 0.04
PRICES = {
    "S3 Standard": {"put": 0.005e-3, "get": 0.0004e-3, "upload": 0.0, "retrieval": 0.0,
                    "storage": 0.023, "append": False},
    "S3 Express": {"put": 0.00113e-3, "get": 0.00003e-3, "upload": 0.0032, "retrieval": 0.0006,
                   "storage": 0.11, "append": True},
}
READ_RATES = [0.1, 1, 10]
INTERVALS = {"5 min": 5, "1 day": 24 * 60}
FIGURE_SCENARIO = ("S3 Express", 1, "5 min")  # H6's
MINUTES_PER_MONTH = 30 * 24 * 60

LEVELS = {0.25: "aggressive", 0.5: "default", 0.75: "lazy"}  # by garbage ratio


def policy_of(layout):
    """(sort key, name) of a run's compaction policy: none, then by level and period."""
    p = layout.get("compactEvery", 0)
    if p == 0:
        return (0, 0), "none"
    g = layout.get("compactGarbageRatio", 0.5)
    level = list(LEVELS).index(g) + 1 if g in LEVELS else len(LEVELS) + 1
    return (level, p), f"{LEVELS.get(g, f'g={g}')} P={p}"


def summarize(run):
    run = Path(run)
    manifest = json.loads((run / "manifest.json").read_text())
    layout = manifest["config"]["layout"]
    key, policy = policy_of(layout)
    df = pd.read_json(run / "steps.jsonl", lines=True)
    rows = []
    for arm in [a for a in ARMS if a in set(df.arm)]:
        d = df[df.arm == arm]
        work = d[d.phase.isin(["batch", "compact"]) & (d.step > 0)]
        batches = d[(d.phase == "batch") & (d.step > 0)].step.nunique()
        probes = d[d.phase.str.startswith("read") & (d.step > 0)]
        if "reader" in probes:
            probes = probes[probes["reader"].eq(READER)]
        full = probes[probes.phase == "read_full"]
        recent = probes[probes.phase == "read_recent"]
        state = d[(d.phase == "state") & (d.step > 0)]
        kept = state.stored_bytes - state.reclaimable_bytes
        end = state[state.step == state.step.max()].iloc[0]
        rows.append({
            "policy": policy,
            "key": key,
            "P": layout.get("compactEvery", 0),
            "arm": arm,
            "write_mb": work.bytes_written.sum() / batches / 1e6,
            "compact_mb": work[work.phase == "compact"].bytes_written.sum() / batches / 1e6,
            "puts": (work.puts + work.appends).sum() / batches,
            "write_gets": (work.gets + work.heads).sum() / batches,
            "write_read_mb": work.bytes_read.sum() / batches / 1e6,
            "write_cpu_s": work.cpu_ns.sum() / batches / 1e9,
            "read_gets": (full.gets + full.heads).mean(),
            "read_mb": full.bytes_read.mean() / 1e6,
            "read_cpu_s": full.cpu_ns.mean() / 1e9,
            "recent_gets": (recent.gets + recent.heads).mean(),
            "kept_gb": kept.mean() / 1e9,
            "kept_over_live": (kept / state.live_bytes).mean(),
            "kept_over_live_end": (end.stored_bytes - end.reclaimable_bytes) / end.live_bytes,
            "row_groups_end": end.row_groups,
        })
    return rows


def costs(r, prices, q, minutes):
    """The cost components of one arm under one policy, in USD per batch."""
    cpu = CPU_PER_VCPU_HOUR / 3600
    return {
        "write requests": r.puts * prices["put"] + r.write_gets * prices["get"],
        "write transfer": (r.write_mb * prices["upload"] + r.write_read_mb * prices["retrieval"])
        / 1e3,
        "write compute": r.write_cpu_s * cpu,
        "reads": q * (r.read_gets * prices["get"] + r.read_mb / 1e3 * prices["retrieval"]
                      + r.read_cpu_s * cpu),
        "storage": r.kept_gb * prices["storage"] * minutes / MINUTES_PER_MONTH,
    }


IO = ["write_mb", "puts", "write_gets", "write_read_mb", "read_gets", "read_mb", "kept_gb"]


def total_cost(r, prices, q, minutes):
    return sum(costs(r, prices, q, minutes).values())


def noise_band(t):
    """The largest cost difference, relative to no compaction, between runs of an arm that did
    the same I/O (a policy that never fired): what CPU noise alone moves the cost by. None if no
    such pair."""
    band = None
    for arm in ARMS:
        d = t[t.arm == arm]
        if "none" not in set(d.policy):
            continue
        none = d[d.policy == "none"].iloc[0]
        for _, r in d[d.policy != "none"].iterrows():
            if all(abs(r[c] - none[c]) <= 1e-9 * max(1.0, abs(none[c])) for c in IO):
                for prices in PRICES.values():
                    for minutes in INTERVALS.values():
                        for q in READ_RATES:
                            a = total_cost(none, prices, q, minutes)
                            diff = abs(total_cost(r, prices, q, minutes) - a) / a
                            band = diff if band is None else max(band, diff)
    return band


def merged(t):
    """One row per arm and distinct I/O: runs of an arm whose I/O was identical (policies that
    fired alike, or never) are one policy, with their CPU averaged, named by what they share."""
    rows = []
    for arm in ARMS:
        d = t[t.arm == arm].sort_values("key")
        groups = []
        for _, r in d.iterrows():
            for g in groups:
                if all(abs(r[c] - g[0][c]) <= 1e-9 * max(1.0, abs(g[0][c])) for c in IO):
                    g.append(r)
                    break
            else:
                groups.append([r])
        for g in groups:
            row = g[0].copy()
            row["write_cpu_s"] = sum(r.write_cpu_s for r in g) / len(g)
            names = [r.policy for r in g]
            periods = {r.P for r in g}
            if "none" in names:
                others = len(names) - 1
                row["policy"] = "none" + (f" (= {others} that never fired)" if others else "")
            elif len(periods) == 1 and len(names) == len(LEVELS):
                row["policy"] = f"any level P={periods.pop()}"
            else:
                row["policy"] = " = ".join(names)
            rows.append(row)
    return pd.DataFrame(rows)


def fmt(v, digits=3):
    if pd.isna(v):
        return ""
    return f"{v:.{digits}g}" if isinstance(v, float) else str(v)


def pareto(points):
    """Indexes of the points no other point beats on both coordinates (lower is better)."""
    keep = []
    for i, (x, y) in enumerate(points):
        if not any((x2 <= x and y2 <= y) and (x2 < x or y2 < y) for x2, y2 in points):
            keep.append(i)
    return keep


def place_labels(ax, labels, size=6.5):
    """Annotates points in order, skipping a label that would overlap one already placed (the
    point stays, and the table has it)."""
    ax.autoscale_view()
    pt = ax.figure.dpi / 72  # pixels per point
    placed = []
    for x, y, text, dy in labels:
        px, py = ax.transData.transform((x, y))
        box = (px + 5 * pt, py + dy * pt, px + (5 + 0.55 * size * len(text)) * pt,
               py + (dy + size) * pt)
        if any(box[0] < b[2] and b[0] < box[2] and box[1] < b[3] and b[1] < box[3]
               for b in placed):
            continue
        placed.append(box)
        ax.annotate(text, (x, y), xytext=(5, dy), textcoords="offset points", fontsize=size,
                    color=plots.INK2)


def frontier(t, out):
    """Writes per batch against reads (top) and space (bottom), one column per arm family."""
    columns = [("MoR and Iceberg CoW", ["mor-v3", "tailcow-c"]),
               ("RMoW, appendable", ["rmow-b", "rmow-c"]),
               ("RMoW, split files", ["rmow-b-split", "rmow-c-split"])]
    metrics = [("read_gets", "Full read, mean GETs"), ("kept_over_live", "Kept / live, mean")]
    fig, axes = plt.subplots(2, 3, figsize=(12, 7.2), constrained_layout=True)
    for col, (title, arms) in enumerate(columns):
        for row, (metric, ylabel) in enumerate(metrics):
            ax = axes[row][col]
            ax.set_xscale("log")
            labels = []  # (x, y, text, dy), placed once the panel's limits are known
            for n, arm in enumerate(arms):
                d = t[t.arm == arm].sort_values("key")
                fam, marker, label = plots.ARMS[arm]
                color = plots.FAMILY[fam]
                is_none = d.policy.str.startswith("none")
                compacted = d[~is_none]
                ax.scatter(compacted.write_mb, compacted[metric], color=color, marker=marker,
                           s=36, edgecolors=plots.SURFACE, linewidths=1, label=label, zorder=3)
                none = d[is_none]
                ax.scatter(none.write_mb, none[metric], facecolors=plots.SURFACE,
                           edgecolors=color, marker=marker, s=48, linewidths=1.5, zorder=3)
                front = d.iloc[pareto(list(zip(d.write_mb, d[metric])))].sort_values("write_mb")
                ax.plot(front.write_mb, front[metric], color=color, lw=1, alpha=0.6, zorder=2)
                # one label per point; policies that land on the same point share it
                alike = {}
                for _, r in d.iterrows():
                    alike.setdefault((round(r.write_mb, 4), round(r[metric], 4)), []).append(
                        r.policy.replace("aggressive", "aggr"))
                for _, r in front.iterrows():
                    names = alike[(round(r.write_mb, 4), round(r[metric], 4))]
                    text = names[0] + (f" (+{len(names) - 1} alike)" if len(names) > 1 else "")
                    labels.append((r.write_mb, r[metric], text, 4 if n == 0 else -10))
            place_labels(ax, labels)
            ax.xaxis.set_major_locator(matplotlib.ticker.LogLocator(subs=(1, 2, 5)))
            ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
            ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
            ax.set_xlabel("MB written per batch, compaction included")
            ax.set_ylabel(ylabel)
            if row == 0:
                ax.set_title(title, loc="left")
            ax.legend(loc="best", fontsize=7)
    fig.suptitle("Compaction policies: what each costs in writes and buys in reads and space "
                 "(hollow: no compaction; line: the Pareto front, labeled where room allows)",
                 x=0.01, ha="left", color=plots.INK, fontsize=10)
    fig.savefig(out / "compaction-frontier.png", dpi=150)


def cost_figure(t, out, band):
    store, q, interval = FIGURE_SCENARIO
    parts = ["write requests", "write transfer", "write compute", "reads", "storage"]
    colors = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]  # categorical slots 1-5
    fig, axes = plt.subplots(2, 3, figsize=(12, 7.4), constrained_layout=True, sharex=True)
    for ax, arm in zip(axes.flat, ARMS):
        d = t[t.arm == arm].sort_values("key", ascending=False)
        comp = pd.DataFrame([costs(r, PRICES[store], q, INTERVALS[interval])
                             for _, r in d.iterrows()]) * 1e6
        total = comp.sum(axis=1).to_numpy()
        left = [0.0] * len(d)
        for part, color in zip(parts, colors):
            ax.barh(range(len(d)), comp[part], left=left, color=color, height=0.7,
                    edgecolor=plots.SURFACE, linewidth=1, label=part)
            left = [a + b for a, b in zip(left, comp[part])]
        best = total.argmin()
        none = next(i for i, p in enumerate(d.policy) if p.startswith("none"))
        mark = " cheapest"
        if best != none and total[best] > (1 - band) * total[none]:
            best, mark = none, " cheapest, within noise"  # of the nominal winner
        for i, v in enumerate(total):
            mark_i = mark if i == best else ""
            ax.text(v, i, f" {v:.0f}{mark_i}", va="center", fontsize=7,
                    color=plots.INK if i == best else plots.INK2)
        ax.set_yticks(range(len(d)), d.policy)
        ax.tick_params(axis="y", labelsize=7, colors=plots.INK2)
        ax.grid(axis="y", visible=False)
        ax.set_title(plots.ARMS[arm][2], loc="left")
        ax.set_xlabel("µ$ per batch")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="outside lower center", ncol=len(parts), fontsize=8)
    for ax in axes.flat:
        ax.margins(x=0.25)
    fig.suptitle(f"Cost per batch by policy: {store}, {q:g} full read per batch, a batch every "
                 f"{interval}, compute ${CPU_PER_VCPU_HOUR} per vCPU-hour", x=0.01, ha="left",
                 color=plots.INK, fontsize=10)
    fig.savefig(out / "compaction-cost.png", dpi=150)


def main(argv):
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    t = pd.DataFrame([r for run in argv[2:] for r in summarize(run)])
    dup = t.duplicated(["policy", "arm"])
    if dup.any():
        print(f"two runs share a policy: {sorted(set(t[dup].policy))}", file=sys.stderr)
        return 1
    # a full read reads the same rows under every policy, and its CPU varies between runs more
    # than with the layout, so the cost model takes each arm's mean
    t["read_cpu_s_run"] = t.read_cpu_s
    t["read_cpu_s"] = t.groupby("arm").read_cpu_s.transform("mean")

    cols = ["policy", "write_mb", "compact_mb", "puts", "write_gets", "write_read_mb",
            "write_cpu_s", "read_gets", "read_mb", "read_cpu_s_run", "recent_gets",
            "kept_over_live", "kept_over_live_end", "row_groups_end"]
    md = ["# When to compact", "",
          "Runs: " + ", ".join(f"`{Path(r).parent.name}/{Path(r).name}`" for r in argv[2:]), "",
          "Per batch (batch and compaction phases): MB written (`write_mb`, of which compaction "
          "`compact_mb`), PUT-class requests (`puts`, appends included), GET-class requests "
          "(`write_gets`, HEADs included), MB read and CPU seconds. Full reads (`read_*`) and "
          "recent reads (`recent_gets`) are means over every probe after the initial load, under "
          f"the {READER} reader. Kept / live: stored minus reclaimable bytes over live bytes, the "
          "mean over every step and at the last. The cost model takes a full read's CPU as the "
          "mean over the arm's runs (`read_cpu_s_run` is each run's): the rows read are the "
          "same, and the CPU varies between runs more than with the policy.", ""]
    for arm in ARMS:
        d = t[t.arm == arm].sort_values("key")
        if d.empty:
            continue
        md += [f"## {plots.ARMS[arm][2]}", "", "| " + " | ".join(cols) + " |",
               "|" + "---|" * len(cols)]
        for _, r in d[cols].iterrows():
            md.append("| " + " | ".join(fmt(v) for v in r) + " |")
        md.append("")

    measured = noise_band(t)
    band = max(measured or 0.0, 0.02)
    md += ["## Cheapest policy per scenario", "",
           f"Cost per batch in µ$ (compute at ${CPU_PER_VCPU_HOUR} per vCPU-hour), the cheapest "
           "policy and, in parentheses, its saving against no compaction. A longer batch interval "
           "weighs storage more, as a larger table would. Savings under "
           f"{band:.1%} are within noise and reported as no compaction, with the nominal winner: "
           "runs of one arm that did the same I/O (a policy that never fired) differ by up to "
           + (f"{measured:.1%}" if measured is not None else "an unmeasured amount")
           + " in cost, from CPU time alone (the floor is 2%).", "",
           "| store | full reads per batch | batch interval | "
           + " | ".join(plots.ARMS[a][2] for a in ARMS) + " |", "|" + "---|" * (3 + len(ARMS))]
    m = merged(t)
    for store, prices in PRICES.items():
        for interval, minutes in INTERVALS.items():
            for q in READ_RATES:
                cells = []
                for arm in ARMS:
                    d = m[m.arm == arm]
                    if d.empty:
                        cells.append("")
                        continue
                    if not prices["append"] and arm.startswith("rmow-") and "-split" not in arm:
                        cells.append("n/a (no append)")
                        continue
                    total = pd.Series([total_cost(r, prices, q, minutes) * 1e6
                                       for _, r in d.iterrows()], index=d.policy)
                    best = total.idxmin()
                    none = next(p for p in total.index if p.startswith("none"))
                    saving = 1 - total[best] / total[none]
                    if best == none:
                        cells.append(f"none: {total[none]:.0f}")
                    elif saving < band:
                        cells.append(f"none: {total[none]:.0f} (≈ {best}, {saving:.0%})")
                    else:
                        cells.append(f"**{best}**: {total[best]:.0f} ({saving:.0%})")
                md.append(f"| {store} | {q:g} | {interval} | " + " | ".join(cells) + " |")
    md.append("")
    (out / "compaction.md").write_text("\n".join(md))
    t.to_csv(out / "compaction.csv", index=False)
    frontier(m, out)
    cost_figure(m, out, band)
    for f in ["compaction.md", "compaction.csv", "compaction-frontier.png", "compaction-cost.png"]:
        print(out / f)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
