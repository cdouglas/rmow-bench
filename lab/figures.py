"""rmow-bench figures for papers and docs (`lab export NAME`), and helpers the pages share
(`labbook.figures_module().arm_encoding()`).

The matplotlib figures reuse analysis/plots.py (palette, arm encoding, direct labels), so an
exported figure looks like the scripts' PNGs.
"""

import sys
from pathlib import Path

import altair as alt

from labbook import figure, palette, q

ANALYSIS = Path(__file__).resolve().parent.parent / "analysis"

# analysis/sweep.py's arms and panels
SWEEP_ARMS = ["mor-v3", "tailcow-c", "rmow-a", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
SWEEP_PANELS = [
    ("write_mb_per_batch", "Bytes written per batch", "MB"),
    ("requests_per_batch", "Requests per batch", "requests"),
    ("read_full_gets", "Full read, last probe", "GETs"),
    ("read_recent_gets", "Newest 1% of event time, last probe", "GETs"),
    ("stored_over_live", "Stored / live bytes, last step", "ratio"),
    ("footer_kb", "Footer bytes, last step (RMoW)", "KB"),
]


def arm_encoding(arms: list[str] | None = None, title: str = "arm") -> tuple[alt.Color, alt.Shape]:
    """Color by family and shape by variant, as analysis/plots.py draws arms, with one legend
    listing the given arms (default: all of them) in plots.py's order."""
    arms = q("SELECT arm_label, family, marker FROM arm_meta "
             "WHERE $arms IS NULL OR list_contains($arms, arm) ORDER BY ord", arms=arms)
    family = palette("family")
    domain = list(arms.arm_label)
    color = alt.Color("arm_label:N", title=title, sort=domain,
                      scale=alt.Scale(domain=domain, range=[family[f] for f in arms.family]))
    shape = alt.Shape("arm_label:N", title=title, sort=domain,
                      scale=alt.Scale(domain=domain, range=list(arms.marker)))
    return color, shape


def sweep_summary():
    return q("""
        SELECT a.arm, s.* EXCLUDE (run_id, arm), m.arm_label, m.family, m.ord
        FROM arm_summary s JOIN canonical_runs r USING (run_id) JOIN arm_meta m USING (arm)
        JOIN (SELECT unnest($arms) AS arm) a USING (arm)
        WHERE list_contains(r.campaigns, 'rg-sweep')
        ORDER BY m.ord, s.rg_mib""", arms=SWEEP_ARMS)


def _plots():
    if str(ANALYSIS) not in sys.path:
        sys.path.insert(0, str(ANALYSIS))
    import plots

    return plots


@figure("sweep", title="Row-group size sweep (one-footer reader)",
        alt="Six panels against row-group and tail-seal size (1 to 8 MiB): bytes written and "
            "requests per batch, GETs of the last full and recent reads, stored over live bytes "
            "and RMoW footer size, one line per arm")
def sweep():
    import matplotlib.pyplot as plt

    plots = _plots()
    t = sweep_summary()
    fig, axes = plt.subplots(2, 3, figsize=(12, 6.6), constrained_layout=True)
    sizes = sorted(t.rg_mib.unique())
    for ax, (metric, title, unit) in zip(axes.flat, SWEEP_PANELS, strict=True):
        ends = []
        for arm in SWEEP_ARMS:
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
    fig.suptitle("Row-group size sweep (one-footer reader)", x=0.01, ha="left",
                 color=plots.INK, fontsize=11)
    cols = ["arm", "rg_mib"] + [m for m, _, _ in SWEEP_PANELS] + [
        "read_old_gets", "row_groups", "rg_mb_median", "new_rg_mb_median"]
    return fig, t[cols]


@figure("write", title="Write path, M1 recency-skewed with compaction every 10 batches",
        alt="Bytes written and requests per batch against batch number for the headline arms "
            "of the M1 recency-skewed run with compaction every 10 batches, log scale")
def write():
    import matplotlib.pyplot as plt

    plots = _plots()
    df = q("""
        SELECT s.arm, s.step, s.bytes_written / 1e6 AS mb_written, s.requests
        FROM steps s JOIN canonical_runs r USING (run_id) JOIN arm_meta m USING (arm)
        WHERE s.phase = 'batch' AND m.headline AND r.label = 'local-m1-recency-exp-compact10'
        ORDER BY m.ord, s.step""")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    plots.line_panel(axes[0], df, "mb_written", "Bytes written per batch", "MB", True)
    plots.line_panel(axes[1], df, "requests", "Requests per batch", "requests", True)
    axes[1].legend(loc="upper left", bbox_to_anchor=(1.18, 1), fontsize=8)
    fig.suptitle("local-m1-recency-exp-compact10: write path", x=0.01, ha="left",
                 color=plots.INK, fontsize=11)
    summary = df.groupby("arm", sort=False).agg(
        mb_written_per_batch=("mb_written", "mean"), requests_per_batch=("requests", "mean"))
    return fig, summary.reset_index()
