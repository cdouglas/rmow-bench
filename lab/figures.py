"""rmow-bench figures for papers and docs (`lab export NAME`), and helpers the pages share
(`labbook.figures_module().arm_encoding()`).

The matplotlib figures reuse analysis/plots.py (palette, arm encoding, direct labels), so an
exported figure looks like the scripts' PNGs.
"""

import sys
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd

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


# The store latency replay: what each outermost phase of a run with a request log
# (--log-requests) would take on a simulated store. The model is the one the harness injects
# (org.rmow.bench.io.StoreLatency) and lab/views/40_latency.sql sums: a request costs its kind's
# expected latency (lognormal median x e^(sigma^2 / 2)), a GET a part's latency more per appended
# part of its object, and each MiB its transfer time. Requests on the phase's own thread add up;
# each group of split tasks runs on its reader's threads, first come first served, and costs its
# makespan.

def _expected(median_ms, sigma):
    return median_ms * np.exp(sigma ** 2 / 2)


def replay(run_id, profile, parallelism=None, cpu="time"):
    """Simulated latency (ms) of each outermost phase of `run_id` on the store `profile` (a dict
    or row with store_profiles' columns, plus an optional part_ms). `parallelism` overrides the
    threads of every reader model that ran on a pool. `cpu` is "time" (each thread's measured
    CPU) or "work" (its instructions at its arm's pace, as phase_work times a phase; needs a run
    with counts). Returns arm, step, phase, reader, the phase's measured wall and CPU (ms), and
    its simulated latency: NaN for a phase that appends on a store without append (a NULL
    append_ms)."""
    import heapq

    ev = q("""
        SELECT arm, step, phase, coalesce(reader, '') AS reader, seq, task, kind, object,
               "offset", length, cpu_ns, instructions
        FROM requests WHERE run_id = $r ORDER BY arm, seq""", r=run_id)
    steps = q("""
        SELECT s.arm, s.step, s.phase, coalesce(s.reader, '') AS reader, s.wall_ns / 1e6 AS wall_ms,
               s.cpu_ns / 1e6 AS cpu_ms, s.instructions, w.cpi, w.ghz
        FROM steps s LEFT JOIN phase_work w
          ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step AND w.phase = s.phase
         AND w.reader IS NOT DISTINCT FROM s.reader
        WHERE s.run_id = $r
          AND s.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_old')""", r=run_id)
    if cpu == "work":
        if steps.instructions.isna().all():
            raise ValueError(f"{run_id} has no instruction counts")
        # a thread's work at its phase's pace: instructions x cpi / clock, in ms
        pace = steps.set_index(["arm", "step", "phase", "reader"])
        ms_per_instruction = (pace.cpi / pace.ghz / 1e6).rename("ms_per_instruction")
        ev = ev.merge(ms_per_instruction.reset_index(), on=["arm", "step", "phase", "reader"],
                      how="left")
        ev["cpu_ns"] = np.where(ev.kind == "task", ev.instructions * ev.ms_per_instruction * 1e6,
                                ev.cpu_ns)
        steps["cpu_ms"] = steps.instructions * steps.cpi / steps.ghz / 1e6
    elif cpu != "time":
        raise ValueError(f"cpu is time or work, not {cpu}")
    threads = dict(q("SELECT reader, parallelism FROM run_readers WHERE run_id = $r",
                     r=run_id).itertuples(index=False))
    pr = dict(profile)
    get = _expected(pr["get_ms"], pr["get_sigma"])
    put = _expected(pr["put_ms"], pr["put_sigma"])
    can_append = pr.get("append_ms") is not None and not pd.isna(pr.get("append_ms"))
    append = _expected(pr["append_ms"], pr["append_sigma"]) if can_append else 0.0
    part = pr.get("part_ms") or 0.0
    read, write = pr["read_ms_per_mib"] / 2 ** 20, pr["write_ms_per_mib"] / 2 ** 20

    # parts of each object: appends since its last PUT or DELETE, before this event
    obj = ev.arm + "/" + ev.object
    gen = ev.kind.isin(["put", "delete"]).groupby(obj).cumsum()
    app = (ev.kind == "append").astype(int)
    parts = app.groupby([obj, gen]).cumsum() - app
    lat = np.select(
        [ev.kind == "get_range", ev.kind == "get_stream",
         ev.kind.isin(["stream_bytes", "skip_through"]), ev.kind == "head", ev.kind == "put",
         ev.kind == "append", ev.kind == "delete"],
        [get + parts * part + ev.length * read, get + parts * part, ev.length * read, get,
         put + ev.length * write, append + ev.length * write, put],
        0.0)
    ev = ev.assign(lat=lat)
    key = ["arm", "step", "phase", "reader"]
    req = ev[ev.kind != "task"]
    serial = req[req.task < 0].groupby(key).lat.sum()
    tasks = ev[ev.kind == "task"][key + ["task", "offset", "length", "cpu_ns"]].rename(
        columns={"offset": "group", "length": "inline"})
    tasks = tasks.merge(req[req.task >= 0].groupby(key + ["task"]).lat.sum().reset_index(),
                        on=key + ["task"], how="left").fillna({"lat": 0.0})
    tasks["ms"] = tasks.cpu_ns / 1e6 + tasks.lat

    rows = []
    for k, g in tasks.groupby(key + ["group"]):
        if g.inline.iloc[0] == 1:
            span = g.ms.sum()
        else:
            n = parallelism or threads.get(k[3], 1)
            free = [0.0] * n  # each thread's finish time; tasks start in order on the first free
            for ms in g.sort_values("task").ms:
                heapq.heappush(free, heapq.heappop(free) + ms)
            span = max(free)
        inline_cpu = g.cpu_ns.sum() / 1e6 if g.inline.iloc[0] == 1 else 0.0
        rows.append((*k[:4], span, inline_cpu))
    groups = pd.DataFrame(rows, columns=key + ["span", "inline_cpu"]).groupby(key).sum()

    out = steps.set_index(key)
    out["serial_io"] = serial.reindex(out.index).fillna(0.0)
    out["tasks"] = groups.span.reindex(out.index).fillna(0.0)
    # the phase thread's CPU includes the tasks it ran inline; those count in their tasks
    out["serial_cpu"] = out.cpu_ms - groups.inline_cpu.reindex(out.index).fillna(0.0)
    out["simulated_ms"] = out.serial_cpu + out.serial_io + out.tasks
    if not can_append:
        appends = ev[ev.kind == "append"].groupby(key).size()
        out.loc[appends.reindex(out.index).fillna(0) > 0, "simulated_ms"] = np.nan
    return out.reset_index()


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
