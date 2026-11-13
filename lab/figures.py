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
# part of its object, and each MiB its transfer time. On a store that caps an append
# (max_append_mib), a larger append costs an append more per part beyond the first, and so does a
# larger appendable create's staging (phase_splits). Requests on the phase's own thread add up;
# each group of split tasks runs on its reader's threads, first come first served, and costs its
# makespan.

def _expected(median_ms, sigma):
    return median_ms * np.exp(sigma ** 2 / 2)


CAPPED_APPEND_BYTES = 100 * 2 ** 20  # IoTrace.CAPPED_APPEND_BYTES, the cap append_splits counts at


def replay(run_id, profile, parallelism=None, cpu="time"):
    """Simulated latency (ms) of each outermost phase of `run_id` on the store `profile` (a dict
    or row with store_profiles' columns, plus an optional part_ms; a max_append_mib, if set, must
    be the 100 MiB the runs count splits at). `parallelism` overrides the
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
               s.cpu_ns / 1e6 AS cpu_ms, s.instructions, w.cpi, w.ghz, x.append_splits
        FROM steps s LEFT JOIN phase_work w
          ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step AND w.phase = s.phase
         AND w.reader IS NOT DISTINCT FROM s.reader
        JOIN phase_splits x
          ON x.run_id = s.run_id AND x.arm = s.arm AND x.step = s.step AND x.phase = s.phase
         AND x.reader IS NOT DISTINCT FROM s.reader
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
    cap = pr.get("max_append_mib")
    capped = cap is not None and not pd.isna(cap)
    if capped and cap * 2 ** 20 != CAPPED_APPEND_BYTES:
        raise ValueError(f"max_append_mib is 100 or none, not {cap}")

    # the parts beyond the first a capped store splits each append into (IoTrace.splits)
    split = (np.maximum(0, (ev.length - 1) // CAPPED_APPEND_BYTES)
             if capped else 0) * (ev.kind == "append")
    # parts of each object: appends' parts since its last PUT or DELETE, before this event
    obj = ev.arm + "/" + ev.object
    gen = ev.kind.isin(["put", "delete"]).groupby(obj).cumsum()
    app = (ev.kind == "append").astype(int) * (1 + split)
    parts = app.groupby([obj, gen]).cumsum() - app
    lat = np.select(
        [ev.kind == "get_range", ev.kind == "get_stream",
         ev.kind.isin(["stream_bytes", "skip_through"]), ev.kind == "head", ev.kind == "put",
         ev.kind == "append", ev.kind == "delete"],
        [get + parts * part + ev.length * read, get + parts * part, ev.length * read, get,
         put + ev.length * write, append * (1 + split) + ev.length * write, put],
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
    if capped:
        # the splits the log does not show: appendable creates', staged on the phase's thread
        # (the log does not say which PUT made an appendable file, so their parts are not counted
        # as the object's, as injected latency counts them)
        logged = ev.assign(split=split).groupby(key).split.sum()
        out["serial_io"] += (out.append_splits - logged.reindex(out.index).fillna(0)) * append
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


def tuner():
    """analysis/tune.py, the policy tuner (retired page tune.md: docs/LESSONS.md); its functions take a connection."""
    if str(ANALYSIS) not in sys.path:
        sys.path.insert(0, str(ANALYSIS))
    import tune

    return tune


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


def compaction_analysis(build, arms=None, readers=("one-footer", "cached")):
    """The compaction sweep (configs/local-compact-*.json) from one build: per run, arm and
    reader, writes per batch, one full read and the table kept; the cost model over two price
    lists, three read rates and two batch intervals; each arm's cheapest policy per scenario
    against a noise band; and H6's checks. lab/pages/compaction.md presents it, and the summary
    page quotes it. Returns the analysis's names."""
    from types import SimpleNamespace
    ARMS = list(arms or ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"])
    READERS = list(readers)  # one-footer decides H6 (as registered); cached is the post's
    LEVELS = {0.25: "aggressive", 0.5: "default", 0.75: "lazy"}  # by RMoW's garbage ratio
    BUILD = build
    labels = dict(q("SELECT arm, arm_label FROM arm_meta").itertuples(index=False))

    runs = q("""
        SELECT r.run_id, r.label, r.versions ->> 'build_snapshot' AS snapshot,
               max(p.value_num) FILTER (WHERE p.key = 'layout.compactEvery') AS P,
               max(p.value_num) FILTER (WHERE p.key = 'layout.compactGarbageRatio') AS g
        FROM canonical_runs r JOIN run_params p USING (run_id)
        WHERE r.label LIKE 'local-compact-%' AND r.git_sha LIKE $b || '%'
        GROUP BY ALL""", b=BUILD)
    runs["P"] = runs.P.fillna(0).astype(int)
    runs["policy"] = [("none" if p == 0 else f"{LEVELS.get(g, f'g={g}')} P={p}") for p, g in zip(runs.P, runs.g)]
    runs["key"] = [((0, 0) if p == 0 else (list(LEVELS).index(g) + 1, p)) for p, g in zip(runs.P, runs.g)]

    # CPU is each phase's instructions at its arm's pace (lab/pages/cpu-measure.md). The sweep's
    # runs are one workload on one machine, so the pace (median cycles per instruction of an arm's
    # phases of one kind and reader) is estimated over all of them, as is the clock: identical work
    # then costs identical CPU, where per-run paces would differ by several percent
    PACE = """
        pace AS (SELECT arm, phase, reader, median(cycles / instructions) AS cpi FROM phase_work
                 WHERE list_contains($ids, run_id) GROUP BY ALL),
        clock AS (SELECT median(cycles / user_ns) AS ghz FROM phase_work
                  WHERE list_contains($ids, run_id) AND user_ns > 0),
        work AS (SELECT w.run_id, w.arm, w.step, w.phase, w.reader,
                        w.instructions * p.cpi / (SELECT ghz FROM clock) / 1e6 AS work_ms
                 FROM phase_work w JOIN pace p ON p.arm = w.arm AND p.phase = w.phase
                  AND p.reader IS NOT DISTINCT FROM w.reader
                 WHERE list_contains($ids, w.run_id))"""
    # per run and arm: writes per batch (batches and compactions), and the table kept
    summary = q("""
        WITH """ + PACE + """, s AS (
            SELECT s.*, coalesce(w.work_ms, (s.cpu_ns + coalesce(s.pool_cpu_ns, 0)) / 1e6) AS cpu_ms,
                   w.work_ms IS NOT NULL AS counted
            FROM steps s LEFT JOIN work w
              ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step AND w.phase = s.phase
             AND w.reader IS NOT DISTINCT FROM s.reader
            WHERE list_contains($ids, s.run_id) AND list_contains($arms, s.arm)
        ), b AS (
            SELECT run_id, arm, count(DISTINCT step) FILTER (WHERE phase = 'batch' AND step > 0) AS batches
            FROM s GROUP BY ALL
        )
        SELECT s.run_id, s.arm,
               sum(bytes_written) FILTER (WHERE phase IN ('batch', 'compact') AND step > 0) / any_value(batches) / 1e6 AS write_mb,
               coalesce(sum(bytes_written) FILTER (WHERE phase = 'compact'), 0) / any_value(batches) / 1e6 AS compact_mb,
               sum(puts + appends) FILTER (WHERE phase IN ('batch', 'compact') AND step > 0) / any_value(batches) AS puts,
               sum(gets + heads) FILTER (WHERE phase IN ('batch', 'compact') AND step > 0) / any_value(batches) AS write_gets,
               sum(bytes_read) FILTER (WHERE phase IN ('batch', 'compact') AND step > 0) / any_value(batches) / 1e6 AS write_read_mb,
               sum(cpu_ms) FILTER (WHERE phase IN ('batch', 'compact') AND step > 0) / any_value(batches) / 1e3 AS write_cpu_s,
               avg(stored_bytes - reclaimable_bytes) FILTER (WHERE phase = 'state' AND step > 0) / 1e9 AS kept_gb,
               avg((stored_bytes - reclaimable_bytes) / live_bytes) FILTER (WHERE phase = 'state' AND step > 0) AS kept_over_live,
               arg_max((stored_bytes - reclaimable_bytes) / live_bytes, step) FILTER (WHERE phase = 'state') AS kept_over_live_end,
               bool_and(counted) FILTER (WHERE phase IN ('batch', 'compact')) AS counted
        FROM s JOIN b USING (run_id, arm) GROUP BY ALL""", ids=list(runs.run_id), arms=ARMS)
    # per run, arm and reader: one full read, the mean over the probes after the initial load
    reads = q("""
        WITH """ + PACE + """
        SELECT s.run_id, s.arm, s.reader,
               avg(s.gets + s.heads) FILTER (WHERE s.phase = 'read_full') AS read_gets,
               avg(s.bytes_read) FILTER (WHERE s.phase = 'read_full') / 1e6 AS read_mb,
               avg(coalesce(w.work_ms, (s.cpu_ns + coalesce(s.pool_cpu_ns, 0)) / 1e6))
                   FILTER (WHERE s.phase = 'read_full') / 1e3 AS read_cpu_s,
               avg(s.gets + s.heads) FILTER (WHERE s.phase = 'read_recent') AS recent_gets
        FROM steps s LEFT JOIN work w
          ON w.run_id = s.run_id AND w.arm = s.arm AND w.step = s.step AND w.phase = s.phase
         AND w.reader IS NOT DISTINCT FROM s.reader
        WHERE list_contains($ids, s.run_id) AND list_contains($arms, s.arm) AND s.step > 0
          AND list_contains($readers, s.reader)
        GROUP BY ALL""", ids=list(runs.run_id), arms=ARMS, readers=READERS)
    t_all = summary.merge(reads, on=["run_id", "arm"]).merge(runs[["run_id", "policy", "key", "P", "g"]], on="run_id")
    t_all["arm_label"] = t_all.arm.map(labels)
    # a full read's CPU is the arm's mean over its runs, as analysis/compaction.py charges it: reads
    # do the same work under every policy that leaves the layout alone
    t_all["read_cpu_s"] = t_all.groupby(["arm", "reader"]).read_cpu_s.transform("mean")
    COUNTED = bool(t_all.counted.all())

    # US East list prices, per request and per GB (storage per GB-month): S3 Standard (in-region
    # transfer free) and S3 Express One Zone since its 2025-04-10 price cut; DELETEs are free. Compute
    # is an assumption, about a general-purpose vCPU on demand (analysis/compaction.py)
    CPU_PER_VCPU_HOUR = 0.04
    PRICES = {
        "S3 Standard": {"put": 0.005e-3, "get": 0.0004e-3, "upload": 0.0, "retrieval": 0.0,
                        "storage": 0.023, "append": False},
        "S3 Express": {"put": 0.00113e-3, "get": 0.00003e-3, "upload": 0.0032, "retrieval": 0.0006,
                       "storage": 0.11, "append": True},
    }
    READ_RATES = [0.1, 1, 10]
    INTERVALS = {"5 min": 5, "1 day": 24 * 60}
    MINUTES_PER_MONTH = 30 * 24 * 60
    IO = ["write_mb", "puts", "write_gets", "write_read_mb", "read_gets", "read_mb", "kept_gb"]


    def costs(r, prices, q_, minutes):
        """One arm and policy's cost per batch, in USD, by component."""
        cpu = CPU_PER_VCPU_HOUR / 3600
        return {
            "write requests": r.puts * prices["put"] + r.write_gets * prices["get"],
            "write transfer": (r.write_mb * prices["upload"] + r.write_read_mb * prices["retrieval"]) / 1e3,
            "write compute": r.write_cpu_s * cpu,
            "reads": q_ * (r.read_gets * prices["get"] + r.read_mb / 1e3 * prices["retrieval"] + r.read_cpu_s * cpu),
            "storage": r.kept_gb * prices["storage"] * minutes / MINUTES_PER_MONTH,
        }


    def total(r, prices, q_, minutes):
        return sum(costs(r, prices, q_, minutes).values())


    def same_io(a, b):
        return all(abs(a[c] - b[c]) <= 1e-9 * max(1.0, abs(b[c])) for c in IO)


    def noise_band(t):
        """The largest cost difference, against no compaction, between runs of an arm whose policy
        never fired (no compaction bytes, the same batches, reads and space), with the GETs and bytes
        its checks read set to none's: what CPU alone moves the cost by."""
        band = 0.0
        for arm in ARMS:
            d = t[t.arm == arm]
            none = d[d.policy == "none"].iloc[0]
            for _, r in d[d.policy != "none"].iterrows():
                r = r.copy()
                r["write_gets"], r["write_read_mb"] = none.write_gets, none.write_read_mb
                if r.compact_mb == 0 and same_io(r, none):
                    for prices in PRICES.values():
                        for minutes in INTERVALS.values():
                            for q_ in READ_RATES:
                                a = total(none, prices, q_, minutes)
                                band = max(band, abs(total(r, prices, q_, minutes) - a) / a)
        return band


    def merged(t):
        """One row per arm and distinct I/O: policies of an arm that did the same I/O (that fired
        alike, or never) are one, with their CPU averaged, named by what they share."""
        rows = []
        for arm in ARMS:
            groups = []
            for _, r in t[t.arm == arm].sort_values("key").iterrows():
                for g in groups:
                    if same_io(r, g[0]):
                        g.append(r)
                        break
                else:
                    groups.append([r])
            for g in groups:
                row = g[0].copy()
                row["write_cpu_s"] = np.mean([r.write_cpu_s for r in g])
                row["read_cpu_s"] = np.mean([r.read_cpu_s for r in g])
                names = [r.policy for r in g]
                periods = {r.P for r in g}
                if "none" in names:
                    row["policy"] = "none" + (f" (= {len(names) - 1} that never fired)" if len(names) > 1 else "")
                elif len(periods) == 1 and len(names) == len(LEVELS):
                    row["policy"] = f"any level P={periods.pop()}"
                else:
                    row["policy"] = " = ".join(names)
                rows.append(row)
        return pd.DataFrame(rows)


    def cheapest(t, band):
        """Per store, read rate and interval: each arm's cheapest policy, its cost (µ$ per batch) and
        its saving against no compaction; a saving within the noise band counts as none."""
        rows = []
        m = merged(t)
        for store, prices in PRICES.items():
            for interval, minutes in INTERVALS.items():
                for q_ in READ_RATES:
                    row = {"store": store, "full reads per batch": q_, "batch interval": interval}
                    for arm in ARMS:
                        if not prices["append"] and arm in ("rmow-b", "rmow-c"):
                            row[labels[arm]] = "n/a (no append)"
                            continue
                        d = m[m.arm == arm].assign(cost=lambda x: [total(r, prices, q_, minutes) for _, r in x.iterrows()])
                        none = d[d.policy.str.startswith("none")].iloc[0]
                        best = d.loc[d.cost.idxmin()]
                        saving = 1 - best.cost / none.cost
                        if saving <= band:
                            row[labels[arm]] = f"none: {none.cost * 1e6:.0f}"
                        else:
                            pct = f"{100 * saving:.1f}" if saving < 0.01 else f"{100 * saving:.0f}"
                            row[labels[arm]] = f"{best.policy}: {best.cost * 1e6:.0f} ({pct}%)"
                    rows.append(row)
        return pd.DataFrame(rows)


    T = {r: t_all[t_all.reader == r] for r in READERS}
    BAND = {r: noise_band(T[r]) for r in READERS}
    CHEAPEST = {r: cheapest(T[r], BAND[r]) for r in READERS}

    # H6, as registered: on the one-footer reader
    H = T["one-footer"]
    M = merged(H)


    def arm_rows(arm):
        return H[H.arm == arm]


    def none_of(arm):
        return arm_rows(arm)[arm_rows(arm).policy == "none"].iloc[0]


    def span(xs, fmt="{:.2f}"):
        lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
        return lo if lo == hi else f"{lo}–{hi}"


    # reads barely depend on compaction: every policy within 2 GETs of none
    read_dev = {a: (arm_rows(a).read_gets - none_of(a).read_gets) for a in ["rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]}
    reads_b = all(read_dev[a].abs().max() <= 2 for a in ["rmow-b", "rmow-b-split"])
    reads_c = all(read_dev[a].abs().max() <= 2 for a in ["rmow-c", "rmow-c-split"])
    # appendable: kept at P = 1 under 1 / (1 - g), compaction writes falling as g rises
    app = H[H.arm.isin(["rmow-b", "rmow-c"])]
    p1 = app[app.P == 1]
    kept_p1 = {g: p1[p1.g == g].kept_over_live.max() for g in LEVELS}
    comp_p1 = {g: p1[p1.g == g].compact_mb.mean() for g in LEVELS}
    space_app = (all(kept_p1[g] < 1 / (1 - g) for g in LEVELS)
                 and comp_p1[0.25] > comp_p1[0.5] > comp_p1[0.75])
    # split: at g = 0.25, kept 1.3 or less for under 0.5 MB of compaction writes; at 0.75 nothing moves
    spl = H[H.arm.isin(["rmow-b-split", "rmow-c-split"])]
    aggr = spl[spl.g == 0.25]
    lazy = spl[spl.g == 0.75]
    space_split = aggr.kept_over_live.max() <= 1.3 and aggr.compact_mb.max() < 0.5 and lazy.compact_mb.max() == 0
    # MoR-v3 and TailCoW: as P falls (none, 20, 10, 5, 1), writes rise and full-read GETs fall
    ORDER = [0, 20, 10, 5, 1]


    def by_p(arm, col):
        d = arm_rows(arm)
        return [d[d.P == p_][col].mean() for p_ in ORDER]


    def monotone(xs, rising):
        return all((b > a) if rising else (b < a) for a, b in zip(xs, xs[1:]))


    trade = all(monotone(by_p(a, "write_mb"), True) and monotone(by_p(a, "read_gets"), False)
                for a in ["mor-v3", "tailcow-c"])
    # costs on S3 Express at 5-minute batches and one full read per batch
    EXPRESS = PRICES["S3 Express"]


    def best(arm, prices, q_, minutes, frame=M):
        d = frame[frame.arm == arm].assign(cost=lambda x: [total(r, prices, q_, minutes) for _, r in x.iterrows()])
        return d.loc[d.cost.idxmin()], d[d.policy.str.startswith("none")].iloc[0]


    split_best = [best(a, EXPRESS, 1, 5) for a in ["rmow-b-split", "rmow-c-split"]]
    split_within = all(b.cost <= 1.05 * n.cost for b, n in split_best)
    mor_best, mor_none = best("mor-v3", EXPRESS, 1, 5)
    mor_p = int(mor_best.P)
    mor_saving = 1 - mor_best.cost / mor_none.cost
    mor_ok = mor_p != 0 and mor_p <= 5 and mor_saving > BAND["one-footer"]
    std_best, std_none = best("mor-v3", PRICES["S3 Standard"], 1, 5)


    def verdict(ok):
        return "held" if ok else "contradicted"


    # the cheapest policies at a day's weight on S3 Express, at up to one full read per batch
    day = [(a, *best(a, EXPRESS, q_, 24 * 60)) for a in ARMS for q_ in (0.1, 1)]
    day_saving = {a: [1 - b.cost / n.cost for x, b, n in day if x == a] for a in ARMS}
    day_cost = {a: [b.cost for x, b, n in day if x == a] for a in ARMS}
    split_vs_app = [1 - day_cost[s_][i] / day_cost[a][i] for s_, a in [("rmow-b-split", "rmow-b"), ("rmow-c-split", "rmow-c")] for i in range(2)]
    # storage's share of each arm's cost at 5-minute batches
    share5 = max(costs(none_of(a), EXPRESS, q_, 5)["storage"] / total(none_of(a), EXPRESS, q_, 5)
                 for a in ARMS for q_ in READ_RATES)
    N = {
        "band": f"{100 * BAND['one-footer']:.2f}%",
        "b_dev": f"{max(read_dev[a].abs().max() for a in ['rmow-b', 'rmow-b-split']):.1f}",
        "c_dev": f"{max(read_dev[a].abs().max() for a in ['rmow-c', 'rmow-c-split']):.1f}",
        "c_none": f"{none_of('rmow-c').read_gets:.1f}",
        "c_min": f"{arm_rows('rmow-c').read_gets.min():.1f}",
        "b_none": f"{none_of('rmow-b').read_gets:.1f}",
        "b_rng": span(list(arm_rows("rmow-b").read_gets) + list(arm_rows("rmow-b-split").read_gets), "{:.1f}"),
        "kept_end_app": span([none_of(a).kept_over_live_end for a in ["rmow-b", "rmow-c"]], "{:.1f}"),
        "kept_end_split": span([none_of(a).kept_over_live_end for a in ["rmow-b-split", "rmow-c-split"]]),
        "kept_p1": ", ".join(f"{kept_p1[g]:.2f}" for g in LEVELS),
        "comp_p1": ", ".join(f"{comp_p1[g]:.2f}" for g in LEVELS),
        "aggr_kept": span(aggr.kept_over_live), "aggr_mb": span(aggr.compact_mb),
        "none_split_kept": span([none_of(a).kept_over_live for a in ["rmow-b-split", "rmow-c-split"]]),
        "mor_w": " → ".join(f"{x:.2f}" for x in by_p("mor-v3", "write_mb")),
        "mor_r": " → ".join(f"{x:.1f}" for x in by_p("mor-v3", "read_gets")),
        "tc_w": " → ".join(f"{x:.2f}" for x in by_p("tailcow-c", "write_mb")),
        "tc_r": " → ".join(f"{x:.1f}" for x in by_p("tailcow-c", "read_gets")),
        "mor_best": mor_best.policy, "mor_saving": f"{100 * mor_saving:.0f}%",
        "std_best": std_best.policy, "std_saving": f"{100 * (1 - std_best.cost / std_none.cost):.0f}%",
        "day_app": span([100 * x for a in ["rmow-b", "rmow-c"] for x in day_saving[a]], "{:.0f}%"),
        "day_split": span([100 * x for a in ["rmow-b-split", "rmow-c-split"] for x in day_saving[a]], "{:.0f}%"),
        "split_vs_app": span([100 * x for x in split_vs_app], "{:.0f}%"),
        "share5": f"{100 * share5:.0f}%",
        "v_reads": ("held for B, not for C" if reads_b and not reads_c else verdict(reads_b and reads_c)),
        "v_space_app": verdict(space_app), "v_space_split": verdict(space_split), "v_trade": verdict(trade),
        "v_cost_split": verdict(split_within), "v_cost_mor": verdict(mor_ok),
    }

    return SimpleNamespace(**{k: v for k, v in locals().items() if not k.startswith("_")})


# The cloud calibration (lab/pages/cloud.md, docs/S3X_RUNBOOK.md, docs/AZURE_RUNBOOK.md). Each
# provider is a list of repeated runs of one configuration, {provider: [run_id, ...]}: the cloud
# runs, or until they come back, stand-ins. Latency is each phase's measured wall time; requests,
# bytes and storage come from the views in lab/views/60_cloud.sql.

READS = {"read_full": "full read", "read_recent": "newest 1%", "read_old": "time travel"}
FILE_TYPES = {"parquet": "Parquet", "puffin": "Puffin (DV)", "session": "CreateSession"}
PARTS = {"base": "initial load", "base_appended": "appended to it", "data": "new data files",
         "data_appended": "appended to them", "dv": "DVs"}


def _groups(groups):
    """{provider: [run, ...]} as two parallel lists, for unnest in SQL."""
    pairs = [(p, r) for p, runs in groups.items() for r in runs]
    return {"providers": [p for p, _ in pairs], "group_runs": [r for _, r in pairs]}


_GROUPS = "(SELECT unnest($providers) AS provider, unnest($group_runs) AS run_id)"


def update_latency(groups, reader="cached"):
    """Measured wall time (ms) per provider, run, arm and update: each batch (a write; steps from
    1) and each read under `reader` (steps 0, every `reads.every`)."""
    return q(f"""
        SELECT g.provider, s.run_id, s.arm, m.arm_label, m.ord, s.step, s.phase,
               s.wall_ns / 1e6 AS ms
        FROM steps s JOIN {_GROUPS} g USING (run_id) JOIN arm_meta m USING (arm)
        WHERE s.phase = 'batch' OR (list_contains($reads, s.phase) AND s.reader = $reader)
        ORDER BY g.provider, m.ord, s.phase, s.step""",
             reads=list(READS), reader=reader, **_groups(groups))


def summary_points(groups, reader="cached", k=None, reads=("read_full", "read_recent")):
    """Per provider, arm and kind of read, over the updates up to `k` deltas against the
    checkpoint (default: all of them): the mean batch (write_ms), the mean read over the reads at
    0..k deltas (read_ms), and the reads at 0 and at k (read_k0, read_kk), in ms; each a mean over
    the provider's runs."""
    lat = update_latency(groups, reader)
    k = int(lat.step.max()) if k is None else k
    lat = lat[lat.step <= k]
    keys = ["provider", "arm", "arm_label", "ord"]
    writes = (lat[(lat.phase == "batch") & (lat.step >= 1)].groupby(keys).ms.mean()
              .rename("write_ms").reset_index())
    r = (lat[lat.phase.isin(reads)].groupby(keys + ["phase", "step"]).ms.mean().reset_index()
         .sort_values("step"))
    rd = r.groupby(keys + ["phase"]).agg(read_ms=("ms", "mean"), read_k0=("ms", "first"),
                                         read_kk=("ms", "last"), reads=("ms", "size"))
    out = rd.reset_index().merge(writes, on=keys)
    out["read"] = out.phase.map(READS)
    out["k"] = k
    out["runs"] = out.provider.map({p: len(v) for p, v in groups.items()})
    out["porder"] = out.provider.map({p: i for i, p in enumerate(groups)})
    return out.sort_values(["porder", "phase", "ord"]).reset_index(drop=True)


def summary_chart(df):
    """Write latency against read latency per arm: a point at the means, a whisker from the read
    at 0 deltas to the read at k; one panel per provider (columns) and kind of read (rows), each
    on its own scales."""
    color, shape = arm_encoding(list(df.arm.unique()))
    base = alt.Chart().encode(
        x=alt.X("write_ms:Q", title="write (one batch), ms", scale=alt.Scale(zero=False)),
        color=color)
    whisker = base.mark_rule(strokeWidth=2, opacity=0.6).encode(
        y=alt.Y("read_k0:Q", title="read, ms", scale=alt.Scale(zero=False)), y2="read_kk:Q")
    point = base.mark_point(filled=True, size=90, opacity=1).encode(
        y="read_ms:Q", shape=shape,
        tooltip=["provider", "read", "arm_label",
                 alt.Tooltip("write_ms:Q", format=".1f"), alt.Tooltip("read_ms:Q", format=".1f"),
                 alt.Tooltip("read_k0:Q", format=".1f"), alt.Tooltip("read_kk:Q", format=".1f")])
    return (alt.layer(whisker, point, data=df)
            .properties(width=260, height=220)
            .facet(column=alt.Column("provider:N", title=None,
                                     sort=list(dict.fromkeys(df.provider))),
                   row=alt.Row("read:N", title=None, sort=[READS[p] for p in READS]))
            .resolve_scale(x="independent", y="independent"))


def update_latency_chart(lat, provider, phases):
    """Bars of measured latency per update for one provider: the mean over its runs, with the
    least and the most as a whisker; one row per arm, one column per phase."""
    d = (lat[(lat.provider == provider) & lat.phase.isin(phases)]
         .groupby(["arm_label", "ord", "phase", "step"]).ms
         .agg(["mean", "min", "max", "size"]).reset_index())
    d["what"] = d.phase.map({"batch": "write (batch)", **READS})
    color, _ = arm_encoding(list(lat.arm.unique()))
    base = alt.Chart().encode(x=alt.X("step:O", title="update (deltas since the checkpoint)"))
    bars = base.mark_bar().encode(y=alt.Y("mean:Q", title="ms"), color=color,
                                  tooltip=["arm_label", "what", "step",
                                           alt.Tooltip("mean:Q", format=".1f"),
                                           alt.Tooltip("min:Q", format=".1f"),
                                           alt.Tooltip("max:Q", format=".1f"), "size"])
    spread = base.mark_rule(color="black", opacity=0.5).encode(y="min:Q", y2="max:Q")
    arms = list(d.sort_values("ord").arm_label.unique())
    return (alt.layer(bars, spread, data=d).properties(width=300, height=90)
            .facet(row=alt.Row("arm_label:N", title=None, sort=arms),
                   column=alt.Column("what:N", title=None,
                                     sort=[{"batch": "write (batch)", **READS}[p] for p in phases]))
            .resolve_scale(x="independent", y="independent"))


def update_requests(groups, reader="cached"):
    """What each update sent the store, per provider, arm, phase, step, file type and the SDK's
    kind of request (sent_requests): the mean over the provider's runs. Reads under `reader`."""
    return q(f"""
        WITH x AS (
            SELECT g.provider, x.run_id, x.arm, x.step, x.phase, x.file_type, x.kind, x.source,
                   sum(x.requests) AS requests
            FROM sent_requests x JOIN {_GROUPS} g USING (run_id)
            WHERE x.phase = 'batch' OR x.reader = $reader
            GROUP BY ALL
        )
        SELECT provider, arm, m.arm_label, m.ord, step, phase, file_type, kind, source,
               sum(requests) / count(DISTINCT run_id) AS requests, count(DISTINCT run_id) AS runs
        FROM x JOIN arm_meta m USING (arm)
        GROUP BY ALL ORDER BY provider, m.ord, phase, step""",
             reader=reader, **_groups(groups))


def update_bytes(groups, reader="cached"):
    """The model's bytes per update (file_requests), per provider, arm, phase, step, file type,
    object class and direction, in MB: the mean over the provider's runs (they agree: bytes are
    deterministic). Reads under `reader`."""
    return q(f"""
        WITH x AS (
            SELECT g.provider, f.run_id, f.arm, f.step, f.phase, f.file_type, f.object_class,
                   f.appended, sum(f.bytes_read) AS r, sum(f.bytes_written) AS w
            FROM file_requests f JOIN {_GROUPS} g USING (run_id)
            WHERE f.phase = 'batch' OR f.reader = $reader
            GROUP BY ALL
        )
        SELECT provider, arm, m.arm_label, m.ord, step, phase, file_type, object_class, appended,
               sum(r) / count(DISTINCT run_id) / 1e6 AS mb_read,
               sum(w) / count(DISTINCT run_id) / 1e6 AS mb_written
        FROM x JOIN arm_meta m USING (arm)
        GROUP BY ALL ORDER BY provider, m.ord, phase, step""",
             reader=reader, **_groups(groups))


def stacked_per_update(d, value, title, stack="file_type", labels=FILE_TYPES):
    """Stacked bars per update, one row per arm and one column per provider (in the frame's
    order): `value` stacked by `stack`."""
    providers = list(dict.fromkeys(d.provider)) if "provider" in d else [""]
    d = d.assign(provider=d.provider if "provider" in d else "")
    d = d.groupby(["provider", "arm_label", "ord", "step", stack], as_index=False)[value].sum()
    d["stack"] = d[stack].map(labels).fillna(d[stack])
    arms = list(d.sort_values("ord").arm_label.unique())
    return (alt.Chart(d).mark_bar().encode(
                x=alt.X("step:O", title="update (deltas since the checkpoint)"),
                y=alt.Y(f"{value}:Q", title=title),
                color=alt.Color("stack:N", title=None, sort=[labels[k] for k in labels]),
                tooltip=["provider", "arm_label", "step", "stack",
                         alt.Tooltip(f"{value}:Q", format=".2f")])
            .properties(width=300, height=80)
            .facet(row=alt.Row("arm_label:N", title=None, sort=arms),
                   column=alt.Column("provider:N", title=None, sort=providers))
            .resolve_scale(y="independent"))


def storage(groups):
    """What each arm stores after each update, per provider's first run (storage is
    deterministic): by part (stored_by_part), and the state's live, dead and reclaimable bytes
    (stored_state), in MB."""
    first = {p: runs[:1] for p, runs in groups.items()}
    parts = q(f"""
        SELECT g.provider, s.arm, m.arm_label, m.ord, s.step, s.part, s.bytes / 1e6 AS mb,
               s.stored_check
        FROM stored_by_part s JOIN {_GROUPS} g USING (run_id) JOIN arm_meta m USING (arm)
        ORDER BY g.provider, m.ord, s.step""", **_groups(first))
    state = q(f"""
        SELECT g.provider, s.arm, m.arm_label, m.ord, s.step, s.live_bytes / 1e6 AS live,
               s.dead_bytes / 1e6 AS dead, s.reclaimable_bytes / 1e6 AS reclaimable,
               s.stored_bytes / 1e6 AS stored
        FROM stored_state s JOIN {_GROUPS} g USING (run_id) JOIN arm_meta m USING (arm)
        ORDER BY g.provider, m.ord, s.step""", **_groups(first))
    return parts, state


def spike_ops(spikes):
    """Per spike ({provider: spike run}), op and size or parts: median, p10 and p90 in ms."""
    d = q(f"""
        SELECT g.provider, o.* EXCLUDE (run_id), o.bytes / 1048576.0 AS mib
        FROM spike_ops o JOIN {_GROUPS} g USING (run_id)
        ORDER BY o.op, o.bytes, o.parts""",
          **_groups({p: [r] for p, r in spikes.items()}))
    order = {p: i for i, p in enumerate(spikes)}
    return d.sort_values("provider", key=lambda c: c.map(order), kind="stable")


def spike_size_chart(ops, kinds):
    """Latency against size, log-log: the median per op and size, with a p10-p90 whisker; one
    panel per provider."""
    d = ops[ops.op.isin(kinds) & (ops.bytes > 0)]
    base = alt.Chart().encode(
        x=alt.X("mib:Q", title="size, MiB", scale=alt.Scale(type="log", base=2)),
        color=alt.Color("op:N", title=None, sort=list(kinds)))
    line = base.mark_line(point=True).encode(
        y=alt.Y("median_ms:Q", title="ms", scale=alt.Scale(type="log")),
        tooltip=["provider", "op", alt.Tooltip("mib:Q", format=".3g"), "n",
                 alt.Tooltip("median_ms:Q", format=".2f"), alt.Tooltip("p10_ms:Q", format=".2f"),
                 alt.Tooltip("p90_ms:Q", format=".2f")])
    spread = base.mark_rule(opacity=0.5).encode(y="p10_ms:Q", y2="p90_ms:Q")
    return (alt.layer(spread, line, data=d).properties(width=300, height=220)
            .facet(column=alt.Column("provider:N", title=None,
                                     sort=list(dict.fromkeys(d.provider))))
            .resolve_scale(y="independent"))


def spike_parts_chart(ops):
    """A GET of an appended object against its parts (1 to 1000): whole, its last 64 KiB and
    64 KiB from its middle; the median with a p10-p90 whisker, one panel per provider."""
    d = ops[ops.op.isin(["get_full", "get_tail", "get_mid"])]
    base = alt.Chart().encode(
        x=alt.X("parts:Q", title="parts of the object", scale=alt.Scale(type="log")),
        color=alt.Color("op:N", title=None))
    line = base.mark_line(point=True).encode(
        y=alt.Y("median_ms:Q", title="ms", scale=alt.Scale(type="log")),
        tooltip=["provider", "op", "parts", "n", alt.Tooltip("median_ms:Q", format=".2f")])
    spread = base.mark_rule(opacity=0.5).encode(y="p10_ms:Q", y2="p90_ms:Q")
    return (alt.layer(spread, line, data=d).properties(width=300, height=200)
            .facet(column=alt.Column("provider:N", title=None,
                                     sort=list(dict.fromkeys(d.provider))))
            .resolve_scale(y="independent"))


def model_check(groups, profiles):
    """Per provider ({provider: profile}, a measured_profiles or store_profiles name), each
    one-thread outermost phase of its runs: measured wall time against the model's (CPU +
    requests + transfer), in ms."""
    frames = []
    for provider, runs in groups.items():
        for run in runs:
            d = q("SELECT * FROM model_check($run, $profile)", run=run, profile=profiles[provider])
            d["provider"] = provider
            frames.append(d)
    d = pd.concat(frames, ignore_index=True)
    d["model_ms"] = d.cpu_ms + d.request_ms + d.transfer_ms
    d["what"] = d.phase.map({"batch": "write (batch)", **READS})
    return d.merge(q("SELECT arm, arm_label, ord FROM arm_meta"), on="arm")


# The sweeps' base point (configs/sweep-base.json) on S3 Express, for the pages base-point
# (results), base-point-model (calibration) and vectorized-reader (decisions). Two builds: Iceberg's
# row reader, which builds a Java object per row (until 2026-10-31, f51796a), and its vectorized
# reader, as Spark reads an Iceberg table (6f24575). Each build has a laptop twin (instruction
# counts) and an S3 Express run from a c7a.4xlarge, with the spike its VM ran just before; havoc's
# run of the row reader's build counts requests and bytes only (foreign load).

BP_OLD, BP_NEW = "row reader", "vectorized reader"
BP_BUILDS = [BP_OLD, BP_NEW]
BP_LAPTOP = {BP_OLD: "sweep-base-f970149711a9/20261030T054758Z",
             BP_NEW: "sweep-base-f970149711a9/20261031T083323Z"}
BP_CLOUD = {BP_OLD: "sweep-base-s3x-89a55338cd9a/20261030T205713Z",
            BP_NEW: "sweep-base-s3x-89a55338cd9a/20261031T164030Z"}
BP_SPIKE = {BP_OLD: "spike/s3x-spike-20261030T205211Z",
            BP_NEW: "spike/s3x-spike-20261031T163533Z"}
BP_HAVOC = "sweep-base-f970149711a9/20261030T055938Z"
# The vectorized reader's build on each store: the S3 Express run, and the Azure run from a
# D8as_v6 in westus (2026-11-01; its hypervisor exposes no instruction counters), each with its VM's
# spike
BP_AZURE = "sweep-base-azure-0c88452d93b4/20261031T234956Z"
BP_AZURE_SPIKE = "spike/azure-spike-20261031T234510Z"
BP_STORES = ["S3 Express", "Azure Blob"]
BP_STORE_RUNS = {"S3 Express": BP_CLOUD[BP_NEW], "Azure Blob": BP_AZURE}
BP_STORE_SPIKES = {"S3 Express": BP_SPIKE[BP_NEW], "Azure Blob": BP_AZURE_SPIKE}
# The phases compared: the 30 batches, and the full reads after the initial load under cached (one
# thread, each file's footer and deletes loaded once), RMoR-C's also under cached-skip
BP_JUDGED = """(phase = 'batch' OR (phase = 'read_full' AND step > 0
               AND (reader = 'cached' OR (arm = 'rmor-c' AND reader = 'cached-skip'))))"""
BP_WHAT = """CASE WHEN phase = 'batch' THEN 'a write'
                 WHEN reader = 'cached-skip' THEN 'a full read, skipping dead row groups'
                 ELSE 'a full read' END"""
BP_WHATS = ["a write", "a full read", "a full read, skipping dead row groups"]  # pages' order
# what a phase's own thread did, from phase_breakdown: decoding and encoding Parquet, the store
# (its client's CPU and waiting on it), and the rest (footers, deletes, other CPU and waiting)
BP_PARTS = ["decoding", "encoding", "other", "the store"]
# a write's steps, nested in its batch phase (README, Phases), and the rest of the batch
BP_STEPS = ["locate", "merge-read", "write", "delete-write", "rest"]
_BP_PART = """CASE component WHEN 'decode CPU' THEN 'decoding' WHEN 'encode CPU' THEN 'encoding'
                             WHEN 'store client CPU' THEN 'the store'
                             WHEN 'waiting on the store' THEN 'the store' ELSE 'other' END"""
# The results pages' parts (finer): Parquet's decoding and encoding; deletion vectors and footers
# (building, loading and merging them); rows and planning (the rest of the thread's CPU: making the
# rows a write encodes, merging the changes into the regions it rewrites and sorting them, and
# planning scans, RMoR's check of each row group against its deletion vector among them); the store
# (its client's CPU and waiting on it); and paused, off the CPU outside requests (garbage
# collection, the JIT, scheduling)
TIME_PARTS = ["decoding", "encoding", "deletion vectors and footers", "rows and planning", "the store",
         "paused (GC, JIT)"]
_PART = """CASE component WHEN 'decode CPU' THEN 'decoding' WHEN 'encode CPU' THEN 'encoding'
                          WHEN 'footer CPU' THEN 'deletion vectors and footers'
                          WHEN 'deletes CPU' THEN 'deletion vectors and footers'
                          WHEN 'other CPU' THEN 'rows and planning'
                          WHEN 'store client CPU' THEN 'the store'
                          WHEN 'waiting on the store' THEN 'the store'
                          ELSE 'paused (GC, JIT)' END"""


def _bp(sql, runs, spikes=None, key="build", **params):
    """sql once per case of runs (a build, or a store: BP_STORE_RUNS), with $run bound to its run
    (and $spike to its VM's spike, from spikes, where the sql takes one), tagged with the case in
    column `key` and with the arms' labels."""
    spikes = BP_SPIKE if spikes is None else spikes
    frames = []
    for b, run in runs.items():
        p = {"run": run, "spike": spikes.get(b), **params}
        frames.append(q(sql, **{k: v for k, v in p.items() if f"${k}" in sql}).assign(**{key: b}))
    labels = q("SELECT arm, arm_label, ord FROM arm_meta")
    return pd.concat(frames, ignore_index=True).merge(labels, on="arm")


def bp_sorted(df, *by):
    """df in BP_WHATS' order of its kinds of phase (writes first; their names sort otherwise),
    then by `by`."""
    return df.assign(_w=df.what.map(BP_WHATS.index)).sort_values(["_w", *by]).drop(columns="_w")


def bp_times(runs=BP_CLOUD, key="build"):
    """Per case (build or store), arm and compared phase: the mean wall time (s) and what it sent
    and moved."""
    return _bp(f"""
        SELECT arm, {BP_WHAT} AS what, count(*) AS n, avg(wall_ns) / 1e9 AS s,
               avg(requests) AS requests, avg(gets) AS gets, avg(puts + appends) AS writes,
               avg(bytes_read) / 1e6 AS mb_read, avg(bytes_written) / 1e6 AS mb_written,
               avg(instructions) / 1e9 AS g_instructions
        FROM steps WHERE run_id = $run AND {BP_JUDGED} GROUP BY ALL""", runs, key=key)


def bp_parts(runs=BP_CLOUD, key="build", fine=False):
    """Per case, arm, compared phase and part (BP_PARTS, or TIME_PARTS with fine): the mean seconds its
    own thread spent."""
    return _bp(f"""
        WITH s AS (SELECT arm, step, phase, reader, {_PART if fine else _BP_PART} AS part,
                          sum(ms) AS ms
                   FROM phase_breakdown WHERE run_id = $run AND thread = 'phase' AND {BP_JUDGED}
                   GROUP BY ALL)
        SELECT arm, {BP_WHAT} AS what, part, avg(ms) / 1e3 AS s FROM s GROUP BY ALL""", runs, key=key)


def bp_spread(runs=BP_CLOUD, key="build"):
    """Per case, arm and compared phase: the mean wall time (s), the 10th and 90th percentiles over
    its phases, and the first and the last of them (for a full read, the read after the fewest and
    the most writes)."""
    return _bp(f"""
        SELECT arm, {BP_WHAT} AS what, count(*) AS n, avg(wall_ns) / 1e9 AS s,
               quantile_cont(wall_ns, 0.1) / 1e9 AS p10, quantile_cont(wall_ns, 0.9) / 1e9 AS p90,
               arg_min(wall_ns, step) / 1e9 AS first_s, arg_max(wall_ns, step) / 1e9 AS last_s,
               min(step) AS first_step, max(step) AS last_step
        FROM steps WHERE run_id = $run AND {BP_JUDGED} GROUP BY ALL""", runs, key=key)


def bp_steps(runs=BP_CLOUD, key="build"):
    """Per case, arm, write step (locate, merge-read, write, delete-write, and the rest of the
    batch: committing it) and part (TIME_PARTS): the mean seconds the write's own thread spent."""
    return _bp(f"""
        WITH s AS (SELECT arm, step, phase, {_PART} AS part, sum(ms) AS ms
                   FROM phase_breakdown
                   WHERE run_id = $run AND thread = 'phase' AND reader IS NULL
                     AND phase IN ('batch', 'locate', 'merge-read', 'write', 'delete-write')
                   GROUP BY ALL),
             rest AS (SELECT arm, step, 'rest' AS phase, part,
                             sum(CASE WHEN phase = 'batch' THEN ms ELSE -ms END) AS ms
                      FROM s GROUP BY ALL),
             -- a mean over every batch: some batches have no merge-read or delete-write
             n AS (SELECT arm, count(DISTINCT step) AS batches FROM s WHERE phase = 'batch'
                   GROUP BY ALL)
        SELECT arm, phase AS write_step, part, sum(ms) / any_value(batches) / 1e3 AS s
        FROM (SELECT * FROM s WHERE phase <> 'batch' UNION ALL BY NAME SELECT * FROM rest)
             JOIN n USING (arm)
        GROUP BY ALL""", runs, key=key)


def bp_series(runs=BP_CLOUD, key="build"):
    """Per case, arm and compared phase, each one's wall time (s) by step: every write (steps 1 to
    30) and every full read, from the one after the initial load (step 0)."""
    return _bp(f"""
        SELECT arm, {BP_WHAT} AS what, step, wall_ns / 1e9 AS s
        FROM steps WHERE run_id = $run AND (phase = 'batch' OR (phase = 'read_full'
              AND (reader = 'cached' OR (arm = 'rmor-c' AND reader = 'cached-skip'))))""",
               runs, key=key)


def bp_model(runs=BP_CLOUD, spikes=None, key="build"):
    """Per case, arm and compared phase: measured wall time against the model (the thread's CPU
    plus its requests priced at their sizes with the VM's spike, lab/views/65_sized.sql), in s."""
    d = _bp(f"""
        SELECT arm, {BP_WHAT} AS what, count(*) AS n, avg(measured_ms) / 1e3 AS measured_s,
               avg(cpu_ms) / 1e3 AS cpu_s, avg(request_ms) / 1e3 AS request_s
        FROM sized_latency($run, $spike) WHERE {BP_JUDGED} GROUP BY ALL""", runs, spikes, key)
    d["simulated_s"] = d.cpu_s + d.request_s
    d["sim_over_measured"] = d.simulated_s / d.measured_s
    return d


def bp_over_mor(times, key="build"):
    """bp_times' mean wall time of each arm but MoR-v3 over MoR-v3's in the same case and kind of
    phase (a skipping read over MoR-v3's full read)."""
    mor = times[times.arm == "mor-v3"].set_index([key, "what"]).s
    d = times[times.arm != "mor-v3"].copy()
    d["mor_v3_s"] = [mor[(b, "a write" if w == "a write" else "a full read")]
                     for b, w in zip(d[key], d.what)]
    d["over_mor_v3"] = d.s / d.mor_v3_s
    return d


def bp_storage(runs=BP_CLOUD, key="build"):
    """Per case and arm: the bytes stored and live after the last batch (GB)."""
    return _bp("""
        SELECT arm, arg_max(stored_bytes, step) / 1e9 AS stored_gb,
               arg_max(live_bytes, step) / 1e9 AS live_gb
        FROM steps WHERE run_id = $run AND phase = 'state' GROUP BY ALL""", runs, key=key)


def bp_parts_chart(parts, rows="build"):
    """Stacked bars of bp_parts: seconds per arm by part, a panel for writes and one for full
    reads (RMoR-C's read skipping dead row groups a bar of its own there, "RMoR-C (skipping)"),
    each with a row per `rows` value (None: one row) on one time axis, so the rows compare."""
    parts = parts.copy()
    skip = parts.what == BP_WHATS[2]
    parts.loc[skip, "arm_label"] = parts.loc[skip, "arm_label"] + " (skipping)"
    parts.loc[skip, "what"] = BP_WHATS[1]
    arms = list(parts.sort_values(["ord", "arm_label"]).arm_label.unique())

    def panel(what):
        d = parts[parts.what == what]
        chart = alt.Chart(d).mark_bar().encode(
            x=alt.X("s:Q", title="seconds, mean phase", stack="zero"),
            y=alt.Y("arm_label:N", title=None, sort=[a for a in arms if a in set(d.arm_label)]),
            color=alt.Color("part:N", title=None, sort=BP_PARTS),
            order=alt.Order("part:N", sort="ascending"),
            tooltip=[*([rows] if rows else []), "arm_label", "what", "part",
                     alt.Tooltip("s:Q", format=".2f")],
        ).properties(width=230)
        if rows:
            order = {"build": BP_BUILDS, "store": BP_STORES}.get(rows, "ascending")
            chart = chart.facet(row=alt.Row(f"{rows}:N", title=None, sort=order))
        return chart.properties(title=what)

    return alt.hconcat(*[panel(w) for w in BP_WHATS[:2]]).resolve_scale(x="independent",
                                                                        y="independent")


# The sweeps in the cloud (docs/HYPOTHESES.md, The sweeps in the cloud; pages cloud-sweep, results,
# and cloud-sweep-model, calibration): each sweep configuration on S3 Express (a VM each, nine at
# once; sweep-rg64m on sweep-p30's VM after it) and on Azure Blob (one VM, in turn), each after its
# VM's spike; each configuration's twin on its S3 Express VM (a local store, the same build and
# cores, with instruction counts). The base point's are the base point's runs (BP_*).
CS_STORES = ["S3 Express", "Azure Blob"]
CS_SUFFIX = {"S3 Express": "s3x", "Azure Blob": "azure"}
# Each configuration's setting on its axis; the base point lies on the first four axes
CS_AXES = {"update share": "p", "region skew": "s", "row group (MiB)": "MiB",
           "lateness, p99 (batches)": "batches"}
CS_POINTS = [  # (configuration, axis, value)
    ("sweep-base", "update share", 0.03), ("sweep-p10", "update share", 0.1),
    ("sweep-p30", "update share", 0.3),
    ("sweep-s0", "region skew", 0.0), ("sweep-base", "region skew", 1.0),
    ("sweep-s2", "region skew", 2.0),
    ("sweep-base", "row group (MiB)", 16), ("sweep-rg32m", "row group (MiB)", 32),
    ("sweep-rg64m", "row group (MiB)", 64),
    ("sweep-late99-1", "lateness, p99 (batches)", 1), ("sweep-base", "lateness, p99 (batches)", 5),
    ("sweep-late99-20", "lateness, p99 (batches)", 20),
]
# The other workloads: the ingest window (recency keys), with region statements or a row a change
CS_WORKLOADS = {"sweep-base": "region statements (the base point)",
                "sweep-ingest": "the ingest window, statements",
                "sweep-ingest-rows": "the ingest window, a row a change"}
CS_CONFIGS = ["sweep-base", "sweep-p10", "sweep-p30", "sweep-s0", "sweep-s2", "sweep-rg32m",
              "sweep-rg64m", "sweep-late99-1", "sweep-late99-20", "sweep-ingest",
              "sweep-ingest-rows"]


def cs_runs():
    """The cloud runs as the lab finds them, per store and configuration: (run, its VM's spike). The
    newest run of each configuration's cloud label, and the newest spike on its host that started
    before it. The pages read CS_RUNS, these pinned."""
    d = q("""
        WITH r AS (SELECT run_id, label, host, started_at FROM runs
                   WHERE regexp_matches(label, '^sweep-.*-(s3x|azure)$') AND status = 'finished'
                   QUALIFY row_number() OVER (PARTITION BY label ORDER BY started_at DESC) = 1)
        SELECT r.run_id, r.label,
               (SELECT s.run_id FROM runs s WHERE s.run_id LIKE 'spike/%' AND s.host = r.host
                  AND s.started_at <= r.started_at ORDER BY s.started_at DESC LIMIT 1) AS spike
        FROM r""")
    out = {s: {} for s in CS_STORES}
    for store in CS_STORES:
        suffix = "-" + CS_SUFFIX[store]
        for _, row in d[d.label.str.endswith(suffix)].iterrows():
            out[store][row.label[: -len(suffix)]] = (row.run_id, row.spike)
    return out


def cs_frame(fn, runs=None, **kw):
    """fn (bp_times, bp_parts, bp_model, bp_storage) over every cloud run, tagged with the store
    and the configuration (column `config`)."""
    runs = CS_RUNS if runs is None else runs
    frames = []
    for store, by in runs.items():
        if not by:
            continue
        r = {c: v[0] for c, v in by.items()}
        if fn is bp_model:
            frames.append(fn(r, {c: v[1] for c, v in by.items()}, key="config", **kw)
                          .assign(store=store))
        else:
            frames.append(fn(r, key="config", **kw).assign(store=store))
    return pd.concat(frames, ignore_index=True)


def cs_over_mor(times):
    """cs_frame(bp_times)'s mean wall time of each arm over MoR-v3's in the same store,
    configuration and kind of phase."""
    return pd.concat([bp_over_mor(t, key="config") for _, t in times.groupby("store")],
                     ignore_index=True)


def cs_axes(df):
    """df's rows of each configuration on an axis, once per axis it lies on (the base point on four),
    with the axis and the setting (`value`)."""
    pts = pd.DataFrame(CS_POINTS, columns=["config", "axis", "value"])
    return df.merge(pts, on="config")


# Each configuration's setting, named on its own (CS_CONFIGS' order)
CS_SETTING = {"sweep-base": "base point", "sweep-p10": "update share 0.1",
              "sweep-p30": "update share 0.3", "sweep-s0": "region skew 0",
              "sweep-s2": "region skew 2", "sweep-rg32m": "row groups 32 MiB",
              "sweep-rg64m": "row groups 64 MiB", "sweep-late99-1": "lateness p99 1 write",
              "sweep-late99-20": "lateness p99 20 writes",
              "sweep-ingest": "ingest window, statements",
              "sweep-ingest-rows": "ingest window, a row a change"}
CS_SETTINGS = [CS_SETTING[c] for c in CS_CONFIGS]
# The write's steps as the charts name them (BP_STEPS)
CS_STEP = {"locate": "locate", "merge-read": "merge-read", "write": "write",
           "delete-write": "delete-write", "rest": "the rest"}

# The designs the results pages show (base-point, cloud-sweep), in their order: the name, the arm,
# whether its full read skips the row groups its deletes cover whole (the cached-skip reader), its
# color and shape, and whether it is one of the main four. The other two are the pages' appendix:
# TailCoW, a control (MoR with RMoR's tail of inserts), and RMoR read by a reader that fetches the
# row groups its deletion vectors cover whole. The main four's colors are slots of the lab's
# palette (the dataviz reference's) that pass its validator on every pair; the two RMoW designs are
# its two greens.
DESIGNS = pd.DataFrame([
    ("MoR", "mor-v3", False, "#2a78d6", "circle", True),
    ("RMoR", "rmor-c", True, "#eda100", "triangle-up", True),
    ("RMoW-external", "rmor-c-footer", False, "#008300", "square", True),
    ("RMoW-append", "rmow-c", False, "#1baf7a", "diamond", True),
    ("TailCoW", "tailcow-c", False, "#eb6834", "triangle-down", False),
    ("RMoR without skipping", "rmor-c", False, "#eda100", "cross", False),
], columns=["design", "arm", "skipping", "color", "shape", "main"])
MAIN = list(DESIGNS[DESIGNS.main].design)
REGION_DESIGNS = MAIN[1:]
WRITE, READ = BP_WHATS[:2]
# Where the time goes (TIME_PARTS): the work on Parquet's format in shades of one violet, rows and
# planning in orange (TailCoW's, which no results chart draws), the store in gray and pauses in a
# lighter gray, so that no part shares a color with a main design
PART_COLORS = {"decoding": "#3b2ca6", "encoding": "#7d6fdc", "deletion vectors and footers": "#c4bdf0",
               "rows and planning": "#eb6834", "the store": "#7f8a93",
               "paused (GC, JIT)": "#c9ced3"}
TIME_TITLE = {"a write": "write latency", "a full read": "read latency"}


def designs(df, main=True):
    """df (from a bp_* function or cs_frame: rows per arm, with `what` in BP_WHATS, or without it
    for writes alone) per design: `design` (and `arm_label`) its name, `ord` its place, and `what`
    a write or a full read. RMoR's full read is the one skipping dead row groups; its other is that
    of "RMoR without skipping", which has RMoR's write. Only the main four with `main`."""
    out = []
    for i, d in enumerate(DESIGNS.itertuples()):
        if main and not d.main:
            continue
        rows = df[df.arm == d.arm]
        if "what" in rows:
            read = BP_WHATS[2] if d.skipping else READ
            rows = rows[rows.what.isin([WRITE, read])]
            rows = rows.assign(what=rows.what.replace(BP_WHATS[2], READ))
        out.append(rows.assign(design=d.design, arm_label=d.design, ord=i))
    return pd.concat(out, ignore_index=True)


def design_encoding(names=None, title="design"):
    """Color and shape by design (field `design`), for `names` (default: the main four)."""
    d = DESIGNS.set_index("design").loc[MAIN if names is None else names]
    dom = list(d.index)
    return (alt.Color("design:N", title=title, sort=dom,
                      scale=alt.Scale(domain=dom, range=list(d.color))),
            alt.Shape("design:N", title=title, sort=dom,
                      scale=alt.Scale(domain=dom, range=list(d["shape"]))))


def part_color(title=None):
    return alt.Color("part:N", title=title, sort=TIME_PARTS,
                     scale=alt.Scale(domain=TIME_PARTS, range=[PART_COLORS[p] for p in TIME_PARTS]))


def _part_order(d):
    """d with `part_ord`, its part's place in TIME_PARTS, to stack the parts in the legend's order."""
    return d.assign(part_ord=d.part.map(TIME_PARTS.index))


def over_mor(df, keys, own=None):
    """designs(df) without MoR, with `mor_s`, MoR's mean time `s` in the same `keys` and kind of
    phase, and `over_mor`, the design's over it. With `own`, designs() of the frame RMoR's rows
    came from (rmor_spliced), RMoR's (with and without skipping) are over that frame's MoR: the MoR
    of its own run."""
    keys = list(keys) + ["what"]

    def mor(f):
        return f[f.design == "MoR"][keys + ["s"]].rename(columns={"s": "mor_s"})

    d = df[df.design != "MoR"]
    if own is None:
        d = d.merge(mor(df), on=keys)
    else:
        r = d.arm == "rmor-c"
        d = pd.concat([d[~r].merge(mor(df), on=keys), d[r].merge(mor(own), on=keys)],
                      ignore_index=True)
    d["over_mor"] = d.s / d.mor_s
    return d


def tradeoff(spread, keys=("store", "config")):
    """designs(spread) (spread: bp_spread's, or cs_frame's of it) as one point per `keys` and
    design: the mean write and full read (s), the writes' 10th and 90th percentiles, and the full
    reads after the fewest and the most writes (`setting` named where `keys` has `config`)."""
    keys = list(keys)
    d = designs(spread)
    w = (d[d.what == WRITE][keys + ["design", "s", "p10", "p90"]]
         .rename(columns={"s": "write_s", "p10": "write_p10", "p90": "write_p90"}))
    r = (d[d.what == READ]
         .rename(columns={"s": "read_s", "first_s": "read_first", "last_s": "read_last"}))
    out = r.merge(w, on=keys + ["design"])[
        keys + ["design", "ord", "write_s", "write_p10", "write_p90", "read_s", "read_first",
                "read_last", "first_step", "last_step"]]
    if "config" in out:
        out.insert(len(keys), "setting", out.config.map(CS_SETTING))
    return out


TRADEOFF_TOOLTIP = [
    alt.Tooltip("write_s:Q", title="write, mean (s)", format=".2f"),
    alt.Tooltip("write_p10:Q", title="write, p10 (s)", format=".2f"),
    alt.Tooltip("write_p90:Q", title="write, p90 (s)", format=".2f"),
    alt.Tooltip("read_s:Q", title="full read, mean (s)", format=".2f"),
    alt.Tooltip("read_first:Q", title="full read after 5 writes (s)", format=".2f"),
    alt.Tooltip("read_last:Q", title="full read after 30 writes (s)", format=".2f")]


def _tradeoff_layers(d, x, y, x_lo, y_lo, color, shape, tooltip):
    """A point per design at its means, a whisker across its writes' p10 to p90 and one up from its
    full read after 5 writes to the one after 30."""
    base = alt.Chart(d).encode(color=color)
    across = base.mark_rule(strokeWidth=1.5, opacity=0.55).encode(x=x_lo, x2="write_p90:Q", y=y)
    up = base.mark_rule(strokeWidth=1.5, opacity=0.55).encode(x=x, y=y_lo, y2="read_last:Q")
    point = base.mark_point(filled=True, size=70, opacity=1, stroke="white",
                            strokeWidth=0.8).encode(x=x, y=y, shape=shape, tooltip=tooltip)
    return [across, up, point]


def bp_tradeoff_chart(pts, store, color, shape, x_max, y_max):
    """tradeoff(bp_spread(..., key="store"), ["store"])'s points for one store, on linear scales from
    0 to x_max and y_max (pass the same to both stores), each design labeled beside its point."""
    xs, ys = (alt.Scale(domain=[0, m], nice=False) for m in (x_max, y_max))
    x = alt.X("write_s:Q", title="write latency (s)", scale=xs)
    y = alt.Y("read_s:Q", title="read latency (s)", scale=ys)
    d = pts[pts.store == store]
    layers = _tradeoff_layers(d, x, y, alt.X("write_p10:Q", scale=xs),
                              alt.Y("read_first:Q", scale=ys), color, shape,
                              ["design", *TRADEOFF_TOOLTIP])
    # a label above each point, but below RMoW-external's, which sits under RMoR's; right-aligned
    # in the right half
    below = d.design == "RMoW-external"
    right = d.write_s > 0.55 * x_max
    for sel, dy in ((~below, -12), (below, 16)):
        for side, align, dx in ((~right, "left", 7), (right, "right", -7)):
            layers.append(alt.Chart(d[sel & side]).mark_text(
                align=align, dx=dx, dy=dy, fontSize=12, fontWeight="bold").encode(
                x=x, y=y, text="design:N", color=color))
    return alt.layer(*layers).properties(width=420, height=320, title=store)


def _log_axis(domain, ticks=(0.5, 1, 2, 5, 10, 20, 50, 100, 200)):
    return (alt.Scale(type="log", domain=domain, nice=False),
            alt.Axis(values=[t for t in ticks if domain[0] <= t <= domain[1]], format="~g"))


def cs_tradeoff_panels(pts, be, store, color, shape, width=200, row=30):
    """One store's trade-off at every setting, in three panels on one row per setting (CS_SETTINGS,
    the designs side by side within it): each design's mean write (s), with a whisker from its
    writes' 10th to 90th percentile; its mean full read (s), with one from its full read after 5
    writes to the one after 30; and the full reads per write that repay each region design's write
    against MoR's (be: BE's frame, `reads` per store, config and design, infinite where its read
    saves nothing, drawn as "never" at the panel's right). pts: tradeoff(cs_frame(bp_spread)). Each
    panel's scale fits this store's values."""
    d = pts[pts.store == store].assign(setting=lambda x: x.config.map(CS_SETTING))
    b = be[be.store == store].assign(setting=lambda x: x.config.map(CS_SETTING))
    height = row * len(CS_SETTINGS)
    y = alt.Y("setting:N", title=None, sort=CS_SETTINGS, axis=alt.Axis(labelLimit=220))
    y_bare = alt.Y("setting:N", title=None, sort=CS_SETTINGS, axis=None)
    yo = alt.YOffset("design:N", sort=MAIN, scale=alt.Scale(domain=MAIN, paddingOuter=0.6))
    # every other setting's row shaded, so a row reads across the three panels
    shade = alt.Chart(pd.DataFrame({"setting": CS_SETTINGS[1::2]})).mark_rect(
        color="#eef0f2").encode(x=alt.value(0), x2=alt.value(width))

    def log_scale(lo, hi):
        lo = 2 ** np.floor(np.log2(lo * 0.9))
        hi = 2 ** np.ceil(np.log2(hi * 1.1))
        return _log_axis([lo, hi], ticks=(0.5, 1, 2, 4, 8, 16, 32, 64, 128))

    def panel(data, mean, lo, hi, title, x_title, scale, axis, ytick, tooltip, extra=()):
        x = alt.X(f"{mean}:Q", title=x_title, scale=scale, axis=axis)
        base = alt.Chart(data).encode(y=ytick, yOffset=yo, color=color)
        layers = [shade.encode(y=ytick)]
        if lo:
            layers.append(base.mark_rule(strokeWidth=1.5, opacity=0.55).encode(
                x=alt.X(f"{lo}:Q", scale=scale), x2=f"{hi}:Q"))
        layers.append(base.mark_point(filled=True, size=45, opacity=1, stroke="white",
                                      strokeWidth=0.6).encode(x=x, shape=shape, tooltip=tooltip))
        return alt.layer(*layers, *extra).properties(
            width=width, height=height, title=alt.Title(title, anchor="start", fontSize=12))

    ws, wa = log_scale(d.write_p10.min(), d.write_p90.max())
    lo, hi = d.read_first.min(), d.read_last.max()
    pad = 0.05 * (hi - lo)
    rs = alt.Scale(domain=[np.floor(lo - pad), np.ceil(hi + pad)], nice=False, zero=False)
    finite = b[np.isfinite(b.reads)]
    bs, ba = log_scale(finite.reads.min(), finite.reads.max())
    never = b[~np.isfinite(b.reads)].assign(x=bs.domain[1])
    tip = ["setting", "design"]
    write = panel(d, "write_s", "write_p10", "write_p90", "Write latency", "seconds (log scale)",
                  ws, wa, y, tip + TRADEOFF_TOOLTIP[:3])
    read = panel(d, "read_s", "read_first", "read_last", "Read latency", "seconds", rs,
                 alt.Axis(), y_bare, tip + TRADEOFF_TOOLTIP[3:])
    never_text = alt.Chart(never).mark_text(text="never", align="right", dx=-2, fontSize=10).encode(
        x=alt.X("x:Q", scale=bs), y=y_bare, yOffset=yo, color=color, tooltip=tip)
    pays = panel(finite, "reads", None, None, "Full reads per write that repay it",
                 "against MoR (log scale)", bs, ba, y_bare,
                 tip + [alt.Tooltip("reads:Q", format=".1f")], extra=[never_text])
    return alt.hconcat(write, read, pays, spacing=14,
                       title=alt.Title(store, anchor="start", fontSize=15))


def parts_chart(parts, what, rows=None, row_sort=None, width=220):
    """Stacked bars of designs(...bp_parts...) for one kind of phase (WRITE or READ): mean seconds
    per design by part, a column per store and a row per value of `rows` (sorted by row_sort; none
    by default), on one time axis."""
    d = _part_order(parts[parts.what == what])
    tip = [c for c in ("setting", "store") if c in d] + ["design", "part"]
    chart = alt.Chart(d).mark_bar().encode(
        x=alt.X("s:Q", title=f"mean {TIME_TITLE[what]} (s)", stack="zero"),
        y=alt.Y("design:N", title=None, sort=MAIN),
        color=part_color(),
        order=alt.Order("part_ord:Q", sort="ascending"),
        tooltip=[*tip, alt.Tooltip("s:Q", format=".2f")]).properties(width=width,
                                                                       height=alt.Step(13))
    facets = dict(column=alt.Column("store:N", title=None, sort=CS_STORES,
                                    header=alt.Header(labelFontSize=12)))
    if rows:
        facets["row"] = alt.Row(f"{rows}:N", title=None, sort=row_sort,
                                header=alt.Header(labelAngle=0, labelAlign="left",
                                                  labelFontSize=11))
    return chart.facet(**facets)


def steps_chart(steps):
    """Stacked bars of designs(...bp_steps...): a write's mean seconds per design by part, a column
    per step of the write and a row per store, on one time axis. With several configurations
    (column `config`), one at a time, chosen in a dropdown (the base point first)."""
    d = _part_order(steps.assign(step=steps.write_step.map(CS_STEP)))
    many = "config" in d and d.config.nunique() > 1
    if many:
        d["setting"] = d.config.map(CS_SETTING)
    top = d[d.s > 0].groupby([c for c in ("store", "config") if c in d]
                             + ["design", "write_step"]).s.sum().max()
    chart = alt.Chart(d).mark_bar().encode(
        x=alt.X("s:Q", title="mean write latency (s)", stack="zero",
                scale=alt.Scale(domain=[0, 1.02 * top], nice=False)),
        y=alt.Y("design:N", title=None, sort=MAIN),
        color=part_color(),
        order=alt.Order("part_ord:Q", sort="ascending"),
        tooltip=[c for c in ("setting", "store") if c in d] + ["design", "step", "part",
                                                               alt.Tooltip("s:Q", format=".2f")])
    setting = None
    if many:
        options = [s for s in CS_SETTINGS if s in set(d.setting)]
        setting = alt.param(name="setting", value=options[0],
                            bind=alt.binding_select(options=options, name="setting "))
        chart = chart.transform_filter(alt.datum.setting == setting)
    chart = (chart.properties(width=150, height=alt.Step(13))
             .facet(row=alt.Row("store:N", title=None, sort=CS_STORES),
                    column=alt.Column("step:N", title=None, sort=list(CS_STEP.values()))))
    return chart.add_params(setting) if setting is not None else chart


def latency_chart(series, width=300, height=170):
    """designs(...bp_series...) as lines over the snapshots a run adds, one per design: write
    latency at each write, and read latency after the initial load and every 5 writes; a graph for
    each, with a column per store on one latency axis. With several configurations (column
    `config`), one at a time, chosen in a dropdown (the base point first)."""
    d = series.copy()
    many = "config" in d and d.config.nunique() > 1
    if many:
        d["setting"] = d.config.map(CS_SETTING)
    color, shape = design_encoding()
    tip = [c for c in ("setting", "store") if c in d] + ["design", "step",
                                                          alt.Tooltip("s:Q", format=".2f")]
    setting = None
    if many:
        options = [x for x in CS_SETTINGS if x in set(d.setting)]
        setting = alt.param(name="setting", value=options[0],
                            bind=alt.binding_select(options=options, name="setting "))

    def graph(what, point):
        c = alt.Chart(d[d.what == what]).mark_line(strokeWidth=2, point=point).encode(
            x=alt.X("step:Q", title="snapshots (writes since the initial load)",
                    scale=alt.Scale(domain=[0, 30], nice=False), axis=alt.Axis(values=list(range(0, 31, 5)))),
            y=alt.Y("s:Q", title=f"{TIME_TITLE[what]} (s)"),
            color=color, shape=shape, detail="design:N", tooltip=tip)
        if many:
            c = c.transform_filter(alt.datum.setting == setting)
        return (c.properties(width=width, height=height)
                .facet(column=alt.Column("store:N", title=None, sort=CS_STORES))
                .properties(title=alt.Title(TIME_TITLE[what].capitalize(), anchor="start")))

    graphs = [graph(WRITE, alt.OverlayMarkDef(size=18)), graph(READ, alt.OverlayMarkDef(size=50))]
    # the dropdown on the whole: add_params on a vconcat of facets fails in Altair 6
    return alt.vconcat(*graphs, params=[setting.param]) if many else alt.vconcat(*graphs)


# Each configuration's twin: its run with a local store on its S3 Express VM (the deploy's twin
# step, 2026-11-01), but the base point's, which is the laptop's on the same build; and havoc's run
# of each configuration (campaign sweep, 2026-10-30), whose requests and bytes the twins match
CS_TWINS = {
    "sweep-base": "sweep-base-f970149711a9/20261031T083323Z",
    "sweep-p10": "sweep-p10-782c1a9d5c5b/20261031T235849Z",
    "sweep-p30": "sweep-p30-5176e9860321/20261031T235835Z",
    "sweep-s0": "sweep-s0-8c50119b643c/20261031T235816Z",
    "sweep-s2": "sweep-s2-ffc470cde349/20261031T235819Z",
    "sweep-rg32m": "sweep-rg32m-0f8daa28524c/20261031T235822Z",
    "sweep-rg64m": "sweep-rg64m-8adf7c79c2bd/20261101T002125Z",
    "sweep-late99-1": "sweep-late99-1-413431114ea5/20261031T235825Z",
    "sweep-late99-20": "sweep-late99-20-3fbd6c1857d4/20261031T235828Z",
    "sweep-ingest": "sweep-ingest-cf4263f112e8/20261031T235831Z",
    "sweep-ingest-rows": "sweep-ingest-rows-7b5f4987c299/20261031T235834Z",
}
CS_HAVOC = {
    "sweep-base": "sweep-base-f970149711a9/20261030T055938Z",
    "sweep-p10": "sweep-p10-782c1a9d5c5b/20261030T123617Z",
    "sweep-p30": "sweep-p30-5176e9860321/20261030T124134Z",
    "sweep-s0": "sweep-s0-8c50119b643c/20261030T023853Z",
    "sweep-s2": "sweep-s2-ffc470cde349/20261030T085227Z",
    "sweep-rg32m": "sweep-rg32m-0f8daa28524c/20261030T023851Z",
    "sweep-rg64m": "sweep-rg64m-8adf7c79c2bd/20261030T023850Z",
    "sweep-late99-1": "sweep-late99-1-413431114ea5/20261030T080602Z",
    "sweep-late99-20": "sweep-late99-20-3fbd6c1857d4/20261030T085025Z",
    "sweep-ingest": "sweep-ingest-cf4263f112e8/20261030T112555Z",
    "sweep-ingest-rows": "sweep-ingest-rows-7b5f4987c299/20261030T023852Z",
}
# The registered predictions (docs/HYPOTHESES.md, The sweeps in the cloud): each design's mean
# write over MoR-v3's, per store and configuration, and RMoW-C's mean full read ("rmow-c read")
CS_PRED = {
    "S3 Express": {
        "sweep-base": {"rmow-c": 4.43, "rmor-c": 9.39, "rmor-c-footer": 8.7, "rmow-c read": 0.87},
        "sweep-ingest": {"rmow-c": 3.6, "rmor-c": 11.94, "rmor-c-footer": 12.16, "rmow-c read": 0.88},
        "sweep-ingest-rows": {"rmow-c": 6.72, "rmor-c": 11.01, "rmor-c-footer": 11.23, "rmow-c read": 0.88},
        "sweep-late99-1": {"rmow-c": 4.48, "rmor-c": 6.89, "rmor-c-footer": 6.09, "rmow-c read": 0.86},
        "sweep-late99-20": {"rmow-c": 4.52, "rmor-c": 11.02, "rmor-c-footer": 10.3, "rmow-c read": 0.86},
        "sweep-p10": {"rmow-c": 3.07, "rmor-c": 7.34, "rmor-c-footer": 6.63, "rmow-c read": 0.88},
        "sweep-p30": {"rmow-c": 2.3, "rmor-c": 3.6, "rmor-c-footer": 3.37, "rmow-c read": 0.89},
        "sweep-rg32m": {"rmow-c": 5.73, "rmor-c": 11.17, "rmor-c-footer": 10.37, "rmow-c read": 0.86},
        "sweep-rg64m": {"rmow-c": 8.97, "rmor-c": 13.97, "rmor-c-footer": 13.19, "rmow-c read": 0.86},
        "sweep-s0": {"rmow-c": 4.78, "rmor-c": 11.41, "rmor-c-footer": 10.65, "rmow-c read": 0.86},
        "sweep-s2": {"rmow-c": 3.86, "rmor-c": 5.19, "rmor-c-footer": 4.42, "rmow-c read": 0.88},
    },
    "Azure Blob": {
        "sweep-base": {"rmow-c": 2.79, "rmor-c": 5.97, "rmor-c-footer": 5.73, "rmow-c read": 0.89},
        "sweep-ingest": {"rmow-c": 2.92, "rmor-c": 9.44, "rmor-c-footer": 9.96, "rmow-c read": 0.91},
        "sweep-ingest-rows": {"rmow-c": 4.62, "rmor-c": 7.4, "rmor-c-footer": 7.82, "rmow-c read": 0.9},
        "sweep-late99-1": {"rmow-c": 2.77, "rmor-c": 4.38, "rmor-c-footer": 3.99, "rmow-c read": 0.89},
        "sweep-late99-20": {"rmow-c": 2.8, "rmor-c": 6.88, "rmor-c-footer": 6.65, "rmow-c read": 0.89},
        "sweep-p10": {"rmow-c": 1.89, "rmor-c": 4.72, "rmor-c-footer": 4.4, "rmow-c read": 0.91},
        "sweep-p30": {"rmow-c": 1.56, "rmor-c": 2.71, "rmor-c-footer": 2.6, "rmow-c read": 0.92},
        "sweep-rg32m": {"rmow-c": 3.52, "rmor-c": 6.8, "rmor-c-footer": 6.55, "rmow-c read": 0.88},
        "sweep-rg64m": {"rmow-c": 5.79, "rmor-c": 8.79, "rmor-c-footer": 8.63, "rmow-c read": 0.81},
        "sweep-s0": {"rmow-c": 2.98, "rmor-c": 7.13, "rmor-c-footer": 6.91, "rmow-c read": 0.89},
        "sweep-s2": {"rmow-c": 2.3, "rmor-c": 3.24, "rmor-c-footer": 2.83, "rmow-c read": 0.91},
    },
}
# The cloud runs, per store and configuration: (run, the spike its VM ran just before it)
CS_RUNS = {
    "S3 Express": {
        "sweep-base": ("sweep-base-s3x-89a55338cd9a/20261031T164030Z",
                       "spike/s3x-spike-20261031T163533Z"),
        "sweep-p10": ("sweep-p10-s3x-0eef50acb2c6/20261101T013642Z",
                      "spike/s3x-spike-sweep-p10-20261101T013138Z"),
        "sweep-p30": ("sweep-p30-s3x-77960a5f0c9d/20261101T013644Z",
                      "spike/s3x-spike-sweep-p30-20261101T013142Z"),
        "sweep-s0": ("sweep-s0-s3x-bd1294996d6b/20261101T013649Z",
                     "spike/s3x-spike-sweep-s0-20261101T013146Z"),
        "sweep-s2": ("sweep-s2-s3x-be6059984bfd/20261101T013648Z",
                     "spike/s3x-spike-sweep-s2-20261101T013150Z"),
        "sweep-rg32m": ("sweep-rg32m-s3x-3ffd1de9950a/20261101T013653Z",
                        "spike/s3x-spike-sweep-rg32m-20261101T013154Z"),
        "sweep-rg64m": ("sweep-rg64m-s3x-e73e2f618730/20261101T024227Z",
                        "spike/s3x-spike-sweep-p30-20261101T023726Z"),
        "sweep-late99-1": ("sweep-late99-1-s3x-0599ad56658b/20261101T013700Z",
                           "spike/s3x-spike-sweep-late99-1-20261101T013158Z"),
        "sweep-late99-20": ("sweep-late99-20-s3x-c11a9fc0ff5c/20261101T013705Z",
                            "spike/s3x-spike-sweep-late99-20-20261101T013202Z"),
        "sweep-ingest": ("sweep-ingest-s3x-53a9be2342ad/20261101T013709Z",
                         "spike/s3x-spike-sweep-ingest-20261101T013206Z"),
        "sweep-ingest-rows": ("sweep-ingest-rows-s3x-90a3943b27f0/20261101T013709Z",
                              "spike/s3x-spike-sweep-ingest-rows-20261101T013210Z"),
    },
    "Azure Blob": {
        "sweep-base": ("sweep-base-azure-0c88452d93b4/20261031T234956Z",
                       "spike/azure-spike-20261031T234510Z"),
        "sweep-p10": ("sweep-p10-azure-a9c861b22333/20261101T021353Z",
                      "spike/azure-spike-20261101T020915Z"),
        "sweep-p30": ("sweep-p30-azure-5a52192f31d2/20261101T040944Z",
                      "spike/azure-spike-20261101T040443Z"),
        "sweep-s0": ("sweep-s0-azure-3f3a336b1a15/20261101T101950Z",
                     "spike/azure-spike-20261101T101445Z"),
        "sweep-s2": ("sweep-s2-azure-e7992fbdbc41/20261101T125618Z",
                     "spike/azure-spike-20261101T125133Z"),
        "sweep-rg32m": ("sweep-rg32m-azure-dbed8dac77cc/20261101T053428Z",
                        "spike/azure-spike-20261101T052959Z"),
        "sweep-rg64m": ("sweep-rg64m-azure-9e1663679938/20261101T075257Z",
                        "spike/azure-spike-20261101T074750Z"),
        "sweep-late99-1": ("sweep-late99-1-azure-66f451713990/20261101T144745Z",
                           "spike/azure-spike-20261101T144316Z"),
        "sweep-late99-20": ("sweep-late99-20-azure-264672b41c7a/20261101T165543Z",
                            "spike/azure-spike-20261101T165056Z"),
        "sweep-ingest": ("sweep-ingest-azure-a1b07fde6a9c/20261101T191915Z",
                         "spike/azure-spike-20261101T191431Z"),
        "sweep-ingest-rows": ("sweep-ingest-rows-azure-05a13b56e3bb/20261101T205331Z",
                              "spike/azure-spike-20261101T204829Z"),
    },
}


# RMoR re-measured (docs/HYPOTHESES.md, RMoR with the check by range, in the cloud, 2026-11-02, and
# its build note of 2026-11-03; page rmor-rerun, calibration): the sweeps' eleven configurations with
# MoR and RMoR alone, at d30544a (RMoR's check of dead row groups by range, 505ba07's code), one VM
# per store running them in turn after one spike. RR_LAPTOP holds each one's reference run on the
# laptop (the first, where it ran twice), RR_PRED the registered predictions of RMoR's mean write and
# skipping full read in s (the tables in HYPOTHESES.md round them to 0.1 s), and RR_RUNS the cloud
# runs with their VM's spike. The earlier runs of each setting (per-position check, 6f24575) are
# CS_RUNS' sweep-<setting>.
RR_CONFIGS = ["rmor-base", "rmor-p10", "rmor-p30", "rmor-s0", "rmor-s2", "rmor-rg32m", "rmor-rg64m",
              "rmor-late99-1", "rmor-late99-20", "rmor-ingest", "rmor-ingest-rows"]
RR_LAPTOP = {
    "rmor-base": "rmor-base-b6591653aff1/20261103T005121Z",
    "rmor-p10": "rmor-p10-e6848db0117d/20261103T011425Z",
    "rmor-p30": "rmor-p30-877a0064a43b/20261103T013038Z",
    "rmor-s0": "rmor-s0-d438be1a44a8/20261103T045630Z",
    "rmor-s2": "rmor-s2-5af74f61f041/20261103T052944Z",
    "rmor-rg32m": "rmor-rg32m-a08959d367e1/20261103T054830Z",
    "rmor-rg64m": "rmor-rg64m-7566db0cc088/20261102T235630Z",
    "rmor-late99-1": "rmor-late99-1-50c139067210/20261103T002610Z",
    "rmor-late99-20": "rmor-late99-20-8c863b8bb3a6/20261103T004756Z",
    "rmor-ingest": "rmor-ingest-8f60b490af63/20261103T011155Z",
    "rmor-ingest-rows": "rmor-ingest-rows-9cfbf1a760d1/20261103T012519Z",
}
RR_PRED = {
    "S3 Express": {
        "rmor-base": {"write": 18.87, "read": 9.14},
        "rmor-p10": {"write": 10.23, "read": 9.35},
        "rmor-p30": {"write": 3.70, "read": 9.34},
        "rmor-s0": {"write": 25.18, "read": 9.28},
        "rmor-s2": {"write": 8.19, "read": 9.21},
        "rmor-rg32m": {"write": 21.51, "read": 8.73},
        "rmor-rg64m": {"write": 27.00, "read": 8.84},
        "rmor-late99-1": {"write": 13.14, "read": 8.95},
        "rmor-late99-20": {"write": 21.84, "read": 9.18},
        "rmor-ingest": {"write": 12.24, "read": 9.25},
        "rmor-ingest-rows": {"write": 18.04, "read": 9.36},
    },
    "Azure Blob": {
        "rmor-base": {"write": 24.15, "read": 17.85},
        "rmor-p10": {"write": 13.26, "read": 16.61},
        "rmor-p30": {"write": 4.82, "read": 14.11},
        "rmor-s0": {"write": 31.73, "read": 17.36},
        "rmor-s2": {"write": 10.82, "read": 14.63},
        "rmor-rg32m": {"write": 26.61, "read": 17.13},
        "rmor-rg64m": {"write": 33.14, "read": 20.65},
        "rmor-late99-1": {"write": 18.20, "read": 15.43},
        "rmor-late99-20": {"write": 28.79, "read": 16.36},
        "rmor-ingest": {"write": 15.76, "read": 13.80},
        "rmor-ingest-rows": {"write": 22.67, "read": 14.39},
    },
}
RR_RUNS = {
    "S3 Express": {
        "rmor-base": ("rmor-base-s3x-62a8a0f25695/20261103T185354Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-p10": ("rmor-p10-s3x-021475e60a00/20261103T194321Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-p30": ("rmor-p30-s3x-fe6d011b4604/20261103T202116Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-s0": ("rmor-s0-s3x-a4c2005b1dd5/20261103T204827Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-s2": ("rmor-s2-s3x-c86000ef9786/20261103T213944Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-rg32m": ("rmor-rg32m-s3x-b4f09cd9a4bb/20261103T221936Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-rg64m": ("rmor-rg64m-s3x-db34e42a1484/20261103T230823Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-late99-1": ("rmor-late99-1-s3x-dfd3a458d7e1/20261104T000012Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-late99-20": ("rmor-late99-20-s3x-0ed69560c749/20261104T004503Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-ingest": ("rmor-ingest-s3x-71a422135f0b/20261104T013353Z", "spike/s3x-spike-20261103T184852Z"),
        "rmor-ingest-rows": ("rmor-ingest-rows-s3x-b5a5d697c9f7/20261104T020133Z", "spike/s3x-spike-20261103T184852Z"),
    },
    "Azure Blob": {
        "rmor-base": ("rmor-base-azure-2f5561c3ee58/20261103T185700Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-p10": ("rmor-p10-azure-1f0113c0df89/20261103T201150Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-p30": ("rmor-p30-azure-6af3da9feb95/20261103T210822Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-s0": ("rmor-s0-azure-56d25433ddf3/20261103T214550Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-s2": ("rmor-s2-azure-3cd08b1c2b26/20261103T230919Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-rg32m": ("rmor-rg32m-azure-56b5aaeadedc/20261104T000611Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-rg64m": ("rmor-rg64m-azure-59549fe1fa6a/20261104T011845Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-late99-1": ("rmor-late99-1-azure-517b2471e7ba/20261104T023259Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-late99-20": ("rmor-late99-20-azure-ab2828f0c62f/20261104T033909Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-ingest": ("rmor-ingest-azure-4b9c3a9933e1/20261104T045328Z", "spike/azure-spike-20261103T185155Z"),
        "rmor-ingest-rows": ("rmor-ingest-rows-azure-e391e5690a3e/20261104T053132Z", "spike/azure-spike-20261103T185155Z"),
    },
}


def rr_earlier(store, config):
    """The earlier cloud run (per-position check) of an rmor-* configuration's setting."""
    return CS_RUNS[store]["sweep-" + config[len("rmor-"):]][0]


def rr_times(runs=None):
    """Per store and rmor-* configuration with a run in runs (RR_RUNS): each arm's mean write and
    full read (RMoR's skipping, and without skipping) over MoR's in the same run (bp_over_mor; MoR's
    own in column mor_v3_s), in this run (column build: "range check") and in the earlier run of
    its setting ("per-position check"), which held every design."""
    runs = RR_RUNS if runs is None else runs
    frames = []
    for store, by in runs.items():
        if not by:
            continue
        for build, rs in [("range check", {c: r for c, (r, _) in by.items()}),
                          ("per-position check", {c: rr_earlier(store, c) for c in by})]:
            frames.append(bp_over_mor(bp_times(rs, key="config"), key="config")
                          .assign(build=build, store=store))
    return pd.concat(frames, ignore_index=True)


# The results pages (base-point, cloud-sweep) take RMoR's times from its rerun with the check by
# range, the other designs' from the runs pinned for them (BP_STORE_RUNS, CS_RUNS). The rerun holds
# MoR and RMoR alone, and RMoR's ratios are over the MoR of its own run: the store drifts between
# runs (docs/HYPOTHESES.md, 2026-11-02). The calibration pages keep the pinned runs whole, as they
# price those runs.
BP_RMOR_RUNS = {s: RR_RUNS[s]["rmor-base"][0] for s in BP_STORES}
CS_RMOR_RUNS = {s: {"sweep-" + c[len("rmor-"):]: v for c, v in by.items()}
                for s, by in RR_RUNS.items()}


def rmor_spliced(main, rerun):
    """main (a bp_* function's frame, or cs_frame's) with RMoR's rows (arm rmor-c, with and without
    skipping) from rerun, the same function's frame over RMoR's runs."""
    return pd.concat([main[main.arm != "rmor-c"], rerun[rerun.arm == "rmor-c"]],
                     ignore_index=True)


# The tuner's policy map (pages/tune.md): the cheapest policy at each read mix, its hue the
# design's (the results pages' colors; RMoR-density, which they do not show, take the
# palette's red, the one slot that clears the validator's normal-vision floor beside the three),
# white edges between policies, and each region labelled with what sets its policy apart.
TUNE_COLORS = {"MoR": "#2a78d6", "RMoR": "#eda100", "RMoW-append": "#1baf7a", "deferred": "#e34948",
               "RMoW-external": "#008300"}


def _short(policy):
    return (policy.replace("compacted every", "every").replace(" row groups", "")
            .replace("sealed at ", "seal ").replace("deferred, ", "deferred "))


def tune_map(pm, W, F, width=300, height=250, min_cells=6):
    """pm: analysis/tune.py's policy_map on the grid W x F (windows, full reads a write), with
    `facet`. The chart, and its regions (each connected run of one policy's cells)."""
    rw, rf = np.sqrt(W[1] / W[0]), np.sqrt(F[1] / F[0])
    d = pm.copy()
    d["i"] = d.windows.map({w: i for i, w in enumerate(W)})
    d["j"] = d.fulls.map({f: j for j, f in enumerate(F)})
    d["w0"], d["w1"], d["f0"], d["f1"] = d.windows / rw, d.windows * rw, d.fulls / rf, d.fulls * rf
    edges, labels, regions = [], [], []
    for facet, g in d.groupby("facet"):
        cell = {(r.i, r.j): r for r in g.itertuples()}
        for (i, j), r in cell.items():
            right, up = cell.get((i + 1, j)), cell.get((i, j + 1))
            if right is not None and right.policy != r.policy:
                edges.append(dict(facet=facet, x=r.w1, x2=r.w1, y=r.f0, y2=r.f1))
            if up is not None and up.policy != r.policy:
                edges.append(dict(facet=facet, x=r.w0, x2=r.w1, y=r.f1, y2=r.f1))
        seen = set()
        for start in cell:
            if start in seen:
                continue
            comp, stack = [], [start]
            seen.add(start)
            while stack:
                c = stack.pop()
                comp.append(c)
                for n in ((c[0] + 1, c[1]), (c[0] - 1, c[1]), (c[0], c[1] + 1), (c[0], c[1] - 1)):
                    if n in cell and n not in seen and cell[n].policy == cell[start].policy:
                        seen.add(n)
                        stack.append(n)
            ci, cj = np.mean([c[0] for c in comp]), np.mean([c[1] for c in comp])
            at = min(comp, key=lambda c: (c[0] - ci) ** 2 + (c[1] - cj) ** 2)
            r = cell[at]
            regions.append(dict(facet=facet, policy=r.policy, cells=len(comp)))
            if len(comp) >= min_cells:
                labels.append(dict(facet=facet, x=r.windows, y=r.fulls, text=_short(r.policy)))
    dom = [k for k in TUNE_COLORS if k in set(d.design)]
    xs = alt.Scale(type="log", domain=[W[0] / rw, W[-1] * rw], nice=False)
    ys = alt.Scale(type="log", domain=[F[0] / rf, F[-1] * rf], nice=False)
    dscale = alt.Scale(domain=dom, range=[TUNE_COLORS[k] for k in dom])
    color = alt.Color("design:N", title="design", sort=dom, scale=dscale)
    edges, labels = pd.DataFrame(edges), pd.DataFrame(labels)
    panels = []
    for facet in sorted(d.facet.unique()):
        cells = alt.Chart(d[d.facet == facet][["design", "policy", "w0", "w1", "f0", "f1", "margin"]]) \
            .mark_rect(strokeWidth=0.8).encode(
                x=alt.X("w0:Q", scale=xs, title="windows per write"), x2="w1",
                y=alt.Y("f0:Q", scale=ys, title="full reads per write"), y2="f1", color=color,
                stroke=alt.Stroke("design:N", scale=dscale, legend=None),
                tooltip=["policy", alt.Tooltip("margin:Q", format=".3f", title="runner-up over it")])
        layers = [cells]
        e = edges[edges.facet == facet] if len(edges) else edges
        if len(e):
            layers.append(alt.Chart(e[["x", "x2", "y", "y2"]]).mark_rule(color="white", strokeWidth=2)
                          .encode(x=alt.X("x:Q", scale=xs), x2="x2", y=alt.Y("y:Q", scale=ys), y2="y2"))
        t = labels[labels.facet == facet][["x", "y", "text"]] if len(labels) else labels
        if len(t):
            pos = dict(x=alt.X("x:Q", scale=xs), y=alt.Y("y:Q", scale=ys), text="text")
            layers.append(alt.Chart(t).mark_text(fontSize=10, stroke="white", strokeWidth=3, strokeJoin="round", strokeOpacity=0.9).encode(**pos))
            layers.append(alt.Chart(t).mark_text(fontSize=10, color="#1f1f1e").encode(**pos))
        panels.append(alt.layer(*layers).properties(width=width, height=height, title=facet))
    rows = [alt.hconcat(*panels[k:k + 2]) for k in range(0, len(panels), 2)]
    return alt.vconcat(*rows), pd.DataFrame(regions)


# The two workloads (pages/workloads.md, simulated; pages/workloads-cloud.md, measured): each
# design's write latency against its reads, its latency and storage as snapshots are added, and
# where its time goes. A spec is a list of (configuration, arm, family, curve it lies on, its knob's
# value, its name if a main point, order on the curve); a curve runs through one family's settings
# of one knob.
WL_STORES = ["S3 Express", "Azure Blob"]
WL_STORE = {"s3x": WL_STORES[0], "azure": WL_STORES[1]}
WL_COLOR = {"MoR": "#2a78d6", "RMoR": "#eda100", "RMoW-external": "#008300", "RMoW-append": "#1baf7a",
            "RMoR-density": "#b8478f"}
WL_SHAPE = {"MoR": "circle", "RMoR": "triangle-up", "RMoW-external": "square", "RMoW-append": "diamond",
            "RMoR-density": "triangle-down"}
# a main point's own color where two share a family (MoR and MoR (compacted))
WL_NCOLOR = {"MoR (compacted)": "#7fb2ea"}
# what the charts and tables call a write and each kind of read
WL_WRITE = "snapshot write latency"
WL_READ = {"window_s": "1% range read latency", "full_s": "full scan latency", "sorted_s": "sorted read latency"}
# Where a phase's time goes: its thread's components (phase_breakdown, views/47_breakdown.sql) by
# part. "DVs and footers" is delete files' work (deletion vectors built, encoded, loaded and
# decoded) and RMoW's footers'; "other CPU" the rest of the thread's CPU: making the rows a write
# writes, planning scans, Iceberg's manifests and commit; "I/O" the store client's CPU and the wait
# for its requests (simulated: the requests priced by size).
WL_CPU_PART = {"decode CPU": "decode", "encode CPU": "encode", "footer CPU": "DVs and footers",
               "deletes CPU": "DVs and footers", "other CPU": "other CPU",
               "store client CPU": "I/O", "waiting on the store": "I/O",
               "other waiting": "paused (GC, JIT)"}
WL_PARTS = ["decode", "encode", "DVs and footers", "other CPU", "I/O", "paused (GC, JIT)"]
WL_PCOL = {"decode": "#3b2ca6", "encode": "#7d6fdc", "DVs and footers": "#c4bdf0", "other CPU": "#eb6834",
           "I/O": "#7f8a93", "paused (GC, JIT)": "#c9ced3"}
WL_SNAPSHOT_X = alt.X("step:Q", title="snapshot", scale=alt.Scale(domain=[0, 30], nice=False),
                      axis=alt.Axis(values=list(range(0, 31, 5))))


def _cap(s):
    return s[0].upper() + s[1:]


def wl_whiskers(S):
    """Per store, configuration and arm of S (one row per run, arm, step, phase and reader, with
    Store and its time `s` in seconds): the 10th and 90th percentile of its writes (with its
    compactions spread over them, as its mean has), and its fastest and slowest read of each kind
    over the probes after 5 to 30 writes (s)."""
    reads = tuner().READS
    rows = []
    for (store, label, arm), g in S.groupby(["Store", "label", "arm"]):
        w = g[(g.phase == "batch") & (g.step >= 1)]
        c = g[g.phase == "compact"].s.sum() / w.step.nunique()
        row = dict(Store=store, label=label, arm=arm, write_lo=w.s.quantile(0.1) + c,
                   write_hi=w.s.quantile(0.9) + c)
        for kind, (phase, reader) in reads.items():
            r = g[(g.phase == phase) & (g.reader == reader) & (g.step >= 5)].s
            row[f"{kind}_s_lo"], row[f"{kind}_s_hi"] = (r.min(), r.max()) if len(r) else (np.nan, np.nan)
        rows.append(row)
    return pd.DataFrame(rows)


def wl_frame(spec, P, W):
    """The spec's points: P's means (tune.policies(), with Store) in seconds, a write's with its
    compactions spread over the writes, and W's whiskers (wl_whiskers())."""
    rows = []
    for label, arm, family, curve, knob, name, order in spec:
        for _, r in P[(P.label == label) & (P.arm == arm)].iterrows():
            rows.append(dict(Store=r.Store, label=label, arm=arm, family=family, curve=curve, knob=knob,
                             name=name or "", main=name is not None, order=order,
                             write_s=(r.write_ms + r.compact_ms) / 1e3, compact_s=r.compact_ms / 1e3,
                             window_s=r.window_ms / 1e3, full_s=r.full_ms / 1e3, sorted_s=r.sorted_ms / 1e3))
    return pd.DataFrame(rows).merge(W, on=["Store", "label", "arm"], how="left")


def wl_tradeoff(d, reads, width=260, height=190):
    """Write latency against read latency, one point per design (d: wl_frame()'s rows, `name` the
    design), a panel per kind of read and a row per store. Whiskers across span the writes' 10th
    to 90th percentile, and up the fastest to slowest read after 5 to 30 writes."""
    names = list(dict.fromkeys(d.name))
    fam = d.drop_duplicates("name").set_index("name").family
    L = d.melt(id_vars=["Store", "name", "write_s", "write_lo", "write_hi"], value_vars=list(reads),
               var_name="kind", value_name="read_s")
    recs = L.to_dict("records")
    L["read_lo"] = [d.loc[(d.Store == r["Store"]) & (d.name == r["name"]), f"{r['kind']}_lo"].iloc[0] for r in recs]
    L["read_hi"] = [d.loc[(d.Store == r["Store"]) & (d.name == r["name"]), f"{r['kind']}_hi"].iloc[0] for r in recs]
    L["read"] = L.kind.map(WL_READ)
    legend = alt.Legend(title=None, orient="bottom", direction="horizontal")
    color = alt.Color("name:N", sort=names, legend=legend,
                      scale=alt.Scale(domain=names, range=[WL_NCOLOR.get(n, WL_COLOR[fam[n]]) for n in names]))
    shape = alt.Shape("name:N", sort=names, legend=legend,
                      scale=alt.Scale(domain=names, range=[WL_SHAPE[fam[n]] for n in names]))
    xs, ys = alt.Scale(zero=True), alt.Scale(zero=True)
    x = alt.X("write_s:Q", title="write latency (s)", scale=xs)
    y = alt.Y("read_s:Q", title="read latency (s)", scale=ys)
    tip = ["Store", "name", alt.Tooltip("write_s:Q", format=".2f", title="write (s)"), "read",
           alt.Tooltip("read_s:Q", format=".2f", title="read (s)")]
    across = alt.Chart().mark_rule(strokeWidth=1.5, opacity=0.6).encode(
        x=alt.X("write_lo:Q", title="write latency (s)", scale=xs), x2="write_hi:Q", y=y, color=color)
    up = alt.Chart().mark_rule(strokeWidth=1.5, opacity=0.6).encode(
        x=x, y=alt.Y("read_lo:Q", title="read latency (s)", scale=ys), y2="read_hi:Q", color=color)
    points = alt.Chart().mark_point(size=110, filled=True, opacity=1, stroke="white", strokeWidth=0.8).encode(
        x=x, y=y, color=color, shape=shape, tooltip=tip)
    return alt.layer(across, up, points, data=L).properties(width=width, height=height).facet(
        row=alt.Row("Store:N", sort=WL_STORES, title=None),
        column=alt.Column("read:N", sort=[WL_READ[r] for r in reads], title=None)).resolve_scale(y="independent")


def wl_twin(d, reads):
    """wl_tradeoff()'s table twin: each design's mean write and reads (s), with their whiskers."""
    cols = {"Store": "Store", "name": "design", "write_s": "write", "write_lo": "write p10", "write_hi": "write p90"}
    short = {"window_s": "range read", "full_s": "full scan", "sorted_s": "sorted read"}
    for r in reads:
        cols |= {r: short[r], f"{r}_lo": f"{short[r]} min", f"{r}_hi": f"{short[r]} max"}
    t = d[list(cols)].rename(columns=cols)
    t["_s"] = t.Store.map(WL_STORES.index)
    return t.sort_values("_s", kind="stable").drop(columns="_s").round(3)


def wl_snapshots(spec, S, reads):
    """Each design's (main point's) time `s` in S at every write, compaction and read probe."""
    tread = tuner().READS
    rows = []
    for label, arm, family, curve, knob, name, order in spec:
        if name is None:
            continue
        g = S[(S.label == label) & (S.arm == arm)]
        parts = [g[(g.phase == "batch") & (g.step >= 1)].assign(what=WL_WRITE),
                 g[g.phase == "compact"].assign(what="a compaction")]
        for r in reads:
            phase, reader = tread[r[:-2]]
            parts.append(g[(g.phase == phase) & (g.reader == reader)].assign(what=WL_READ[r]))
        rows += [x.assign(name=name, family=family)[["Store", "name", "family", "what", "step", "s"]] for x in parts]
    return pd.concat(rows, ignore_index=True)


def wl_snapshot_chart(d, reads, width=300, height=150):
    """Lines over the snapshots: write latency at every write (a compaction a cross at its write),
    and each kind of read's latency at every probe; a column per store."""
    names = list(dict.fromkeys(d.name))
    fam = d.drop_duplicates("name").set_index("name").family
    color = alt.Color("name:N", title=None, sort=names, legend=alt.Legend(orient="bottom"),
                      scale=alt.Scale(domain=names, range=[WL_NCOLOR.get(n, WL_COLOR[fam[n]]) for n in names]))
    shape = alt.Shape("name:N", title=None, sort=names,
                      scale=alt.Scale(domain=names, range=[WL_SHAPE[fam[n]] for n in names]))
    x = WL_SNAPSHOT_X
    tip = ["Store", "name", "what", "step", alt.Tooltip("s:Q", format=".2f")]

    def graph(what, ytitle, size):
        y = alt.Y("s:Q", title=ytitle, scale=alt.Scale(zero=True))
        line = alt.Chart().transform_filter(alt.datum.what == what).mark_line(
            strokeWidth=2, point=alt.OverlayMarkDef(size=size)).encode(
            x=x, y=y, color=color, shape=shape, detail="name:N", tooltip=tip)
        layers = [line]
        if what == WL_WRITE and (d.what == "a compaction").any():
            layers.append(alt.Chart().transform_filter(alt.datum.what == "a compaction").mark_point(
                shape="cross", filled=True, size=90, opacity=1).encode(x=x, y=y, color=color, tooltip=tip))
        return (alt.layer(*layers, data=d).properties(width=width, height=height)
                .facet(column=alt.Column("Store:N", title=None, sort=WL_STORES))
                .properties(title=alt.Title(_cap(what), anchor="start")))

    return alt.vconcat(graph(WL_WRITE, "write latency (s)", 18),
                       *[graph(WL_READ[r], "read latency (s)", 50) for r in reads])


def wl_snapshot_table(d, what, basis="simulated"):
    from labbook import table

    t = d[d.what == what].pivot_table(index="step", columns=["Store", "name"], values="s")
    names = list(dict.fromkeys(d.name))
    t = t.reindex(columns=[(st, n) for st in WL_STORES for n in names if (st, n) in t.columns])
    t.columns = [f"{st}: {n}" for st, n in t.columns]
    t = t.reset_index().rename(columns={"step": "snapshot"})
    return table(t.round(2), caption=f"{_cap(what)}, {basis} (s), at each snapshot")


# Compaction, apart from the designs: MoR at each interval, light (never) to dark (every 3 writes),
# one hue (validated as an ordinal ramp: the dataviz skill's validate_palette.js --ordinal)
WL_INTERVALS = ["never", "every 12 writes", "every 10 writes", "every 6 writes", "every 3 writes"]
WL_INTERVAL_COLORS = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]


def wl_interval_series(name):
    """A compaction interval's name in the charts' legends."""
    return "MoR (never compacted)" if name == "never" else f"MoR (compacted {name})"


def wl_compaction_chart(d, reads, ref, shown=("never", "every 10 writes", "every 3 writes"),
                        width=300, height=150):
    """MoR at the `shown` compaction intervals as snapshots are added: each kind of read's latency
    at every probe, a column per store, beside `ref` (a design's name in d, dashed, uncompacted).
    d is wl_snapshots()'s frame, its names the intervals (WL_INTERVALS) and ref. More than three
    intervals tangle: reads every 5 writes catch each at another point of its cycle."""
    names = [n for n in WL_INTERVALS if n in set(d.name) and n in shown]
    fam = d.drop_duplicates("name").set_index("name").family
    ref_name = ref
    series = [wl_interval_series(n) for n in names]
    label = dict(zip(names, series)) | {ref: ref_name}
    color = alt.Color("series:N", title=None, sort=[*series, ref_name],
                      legend=alt.Legend(orient="bottom", direction="horizontal", symbolStrokeWidth=2,
                                        labelLimit=320),
                      scale=alt.Scale(domain=[*series, ref_name],
                                      range=[*[WL_INTERVAL_COLORS[WL_INTERVALS.index(n)] for n in names],
                                             WL_COLOR[fam[ref]]]))
    dash = alt.StrokeDash("series:N", legend=None, scale=alt.Scale(
        domain=[*series, ref_name], range=[[1, 0]] * len(series) + [[5, 3]]))
    tip = ["Store", "series", "what", "step", alt.Tooltip("s:Q", format=".2f")]

    def graph(what):
        g = d[(d.what == what) & d.name.isin([*names, ref])].assign(series=lambda x: x.name.map(label))
        y = alt.Y("s:Q", title="read latency (s)", scale=alt.Scale(zero=True))
        lines = alt.Chart(g).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=40)).encode(
            x=WL_SNAPSHOT_X, y=y, color=color, strokeDash=dash, tooltip=tip)
        return (lines.properties(width=width, height=height)
                .facet(column=alt.Column("Store:N", title=None, sort=WL_STORES))
                .properties(title=alt.Title(_cap(what), anchor="start")))

    return alt.vconcat(*[graph(WL_READ[r]) for r in reads])


def wl_written(spec):
    """What each (configuration, arm, name) of spec has written after every write, in GB: its
    writes' bytes and its compactions', from the initial load on (0 at snapshot 0). Havoc's runs;
    the same on both stores, since every cloud run sends what havoc's run of its configuration
    counts. With nothing deleted, it is also what the store holds beyond the initial load while
    every snapshot is kept."""
    d = q("""SELECT r.label, s.arm, s.step, sum(s.bytes_written) / 1e9 AS gb
             FROM steps s JOIN canonical_runs r USING (run_id)
             WHERE r.host = 'havoc' AND list_contains($l, r.label)
               AND ((s.phase = 'batch' AND s.step >= 1) OR s.phase = 'compact')
             GROUP BY ALL ORDER BY 1, 2, 3""", l=sorted({l for l, _, _ in spec}))
    names = {(l, a): n for l, a, n in spec}
    d["name"] = [names.get(k) for k in zip(d.label, d.arm)]
    d = d[d.name.notna()]
    d["gb"] = d.groupby(["label", "arm"]).gb.cumsum()
    start = d.drop_duplicates(["label", "arm"]).assign(step=0, gb=0.0)
    return pd.concat([start, d]).sort_values(["label", "arm", "step"]).reset_index(drop=True)


def wl_written_chart(d, colors, shapes=None, dashed=(), width=420, height=180):
    """Data written as snapshots are added (d: wl_written()), one line per name, `colors` (and
    `shapes`) by name in legend order, the names in `dashed` dashed."""
    names = [n for n in colors if n in set(d.name)]
    legend = alt.Legend(orient="bottom", direction="horizontal", title=None, symbolStrokeWidth=2, labelLimit=320)
    enc = dict(x=WL_SNAPSHOT_X, y=alt.Y("gb:Q", title="data written (GB)", scale=alt.Scale(zero=True)),
               color=alt.Color("name:N", sort=names, legend=legend,
                               scale=alt.Scale(domain=names, range=[colors[n] for n in names])),
               strokeDash=alt.StrokeDash("name:N", sort=names, legend=None, scale=alt.Scale(
                   domain=names, range=[[5, 3] if n in dashed else [1, 0] for n in names])),
               tooltip=["name", "step", alt.Tooltip("gb:Q", format=".3f")])
    if shapes:
        enc["shape"] = alt.Shape("name:N", sort=names, legend=legend,
                                 scale=alt.Scale(domain=names, range=[shapes[n] for n in names]))
    return alt.Chart(d).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=25)).encode(**enc).properties(
        width=width, height=height)


def wl_written_table(d, names):
    """wl_written_chart()'s table twin: GB written by each name by every 5th snapshot."""
    from labbook import table

    t = d[d.step % 5 == 0].pivot_table(index="step", columns="name", values="gb")
    t = t.reindex(columns=[n for n in names if n in t.columns])
    return table(t.reset_index().rename(columns={"step": "snapshot"}).round(3),
                 caption="Data written (GB) by every 5th snapshot, from the initial load on")


def wl_compaction_written_chart(wr, shown=("never", "every 10 writes", "every 3 writes")):
    """What MoR writes at the `shown` compaction intervals (wr: wl_written(), its names the
    intervals), as wl_written_chart() draws it, colored as wl_compaction_chart() colors them."""
    d = wr[wr.name.isin(shown)].assign(name=lambda x: x.name.map(wl_interval_series))
    colors = {wl_interval_series(n): c for n, c in zip(WL_INTERVALS, WL_INTERVAL_COLORS) if n in shown}
    return wl_written_chart(d, colors)


def wl_compaction_costs(spec, P, cm, wr):
    """Each compaction interval's costs per store, for spec's (configuration, interval name): P's
    means (tune.policies() or tune.per_write(), with Store; a write without and with its compactions
    spread over the writes, each read), a compaction's mean and the range read after 30 writes
    (cm, wl_snapshots()), and the data written over the 30 writes (wr, wl_written())."""
    rows = []
    for label, name in spec:
        gb30 = wr[(wr.label == label) & (wr.arm == "mor-v3") & (wr.step == 30)].gb.iloc[0]
        for _, r in P[(P.label == label) & (P.arm == "mor-v3")].iterrows():
            g = cm[(cm.Store == r.Store) & (cm.name == name)]
            comp = g[g.what == "a compaction"].s
            rows.append({"Store": r.Store, "compacted": name, "write": r.write_ms / 1e3,
                         "write with compactions": (r.write_ms + r.compact_ms) / 1e3,
                         "a compaction": comp.mean() if len(comp) else np.nan,
                         "range read": r.window_ms / 1e3,
                         "range read after 30": g[(g.what == WL_READ["window_s"]) & (g.step == 30)].s.iloc[0],
                         "full scan": r.full_ms / 1e3, "GB written": gb30})
    out = pd.DataFrame(rows)
    out["_s"] = out.Store.map(WL_STORES.index)
    return out.sort_values("_s", kind="stable").drop(columns="_s").round(3)


def wl_parts_chart(d, whats, names, parts=WL_PARTS, colors=None):
    """Where each design's time goes: a stacked bar per design (d: Store, name, what, part, s, and
    pord, the part's place in `parts`), a panel per kind of operation and a row per store; each
    part colored by `colors` (WL_PCOL by default). The legend lists the parts d has."""
    colors = colors or WL_PCOL
    parts = [p for p in parts if p in set(d.part)]

    def panel(w):
        return alt.Chart(d[d.what == w]).mark_bar().encode(
            x=alt.X("s:Q", title="seconds", stack="zero"),
            y=alt.Y("name:N", title=None, sort=names),
            color=alt.Color("part:N", title=None, sort=parts,
                            scale=alt.Scale(domain=parts, range=[colors[p] for p in parts]),
                            legend=alt.Legend(orient="top", columns=len(parts), labelLimit=260)),
            order=alt.Order("pord:Q"),
            tooltip=["Store", "name", "what", "part", alt.Tooltip("s:Q", format=".2f")],
        ).properties(width=190).facet(row=alt.Row("Store:N", sort=WL_STORES, title=None)).properties(title=w)
    return alt.hconcat(*[panel(w) for w in whats]).resolve_scale(x="independent", y="independent")



# --- The inserts at 128 MiB, projected (analysis/event_time_model.py; pages/inserts-128.md) ------
# The model's designs by the names the pages use. "merge" is the event-time merge at theta 0, which
# RMoR, RMoW-external and RMoW-append share: they write the same bytes and keep the same row groups.
ET_NAMES = {"mor": "MoR", "compact": "MoR (compacted)", "density": "RMoR-density",
            "merge": "RMoR, RMoW-external, RMoW-append"}
ET_COLOR = {"MoR": WL_COLOR["MoR"], "MoR (compacted)": WL_NCOLOR["MoR (compacted)"],
            "RMoR-density": WL_COLOR["RMoR-density"],
            "RMoR, RMoW-external, RMoW-append": WL_COLOR["RMoR"]}
_ET_CACHE = {}


def et_projection(**overrides):
    """analysis/event_time_model.py's projection (its defaults: the 128 MiB suite's inserts), run
    once a kernel for each set of overrides; about a minute at the defaults."""
    key = tuple(sorted(overrides.items()))
    if key not in _ET_CACHE:
        if str(ANALYSIS) not in sys.path:
            sys.path.insert(0, str(ANALYSIS))
        import event_time_model

        _ET_CACHE[key] = event_time_model.project(**overrides)
    return _ET_CACHE[key]


def et_written(proj, names=ET_NAMES):
    """Projected data written as snapshots are added: name, step, gb (cumulative), with step 0 at 0,
    in wl_written()'s form, so wl_written_chart draws it."""
    rows = []
    for k, per in proj["written"].items():
        if k not in names:
            continue
        rows.append((names[k], 0, 0.0))
        total = 0.0
        for step, b in enumerate(per, start=1):
            total += b
            rows.append((names[k], step, total / 1e9))
    return pd.DataFrame(rows, columns=["name", "step", "gb"])


def et_reads(proj, names=ET_NAMES):
    """Projected reads at each read point: name, step, window (its position in the event-time
    span), ratio (bytes read over live bytes), and held_gb (the sorted read's peak)."""
    rows = []
    for k, by_step in proj["reads"].items():
        if k not in names:
            continue
        for step, (windows, held) in by_step.items():
            for at, ratio in windows.items():
                rows.append((names[k], step, at, ratio, held / 1e9))
    return pd.DataFrame(rows, columns=["name", "step", "window", "ratio", "held_gb"])
