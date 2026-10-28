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
