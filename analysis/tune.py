#!/usr/bin/env python3
"""Tunes write policy by simulation (docs/HYPOTHESES.md, 2026-11-08, T1-T4).

A policy is a configuration and an arm on one workload: the updates (`upd`, the `win-upd-*` and
`tune-upd-*` configurations) or the inserts at lateness p99 5 (`ing`, `win-ing-base*`,
`win-ing-seal160`, `tune-ing-*`). Each one's time per write, given a read mix, is

    write + compactions / writes + sum over reads of (reads per write x the read's time)

where a phase's time is its work plus its requests priced by size with a store's spike (the lab's
`sized_latency`, lab/views/65_sized.sql), as the lab simulates every local run. The reads are the
probes' (steps 5 to 30): a full read, a 1% window (the mean of the probe's five) and the newest 1%
under `cached-skip`, which skips dead row groups, and the sorted read under `cached`, the one
reader that reads in event-time order.

Requests and bytes come from havoc's runs, which count no instructions. A phase's work is
predicted from its counts by a linear formula fitted on the laptop's runs of the `windows`
campaign (the same build, every arm), which count instructions:

    instructions = c0 + c1 x bytes read + c2 x bytes written + c3 x requests [+ c4 x rows x log2 fan-in]

per kind of phase (the write's sub-phases, the compaction, each read), with coefficients of their
own per arm where that predicted better when one configuration at a time was held out (FORMULA),
and per workload and arm where two configurations or more train them, none negative, fitted to
relative error. The sorted read adds its merge, rows times log2 of the
row groups it holds open at its peak (its held column chunks over a row group's bytes). The work
is the instructions at the arm's pace for that phase on the laptop (`phase_work`: cycles an
instruction over the clock), over the training runs of the same workload, weighted by
instructions; a write is its parts, each at its own pace. With `--measured`, configurations the laptop ran take their measured work instead,
for the check.

Usage:
  .venv/bin/python analysis/tune.py [--measured] [--loo] [--judge] [--cloud STORE] [--out DIR]

`--loo` prints the formula's error on the training runs, each configuration held out in turn.
`--judge` judges T1 and T2 on the finalists (each mix's two cheapest policies) the laptop ran.
`--cloud s3x|azure` judges K2 and K3 (docs/HYPOTHESES.md, 2026-11-08) on campaign `tune-cloud`.
`--out` writes policies.csv (every policy's phases) and ranks.csv (every mix's ranking).
"""
import argparse
import os
import sys

import duckdb
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "lab", "lab.duckdb")

SPIKES = {"s3x": "spike/s3x-spike-20261031T163533Z", "azure": "spike/azure-spike-20261031T234510Z"}
# Each priced read: its phase and reader.
READS = {"full": ("read_full", "cached-skip"), "window": ("read_window", "cached-skip"),
         "recent": ("read_recent", "cached-skip"), "sorted": ("read_sorted", "cached")}
# Read mixes, as reads per write (registered with T2).
MIXES = {
    "writes": {"window": 1, "full": 0.1},
    "windows": {"window": 10},
    "scans": {"full": 2},
    "sorted": {"sorted": 1},
}
WORKLOADS = {
    "upd": "regexp_matches(label, '^(win-upd-|tune-upd-)')",
    "ing": "regexp_matches(label, '^(win-ing-base|win-ing-seal160$|tune-ing-)')",
}
GRID = "(list_contains(campaigns, 'windows') OR list_contains(campaigns, 'tune'))"
TRAIN = "list_contains(campaigns, 'windows') AND host <> 'havoc'"
WRITE_PARTS = ["locate", "update-read", "merge-read", "write", "delete-write"]
# Per kind of phase: features beyond the constant, and whether coefficients are per arm.
B, BR = ["bytes_read", "bytes_written"], ["bytes_read", "bytes_written", "requests"]
FORMULA = {
    "locate": (B, True), "update-read": (BR, False), "merge-read": (B, True),
    "write": (BR, True), "delete-write": (B, False), "compact": (BR, False),
    "read_full": (BR, True), "read_old": (BR, True), "read_recent": (BR, True),
    "read_window": (BR, True), "read_sorted": (["bytes_read", "bytes_written", "requests", "merge"], False),
}
DESIGN = {"mor-v3": "MoR", "rmor-c-kept": "RMoR", "rmor-c": "RMoR", "rmor-c-footer-kept": "RMoW-external",
          "rmow-c": "RMoW-append", "rmor-density": "deferred"}


def connect():
    return duckdb.connect(DB, read_only=True)


def records(con, where):
    """One row per record of the priced phases and the write's parts, with the counts the
    formula takes (and instructions, where counted)."""
    d = con.sql(f"""
        SELECT s.run_id, r.label, r.host, s.arm, s.step, s.phase, s.reader, s.window_at,
               s.instructions + coalesce(s.pool_instructions, 0) AS y,
               s.requests::DOUBLE AS requests, s.bytes_read::DOUBLE AS bytes_read,
               s.bytes_written::DOUBLE AS bytes_written, coalesce(s.rows, 0)::DOUBLE AS rows,
               coalesce(s.held_chunks, 0)::DOUBLE AS held_chunks, s.gets::DOUBLE AS gets
        FROM steps s JOIN canonical_runs r USING (run_id)
        WHERE {where} AND s.step > 0
          AND s.phase IN ({",".join(f"'{p}'" for p in FORMULA)})
          AND coalesce(s.reader, '') <> 'cached-p8'""").df()
    rg = (d.bytes_read / d.gets.clip(lower=1)).clip(lower=1)
    d["merge"] = d.rows * np.log2(1 + (d.held_chunks / rg).clip(lower=1))
    return d


def nnls_rel(X, y):
    """Least squares on relative error, no coefficient negative (the most negative dropped, then
    refitted)."""
    keep = np.ones(X.shape[1], bool)
    while True:
        c, *_ = np.linalg.lstsq(X[:, keep] / y[:, None], np.ones(len(y)), rcond=None)
        if (c >= 0).all():
            out = np.zeros(X.shape[1])
            out[keep] = c
            return out
        idx = np.flatnonzero(keep)
        keep[idx[np.argmin(c)]] = False


class Formula:
    """Per kind of phase: coefficients for every arm together, per arm, and per workload and arm
    where the training runs hold at least two configurations of it; a prediction takes the most
    specific."""

    def __init__(self, train):
        self.coef = {}
        for phase, (feats, per_arm) in FORMULA.items():
            g = train[(train.phase == phase) & (train.y > 0)]
            if g.empty:
                continue
            self.coef[(phase, None, None)] = nnls_rel(self._x(g, feats), g.y.to_numpy(float))
            if not per_arm:
                continue
            for arm, ga in g.groupby("arm"):
                if len(ga) >= 5:
                    self.coef[(phase, None, arm)] = nnls_rel(self._x(ga, feats), ga.y.to_numpy(float))
                for wl, gw in ga.groupby(ga.label.map(wl_of)):
                    if gw.label.nunique() >= 2 and len(gw) >= 5:
                        self.coef[(phase, wl, arm)] = nnls_rel(self._x(gw, feats), gw.y.to_numpy(float))

    @staticmethod
    def _x(g, feats):
        return np.c_[g[feats].to_numpy(float), np.ones(len(g))]

    def predict(self, d):
        out = np.full(len(d), np.nan)
        for (phase, wl, arm), g in d.groupby([d.phase, d.label.map(wl_of), d.arm]):
            feats, per_arm = FORMULA[phase]
            keys = [(phase, wl, arm), (phase, None, arm)] if per_arm else []
            c = next((self.coef[k] for k in keys + [(phase, None, None)] if k in self.coef), None)
            if c is not None:
                out[d.index.get_indexer(g.index)] = self._x(g, feats) @ c
        return out


def wl_of(label):
    return "upd" if "-upd-" in label else "ing"


def pace(con):
    """ns per instruction per workload, arm, phase and reader on the laptop's training runs, weighted
    by instructions (a phase that does little, such as MoR's locate on the inserts, runs at
    another pace), with every workload's together under '*'."""
    d = con.sql(f"""
        SELECT r.label, w.arm, w.phase, coalesce(w.reader, '') AS reader,
               sum(w.work_ms) * 1e6 AS ns, sum(w.instructions) AS instr
        FROM phase_work w JOIN canonical_runs r USING (run_id) WHERE {TRAIN} GROUP BY ALL""").df()
    d["wl"] = d.label.map(wl_of)
    keys = ["arm", "phase", "reader"]
    a = d.groupby(["wl"] + keys)[["ns", "instr"]].sum()
    b = d.groupby(keys)[["ns", "instr"]].sum()
    b.index = pd.MultiIndex.from_tuples([("*",) + k for k in b.index], names=["wl"] + keys)
    t = pd.concat([a, b])
    return (t.ns / t.instr).to_dict()


def ns_of(ns, label, arm, phase, reader):
    wl = wl_of(label)
    for k in ((wl, arm, phase, reader), ("*", arm, phase, reader), (wl, arm, "batch", ""),
              ("*", arm, "batch", "")):
        if k in ns:
            return ns[k]
    return np.nan


def loo(train):
    """The formula's error, one configuration held out at a time: per record, and on each
    configuration's and arm's mean over steps (what a policy's cost takes)."""
    preds = []
    for held in sorted(train.label.unique()):
        f = Formula(train[train.label != held])
        te = train[train.label == held].copy()
        te["pred"] = f.predict(te)
        preds.append(te)
    e = pd.concat(preds)
    e = e[e.y > 0]
    e["kind"] = e.phase
    rec = e.assign(err=e.pred / e.y - 1)
    mean = e.groupby(["kind", "label", "arm"]).agg(y=("y", "mean"), pred=("pred", "mean")).reset_index()
    mean["err"] = mean.pred / mean.y - 1
    w = e[e.phase.isin(WRITE_PARTS)].groupby(["label", "arm"]).agg(y=("y", "sum"), pred=("pred", "sum"))
    w["err"] = w.pred / w.y - 1
    return rec, mean, w.reset_index()


def work(con, runs, formula, ns, measured):
    """Work in ms per run, arm, step, phase and reader of the priced phases: predicted from the
    counts, or with `measured`, the laptop's phase_work where it ran the configuration."""
    d = records(con, f"s.run_id IN ({','.join(repr(r) for r in runs)})")
    d["instr"] = formula.predict(d)
    d["reader"] = d.reader.fillna("")
    d["work_ms"] = [i * ns_of(ns, l, a, p, r) / 1e6
                    for i, l, a, p, r in zip(d.instr, d.label, d.arm, d.phase, d.reader)]
    cols = ["run_id", "label", "arm", "step", "phase", "reader", "instr", "work_ms"]
    write = d[d.phase.isin(WRITE_PARTS)].groupby(["run_id", "label", "arm", "step"])[["instr", "work_ms"]] \
        .sum().reset_index().assign(phase="batch", reader="")
    d = pd.concat([write[cols], d[~d.phase.isin(WRITE_PARTS)][cols]])
    d = d.groupby(["run_id", "label", "arm", "step", "phase", "reader"])[["instr", "work_ms"]].mean().reset_index()
    d["source"] = "formula"
    if measured:
        m = con.sql(f"""
            SELECT r.label, w.arm, w.step, w.phase, coalesce(w.reader, '') AS reader,
                   avg(w.work_ms) AS work_ms
            FROM phase_work w JOIN canonical_runs r USING (run_id)
            WHERE r.host <> 'havoc' AND {GRID.replace('campaigns', 'r.campaigns')}
              AND w.phase IN ('batch', 'compact', 'read_full', 'read_recent', 'read_window', 'read_sorted')
            GROUP BY ALL""").df()
        d = d.merge(m, on=["label", "arm", "step", "phase", "reader"], how="left", suffixes=("", "_m"))
        has = d.work_ms_m.notna()
        d.loc[has, "work_ms"] = d.work_ms_m[has]
        d.loc[has, "source"] = "measured"
        d = d.drop(columns="work_ms_m")
    return d


def prices(con, runs):
    """Requests priced per spike, per run, arm, step, phase and reader (a window's: the mean of the
    probe's)."""
    out = []
    for store, spike in SPIKES.items():
        for run in runs:
            p = con.sql(f"""SELECT run_id, arm, step, phase, coalesce(reader, '') AS reader, request_ms, requests
                            FROM sized_latency('{run}', '{spike}')""").df()
            out.append(p.assign(store=store))
    return pd.concat(out)


def steps(con, workload, formula, ns, measured):
    """Simulated time per store, run, arm, step and priced phase (a write, a compaction and each
    read of READS under its reader): `ms` = the work (`work`) plus the requests priced by size
    (`prices`)."""
    runs = con.sql(f"""SELECT run_id, label FROM canonical_runs
                       WHERE {GRID} AND host = 'havoc' AND status = 'finished' AND {WORKLOADS[workload]}""").df()
    w = work(con, list(runs.run_id), formula, ns, measured)
    p = prices(con, list(runs.run_id))
    d = p.merge(w, on=["run_id", "arm", "step", "phase", "reader"], how="left")
    priced = d.phase.isin(["batch", "compact"])
    for phase, reader in READS.values():
        priced |= (d.phase == phase) & (d.reader == reader)
    d = d[priced].copy()
    d["ms"] = d.work_ms + d.request_ms
    return d


def policies(con, workload, formula, ns, measured, d=None):
    """Each policy's mean write (with its compactions spread over its writes) and reads, per store,
    over `steps` (or `d`, its result, when given)."""
    if d is None:
        d = steps(con, workload, formula, ns, measured)
    rows = []
    for (store, label, arm), g in d.groupby(["store", "label", "arm"]):
        writes = g[(g.phase == "batch") & (g.step >= 1)]
        n = writes.step.nunique()
        row = dict(workload=workload, store=store, label=label, arm=arm, design=DESIGN.get(arm, arm),
                   write_ms=writes.ms.mean(), write_work_ms=writes.work_ms.mean(),
                   compact_ms=g[g.phase == "compact"].ms.sum() / n,
                   source=",".join(sorted(g.source.dropna().unique())))
        for kind, (phase, reader) in READS.items():
            r = g[(g.phase == phase) & (g.reader == reader) & (g.step >= 5)]
            row[f"{kind}_ms"] = r.ms.mean() if len(r) else np.nan
            row[f"{kind}_work_ms"] = r.work_ms.mean() if len(r) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def rank(pol):
    out = []
    for (workload, store), g in pol.groupby(["workload", "store"]):
        for mix, reads in MIXES.items():
            cost = g.write_ms + g.compact_ms
            for kind, n in reads.items():
                cost = cost + n * g[f"{kind}_ms"]
            r = g.assign(mix=mix, cost_ms=cost).sort_values("cost_ms")
            r["rank"] = np.arange(1, len(r) + 1)
            r["over_best"] = r.cost_ms / r.cost_ms.iloc[0]
            out.append(r)
    return pd.concat(out)


def name_policies(con, pol):
    """Adds `policy`: the design and what its configuration sets apart from the defaults (16 MiB
    row groups, no compaction, the tail sealed at once, the deferred merges' θ and deltas)."""
    p = con.sql("""
        SELECT r.label, rp.key, any_value(rp.value) AS value FROM run_params rp JOIN canonical_runs r USING (run_id)
        WHERE r.host = 'havoc' AND rp.key IN ('layout.rowGroupBytes', 'layout.compactEvery',
              'layout.tailSealBytes', 'layout.tierFanout', 'layout.defer.share', 'layout.defer.deltas')
        GROUP BY ALL""").df().pivot(index="label", columns="key", values="value")

    def name(label, arm, design):
        k = p.loc[label] if label in p.index else pd.Series(dtype=object)
        parts = []
        if arm == "rmor-density":
            share = float(k.get("layout.defer.share") or 0.06)
            parts.append(f"θ {share:g}")
            if k.get("layout.defer.deltas") == "write":
                parts.append("deltas by write")
        if arm == "mor-v3" and int(float(k.get("layout.compactEvery") or 0)):
            parts.append(f"compacted every {int(float(k['layout.compactEvery']))}")
        seal = int(float(k.get("layout.tailSealBytes") or 16 * 2**20)) // 2**20
        if arm in ("rmor-c", "rmow-c") and seal != 16:
            fan = int(float(k.get("layout.tierFanout") or 4))
            parts.append(f"sealed at {seal} MiB" + (f", fanout {fan}" if fan != 4 else ""))
        rg = int(float(k.get("layout.rowGroupBytes") or 16 * 2**20)) // 2**20
        if rg != 16:
            parts.append(f"{rg} MiB row groups")
        return design + (", " + ", ".join(parts) if parts else "")

    return pol.assign(policy=[name(l, a, d) for l, a, d in zip(pol.label, pol.arm, pol.design)])


def policy_map(pol, windows=None, fulls=None, sorted_reads=0.0):
    """The cheapest policy at each mix of windows and full reads a write (and `sorted_reads`), per
    workload and store: one row per cell, with its cost and the runner-up's over it."""
    windows = np.geomspace(0.1, 100, 31) if windows is None else windows
    fulls = np.geomspace(0.01, 10, 31) if fulls is None else fulls
    rows = []
    for (workload, store), g in pol.groupby(["workload", "store"]):
        base = (g.write_ms + g.compact_ms + sorted_reads * g.sorted_ms.fillna(np.inf)).to_numpy()
        for w in windows:
            for f in fulls:
                cost = base + w * g.window_ms.to_numpy() + f * g.full_ms.to_numpy()
                o = np.argsort(cost)
                best, second = g.iloc[o[0]], g.iloc[o[1]]
                rows.append(dict(workload=workload, store=store, windows=w, fulls=f,
                                 label=best.label, arm=best.arm, design=best.design,
                                 policy=best.get("policy", best.design), cost_ms=cost[o[0]],
                                 runner_up=second.get("policy", f"{second.label}/{second.design}"),
                                 margin=cost[o[1]] / cost[o[0]]))
    return pd.DataFrame(rows)


# The tuner's picks in the cloud (docs/HYPOTHESES.md, 2026-11-08, K1-K3): its seven policies.
CLOUD_POLICIES = [("upd", "tune-upd-mor-c12", "mor-v3"), ("upd", "tune-upd-rg4m", "rmow-c"),
                  ("upd", "tune-upd-rg4m", "rmor-density"), ("ing", "tune-ing-rg4m", "mor-v3"),
                  ("ing", "tune-ing-rg4m-seal256", "rmor-c"), ("ing", "tune-ing-rg4m", "rmow-c"),
                  ("ing", "tune-ing-rg4m", "rmor-c-kept")]
CLOUD = "list_contains(campaigns, 'tune-cloud')"


def per_write(d, col):
    """A policy's parts of a write, from its phase rows' column `col` (ms): the mean write, the
    compactions over the writes, and each priced read's mean over the probes of steps 5 to 30."""
    writes = d[(d.phase == "batch") & (d.step >= 1)]
    n = writes.step.nunique()
    out = dict(write_ms=writes[col].mean(), compact_ms=d[d.phase == "compact"][col].sum() / n)
    for kind, (phase, reader) in READS.items():
        r = d[(d.phase == phase) & (d.reader == reader) & (d.step >= 5)]
        out[f"{kind}_ms"] = r[col].mean() if len(r) else np.nan
    return out


def cloud(con, store):
    """The seven policies on `store` (s3x or azure), each part of a write in four bases, as
    policies() gives them (one frame per basis): `measured`, the cloud run's wall time; `priced`,
    its S3 Express twin's work (the configuration on that VM's disk, at its arm's pace there) plus
    the cloud run's requests priced by its spike (on S3 Express its own VM's, on Azure the one the
    VM ran first); and the two parts of the price, `work` and `requests`."""
    def one(sql):
        r = con.sql(sql).fetchall()
        if len(r) != 1:
            raise ValueError(f"{len(r)} runs for {sql}")
        return r[0][0]
    rows = {b: [] for b in ("measured", "priced", "work", "requests")}
    for workload, label, arm in CLOUD_POLICIES:
        run = one(f"SELECT run_id FROM canonical_runs WHERE {CLOUD} AND label = '{label}-{store}'")
        twin = one(f"SELECT run_id FROM canonical_runs WHERE {CLOUD} AND label = '{label}'")
        spike = one(f"SELECT run_id FROM runs WHERE {CLOUD} AND run_id LIKE 'spike/{store}-spike-%'"
                    + (f" AND run_id LIKE 'spike/s3x-spike-{label}-2%'" if store == "s3x" else ""))
        m = con.sql(f"SELECT * FROM sized_latency('{run}', '{spike}') WHERE arm = '{arm}'").df()
        t = con.sql(f"SELECT arm, step, phase, reader, work_ms FROM sized_latency('{twin}', '{spike}') "
                    f"WHERE arm = '{arm}'").df()
        d = m.drop(columns="work_ms").merge(t, on=["arm", "step", "phase", "reader"], how="left")
        d["reader"] = d.reader.fillna("")
        d["priced_ms"] = d.work_ms + d.request_ms
        base = dict(workload=workload, store=store, label=label, arm=arm, design=DESIGN[arm],
                    run=run, twin=twin, spike=spike)
        for basis, col in (("measured", "measured_ms"), ("priced", "priced_ms"), ("work", "work_ms"),
                           ("requests", "request_ms")):
            rows[basis].append(dict(base, **per_write(d, col)))
    return {b: pd.DataFrame(r) for b, r in rows.items()}


def cloud_judge(con, formula, ns, store):
    """K2 and K3 on `store`: per workload, mix and policy, its time per write predicted (the
    registered prices: the laptop's measured work, the base point's spikes), measured and priced
    (cloud()), with the pick (rank 1 predicted among the seven), whether the mix is judged (the
    predicted runner-up more than 5% above the pick) and each policy's measured time over the
    cheapest measured."""
    key = ["workload", "label", "arm"]
    seven = pd.DataFrame(CLOUD_POLICIES, columns=key)
    pred = pd.concat([policies(con, wl, formula, ns, True) for wl in WORKLOADS])
    pred = rank(pred[pred.store == store].merge(seven, on=key))
    c = cloud(con, store)
    out = pred[key + ["design", "mix", "rank", "over_best", "cost_ms"]].rename(
        columns={"cost_ms": "pred_ms", "over_best": "pred_over_best"})
    for basis in ("measured", "priced", "work", "requests"):
        r = rank(c[basis])[key + ["mix", "cost_ms"]].rename(columns={"cost_ms": f"{basis}_ms"})
        out = out.merge(r, on=key + ["mix"])
    out["meas_over_best"] = out.measured_ms / out.groupby(["workload", "mix"]).measured_ms.transform("min")
    second = out[out["rank"] == 2].set_index(["workload", "mix"]).pred_over_best
    out["judged"] = [second[(w, m)] > 1.05 for w, m in zip(out.workload, out.mix)]
    out["pick"] = out["rank"] == 1
    out["priced_over_measured"] = out.priced_ms / out.measured_ms
    return out.assign(store=store), c


def finalists(ranks, n=2):
    """Each workload's, store's and mix's n cheapest policies."""
    return ranks[ranks["rank"] <= n]


def judge(con, formula, ns):
    """T1 and T2 (docs/HYPOTHESES.md, 2026-11-08) on the finalists the laptop ran: a dict of
    tables. `missing`: finalists the laptop has not run. `t1`: per new finalist (a tune-*
    configuration) and arm, each priced phase's predicted and measured work. `t1_cost`: their time
    per write under each mix they are finalists in, predicted and on measured work. `t2`: per
    workload, store and mix, the pick and the cheapest finalist on measured work."""
    pred = pd.concat([policies(con, wl, formula, ns, False) for wl in WORKLOADS])
    meas = pd.concat([policies(con, wl, formula, ns, True) for wl in WORKLOADS])
    pr, mr = rank(pred), rank(meas)
    fin = finalists(pr)
    key = ["workload", "store", "label", "arm"]
    measured = meas[meas.source == "measured"]
    missing = fin.merge(measured[key], on=key, how="left", indicator=True)
    missing = missing[missing._merge == "left_only"][key].drop_duplicates()
    # work is the store's alike: each new finalist's (on any store) compared once, on S3 Express's rows
    new = fin[fin.label.str.startswith("tune-")][["workload", "label", "arm"]].drop_duplicates()
    cmp = new.assign(store="s3x").merge(pred, on=key).merge(measured, on=key, suffixes=("_p", "_m"))
    rows = []
    for _, r in cmp.iterrows():
        for k in ["write", "full", "window", "recent", "sorted"]:
            p, m = r[f"{k}_work_ms_p"], r[f"{k}_work_ms_m"]
            if pd.notna(p) and pd.notna(m) and m > 0:
                rows.append(dict(workload=r.workload, label=r.label, arm=r.arm, design=r.design_p,
                                 kind=k, pred_ms=p, meas_ms=m, err=p / m - 1))
    t1 = pd.DataFrame(rows)
    c = fin.merge(mr[key + ["mix", "cost_ms"]], on=key + ["mix"], suffixes=("_p", "_m"))
    t1_cost = c[c.label.str.startswith("tune-")].copy()
    t1_cost["err"] = t1_cost.cost_ms_p / t1_cost.cost_ms_m - 1
    t1_cost = t1_cost[["workload", "store", "mix", "label", "arm", "design", "cost_ms_p", "cost_ms_m", "err"]]
    rows = []
    for (wl, st, mix), g in fin.groupby(["workload", "store", "mix"]):
        m = g[key + ["rank"]].merge(mr[mr.mix == mix], on=key, suffixes=("", "_m"))
        if len(m) < len(g):
            continue
        pick = m[m["rank"] == 1].iloc[0]
        best = m.sort_values("cost_ms").iloc[0]
        rows.append(dict(workload=wl, store=st, mix=mix, pick=pick.label, pick_arm=pick.arm,
                         pick_design=pick.design, best=best.label, best_arm=best.arm,
                         best_design=best.design, pick_ms=pick.cost_ms,
                         best_ms=best.cost_ms, pick_over_best=pick.cost_ms / best.cost_ms))
    return dict(pred=pred, meas=meas, ranks=pr, measured_ranks=mr, finalists=fin, missing=missing,
                t1=t1, t1_cost=t1_cost, t2=pd.DataFrame(rows))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--measured", action="store_true")
    ap.add_argument("--loo", action="store_true")
    ap.add_argument("--judge", action="store_true")
    ap.add_argument("--top", type=int, default=4)
    ap.add_argument("--cloud", choices=["s3x", "azure"])
    ap.add_argument("--out")
    a = ap.parse_args()
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 30)
    con = connect()
    train = records(con, TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
    if a.loo:
        rec, mean, w = loo(train)
        print("held-out error, per record (|err| median, p90) and per configuration and arm (median, max):")
        for kind, g in mean.groupby("kind"):
            rr = rec[rec.kind == kind].err.abs()
            print(f"  {kind:13s} {rr.median():6.1%} {rr.quantile(0.9):6.1%}   "
                  f"{g.err.abs().median():6.1%} {g.err.abs().max():6.1%}  n={len(g)}")
        print(f"  {'write (sum)':13s} {'':13s}   {w.err.abs().median():6.1%} {w.err.abs().max():6.1%}  n={len(w)}")
        print(mean.sort_values("err").round(3).to_string())
        print(w.round(3).to_string())
        return
    formula = Formula(train)
    ns = pace(con)
    if a.cloud:
        j, _ = cloud_judge(con, formula, ns, a.cloud)
        cols = ["workload", "mix", "rank", "label", "design", "arm", "pred_ms", "measured_ms",
                "meas_over_best", "priced_ms", "priced_over_measured", "judged"]
        print(j.sort_values(["workload", "mix", "measured_ms"])[cols].round(3).to_string(index=False))
        k2 = j[j.pick & j.judged]
        print(f"\nK2 ({a.cloud}): the judged picks' measured time over the cheapest measured: "
              f"max {k2.meas_over_best.max():.3f} over {len(k2)}: "
              f"{'held' if (k2.meas_over_best <= 1.05).all() else 'CONTRADICTED'}")
        e = (j.priced_over_measured - 1).abs()
        print(f"priced over measured: {j.priced_over_measured.min():.3f}-{j.priced_over_measured.max():.3f}"
              + (f"; K3: {'held' if (e <= 0.2).all() else 'CONTRADICTED'} (within 20%)" if a.cloud == "s3x" else ""))
        return
    if a.judge:
        j = judge(con, formula, ns)
        if len(j["missing"]):
            print("finalists the laptop has not run:")
            print(j["missing"].to_string(index=False))
        t1 = j["t1"]
        if len(t1):
            print("\nT1, work per phase (predicted against measured, laptop pace):")
            print(t1.round(3).to_string(index=False))
            ns_ = t1[t1.kind != "sorted"].err.abs()
            print(f"  median |err| {ns_.median():.1%} (not sorted), max {ns_.max():.1%}; "
                  f"sorted max {t1[t1.kind == 'sorted'].err.abs().max():.1%}")
            print("\nT1, time per write under each mix (finalists from the grid):")
            print(j["t1_cost"].round(3).to_string(index=False))
        if len(j["t2"]):
            print("\nT2, the pick on measured work against the cheapest finalist:")
            print(j["t2"].round(3).to_string(index=False))
        return
    pol = pd.concat([policies(con, wl, formula, ns, a.measured) for wl in WORKLOADS])
    ranks = rank(pol)
    cols = ["rank", "label", "design", "cost_ms", "over_best", "write_ms", "compact_ms",
            "full_ms", "window_ms", "sorted_ms", "source"]
    for (workload, store, mix), g in ranks.groupby(["workload", "store", "mix"], sort=False):
        print(f"\n== {workload} {store} {mix} {MIXES[mix]}")
        print(g[cols].head(a.top).round(3).to_string(index=False))
    if a.out:
        os.makedirs(a.out, exist_ok=True)
        pol.to_csv(os.path.join(a.out, "policies.csv"), index=False)
        ranks.to_csv(os.path.join(a.out, "ranks.csv"), index=False)


if __name__ == "__main__":
    sys.exit(main())
