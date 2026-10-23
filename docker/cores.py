#!/usr/bin/env python3
# Copied from the shield harness (~/work/shield/shield-rs/docker/cores.py, 2026-10-23), unchanged
# but for this note: CORES_PER_JOB and JOBS mean here what they mean there (docker/README.md).
"""Report a machine's CPU topology and what is busy on it, and propose a cpuset.

Run on the benchmark host (`docker/run.sh cores`). Two questions it answers that
`nproc` cannot:

**Which CPUs are whole physical cores?** On an SMT machine the kernel's CPU ids are threads, not
cores, and the two siblings of one core share execution resources. Taking "the other half" by id
can hand you the siblings of the cores someone else is running on — you would share physical cores
with them, and both measurements would be wrong in a way neither of you could see. This groups CPUs
by `thread_siblings_list` so a cpuset can be chosen in whole cores.

**Which CPUs are actually in use?** Sampled from `/proc/stat` rather than guessed from a core count,
because "72 of 144" does not say *which* 72.
"""

import os
import sys
import time
from pathlib import Path

CPU = Path("/sys/devices/system/cpu")


def read(path, default=""):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return default


def expand(spec):
    """"0-3,8" -> [0,1,2,3,8]"""
    out = []
    for part in spec.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def compress(ids):
    """[0,1,2,3,8] -> "0-3,8" — the form taskset and docker --cpuset-cpus want."""
    ids = sorted(ids)
    if not ids:
        return ""
    runs, start, prev = [], ids[0], ids[0]
    for i in ids[1:]:
        if i == prev + 1:
            prev = i
            continue
        runs.append((start, prev))
        start = prev = i
    runs.append((start, prev))
    return ",".join(str(a) if a == b else f"{a}-{b}" for a, b in runs)


def topology():
    """{cpu_id: (socket, core_id, (siblings...))}"""
    out = {}
    for d in sorted(CPU.glob("cpu[0-9]*")):
        cid = int(d.name[3:])
        sibs = read(d / "topology/thread_siblings_list")
        if not sibs:
            continue
        out[cid] = (
            read(d / "topology/physical_package_id", "?"),
            read(d / "topology/core_id", "?"),
            tuple(expand(sibs)),
        )
    return out


def busy(interval=2.0):
    """Per-CPU busy fraction over `interval` seconds, from /proc/stat."""

    def snap():
        vals = {}
        for line in Path("/proc/stat").read_text().splitlines():
            if line.startswith("cpu") and line[3:4].isdigit():
                f = line.split()
                cid = int(f[0][3:])
                nums = [int(x) for x in f[1:]]
                idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
                vals[cid] = (sum(nums), idle)
        return vals

    a = snap()
    time.sleep(interval)
    b = snap()
    out = {}
    for cid in a:
        dt = b[cid][0] - a[cid][0]
        di = b[cid][1] - a[cid][1]
        out[cid] = 0.0 if dt <= 0 else max(0.0, min(1.0, 1.0 - di / dt))
    return out


def _cpu_jiffies():
    """{pid: utime+stime} for every process we can read."""
    out = {}
    for d in Path("/proc").iterdir():
        if not d.name.isdigit():
            continue
        try:
            stat = (d / "stat").read_text().rsplit(") ", 1)[-1].split()
            out[d.name] = int(stat[11]) + int(stat[12])
        except (OSError, IndexError, ValueError):
            continue
    return out


def consumers(interval=2.0, top=6):
    """Who is using the machine **now**, with thread counts and CPU affinity.

    Ranked by CPU consumed over the interval, not by cumulative time since the process started —
    otherwise a long-lived idle process outranks the job that is actually saturating the machine,
    which is the opposite of useful.

    Affinity is the part that decides what you can do. A neighbour pinned to a cpuset can be worked
    around by taking the complement; an **unpinned** one cannot, because the scheduler will place
    it anywhere, including on whatever you reserve for yourself.
    """
    ticks = os.sysconf("SC_CLK_TCK")
    a = _cpu_jiffies()
    time.sleep(interval)
    b = _cpu_jiffies()

    out = []
    for pid, after in b.items():
        delta = after - a.get(pid, after)
        if delta <= 0:
            continue
        fields = {}
        try:
            for line in (Path("/proc") / pid / "status").read_text().splitlines():
                k, _, v = line.partition(":")
                fields[k] = v.strip()
        except OSError:
            continue
        out.append({
            "pid": pid,
            "comm": fields.get("Name", "?"),
            "threads": int(fields.get("Threads", "1") or 1),
            "affinity": fields.get("Cpus_allowed_list", "?"),
            "cpus": delta / ticks / interval,  # CPUs' worth of work, e.g. 8.0 = eight busy cores
        })
    out.sort(key=lambda p: -p["cpus"])
    return out[:top]


def bar(frac, width=18):
    filled = int(round(frac * width))
    return "#" * filled + "." * (width - filled)


def main():
    # Tolerant on purpose: this is called from a shell wrapper, and a wrapper that passes an empty
    # argument should not turn a diagnostic tool into a traceback.
    threshold = 0.20
    if len(sys.argv) > 1 and sys.argv[1].strip():
        try:
            threshold = float(sys.argv[1])
        except ValueError:
            print(f"ignoring unparseable busy threshold {sys.argv[1]!r}; using {threshold}",
                  file=sys.stderr)
    topo = topology()
    if not topo:
        print("could not read /sys topology; falling back to a flat CPU list")
        return 1
    load = busy()

    # Group by (socket, core): a physical core and the threads that share it.
    cores = {}
    for cid, (sock, core, _sibs) in topo.items():
        cores.setdefault((sock, core), []).append(cid)

    sockets = sorted({s for s, _ in cores})
    tpc = max(len(v) for v in cores.values())
    print(f"topology: {len(topo)} CPUs = {len(sockets)} socket(s) x "
          f"{len(cores) // max(1, len(sockets))} cores x {tpc} thread(s)")
    if tpc > 1:
        sample = [sorted(v) for v in list(cores.values())[:3]]
        pairing = ", ".join("(" + ",".join(str(c) for c in v) + ")" for v in sample)
        stride = sample[0][1] - sample[0][0] if len(sample[0]) > 1 else 0
        print(f"  SMT siblings: {pairing}, ...  (stride {stride})")
    print(f"busy threshold: {threshold:.0%} over 2s\n")

    free_cores, used_cores = [], []
    for key in sorted(cores, key=lambda k: (k[0], int(k[1]))):
        cpus = sorted(cores[key])
        hot = max(load.get(c, 0.0) for c in cpus)
        (used_cores if hot >= threshold else free_cores).append(key)

    # The distribution, not just a count. "144 of 144 busy" describes a machine with one CPU pegged
    # and 143 at 21% exactly as it describes one under uniform half load, and the two call for
    # opposite decisions.
    vals = sorted(load.values())
    n = len(vals)
    mean = sum(vals) / max(1, n)
    pct = lambda q: vals[min(n - 1, int(q * n))] if n else 0.0
    print("utilisation over 2s, per CPU:")
    print(f"  min {vals[0]:.0%}   p50 {pct(0.5):.0%}   p90 {pct(0.9):.0%}   max {vals[-1]:.0%}"
          f"   ({sum(1 for v in vals if v >= threshold)} of {n} at or above {threshold:.0%})")
    for sock in sockets:
        cpus = [c for k in cores if k[0] == sock for c in cores[k]]
        avg = sum(load.get(c, 0.0) for c in cpus) / max(1, len(cpus))
        idle = len([k for k in cores if k[0] == sock and k in free_cores])
        total = len([k for k in cores if k[0] == sock])
        print(f"  socket {sock}  {bar(avg)} {avg:5.0%} avg   {idle}/{total} cores idle")

    # CPUs and cores are counted separately, because on an SMT machine they can disagree
    # completely: every core can be "partly busy" while half the CPUs are idle. Reporting only the
    # core view makes a machine with 72 free hardware threads look entirely full.
    idle_cpus = sorted(c for c in topo if load.get(c, 0.0) < threshold)
    busy_cpus = sorted(c for c in topo if load.get(c, 0.0) >= threshold)
    core_cpus = sorted(c for k in free_cores for c in cores[k])
    print(f"\nCPUs   busy {len(busy_cpus):3d} / idle {len(idle_cpus):3d}"
          f"   idle: {compress(idle_cpus) or '-'}")
    print(f"cores  busy {len(used_cores):3d} / idle {len(free_cores):3d}"
          f"   idle: {compress(core_cpus) or '-'}"
          + ("   <- a core counts as busy if EITHER sibling is" if used_cores else ""))

    all_mask = compress(sorted(topo))
    top = consumers()
    unpinned_load = sum(p["cpus"] for p in top if p["affinity"] == all_mask)
    if top:
        print("\nusing the machine now (2s sample):")
        print(f"  {'PID':>8}  {'CPUS':>5}  {'THREADS':>7}  {'AFFINITY':<24} COMMAND")
        for p in top:
            tag = "" if p["affinity"] == all_mask else "  <- pinned"
            print(f"  {p['pid']:>8}  {p['cpus']:5.1f}  {p['threads']:>7}  "
                  f"{p['affinity']:<24} {p['comm']}{tag}")

    # The candidate reservation: every whole idle core, on every socket. A sweep no longer needs its
    # cpuset confined to one socket to keep a run inside one: the sweep places each round itself,
    # never splitting a job across a NUMA node (analysis/shieldlib/cores.py), and a cpuset larger
    # than a round is what lets it rotate replicates and route around a neighbour. One socket is
    # the fallback, not the aim.
    candidate, idle_by_sock = [], {}
    for k in free_cores:
        idle_by_sock.setdefault(k[0], []).append(k)
    if free_cores:
        candidate = sorted(c for k in free_cores for c in cores[k])

    print()

    # Unpinned load only dominates the diagnosis when it is large enough to matter — measured
    # against what we would reserve, or against the machine when there is nothing to reserve. A
    # shared host always carries a little unpinned background work (a metrics agent, a shell), and
    # calling that "you cannot isolate" on a 144-CPU machine would be nonsense.
    budget = 0.25 * len(candidate) if candidate else 0.10 * len(topo)
    if unpinned_load >= max(1.0, budget):
        print(f"DIAGNOSIS: about {unpinned_load:.0f} CPUs' worth of work is running UNPINNED "
              f"(affinity {all_mask}).")
        print("  A cpuset will NOT isolate you from it. Reserving CPUs for yourself does not stop")
        print("  the kernel placing someone else's threads there; only the *other* job confining")
        print("  itself, or boot-time isolation, would.")
        print("\n  Ask that job to pin itself (`taskset -pc <cpus> <pid>`) and rerun, wait for it,")
        print("  or run anyway and treat the numbers as CONTENDED.")
        return 2

    if candidate:
        if used_cores:
            print("DIAGNOSIS: load is concentrated on some cores and absent from others, which is")
            print("  what a PINNED neighbour looks like. The idle complement is yours to take.")
        else:
            print("DIAGNOSIS: the machine is idle.")
        if unpinned_load > 0.1:
            print(f"  ({unpinned_load:.1f} CPUs of unpinned background work — small against the "
                  f"{len(candidate)} threads below, but it can wander onto them.)")

        ncores = len(candidate) // max(1, tpc)
        socks = sorted(idle_by_sock)
        print("\nSuggested cpuset (every whole idle core, both SMT siblings):")
        print(f"  CPUSET={compress(candidate)}")
        print(f"  {ncores} physical cores / {len(candidate)} threads, on "
              + ", ".join(f"socket {sk} ({len(idle_by_sock[sk])})" for sk in socks))
        print("\nSized for the sweep. CORES_PER_JOB counts PHYSICAL cores, and a job never spans a")
        print("socket: the sweep runs in rounds and places each job inside one NUMA node, one job")
        print("per socket first, quietest and least-recently-used cores first. So capacity is per")
        print("socket, and cores beyond a round's are not waste — they are what placement uses to")
        print("rotate replicates and avoid a neighbour:")
        for per in (9, 6, 4):
            cap = {sk: len(idle_by_sock[sk]) // per for sk in socks}
            total = sum(cap.values())
            if not total:
                continue
            one_each = sum(1 for v in cap.values() if v)
            print(f"  CORES_PER_JOB={per}:  JOBS={one_each} gives each job a socket to itself; "
                  f"at most JOBS={total}"
                  + (" (full: no spare cores, so no avoidance)" if total > one_each else ""))
        print("\nThe siblings of each core are taken together on purpose: two runs sharing one"
              "\nphysical core through SMT interfere invisibly, and neither result is trustworthy.")
        return 0

    # Idle CPUs but no idle *cores*: the busy set covers one sibling of many cores. This is the
    # single most confusing state a shared SMT machine can be in, because "half the CPUs are free"
    # and "there is nothing to give you" are both true.
    if idle_cpus:
        print(f"DIAGNOSIS: {len(idle_cpus)} of {len(topo)} CPUs are idle, but NO physical core "
              f"is —")
        print("  the busy set holds one hardware thread on essentially every core.")
        print()
        print(f"  The idle CPUs ({compress(idle_cpus)}) are the SMT siblings of the busy ones.")
        print("  Taking them would put your run on the same physical cores as the other job, "
              "sharing")
        print("  execution units with it on every core. Both sets of numbers would be wrong, and")
        print("  nothing in either would say so.")

        # Size the proposal from the neighbour's own affinity mask, not from the instantaneous
        # busy count. Those differ by whatever transient work happens to be running — a metrics
        # agent, someone's `find` — and sizing from the noise hands the other job cores it never
        # asked for and knocks the split off a socket boundary.
        pinned = [p for p in top if p["affinity"] != all_mask]
        masks = {}
        for p in pinned:
            masks[p["affinity"]] = masks.get(p["affinity"], 0.0) + p["cpus"]
        if masks:
            dominant = max(masks, key=lambda m: masks[m])
            busy_n = len(expand(dominant))
            print(f"\n  Sizing from the pinned job's own mask ({dominant} = {busy_n} CPUs), not")
            print("  from the instantaneous busy count, which includes transient work.")
        else:
            busy_n = len(busy_cpus)
        repin, ordered = [], sorted(cores, key=lambda k: (k[0], int(k[1])))
        for k in ordered:
            if len(repin) + len(cores[k]) > busy_n:
                break
            repin.extend(sorted(cores[k]))
        if repin and len(repin) == busy_n:
            leftover = sorted(set(topo) - set(repin))
            print()
            print("  This is worth raising with whoever owns that job: the same number of CPUs,")
            print("  arranged as whole cores, would isolate both of you.")
            print(f"    they take : {compress(repin)}   ({len(repin) // tpc} whole cores)")
            print(f"    you take  : {compress(leftover)}   ({len(leftover) // tpc} whole cores)")
            sib = sorted(next(iter(cores.values())))
            stride = sib[1] - sib[0] if len(sib) > 1 else 1
            if stride > 1:
                print(f"  SMT siblings here are strided by {stride}, so \"the first half of the "
                      f"CPU ids\" is")
                print("  one hardware thread on every core rather than half of the cores — which "
                      "is")
                print("  almost certainly how this happened.")
            else:
                print("  SMT siblings here are adjacent, so whichever way that mask was chosen it")
                print("  landed on one thread of each core rather than on whole cores.")
            print()
            print("  To apply it to a RUNNING job (taskset(1)):")
            print(f"    taskset -a -pc {compress(repin)} <pid>")
            print("    ^^ -a/--all-tasks is not optional for a multithreaded process: without it")
            print("       taskset moves only the one thread you named, and the rest stay put.")
            print("       (taskset(1), OPTIONS: \"-a, --all-tasks: Set or retrieve the CPU")
            print("       affinity of all the tasks (threads) for a given PID.\")")
            print()
            print("  To see the mapping for yourself — which CPUs share a physical core:")
            print("    lscpu -e=CPU,CORE,SOCKET        # one row per CPU; equal CORE = same core")
            print("    cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list")
            print("  Further reading: taskset(1), lscpu(1), sched_setaffinity(2), cpuset(7).")
            print("  A job that does not need isolated cores loses nothing by this — it keeps the")
            print("  same number of hardware threads, just arranged as whole cores.")

        # If you are going to run anyway, run inside ONE socket: the cores are shared either way,
        # but memory locality need not be given up as well.
        by_sock_idle = {}
        for c in idle_cpus:
            by_sock_idle.setdefault(topo[c][0], []).append(c)
        if by_sock_idle:
            sock, cpus = max(by_sock_idle.items(), key=lambda kv: len(kv[1]))
            print("\n  Until then: wait, or run on the idle siblings and mark the numbers"
                  " CONTENDED —")
            print("  fine for a pipeline or correctness check, not for a baseline. Stay inside one")
            print("  socket, since the cores are shared either way and locality need not be too:")
            print(f"    CPUSET={compress(sorted(cpus))}   ({len(cpus)} sibling threads on "
                  f"socket {sock})")
            ncores = max(1, len(cpus) // max(1, tpc))
            for jobs in (2, 4):
                if ncores // jobs >= 1:
                    print(f"    JOBS={jobs} CORES_PER_JOB={ncores // jobs}   "
                          f"({ncores} physical cores here, not {len(cpus)})")
            print("  and stamp the contention into the records so nobody later mistakes them for")
            print("  a baseline:")
            print("    SWEEP_ARGS=\"--meta contended=smt-siblings\"")
        else:
            print("\n  Until then: wait, or run on the idle siblings and mark the numbers"
                  " CONTENDED.")
        return 2

    print("DIAGNOSIS: every CPU is at or above the threshold. Nothing to reserve; do not start a")
    print(f"  measurement run. (Raise the threshold, e.g. `cores {min(0.9, mean + 0.3):.1f}`, if "
          f"this is noise.)")
    return 2


if __name__ == "__main__":
    sys.exit(main())
