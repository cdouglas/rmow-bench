#!/usr/bin/env python3
"""Runs a campaign of rmow-bench configurations on whole idle cores, and reports on it.

  campaign.py run docker/campaigns/<name>.json   # in the container (docker/run.sh start)
  campaign.py status <results>/campaigns/<name>  # anywhere the results are (docker/run.sh status)

A campaign (docker/campaigns/*.json) names its jobs, one configuration each, with the heap each
needs, plus flags for every run and how many physical cores a job gets:

  {"description": "...", "flags": ["--log-requests"], "cores_per_job": 10, "counters": true,
   "jobs": [{"config": "configs/x.json", "heap": "32g", "store_gib": 20}, ...]}

`counters` says the runs need each thread's instruction counts (ThreadCounters, the CPU measure):
the campaign does not start unless the host allows perf_event_open (perf_event_paranoid <= 2).

**Placement.** A job owns `cores_per_job` whole physical cores inside one NUMA node, every SMT
sibling of each, and runs on the first `threads_per_core` threads of each (1 by default: the
siblings stay idle, so no SMT neighbour shares a core it measures CPU on). Jobs are placed on the
node with the most free cores, on its quietest cores, and no two jobs share a core. The heaviest
jobs go first, as listed. A job starts only when its heap and its store fit in what is free.

**Measurement hygiene.** CPU is part of every simulated latency (lab/pages/store-latency.md), so a
job's cores are watched while it runs, as shield's interference monitor watches a cell
(~/work/shield/shield-rs/analysis/shieldlib/interference.py). Every second, the busy time of its
cores (siblings included) less its own CPU time is foreign load, and the busy time of its idle
siblings, which it cannot run on, is foreign by construction. Leaving siblings idle does not keep
other users' threads off them: the scheduler places threads on idle CPUs. The samples carry wall-
clock times, as each phase of a run does (`start_ms`), so the lab can tell which phases foreign
load touched (`phase_load`). A job is flagged `interfered` when at least `threshold_cpus` of
foreign load persisted for at least `sustained_frac` of its samples; it is kept either way. The
JVM sees only its CPUs, and sizes its GC and JIT threads to them.

**Records.** `<results>/campaigns/<name>/` holds `progress.json` (rewritten as jobs change),
`<job>.log` (each run's console), `<job>.load.jsonl` (the samples, as they are taken) and
`<job>.json` (placement, run directory, exit status and the load's summary, once the job ends).
Runs go where bin/run.sh puts them, `<results>/<label>-<config hash>/<UTC start>/`, with the host,
the image and the placement in their manifests; the runner adds the samples as `load.jsonl` when
the run ends. Each job's objects go under `<store>/<name>/<job>/`, deleted when the run ends.

**Resuming.** Running a campaign again skips each job whose run finished (its manifest has
`finished`), and runs the rest again from the start. An interrupted job's run directory is left
as it is, without `finished`; `status` lists it so it can be deleted.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from glob import glob
from pathlib import Path

APP = Path("/app")
TICK = os.sysconf("SC_CLK_TCK")
WATCH_S = 1.0
#: shield's defaults for the `interfered` flag; a campaign may override them under "interference"
INTERFERENCE = {"threshold_cpus": 0.5, "sustained_frac": 0.25}


def expand(spec):
    out = []
    for part in spec.strip().split(","):
        if not part:
            continue
        a, _, b = part.partition("-")
        out.extend(range(int(a), int(b or a) + 1))
    return out


def compress(ids):
    ids = sorted(ids)
    runs = []
    for i in ids:
        if runs and i == runs[-1][1] + 1:
            runs[-1][1] = i
        else:
            runs.append([i, i])
    return ",".join(str(a) if a == b else f"{a}-{b}" for a, b in runs)


def read(path, default=""):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return default


def topology(allowed):
    """Whole physical cores among `allowed`, grouped by NUMA node: {node: [(cpu, sibling...)]}."""
    cores = set()
    for cpu in allowed:
        sibs = read(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
        group = tuple(sorted(expand(sibs))) if sibs else (cpu,)
        if set(group) <= allowed:  # a core is ours only with every one of its threads
            cores.add(group)
    node_of = {}
    for path in glob("/sys/devices/system/node/node[0-9]*/cpulist"):
        node = int(Path(path).parent.name[4:])
        for cpu in expand(read(path)):
            node_of[cpu] = node
    nodes = {}
    for core in sorted(cores):
        nodes.setdefault(node_of.get(core[0], 0), []).append(core)
    return nodes


def cpu_times():
    """{cpu: (total jiffies, idle jiffies)} from /proc/stat, which a container sees host-wide."""
    out = {}
    for line in Path("/proc/stat").read_text().splitlines():
        if line.startswith("cpu") and line[3:4].isdigit():
            f = line.split()
            nums = [int(x) for x in f[1:]]
            out[int(f[0][3:])] = (sum(nums[:8]), nums[3] + nums[4])
    return out


def busy(interval=1.0):
    a = cpu_times()
    time.sleep(interval)
    b = cpu_times()
    out = {}
    for c in a:
        dt = b[c][0] - a[c][0]
        out[c] = 0.0 if dt <= 0 else 1.0 - (b[c][1] - a[c][1]) / dt
    return out


def proc_jiffies(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        return int(stat[11]) + int(stat[12])
    except (OSError, IndexError, ValueError):
        return None


def busy_jiffies(cpus):
    """{cpu: busy jiffies} for `cpus`: everything but idle and iowait."""
    return {c: total - idle for c, (total, idle) in cpu_times().items() if c in cpus}


class Monitor:
    """Foreign load on one job's cores, sampled every WATCH_S seconds into a JSONL file.

    Each sample covers the wall-clock interval [t0, t1] (epoch seconds) and gives, in CPUs:
    `own_cpus`, the job's CPU time; `foreign_cpus`, the busy time of every CPU it owns less its
    own (kernel work on its behalf lands here too, so the floor is small but not zero); and
    `sibling_busy_cpus`, the busy time of the owned CPUs it may not run on, foreign by
    construction."""

    def __init__(self, pid, run_cpus, owned_cpus, path):
        self.pid = pid
        self.owned = set(owned_cpus) | set(run_cpus)
        self.idle = self.owned - set(run_cpus)
        self.path = Path(path)
        self.path.write_text("")
        self.samples = []  # (t0, t1, own, foreign, sibling)
        self.last = self.read()

    def read(self):
        return time.time(), time.monotonic(), busy_jiffies(self.owned), proc_jiffies(self.pid)

    def sample(self):
        now = self.read()
        (t0, m0, busy0, own0), (t1, m1, busy1, own1) = self.last, now
        if own1 is None:  # the job has exited
            return
        self.last = now
        dt = m1 - m0
        if dt <= 0 or own0 is None:
            return
        d = {c: busy1.get(c, 0) - busy0.get(c, 0) for c in self.owned}
        own = max(0, own1 - own0) / TICK / dt
        foreign = max(0.0, sum(d.values()) / TICK / dt - own)
        sibling = sum(d[c] for c in self.idle) / TICK / dt
        self.samples.append((t0, t1, own, foreign, sibling))
        with self.path.open("a") as f:
            f.write(json.dumps({"t0": round(t0, 3), "t1": round(t1, 3), "own_cpus": round(own, 3),
                                "foreign_cpus": round(foreign, 3),
                                "sibling_busy_cpus": round(sibling, 3)}) + "\n")

    def summary(self, threshold_cpus, sustained_frac):
        n = len(self.samples)
        fs = [s[3] for s in self.samples]
        over = sum(1 for f in fs if f >= threshold_cpus)
        return {
            "watch_samples": n,
            "foreign_cpu_s": round(sum((s[1] - s[0]) * s[3] for s in self.samples), 1),
            "foreign_max_cpus": round(max(fs, default=0.0), 2),
            "foreign_mean_cpus": round(sum(fs) / n, 3) if n else 0.0,
            "sibling_busy_mean_cpus": round(sum(s[4] for s in self.samples) / n, 3) if n else 0.0,
            "sustained_frac": round(over / n, 3) if n else 0.0,
            "threshold_cpus": threshold_cpus,
            "interfered": bool(n) and over / n >= sustained_frac,
        }


def mem_available():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    return 0


def gib(spec):
    """'32g' -> bytes"""
    n, unit = float(spec[:-1]), spec[-1].lower()
    return int(n * {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}[unit])


def label_of(config):
    return json.loads((APP / config).read_text())["label"]


def finished(run_dir):
    try:
        return "finished" in json.loads((Path(run_dir) / "manifest.json").read_text())
    except (OSError, ValueError):
        return False


def run_dirs(results, label):
    return set(glob(f"{results}/{label}-*/*/manifest.json"))


class Job:
    def __init__(self, spec, out):
        self.config = spec["config"]
        self.name = Path(self.config).stem
        self.heap = spec.get("heap", "8g")
        self.store_gib = float(spec.get("store_gib", 0))
        self.label = label_of(self.config)
        self.record = out / f"{self.name}.json"
        self.log = out / f"{self.name}.log"
        self.load = out / f"{self.name}.load.jsonl"
        prior = json.loads(self.record.read_text()) if self.record.exists() else {}
        self.state = "done" if prior.get("run_dir") and finished(prior["run_dir"]) else "pending"
        self.prior = prior
        self.proc = None
        self.cores = []
        self.cpus = []
        self.node = None
        self.run_dir = prior.get("run_dir") if self.state == "done" else None
        self.started = self.ended = None
        self.monitor = None
        self.interference = INTERFERENCE
        self.before = set()

    def owned(self):
        return [c for core in self.cores for c in core]

    def summary(self):
        if self.state == "done" and self.prior and not self.proc:
            return self.prior
        load = self.monitor.summary(**self.interference) if self.monitor else {}
        return {
            "config": self.config,
            "state": self.state,
            "heap": self.heap,
            "node": self.node,
            "cpus": compress(self.cpus),
            "owned": compress(self.owned()),
            "run_dir": self.run_dir,
            "started": self.started,
            "ended": self.ended,
            "exit": None if self.proc is None else self.proc.returncode,
            **load,
        }


def run(campaign_file):
    spec = json.loads((APP / campaign_file).read_text())
    name = Path(campaign_file).stem
    results = Path(os.environ.get("RESULTS", "/results"))
    store = Path(os.environ.get("STORE", "/store"))
    out = results / "campaigns" / name
    out.mkdir(parents=True, exist_ok=True)

    # The cpuset: rootless Docker drops --cpuset-cpus silently, so apply it here as well
    # (sched_setaffinity needs no privilege, and children inherit it) and check it took.
    if os.environ.get("CPUSET"):
        want = set(expand(os.environ["CPUSET"]))
        os.sched_setaffinity(0, want)
        if set(os.sched_getaffinity(0)) != want:
            sys.exit(f"CPUSET={os.environ['CPUSET']} could not be applied: "
                     f"affinity is {compress(os.sched_getaffinity(0))}")
    allowed = set(os.sched_getaffinity(0))
    nodes = topology(allowed)
    per = int(os.environ.get("CORES_PER_JOB") or spec.get("cores_per_job", 4))
    tpc = int(os.environ.get("THREADS_PER_CORE") or spec.get("threads_per_core", 1))
    max_jobs = int(os.environ.get("JOBS") or 0) or sum(len(c) // per for c in nodes.values())
    if not any(len(c) >= per for c in nodes.values()):
        sys.exit(f"no NUMA node has {per} whole cores in {compress(allowed)}: lower CORES_PER_JOB")

    if spec.get("counters"):
        paranoid = read("/proc/sys/kernel/perf_event_paranoid", "?")
        if not paranoid.lstrip("-").isdigit() or int(paranoid) > 2:
            sys.exit(f"this campaign needs instruction counters, and kernel.perf_event_paranoid is "
                     f"{paranoid}: an admin can allow them with "
                     f"`sysctl -w kernel.perf_event_paranoid=2`")
    jobs = [Job(j, out) for j in spec["jobs"]]
    for job in jobs:
        job.interference = {**INTERFERENCE, **spec.get("interference", {})}
    flags = spec.get("flags", [])
    jar = next(p for p in glob(str(APP / "target/rmow-bench-*.jar")) if "-tests" not in p)
    classpath = f"{jar}:{APP}/target/lib/*"
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    lock = threading.RLock()  # the signal handler takes it on the main thread too
    # what jobs may use, fixed at the start: a JVM grows into its -Xmx, so the memory free when a
    # job starts says nothing about what the running ones will still take
    memory = mem_available() * 0.85
    disk = shutil.disk_usage(store).free / (1 << 30)
    stopping = threading.Event()

    def write_progress():
        state = {
            "campaign": name,
            "description": spec.get("description", ""),
            "image": os.environ.get("RMOW_IMAGE", ""),
            "git": os.environ.get("RMOW_GIT_SHA", ""),
            "git_dirty": os.environ.get("RMOW_GIT_DIRTY", ""),
            "host": os.environ.get("RMOW_HOST_NAME", ""),
            "cpuset": compress(allowed),
            "cores_per_job": per,
            "threads_per_core": tpc,
            "max_jobs": max_jobs,
            "started": started,
            "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "jobs": {j.name: j.summary() for j in jobs},
        }
        tmp = out / "progress.json.tmp"
        tmp.write_text(json.dumps(state, indent=2) + "\n")
        tmp.replace(out / "progress.json")

    def place(job):
        taken = {c for j in jobs if j.state == "running" for c in j.owned()}
        free = {n: [core for core in cs if not set(core) & taken] for n, cs in nodes.items()}
        fits = [n for n, cs in free.items() if len(cs) >= per]
        if not fits:
            return False
        load = busy()
        node = max(fits, key=lambda n: (len(free[n]), -sum(load.get(c, 0) for core in free[n]
                                                            for c in core)))
        quiet = sorted(free[node], key=lambda core: (round(max(load.get(c, 0) for c in core), 1),
                                                     core))
        job.cores = sorted(quiet[:per])
        job.cpus = [c for core in job.cores for c in core[:tpc]]
        job.node = node
        return True

    def launch(job):
        job_store = store / name / job.name
        shutil.rmtree(job_store, ignore_errors=True)  # a killed run's objects
        job_store.mkdir(parents=True)
        placement = (f"campaign={name} job={job.name} node={job.node} cpus={compress(job.cpus)} "
                     f"owned={compress(job.owned())}")
        # Arrow (the arms' vectorized Parquet reads) needs java.nio open on Java 17
        cmd = ["java", f"-Xmx{job.heap}", "--add-opens=java.base/java.nio=ALL-UNNAMED",
               *os.environ.get("JAVA_EXTRA", "").split(),
               f"-Drmow.build.git={os.environ.get('RMOW_GIT_SHA', '')}",
               f"-Drmow.build.dirty={os.environ.get('RMOW_GIT_DIRTY', '') == 'true'}",
               f"-Drmow.build.snapshot=image:{os.environ.get('RMOW_IMAGE', 'unknown')}",
               f"-Drmow.host.name={os.environ.get('RMOW_HOST_NAME', 'unknown')}",
               f"-Drmow.host.os={os.environ.get('RMOW_HOST_OS', '')}",
               f"-Drmow.placement={placement}",
               "-cp", classpath, "org.rmow.bench.bench.Main", str(APP / job.config),
               "--out", str(results), "--store-root", str(job_store), *flags]
        job.before = run_dirs(results, job.label)
        log = open(job.log, "a")
        log.write(f"== {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {placement}\n")
        log.flush()
        cpus = set(job.cpus)
        job.proc = subprocess.Popen(cmd, cwd=str(APP), stdout=log, stderr=subprocess.STDOUT,
                                    preexec_fn=lambda: os.sched_setaffinity(0, cpus))
        log.close()
        job.state = "running"
        job.started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        job.monitor = Monitor(job.proc.pid, job.cpus, job.owned(), job.load)
        print(f"start {job.name}: node {job.node}, cpus {compress(job.cpus)}, heap {job.heap}",
              flush=True)

    def find_run_dir(job):
        new = sorted(run_dirs(results, job.label) - job.before)
        if new:
            job.run_dir = str(Path(new[-1]).parent)

    def end(job):
        """Records a job that has stopped, and gives its run the load samples."""
        job.ended = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        find_run_dir(job)
        if job.run_dir and job.load.exists():
            shutil.copyfile(job.load, Path(job.run_dir) / "load.jsonl")
        job.record.write_text(json.dumps(job.summary(), indent=2) + "\n")

    def watch():
        ticks = 0
        while not stopping.wait(WATCH_S):
            with lock:
                for job in jobs:
                    if job.state != "running" or job.monitor is None:
                        continue
                    job.monitor.sample()
                    if job.run_dir is None:
                        find_run_dir(job)
                ticks += 1
                if ticks % 5 == 0:
                    write_progress()

    def stop(signum, _frame):
        stopping.set()
        with lock:
            for job in jobs:
                if job.state == "running":
                    job.proc.terminate()
                    job.state = "interrupted"
        print(f"signal {signum}: stopping running jobs", flush=True)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    done = [j.name for j in jobs if j.state == "done"]
    if done:
        print(f"resume: {len(done)} of {len(jobs)} jobs already finished: {', '.join(done)}")
    print(f"cpuset {compress(allowed)}: {sum(len(c) for c in nodes.values())} whole cores on "
          f"{len(nodes)} node(s); {per} cores per job, at most {max_jobs} at once; "
          f"{memory / (1 << 30):.0f} GiB for heaps, {disk:.0f} GiB for stores", flush=True)
    write_progress()
    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()

    while not stopping.is_set():
        with lock:
            running = [j for j in jobs if j.state == "running"]
            for job in running:
                if job.proc.poll() is None:
                    continue
                find_run_dir(job)
                ok = job.proc.returncode == 0 and job.run_dir and finished(job.run_dir)
                job.state = "done" if ok else "failed"
                end(job)
                job.prior = job.summary()
                load = job.prior
                print(f"{job.state} {job.name}: exit {job.proc.returncode}, {job.run_dir}, "
                      f"foreign load {load.get('foreign_cpu_s', 0)} CPU-s, mean "
                      f"{load.get('foreign_mean_cpus', 0)} CPUs"
                      f"{' (interfered)' if load.get('interfered') else ''}", flush=True)
            pending = [j for j in jobs if j.state == "pending"]
            running = [j for j in jobs if j.state == "running"]
            if not pending and not running:
                break
            for job in pending:
                if len(running) >= max_jobs:
                    break
                heaps = sum(gib(j.heap) for j in running) + gib(job.heap)
                stores = sum(j.store_gib for j in running) + job.store_gib
                if running and (heaps > memory or stores > disk):
                    break  # wait for a running job's memory or store rather than share it
                if not place(job):
                    break
                launch(job)
                running.append(job)
            write_progress()
        time.sleep(2)

    stopping.set()
    with lock:
        for job in jobs:
            if job.state == "interrupted":
                job.proc.wait()
                end(job)
        write_progress()
    failed = [j.name for j in jobs if j.state != "done"]
    print("campaign done" if not failed else f"not finished: {', '.join(failed)}", flush=True)
    return 0 if not failed else (143 if any(j.state == "interrupted" for j in jobs) else 1)


def status(campaign_dir):
    d = Path(campaign_dir)
    try:
        p = json.loads((d / "progress.json").read_text())
    except (OSError, ValueError):
        print(f"no progress.json in {d}")
        return 1
    print(f"campaign {p['campaign']}: git {p['git'][:12]}{' (dirty)' if p['git_dirty'] == 'true' else ''}"
          f", host {p['host']}, cpuset {p['cpuset']}, {p['cores_per_job']} cores per job")
    print(f"  started {p['started']}, updated {p['updated']}")
    for name, j in p["jobs"].items():
        line = f"  {name:32} {j.get('state', 'done'):11}"
        if j.get("cpus"):
            line += f" node {j['node']} cpus {j['cpus']:14}"
        run_dir = j.get("run_dir")
        if run_dir and j.get("state") == "running":
            local = d.parent.parent / Path(run_dir).relative_to("/results") \
                if run_dir.startswith("/results") else Path(run_dir)
            steps = local / "steps.jsonl"
            try:
                last = steps.read_text().rstrip("\n").rsplit("\n", 1)[-1]
                cfg = json.loads((local / "manifest.json").read_text())["config"]
                line += f" step {json.loads(last)['step']} of {cfg['workload']['batches']}"
            except (OSError, ValueError, KeyError, IndexError):
                pass
        if j.get("foreign_cpu_s"):
            line += f"  foreign {j['foreign_cpu_s']} CPU-s (max {j['foreign_max_cpus']} CPUs)"
        if "sustained_frac" in j:
            line += (f", {100 * j['sustained_frac']:.0f}% of samples at {j['threshold_cpus']}+"
                     f"{' INTERFERED' if j.get('interfered') else ''}")
        print(line)
        if run_dir and j.get("state") in ("failed", "interrupted"):
            print(f"  {'':32} unfinished run (delete it): {run_dir}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "run":
        sys.exit(run(sys.argv[2]))
    if len(sys.argv) == 3 and sys.argv[1] == "status":
        sys.exit(status(sys.argv[2]))
    print(__doc__)
    sys.exit(2)
