# Campaigns in a container (havoc)

The production-sized runs need more disk than this laptop has, and runs whose CPU counts toward
every simulated latency need cores nothing else is using. So they run on havoc, in a container
built there. The approach is the shield harness's (`~/work/shield/shield-rs/docker`), cut down:
- the image is built on the machine that runs it, from a commit;
- each job owns whole idle cores of one NUMA node;
- a job's cores are watched for foreign load;
- `start` resumes after an interruption.

| file | |
|---|---|
| `Dockerfile` | builds the jar with Maven, then a JRE image with the jar, `bin/`, `configs/` and the campaigns |
| `fork-m2.sh` | packs the atomicio fork's artifacts from this machine's Maven repository, for a build where the fork is not installed |
| `run.sh` | `build`, `cores`, `start`, `status`, `logs`, `stop`, `run`, `shell` |
| `campaign.py` | the runner inside the container: placement, launch, the foreign-load watch, resume; `status` on the host |
| `cores.py` | shield's report of topology and idle cores, with a proposed `CPUSET` |
| `campaigns/*.json` | `scale` (Iceberg's defaults, docs/HYPOTHESES.md at 1145f89), `scale-rerun` (the same, judged per phase), `pipeline` (a check of the image, minutes) |

## Running a campaign

The repository's remote (`self`, on GitHub) is private, so the code travels as a bundle of `main`. The atomicio fork
(`1.12.0-atomicio-SNAPSHOT`) is in no remote Maven repository either, so its artifacts travel as
a tar from the laptop's `~/.m2` (`fork-m2.sh`), and `build` installs them in the image's. Every
run's manifest names the fork's commit (`versions.iceberg`); repack after rebuilding the fork.

```sh
# here
git bundle create /tmp/rmow-bench.bundle main && docker/fork-m2.sh /tmp/rmow-fork-m2.tar
scp /tmp/rmow-bench.bundle /tmp/rmow-fork-m2.tar havoc:
# on havoc, the first time
git clone ~/rmow-bench.bundle rmow-bench
# on havoc, later
cd rmow-bench && git pull ~/rmow-bench.bundle main
```

Then, on havoc:

```sh
cd rmow-bench
docker/run.sh build                          # the image of HEAD (git archive: clean by construction),
                                             # on the fork in ~/rmow-fork-m2.tar (FORK_M2)
docker/run.sh cores                          # what is idle; proposes a CPUSET of whole cores
df -h .                                      # the scale campaign stores tens of GiB under STORE
CPUSET=36-71,108-143 STORE=/data/$USER/rmow-store docker/run.sh start pipeline   # minutes
CPUSET=36-71,108-143 STORE=/data/$USER/rmow-store docker/run.sh start scale
docker/run.sh status scale                   # any time, from any session
docker/run.sh logs scale                     # the runner's output; each run's is in campaigns/scale/<job>.log
```

`start` detaches. Under rootless Docker the daemon is a systemd user service, so without
lingering, logging out stops the campaign; `start` says when that applies (`loginctl
enable-linger`, once). Running `start` again resumes: jobs whose run finished are skipped, and
the rest start over. To rerun finished jobs, give the rerun a campaign of its own name.

| env | |
|---|---|
| `CPUSET` | the CPUs the campaign may use; give it both SMT siblings of every core (`cores` proposes one). Rootless Docker drops `--cpuset-cpus` silently, so the runner also sets it with `sched_setaffinity` and refuses to start if it did not take |
| `CORES_PER_JOB` | physical cores per job (the campaign's `cores_per_job`; `scale` has 10: one runner thread and the 8-thread reader, plus GC). A job owns every sibling of its cores and runs on `THREADS_PER_CORE` of them (1) |
| `JOBS` | at most this many at once (default: as many as fit). A job also waits until its heap fits in 85% of the memory free at the start, and its `store_gib` in the free disk |
| `RESULTS`, `STORE` | host directories for runs (`<repo>/results`) and the local store's objects (`<repo>/store`) |
| `JAVA_EXTRA` | more JVM options for every run |
| `DIRTY_OK=1` | `build` only: build the working tree instead of `HEAD`; the runs' manifests say dirty |
| `FORK_M2` | `build` only: the fork's artifacts (`fork-m2.sh`; default `~/rmow-fork-m2.tar`) |

## What a run records

Runs go where `bin/run.sh` puts them, `RESULTS/<label>-<config hash>/<UTC start>/`. A run's
`manifest.json` has:
- the host's name and OS (not the container's);
- the CPU model and the CPUs the run could use;
- the job's placement (`host.placement`: node, the CPUs it ran on, the cores it owned);
- the image (`versions.build_snapshot`) and its commit.

Each phase record in `steps.jsonl` has its start on the wall clock (`start_ms`), and the run gets
`load.jsonl` from the runner when it ends: the foreign-load samples below.

`RESULTS/campaigns/<name>/` holds the runner's records:
- `progress.json`;
- `<job>.log`, each run's console;
- `<job>.load.jsonl`, one sample a second while the job runs: over `[t0, t1]` (epoch seconds),
  the job's own CPUs, its **foreign load** (the busy time of its cores, siblings included, less
  its own CPU time) and the busy time of its idle siblings (foreign by construction);
- `<job>.json`: placement, exit, run directory, and the load's summary. As in shield, the job is
  flagged `interfered` when 0.5 CPUs of foreign load lasted for a quarter of its samples (a
  campaign's `"interference": {"threshold_cpus": ..., "sustained_frac": ...}` changes that), and
  kept either way.

Idle siblings do not keep other users' work off a job's cores: the scheduler places threads on
idle CPUs. The scale campaign's three longer jobs saw 1.6–2.9 CPUs of it on average
(lab/pages/scale.md). So CPU is judged per phase: the lab's `phase_load` matches each phase to
the samples it overlaps, and only a `clean` phase (under 0.25 CPUs of foreign load throughout)
decides anything about CPU. Runs from before the samples (the first scale campaign) have none,
and decide nothing about CPU if their job had foreign load.

## Bringing results back

```sh
# here
rsync -av havoc:rmow-bench/results/scale-recency-* results/
rsync -av havoc:rmow-bench/results/campaigns/ results/campaigns/
lab ingest
```

The lab reads the runner's records into `container_jobs` (one row per job and run, with its
foreign load). `runs.host` and `runs.cpu_model` tell havoc's runs from the laptop's. Compare CPU
only between runs of one host.

## Notes

- **The build is part of the record.** The image is a commit: `build` sends `git archive HEAD`,
  and `start` warns when the checkout has moved on since. Rebuild after pulling.
- **No network.** Runs use the local store (`--network none`); only the build downloads.
- **Instruction counts need the host's leave.** The CPU measure counts each thread's user-mode
  instructions (`ThreadCounters`, through `perf_event_open`). The container gets `CAP_PERFMON`,
  which Docker's seccomp profile asks for, but the host must also allow unprivileged counting:
  `kernel.perf_event_paranoid` at most 2 (an admin: `sysctl -w kernel.perf_event_paranoid=2`).
  havoc's is 4. A campaign with `"counters": true` will not start without it, and each run's
  manifest says whether it had counts (`host.thread_counters`).
- **Interrupted runs** leave a run directory without `finished` in its manifest. `status` lists
  it, and the lab ignores it; delete it by hand. A job's leftover objects are deleted when it
  starts again.
