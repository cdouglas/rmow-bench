# Cloud deployment (S3 Express and Azure, Terraform)

Runs on a cloud store, from VMs beside it: the calibration ([docs/S3X_RUNBOOK.md](../docs/S3X_RUNBOOK.md),
[docs/AZURE_RUNBOOK.md](../docs/AZURE_RUNBOOK.md)), or the cloud twins of runs made on the laptop or
havoc (since 2026-10-29). One VM runs the configurations in turn, or a fleet runs one each, all at
once (`FLEET`, since 2026-10-30). Terraform makes the store, the VMs' credentials and the VMs, and
`remote.sh` runs the runbook's steps on each VM: the builds, the contract suites, the spike and the
runs. `deploy.sh` drives both from here
and brings the results back into `results/`. The layout follows shield-rs's `deploy/`.

> **Status: in use** (the calibration, 2026-10-28; the base point on both clouds, 2026-10-30 to
> 11-01; the sweeps, as a fleet of nine VMs on AWS and one VM in turn on Azure, 2026-11-01). It was
> first written and checked offline: `./deploy/validate.sh` passes
> (`terraform fmt` and `validate` on the four layers, `bash -n`, and shellcheck if installed), and
> a read-only `terraform plan` of the AWS infra layer resolves its lookups (2026-10-28). The VM's
> `build` step ran in an Ubuntu 24.04 container (9 minutes), and the local twin's configuration on
> its build sent the registered twin's requests and bytes (`compare_runs.py`: 0 of 862 records
> differ). `infra-up` and `up` provision **billable** resources; review `terraform plan` and tear
> down when done. The first `infra-up`, in us-east-1 (2026-10-28), failed on `casuser`'s IAM and
> was torn down. AWS now runs in us-west-2 on catalog-bench's bucket and instance profile, as
> catalog-bench's runs did, and its infra layer plans only the key pair and the security group.
> The calibration ran on both clouds from one VM each (2026-10-28). The fleet, checked offline on
> 2026-10-30, ran the sweeps on 2026-11-01: nine VMs on AWS, each running its configuration's twin
> (`run twin`), then its spike and run, each fetched and taken down as it finished; six of the nine
> first boots failed on the Ubuntu mirror's 503s and were rerun by hand, and `cloud-init.sh` now
> retries. On Azure the same deploy ran one VM (`FLEET=false`), one configuration at a time.

## Layout

```
deploy/
  deploy.sh              # the driver (infra-up, up, ship, run, status, wait, fetch, down, ...)
  remote.sh              # the steps on the VM: build, test, spike, bench (detached)
  cloud-init.sh          # the VM's first boot: OpenJDK 17, Maven, git and tools (both clouds)
  deploy.env.example     # copy to deploy.env: the cloud, the SSH key, the builds, the sizes
  validate.sh            # the offline checks
  <cloud>/infra/main.tf  # the store, the VMs' credentials, the network: free while empty
  <cloud>/bench/main.tf  # the VMs: billable, made and destroyed per campaign
```

for `<cloud>` ∈ `aws | azure`. Each `bench` layer reads its `infra` layer's outputs (a local-backend
`terraform_remote_state`), and both expose the same two outputs (`public_ips`, a map of the VMs'
names to their addresses, and `ssh_user`).

| layer | AWS (S3 Express) | Azure (ADLS Gen2) |
|---|---|---|
| `infra` (kept) | a directory bucket in one zone, a role with `s3express:CreateSession` on it and its instance profile (or an existing bucket and profile: `AWS_BUCKET`, `AWS_INSTANCE_PROFILE`), the operator's key pair, a security group, the S3 Express gateway endpoint (unless the VPC has one) | a resource group, a StorageV2 account with a hierarchical namespace and a container, a user-assigned identity with Storage Blob Data Contributor on the account, a vnet and subnet (with the storage service endpoint), an NSG |
| `bench` (churned) | an EC2 instance per VM, in the bucket's zone (64 GiB gp3) | a VM per VM name, with the identity, its public IP, NIC and disk |

**No secrets in configuration.** The VM's credentials are its role or identity, which the SDKs'
default chains find. The two exceptions are the fork's contract suites, which take keys:
- on AWS, `remote.sh` takes the instance role's temporary keys from its metadata for that suite;
- on Azure, `deploy.sh run` signs an account SAS here with the account's key (the key stays here),
  valid for `SAS_HOURS` (8). The SAS sits on the VM, mode 600, only until the test step reads and
  deletes it.

Terraform's state holds the Azure account's keys, as any azurerm state does. It is local and
gitignored.

## Prerequisites

- `terraform` (>= 1.3), `jq`, `curl`, `rsync` and `git` here.
- The cloud's CLI, logged in:
  - **aws:** `aws configure` or SSO. The key pair is imported from `SSH_PUBLIC_KEY`. Making the
    instance's role takes IAM writes (`iam:CreateRole`, `PutRolePolicy`, `CreateInstanceProfile`,
    `AddRoleToInstanceProfile`), and `up` takes `iam:PassRole` on it. Terraform reads a directory
    bucket through a session, so the operator needs `s3express:CreateSession` on it: without that,
    the refresh drops the bucket from the state, and the next apply fails making it again.
    `casuser` has neither IAM writes nor sessions on a new bucket (2026-10-28), so it
    reuses an existing directory bucket and an instance profile whose role may
    open sessions on it: `AWS_BUCKET` and `AWS_INSTANCE_PROFILE` (deploy.env.example names
    catalog-bench's, in usw2-az3). The runs write under the bucket's `rmow-bench/` prefix, and
    `teardown` deletes that prefix rather than the bucket.
  - **azure:** `az login`. Assigning the identity's role needs Owner or User Access Administrator
    on the subscription. Azure takes only RSA keys: `ssh-keygen -t rsa -b 4096 -f
    ~/.ssh/rmow-bench-cloud` (or reuse shield-rs's `~/.ssh/shield-rs-cloud`).
- rmow-bench's commits on `main`: `ship` bundles `main`, since the repository is private. The fork
  is public, and the VM clones it.

## Use

```sh
cp deploy/deploy.env.example deploy/deploy.env
$EDITOR deploy/deploy.env            # CLOUD, the SSH key; the builds' commits are the runbooks'

./deploy/deploy.sh infra-up          # the store and the network, once (free while empty)
./deploy/deploy.sh up                # the VM (in a fleet, every VM)
./deploy/deploy.sh ship              # bundle main, ship it; the VM builds the fork and rmow-bench
./deploy/deploy.sh wait              # until the builds end (about 15 minutes)
./deploy/deploy.sh run               # test, spike, bench, in order, stopping at a failure
./deploy/deploy.sh wait
./deploy/deploy.sh fetch             # results/ and deploy/logs/; prints thread_cpu_per_wall and thread_counters
./deploy/deploy.sh down              # the VMs; the store stays, so the next `up` is fast
./deploy/deploy.sh teardown          # the store and every object in it, once `down`

./deploy/deploy.sh all               # infra-up through fetch
CLOUD=azure ./deploy/deploy.sh all   # the same on Azure, without editing deploy.env
```

`run` takes steps, to run one at a time (`run test`, then `run spike`, and so on). `status` shows
whether the steps still run and the log's tail, and `ssh` opens a shell on the VM. `wait` waits on
a VM it cannot reach, whose steps run on, and gives up on it after `WAIT_UNREACHABLE_MIN` minutes
(60 by default), failing with its name; the likeliest cause is this machine's address leaving the
SSH rule (rerun `infra-up`, then `wait` again). The steps run
detached on the VM (`setsid nohup`), so a dropped connection does not stop them. A step whose suite
skipped every test fails: a suite without credentials skips rather than fails.

**A fleet** (`FLEET=true`): one VM per configuration of `AWS_CONFIGS` or `AZURE_CONFIGS`, named
after it, which runs only that one; every command acts on all of them, each VM in turn, or on those
`VM` names:

```sh
./deploy/deploy.sh up                          # every VM, at once
./deploy/deploy.sh ship && ./deploy/deploy.sh wait
VM=sweep-base ./deploy/deploy.sh run test      # the contract suites, on one VM
./deploy/deploy.sh run spike bench             # each VM's spike, then its run
./deploy/deploy.sh status                      # each VM's state and the log's last lines
VM=sweep-p10 ./deploy/deploy.sh status 40      # one VM's, longer
VM=sweep-p10 ./deploy/deploy.sh fetch          # a VM that finished, while the others run
VM=sweep-p10 ./deploy/deploy.sh down           # and it goes; the others keep running
./deploy/deploy.sh wait && ./deploy/deploy.sh fetch && ./deploy/deploy.sh down
```

`up` adds the VMs it names to those up and never changes a VM that is up: each is made from the
image, `cloud-init.sh` and size of its own `up` (`ignore_changes`), so an apply that adds another
cannot restart it mid-run. `down` with `VM` destroys those VMs only, and fails if it cannot read
which are up; without `VM`, every VM goes. One `up` or `down` runs at a time per cloud (a lock
file). Each VM's spike writes its objects under `<root>/spike/<name>` and its results as
`results/spike/<store>-spike-<name>-<UTC start>`, so spikes that start in the same second stay
apart (the spikes' `--label`; `SPIKE_COMMIT` must have it), and its test step under
`<root>/test/<name>`. Run the contract suites on one VM: they check the store and the code, which
every VM shares.

**What the VM runs** is the runbook's steps:
- **build:** the fork at `FORK_COMMIT` (the twins' build, af875ccfd1) on branch `atomicio`, so its
  jars' build properties are the twins'. Its version comes from the release tag
  `apache-iceberg-1.12.0`, which the fork on GitHub does not carry, so the VM fetches it from
  Apache's repository. Gradle skips the engines (Spark, Flink, Kafka Connect). Then rmow-bench at
  `BENCH_COMMIT` (the calibration's: 9a69028, where C1–C4 were registered) and, in a worktree,
  `SPIKE_COMMIT` (`main`).
- **test, spike:** the runbook's steps 1–2.
- **bench:** step 3, each configuration of `AWS_CONFIGS` or `AZURE_CONFIGS` (`configs/<name>.json`
  at `BENCH_COMMIT`), `BENCH_REPS` times, for the spread of measured latency, with `-Xmx` of
  `BENCH_HEAP` and `bin/run.sh`'s `BENCH_FLAGS`; every run must send its twin's requests and bytes
  (C1). Each configuration goes into a copy outside the checkout, so the manifest records a clean
  tree:
  - **with a local store** (a run made on the laptop or havoc, such as the sweeps'), it runs on the
    cloud's store, under `<root>/<label>-s3x` or `-azure`, its label suffixed alike. Only the label
    and the store differ, so the local run is its twin, as `s3x-calibration-local` is the
    calibration's;
  - **with a cloud store** (the calibration's), it keeps its label, and its placeholder root becomes
    `<root>/calibration`, as before, so a rerun keeps its configuration's hash.

  `fetch` brings back the runs of every configured label. The defaults are the calibration's
  (`s3x-calibration`, `azure-calibration`, 3 reps, 8g, `--log-requests`), and a `deploy.env` from
  before the bench step (`CALIBRATION_COMMIT`, `CALIBRATION_REPS`) runs as it did. **The sweeps'
  twins:** `BENCH_COMMIT` the commit havoc ran, `AWS_CONFIGS="sweep-base ..."`, `BENCH_HEAP="16g"`,
  `BENCH_FLAGS="--log-requests --log-held"`, and an instance with 32 GiB (`c7a.4xlarge` on AWS;
  Azure's `D8as_v6` has 32).
- **twin** (not in `run`'s default steps): each configuration with a local store, once, as it is,
  on the VM's disk (`--store-root ~/store/<label>`): the run's local twin, under the
  configuration's own label and hash, as the laptop's and havoc's are, on the build and the cores
  the cloud run will use, with instruction counts. A sweep's predictions are priced from it before
  its cloud run (`run twin`, `fetch`, register them, then `run spike bench`). Its store holds every
  arm's objects until the run ends, up to 60 GB for the sweeps (`sweep-rg64m`), so set `DISK_GB`
  to 128; the AWS disk is a gp3 at 500 MB/s and 6,000 IOPS, not its 125 MB/s baseline.

**The VM.** Ubuntu 24.04 and its OpenJDK 17 package, which the twins ran on (17.0.20.1 when this
was written). The defaults are AMD, as the twins' laptop is (Zen 4):
- `c7a.2xlarge` on AWS: Zen 4, 8 vCPUs, 16 GiB; offered in usw2-az3 (`c7a.4xlarge`: 16 vCPUs,
  32 GiB, for a 16 GB heap);
- `Standard_D8as_v6` on Azure: Zen 4, 8 vCPUs, 32 GiB (the subscription has no quota for `D8as_v5`, Zen 3, in westus).

The twins' predictions price CPU at the laptop's pace, so a VM of another family moves the
predicted times, but not the requests and bytes C1 checks. Any size works for C2, which uses the
run's own CPU.

**Instruction counts.** `cloud-init.sh` sets `kernel.perf_event_paranoid` to 2 (Ubuntu's default,
4, refused the calibration's runs their counts), so a run counts its instructions where the
hypervisor exposes the counters. Each run's manifest says whether it had them
(`host.thread_counters`).

## Cost

The instance-hours dominate: about $0.41/h for `c7a.2xlarge` ($0.82/h for `c7a.4xlarge`) and $0.43/h for `D8as_v6` (westus), on demand
in us-east-1 and eastus (us-west-2 charges the same for `c7a.2xlarge`). A campaign takes about an hour: the builds, the suites, the spike
(tens of minutes at 20 reps) and the calibration. A sweep configuration takes hours (the base
point: 2.1 h on the laptop, more with the store's latency), and in a fleet each VM costs its own
hours; `fetch` and `down` each VM as it finishes. The stores charge little: the runbooks' Cost
paragraphs. The bucket and the account cost nothing while empty, and every step deletes what it
wrote: each run deletes its objects when it ends (`Store.deleteRun`), so only a run that failed
leaves any, under `<root>`. `teardown` deletes those
with the store (in an existing bucket, with the `rmow-bench/` prefix).

## Caveats

- **The S3 Express zone:** the bucket and the instance share a zone ID (`AWS_AZ_ID`), which must
  offer S3 Express (in us-east-1: use1-az4, -az5, -az6; catalog-bench's bucket is in usw2-az3,
  which offers `c7a.2xlarge`) and the instance type. The default VPC needs a default subnet in it
  (`aws ec2 create-default-subnet` if not). A VPC has one S3 Express gateway endpoint per route
  table: where it has one already (us-west-2's does), set `AWS_CREATE_ENDPOINT=false`.
- **A shared bucket:** in catalog-bench's bucket, don't run a campaign while catalog-bench runs
  one, since both would load the same bucket while measuring its latency.
- **Your public IP:** SSH is open to this machine's address at `infra-up`. If it changes, rerun
  `infra-up`; a `wait` running meanwhile waits on the VMs it cannot reach (up to
  `WAIT_UNREACHABLE_MIN` minutes).
- **One store per cloud, shared by its VMs.** A fleet's runs load the same bucket or account at
  once, so each measures the store under the others' load, as their spikes do. The two clouds'
  states are apart, so both can be up at once.
- **Quotas.** A fleet needs vCPUs for all its VMs: 16 per `c7a.4xlarge`, 8 per `D8as_v6`. Azure's
  westus allowed 10 regional vCPUs on 2026-10-30, one `D8as_v6` (`az vm list-usage -l westus`);
  raise it in the portal's Quotas before a fleet. `casuser` cannot read AWS's quotas, so an `up`
  past them fails at the instances it could not make (`terraform apply` names them); the others
  are up. On 2026-11-01 its limit was 64 vCPUs for standard instances (four `c7a.4xlarge`, by the
  refusal's message): nine launched at once got in before the limit was enforced, and a tenth was
  refused, so a fleet larger than four must not count on it. `sweep-rg64m` ran on `sweep-p30`'s VM
  after that VM's own run.
- **Not provisioned:** spot instances, image baking, auto-teardown.
