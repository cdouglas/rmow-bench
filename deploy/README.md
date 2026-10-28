# Cloud deployment (S3 Express and Azure, Terraform)

The calibration runs ([docs/S3X_RUNBOOK.md](../docs/S3X_RUNBOOK.md),
[docs/AZURE_RUNBOOK.md](../docs/AZURE_RUNBOOK.md)) on one VM beside its store. Terraform makes the
store, the VM's credentials and the VM, and `remote.sh` runs the runbook's steps on the VM: the
builds, the contract suites, the spike and the calibration run. `deploy.sh` drives both from here
and brings the results back into `results/`. The layout follows shield-rs's `deploy/`.

> **Status: written and checked offline, not yet applied.** `./deploy/validate.sh` passes
> (`terraform fmt` and `validate` on the four layers, `bash -n`, and shellcheck if installed), and
> a read-only `terraform plan` of the AWS infra layer resolves its lookups (2026-10-28). The VM's
> `build` step ran in an Ubuntu 24.04 container (9 minutes), and the local twin's configuration on
> its build sent the registered twin's requests and bytes (`compare_runs.py`: 0 of 862 records
> differ). `infra-up` and `up` provision **billable** resources; review `terraform plan` and tear
> down when done. The first `infra-up`, in us-east-1 (2026-10-28), failed on `casuser`'s IAM and
> was torn down. AWS now runs in us-west-2 on catalog-bench's bucket and instance profile, as
> catalog-bench's runs did, and its infra layer plans only the key pair and the security group.

## Layout

```
deploy/
  deploy.sh              # the driver (infra-up, up, ship, run, status, wait, fetch, down, ...)
  remote.sh              # the steps on the VM: build, test, spike, calibrate (detached)
  cloud-init.sh          # the VM's first boot: OpenJDK 17, Maven, git and tools (both clouds)
  deploy.env.example     # copy to deploy.env: the cloud, the SSH key, the builds, the sizes
  validate.sh            # the offline checks
  <cloud>/infra/main.tf  # the store, the VM's credentials, the network: free while empty
  <cloud>/bench/main.tf  # the VM: billable, made and destroyed per campaign
```

for `<cloud>` ∈ `aws | azure`. Each `bench` layer reads its `infra` layer's outputs (a local-backend
`terraform_remote_state`), and both expose the same two outputs (`public_ip`, `ssh_user`).

| layer | AWS (S3 Express) | Azure (ADLS Gen2) |
|---|---|---|
| `infra` (kept) | a directory bucket in one zone, a role with `s3express:CreateSession` on it and its instance profile (or an existing bucket and profile: `AWS_BUCKET`, `AWS_INSTANCE_PROFILE`), the operator's key pair, a security group, the S3 Express gateway endpoint (unless the VPC has one) | a resource group, a StorageV2 account with a hierarchical namespace and a container, a user-assigned identity with Storage Blob Data Contributor on the account, a vnet and subnet (with the storage service endpoint), an NSG |
| `bench` (churned) | an EC2 instance in the bucket's zone (64 GiB gp3) | a VM with the identity, its public IP, NIC and disk |

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
./deploy/deploy.sh up                # the VM
./deploy/deploy.sh ship              # bundle main, ship it; the VM builds the fork and rmow-bench
./deploy/deploy.sh wait              # until the builds end (about 15 minutes)
./deploy/deploy.sh run               # test, spike, calibrate, in order, stopping at a failure
./deploy/deploy.sh wait
./deploy/deploy.sh fetch             # results/ and deploy/logs/; prints thread_cpu_per_wall
./deploy/deploy.sh down              # the VM; the store stays, so the next `up` is fast
./deploy/deploy.sh teardown          # the store and every object in it, once `down`

./deploy/deploy.sh all               # infra-up through fetch
CLOUD=azure ./deploy/deploy.sh all   # the same on Azure, without editing deploy.env
```

`run` takes steps, to run one at a time (`run test`, then `run spike`, and so on). `status` shows
whether the steps still run and the log's tail, and `ssh` opens a shell on the VM. The steps run
detached on the VM (`setsid nohup`), so a dropped connection does not stop them. A step whose suite
skipped every test fails: a suite without credentials skips rather than fails.

**What the VM runs** is the runbook's steps:
- **build:** the fork at `FORK_COMMIT` (the twins' build, af875ccfd1) on branch `atomicio`, so its
  jars' build properties are the twins'. Its version comes from the release tag
  `apache-iceberg-1.12.0`, which the fork on GitHub does not carry, so the VM fetches it from
  Apache's repository. Gradle skips the engines (Spark, Flink, Kafka Connect). Then rmow-bench at
  `CALIBRATION_COMMIT` (9a69028, where C1–C4 were registered) and, in a worktree,
  `SPIKE_COMMIT` (`main`).
- **test, spike, calibrate:** the runbook's steps 1–3. The calibration runs `CALIBRATION_REPS`
  times (3), for the spread of measured latency; every run must send the twin's requests and
  bytes (C1). Its configuration gets the store's root (and on AWS its region) in a copy outside
  the checkout, so the manifest records a clean tree.

**The VM.** Ubuntu 24.04 and its OpenJDK 17 package, which the twins ran on (17.0.20.1 when this
was written). The defaults are AMD, as the twins' laptop is (Zen 4):
- `c7a.2xlarge` on AWS: Zen 4, 8 vCPUs, 16 GiB; offered in usw2-az3;
- `Standard_D8as_v5` on Azure: Zen 3, 8 vCPUs, 32 GiB.

The twins' predictions price CPU at the laptop's pace, so a VM of another family moves the
predicted times, but not the requests and bytes C1 checks. Any size works for C2, which uses the
run's own CPU.

## Cost

The instance-hours dominate: about $0.41/h for `c7a.2xlarge` and $0.34/h for `D8as_v5`, on demand
in us-east-1 and eastus (us-west-2 charges the same for `c7a.2xlarge`). A campaign takes about an hour: the builds, the suites, the spike
(tens of minutes at 20 reps) and the calibration. The stores charge little: the runbooks' Cost
paragraphs. The bucket and the account cost nothing while empty, and every step deletes what it
wrote, but for the calibration run's objects under `<root>/calibration`. `teardown` deletes those
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
  `infra-up`.
- **One VM, one store at a time per cloud.** The two clouds' states are apart, so both can be up
  at once.
- **Not provisioned:** spot instances, image baking, auto-teardown.
