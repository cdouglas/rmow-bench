#!/bin/bash
# The VM's first boot, on either cloud (user data on AWS, custom data on Azure): the packages
# deploy/remote.sh needs. OpenJDK 17, which the twins ran on and the fork builds with, Maven, and
# the tools of the remote steps. The marker tells remote.sh the packages are in.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y openjdk-17-jdk-headless maven git jq rsync unzip curl python3
# The runs' instruction counts (ThreadCounters, perf_event_open), where the hypervisor exposes the
# counters: Ubuntu's default of 4 refuses them. Each run's manifest says whether it had counts
# (host.thread_counters)
echo 'kernel.perf_event_paranoid = 2' > /etc/sysctl.d/60-rmow-bench.conf
sysctl -q -p /etc/sysctl.d/60-rmow-bench.conf
touch /var/lib/cloud/rmow-bench-ready
