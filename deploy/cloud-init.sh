#!/bin/bash
# The VM's first boot, on either cloud (user data on AWS, custom data on Azure): the packages
# deploy/remote.sh needs. OpenJDK 17, which the twins ran on and the fork builds with, Maven, and
# the tools of the remote steps. The marker tells remote.sh the packages are in.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y openjdk-17-jdk-headless maven git jq rsync unzip curl python3
touch /var/lib/cloud/rmow-bench-ready
