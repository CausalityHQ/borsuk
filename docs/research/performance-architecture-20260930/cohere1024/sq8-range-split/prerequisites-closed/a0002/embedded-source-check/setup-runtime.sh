#!/usr/bin/env bash
# Remote-only thin setup for retained release binaries; no compiler installation.
set -euo pipefail
[[ $# == 4 ]] || exit 125
stage=$1 stage_sha=$2 manifest=$3 manifest_sha=$4
[[ $stage == /* && $manifest == /* && -f $stage && ! -L $stage && -f $manifest && ! -L $manifest ]]
[[ $stage_sha =~ ^[0-9a-f]{64}$ && $manifest_sha =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n%s  %s\n' "$stage_sha" "$stage" "$manifest_sha" "$manifest" | sha256sum --strict --check -
[[ ! -e /mnt/borsuk-sq8-split-panel && ! -L /mnt/borsuk-sq8-split-panel ]]
setup=/mnt/borsuk-sq8-split-setup
[[ ! -e $setup && ! -L $setup ]]
mkdir "$setup"
cd "$setup"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time tar gzip util-linux binutils libssl-dev
curl -fsSL --connect-timeout 10 --max-time 90 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum --strict --check -
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
aws --version > aws-version.txt
timeout -k 2 600 bash "$stage" /mnt/borsuk-sq8-split-panel "$manifest" "$manifest_sha"
printf '%s\n' SETUP_RETAINED_RELEASE_ASSETS_READY
