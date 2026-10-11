#!/bin/bash
set -euo pipefail
set -- research/semantic-router/20261011/source-neighborhood-data-gates-a0002/smoke/native.log 8fb76270c863306c43b843646fd16e4d9851aeb5daecaf79252a5528a03941b1 3981
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
work=$(mktemp -d /tmp/borsuk-summary-client.XXXXXX)
cd "$work"
timeout -k 2 60 apt-get -qq -o DPkg::Lock::Timeout=20 update
timeout -k 2 90 apt-get -qq -y -o DPkg::Lock::Timeout=20 install curl unzip
timeout -k 2 90 curl -fsSL --connect-timeout 10 --max-time 85 -o aws.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  aws.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout -k 2 45 unzip -q aws.zip
timeout -k 2 45 ./aws/install
#!/bin/bash
# Evidence-only EC2 operation. No native execution, corpus/query/truth download or reduction.
set -euo pipefail
[[ $# == 3 ]] || exit 125
key=$1 expected=$2 bytes=$3
[[ $key == research/semantic-router/20261011/source-neighborhood-data-gates-a0002/smoke/native.log ]]
[[ $expected =~ ^[0-9a-f]{64}$ && $bytes =~ ^[0-9]+$ ]]
(( bytes <= 1048576 ))
export AWS_MAX_ATTEMPTS=1 AWS_PAGER='' AWS_DEFAULT_REGION=eu-central-1
work=$(mktemp -d /tmp/borsuk-closed-summary.XXXXXX)
timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --checksum-mode ENABLED "$work/native.log" > "$work/get.json"
[[ $(stat -c %s "$work/native.log") == "$bytes" ]]
printf '%s  %s\n' "$expected" "$work/native.log" | sha256sum -c - > "$work/auth.txt"
printf 'BEGIN_AUTHENTICATED_EXISTING_RUST_SUMMARY\n'
cat "$work/native.log"
printf 'END_AUTHENTICATED_EXISTING_RUST_SUMMARY\n'

