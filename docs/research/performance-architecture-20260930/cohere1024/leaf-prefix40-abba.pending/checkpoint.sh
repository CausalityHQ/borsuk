#!/bin/bash
set -euo pipefail
mode=$1 out=$2
aws=/mnt/borsuk-workspace/aws/dist/aws
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261017/leaf-prefix40-abba-a0001/cells
export AWS_MAX_ATTEMPTS=1
if [[ $mode == probe ]]; then
 cell=checkpoint-probe
 archive="$out/$cell.json"
 printf '{"purpose":"create-only checkpoint permission probe"}\n' >"$archive"
 key="$prefix/$cell.json"
elif [[ $mode == cell ]]; then
 cell=$3 leaf=$4
 archive="$out/$cell.tar"
 [[ ! -e $archive ]]
 key="$prefix/$cell.tar"
else
 exit 64
fi
rc=0
trap 'rc=$?; echo "$rc" >"$out/$cell.checkpoint.exit"; if ((rc)); then echo CHECKPOINT_INVALID >"$out/$cell.checkpoint.terminal"; else echo CHECKPOINT_VERIFIED >"$out/$cell.checkpoint.terminal"; fi' EXIT
if [[ $mode == cell ]]; then
 [[ $(cat "$out/$cell.native.exit") == 0 && $(cat "$out/$cell.reader.exit") == 0 ]]
 tar -C "$out" -cf "$archive" "$cell.jsonl" "$cell.reader-config.json" "$cell.reader-report.json" \
  "$cell.exit" "$cell.native.exit" "$cell.timeout.exit" "$cell.drain.exit" "$cell.cleanup.exit" \
  "$cell.reader.exit" "$cell.resources" "$cell.reader.resources" "$cell.command" "$cell.terminal" \
  "$cell.stdout" "$cell.stderr" "$cell.reader.stdout" "$cell.reader.stderr" \
  header-authority.json preparation.sha256 protocol.md config-pins.json \
  "configs/full.leaf$leaf.json" "frozen-headers/leaf$leaf.json"
fi
sha=$(sha256sum "$archive" | cut -d ' ' -f1)
checksum=$(openssl dgst -sha256 -binary "$archive" | base64 -w0)
bytes=$(stat -c %s "$archive")
"$aws" --profile causality --region eu-central-1 s3api put-object \
 --bucket "$bucket" --key "$key" --body "$archive" --if-none-match '*' \
 --checksum-algorithm SHA256 --checksum-sha256 "$checksum" --metadata "sha256=$sha" \
 >"$out/$cell.checkpoint.put.json" 2>"$out/$cell.checkpoint.put.stderr"
"$aws" --profile causality --region eu-central-1 s3api head-object \
 --bucket "$bucket" --key "$key" --checksum-mode ENABLED \
 >"$out/$cell.checkpoint.head.json" 2>"$out/$cell.checkpoint.head.stderr"
python3 - "$out/$cell.checkpoint.head.json" "$sha" "$checksum" "$bytes" <<'VERIFY'
import json,sys
v=json.load(open(sys.argv[1]))
assert v['Metadata']['sha256']==sys.argv[2]
assert v['ChecksumSHA256']==sys.argv[3]
assert v['ContentLength']==int(sys.argv[4])
VERIFY
printf '%s  s3://%s/%s\n' "$sha" "$bucket" "$key" >"$out/$cell.checkpoint.sha256"
