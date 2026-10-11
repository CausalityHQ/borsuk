#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
dest=$out/recovery-collected
mkdir "$dest" "$out/recovery-get-receipts"
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
prefix=research/semantic-router/20261011/reconstruction-real1m-a0001/retained-derived/
timeout -k 2 25 aws --profile causality --region eu-central-1 s3api list-objects-v2 --bucket borsuk-bench-453182569524-euc1 --prefix "$prefix" --no-paginate > "$out/retained-derived-objects.json"
jq -e '.IsTruncated==false and (.Contents|length<=32)' "$out/retained-derived-objects.json" > /dev/null
jq -r '.Contents[]|select(.Key|split("/")[-1]|startswith("recovery-"))|[.Key,(.Size|tostring)]|@tsv' "$out/retained-derived-objects.json" > "$out/recovery-roster.tsv"
while IFS=$'\t' read -r key bytes; do
 name=${key##*/}; name=${name#recovery-}
 [[ $key == "$prefix"recovery-"$name" && $name =~ ^[a-zA-Z0-9._-]+$ && $name != . && $name != .. && $bytes =~ ^[0-9]+$ ]]
 (( bytes <= 65536 ))
 timeout -k 2 25 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --checksum-mode ENABLED "$dest/$name" > "$out/recovery-get-receipts/$name.json"
 test "$(stat -c %s "$dest/$name")" = "$bytes"
 h=$(sha256sum "$dest/$name"); h=${h%% *}
 checksum=$(printf '%s' "$h" | xxd -r -p | base64 -w0)
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$out/recovery-get-receipts/$name.json" > /dev/null
done < "$out/recovery-roster.tsv"
awk '{if($1 !~ /^[0-9a-f]+$/ || length($1)!=64 || $2 !~ /^\.\/[a-zA-Z0-9._-]+$/ || NF!=2) exit 1; print substr($2,3)}' "$dest/SHA256SUMS" | LC_ALL=C sort > "$out/recovery-manifest-names.sorted"
awk -F '\t' '{n=split($1,a,"/"); sub(/^recovery-/,"",a[n]); if(a[n]!="SHA256SUMS") print a[n]}' "$out/recovery-roster.tsv" | LC_ALL=C sort > "$out/recovery-object-names.sorted"
cmp "$out/recovery-manifest-names.sorted" "$out/recovery-object-names.sorted"
(cd "$dest" && sha256sum -c SHA256SUMS) > "$out/recovery-collected.verified.log"
printf '%s  %s\n' a85b49fa008dc3feb699abb96d498dafb872de870cd804bc77aff38785a33771 "$dest/native-receipt.json" | sha256sum -c - > "$out/recovery-original-native-receipt-check.log"
jq -e '.status=="CLOSED_NATIVE_PARITY_VALIDATED" and .original_native_exit==0 and .original_manager_exit==1 and .original_disposition_preserved==true and .native_reexecuted==false and .performance_claim==false' "$dest/terminal.json" > /dev/null
jq -r '([.centroids,.radial_outputs.root,.radial_outputs.membership,.radial_outputs.leaves])[]|[.path,(.bytes|tostring),.sha256]|@tsv' "$dest/native-receipt.json" > "$out/retained-derived-pins.tsv"
mkdir "$out/retained-derived-heads"
while IFS=$'\t' read -r path bytes sha; do
 name=${path##*/}
 timeout -k 2 25 aws --profile causality --region eu-central-1 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix$name" --checksum-mode ENABLED > "$out/retained-derived-heads/$name.json"
 checksum=$(printf '%s' "$sha" | xxd -r -p | base64 -w0)
 jq -e --argjson bytes "$bytes" --arg checksum "$checksum" '.ContentLength==$bytes and .ChecksumSHA256==$checksum' "$out/retained-derived-heads/$name.json" > /dev/null
done < "$out/retained-derived-pins.tsv"
printf 'ORIGINAL_NATIVE_PARITY_AND_RETAINED_OUTPUTS_AUTHENTICATED_NO_RERUN\n'
