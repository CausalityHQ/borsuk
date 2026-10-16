#!/bin/bash
# Root local metadata generation only.
set -euo pipefail
set -C
base=/data/target/borsuk-sq8-panel-attribution.pending
old=$base/launch-split-prerequisites-a0002
out=$base/launch-split-screen-a0001
cp "$out/support-bundle.pending.tar.gz" "$out/support-bundle.tar.gz"
sha=$(sha256sum "$out/support-bundle.tar.gz"); sha=${sha%% *}
bytes=$(stat -c %s "$out/support-bundle.tar.gz")
jq '.ClientToken="borsuk-sq8-split-screen-a0001-8e707695" | .TagSpecifications[0].Tags[0].Value="borsuk-sq8-split-screen-a0001" | .TagSpecifications[0].Tags[2].Value="split-six-cell-scientific-diagnostic"' "$old/launch-request.json" > "$out/launch-request.json"
jq --arg sha "$sha" --argjson bytes "$bytes" '.schema="borsuk-split-screen-launch-admission-v1" | .status="ADMITTED_SETUP_ONLY_MEASUREMENT_SUBMIT_PENDING" | .scope="FROZEN_SIX_CELL_DIAGNOSTIC_SETUP" | .support_commit="7684d273840a2f9fc0c26a4608973cfef1b00b09" | .support_bundle_sha256=$sha | .support_bundle_bytes=$bytes | .remaining_maximum_instances=1 | .prior_compute_upper_usd=0.00895 | .compute_upper_usd_including_120s_accounting_each=0.064 | .prospective_campaign_upper_usd=0.22295 | .next="Actual same-host setup verification and root-frozen master required before one scientific SSM submission"' "$old/launch-admission.json" > "$out/launch-admission.json"
export OLD_DIR="$old" NEW_DIR="$out" BUNDLE_SHA="$sha" BUNDLE_BYTES="$bytes"
perl -0777 -e '
 use strict; use warnings;
 my $s=<>;
 my @p=([$ENV{OLD_DIR},$ENV{NEW_DIR}], ["borsuk-sq8-split-prerequisites-a0002","borsuk-sq8-split-screen-a0001"], ["6c05d7f764bcbb4bfdd6801b67e5144ec8eb7b22","7684d273840a2f9fc0c26a4608973cfef1b00b09"], [".remaining_maximum_instances==2",".remaining_maximum_instances==1"], ["==0.128","==0.064"], ["<=0.00234","<=0.00895"], ["==0.281","==0.22295"]);
 for (@p) {my($a,$b)=@$_; my $n=($s=~s/\Q$a\E/$b/g); die "missing $a" unless $n;}
 # Preserve exact old prerequisite closure and additionally require the second instance closed.
 $s=~s{launch-split-prerequisites-a0001/compute.closed}{launch-split-prerequisites-a0001/compute.closed} or die;
 my $anchor=q{[[ $(cat "$out/embedded-source-check/verified.txt") == EMBEDDED_THREE_INPUTS_BYTE_IDENTICAL ]]};
 my $extra=q{[[ $(cat /data/target/borsuk-sq8-panel-attribution.pending/launch-split-prerequisites-a0002/compute.closed) == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]};
 $s=~s/\Q$anchor\E/$extra\n$anchor/ or die "missing embedded guard";
 print $s;
' "$old/launch-once.sh" > "$out/launch-once.sh"
perl -0777 -e '
 use strict; use warnings; my $s=<>;
 my @p=([$ENV{OLD_DIR},$ENV{NEW_DIR}], ["sq8-split-prerequisites-a0002","sq8-split-screen-a0001"], ["f24d405ba97ff0a2d08dd2347516fb8c1f4defa35524738517dca9c11287cfe5",$ENV{BUNDLE_SHA}], ["46881",$ENV{BUNDLE_BYTES}]);
 for (@p) {my($a,$b)=@$_; my $n=($s=~s/\Q$a\E/$b/g); die "missing $a" unless $n; } print $s;
' "$old/upload-input-once.sh" > "$out/upload-input-once.sh"
for name in launch-once.sh upload-input-once.sh; do bash -n "$out/$name"; shellcheck "$out/$name"; done
(cd "$out"; sha256sum launch-request.json launch-user-data.txt launch-admission.json setup-command.sh support-bundle.tar.gz launch-once.sh upload-input-once.sh watch-terminal-once.sh await-readiness.sh submit-setup-once.sh make-master.sh) > "$out/launch-inputs.sha256"
