#!/bin/bash
# Extract exact pinned members as opaque files, never arbitrary tar paths.
set -euo pipefail
[[ $# == 1 && $EUID == 0 ]] || exit 125
root=/mnt/borsuk-v9-panel
ev=$root/evidence/extract-$1
mkdir -m 700 "$ev"
extract() {
 local archive=$1 archive_sha=$2 member=$3 target=$4 sha=$5 bytes=$6
 [[ -f $archive && ! -L $archive && ! -e $target && ! -L $target ]] || exit 125
 printf '%s  %s\n' "$archive_sha" "$archive" | sha256sum --strict -c - >> "$ev/archive-check.log"
 [[ $(tar -tf "$archive" | grep -Fxc "$member") == 1 ]] || exit 125
 set -o noclobber
 /usr/bin/timeout -k 2 30 tar -xOf "$archive" -- "$member" > "$target"
 set +o noclobber
 [[ -f $target && ! -L $target && $(stat -c %s "$target") -gt 0 && $(stat -c %s "$target") -le 31000000 ]] || exit 125
 if [[ $bytes != none ]]; then [[ $(stat -c %s "$target") == "$bytes" ]] || exit 125; fi
 printf '%s  %s\n' "$sha" "$target" | sha256sum --strict -c - >> "$ev/member-check.log"
 chmod 0400 "$target"
}
case $1 in
 binaries)
  extract "$root/archives/baseline.tar" abd5fd15ddb53b69e21d76f6ba29a09d33bed610ce944ca36e1d649ce454e8c7 target/release/check_cohere_native_baseline "$root/bin/check_cohere_native_baseline" 912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995 none
  extract "$root/archives/helper.tar" 95a6e0035ae8183e6bfcb127c363c65120115e3bd765cd217f3b997032c0a2b0 target/release/examples/compare_native_replay "$root/bin/compare_native_replay" f456ea8a94f983a2266d3df661a7b4dfe120e607b79b13f968687d2af510880a none
  for binary in check_cohere_native_baseline compare_native_replay; do
   [[ $(od -An -tx1 -N4 "$root/bin/$binary" | tr -d ' \n') == 7f454c46 ]] || exit 125
   chmod 0500 "$root/bin/$binary"
  done;;
 receipts)
  extract "$root/archives/cohort-evidence.tar.gz" 84e8538cd79bc4f14afdaed449c5d2de8f3ea571a1e1ae1c1b4b8f9023ed8062 ./prepared-parent_cohort_complete.json "$root/inputs/prepared-parent_cohort_complete.json" e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17 6238
  extract "$root/archives/cohort-evidence.tar.gz" 84e8538cd79bc4f14afdaed449c5d2de8f3ea571a1e1ae1c1b4b8f9023ed8062 ./prepared-parent_derived_derivation.json "$root/inputs/prepared-parent_derived_derivation.json" 00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e 23128
  extract "$root/archives/historical-count.tar.gz" 76d61da772cf11c16193d711315fe6aea6d21e7fcc6ebfb7acb1f2c28b7b7b44 evidence/input-stage/count-full/report.json "$root/inputs/historical-count-report.json" 31dfa30cbf176254e7bec7cc8eb5288071f85cfbb9bb98455b61b3c87f5ec786 3729591;;
 *) exit 125;;
esac
printf '%s\n' EXACT_MEMBERS_AUTHENTICATED_NATIVE_UNRUN > "$ev/extraction.status"
sync -f "$ev/extraction.status"
sync -f "$ev"
