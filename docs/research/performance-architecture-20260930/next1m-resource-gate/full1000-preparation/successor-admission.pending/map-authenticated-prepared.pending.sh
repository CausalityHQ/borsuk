# Embedded successor bootstrap block; source only, not executed.
# $root is the fresh owned instance root; all10 transport bodies authenticated first.
phase=map-prepared
[[ -d $root/assets/prepared && ! -L $root/assets/prepared && ! -e $root/prepared-parent/cohort && ! -L $root/prepared-parent/cohort ]] || exit 91
mkdir -m 0700 "$root/prepared-parent/cohort"
map_deadline=$(( $(date +%s) + 600 ))
map_run() {
 local now left; now=$(date +%s); left=$((map_deadline-now-5))
 (( left > 0 )) || return 125
 timeout --signal=TERM --kill-after=5 "$left" "$@"
}
map_one() {
 local name=$1 bytes=$2 expected=$3 src dst actual
 src=$root/assets/prepared/$name; dst=$root/prepared-parent/cohort/$name
 [[ -f $src && ! -L $src && $(stat -c %h "$src") == 1 && $(stat -c %s "$src") == "$bytes" && ! -e $dst && ! -L $dst ]] || return 1
 map_run cp --no-clobber --reflink=never -- "$src" "$dst" || return
 [[ -f $dst && ! -L $dst && $(stat -c %h "$dst") == 1 && $(stat -c %s "$dst") == "$bytes" ]] || return 1
 actual=$(map_run sha256sum -- "$dst") || return
 [[ ${actual%% *} == "$expected" ]] || return 1
 chmod 0400 "$dst" || return
 map_run sync -f "$dst" || return
}
map_one corpus.f32 4096000000 1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38 || exit 91
map_one queries.f32 4096000 8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e || exit 91
map_one corpus.ids.jsonl 200873446 008191ae3103d78db682276008190cc858a361b19e487f6780c02cea5dd70067 || exit 91
map_one queries.ids.jsonl 196017 f0e6e974c3dfe45fae7db3a9a5146891541d1f3c0d605674d115b6c580a87f68 || exit 91
map_one truth.u64 80000 0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f || exit 91
map_one complete.json 6238 e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17 || exit 91
map_run sync -f "$root/prepared-parent/cohort" || exit 91
