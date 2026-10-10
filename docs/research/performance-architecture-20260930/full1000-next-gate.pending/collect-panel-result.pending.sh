# Embedded outer closure block; source only, not executed.
# Run after the original observer/collector has closed its exact owned unit.
# Preserve native output even on a nonzero exit; this block grants no acceptance.
phase=retain-panel-result
panel_src=$root/prepared-parent/query/baseline-result.jsonl
panel_dst=$root/evidence-root/baseline-result.jsonl
if [[ -e $panel_src || -L $panel_src ]]; then
 [[ -f $panel_src && ! -L $panel_src && $(stat -c %h "$panel_src") == 1 ]] || exit 96
 panel_bytes=$(close_run 5 stat -c %s "$panel_src") || exit 96
 [[ $panel_bytes =~ ^[0-9]+$ ]] && (( panel_bytes > 0 && panel_bytes <= 67108864 )) || exit 96
 panel_sha=$(close_run 45 sha256sum -- "$panel_src") || exit 96
 panel_sha=${panel_sha%% *}
 [[ $panel_sha =~ ^[0-9a-f]{64}$ && ! -e $panel_dst && ! -L $panel_dst ]] || exit 96
 close_run 45 cp --no-clobber --reflink=never -- "$panel_src" "$panel_dst" || exit 96
 [[ -f $panel_dst && ! -L $panel_dst && $(stat -c %h "$panel_dst") == 1 && $(stat -c %s "$panel_dst") == "$panel_bytes" ]] || exit 96
 panel_copy_sha=$(close_run 45 sha256sum -- "$panel_dst") || exit 96
 [[ ${panel_copy_sha%% *} == "$panel_sha" ]] || exit 96
 close_run 20 sync -f "$panel_dst" || exit 96
 close_run 5 jq -n --argjson bytes "$panel_bytes" --arg sha "$panel_sha" \
  '{schema:"borsuk-full1000-panel-result-retention-v1",bytes:$bytes,sha256:$sha,source_closed:true,external_native_replay_required:true,performance_claim:false}' \
  > "$root/evidence-root/baseline-result-retention.json" || exit 96
 close_run 20 sync -f "$root/evidence-root" || exit 96
else
 [[ -n $chain_exit && $chain_exit != 0 ]] || exit 96
 printf 'Native output absent; original chain exit=%s\n' "$chain_exit" > "$root/evidence-root/baseline-result.absent.txt" || exit 96
fi
