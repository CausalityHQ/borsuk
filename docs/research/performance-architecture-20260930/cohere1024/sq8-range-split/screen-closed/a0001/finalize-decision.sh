#!/bin/bash
# Root closed metadata attestation; scientific deltas are native-produced, never recomputed here.
set -euo pipefail
set -C
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
bash "$out/verify-closed.sh" "$out" > "$out/root-verification.final.stdout"
jq -se 'length==7 and ([.[].invocation_id]|unique|length)==7 and ([.[].cgroup]|unique|length)==7 and all(.[];.valid and .owned and .drained and .resources_verified and (.forced_cleanup|not))' "$out"/collected/{B0,C0,B1,C1,C2,B2,reducer}/execution.json > "$out/seven-independent-owned-stages.verified.txt"
jq -se 'length==1 and (.[0]|.schema=="borsuk-sq8-split-panel-v2" and .status=="VALIDATED_FIXED_PANEL" and .complete==true and .screen=="INCONCLUSIVE" and .selected_count==32 and .recorded_ordered_returned_physical_ordinal_parity==true and .recorded_logical_coverage_parity==true and .recorded_ordered_id_scorebit_parity==true and .population_p95_claim==false and .qps_claim==false and .vendor_or_scientific_win_claim==false)' "$out/report-summary.native.json" > "$out/native-report-summary.verified.txt"
archive=$(jq -er .archive_sha256 "$out/terminal.json")
jq -n --arg archive "$archive" --slurpfile native "$out/report-summary.native.json" '{schema:"borsuk-sq8-split-root-verification-v1",status:"VALID_EXECUTION_INCONCLUSIVE_SCREEN",source_commit:"8e7076951936b0434b7947afd5b23e192c98fe70",integrated_main:"a5269de6a54ed61afdd51b61d9fb973d3607fecc",original_command_id:"98e42169-f7ba-4b61-b10e-e613d7df6b46",instance_id:"i-09055af56258dcb86",archive_sha256:$archive,master_sha256:"21a824b3ed20ef266d7834c4f3e200ccdbc05fe61f34ffbc83fa8c9d2b57e1a8",original_native_exit:0,publication_exit:0,shutdown_request_exit:0,wrapper_exit:0,completed_scientific_cells:6,validated_owned_stages:7,all_stages_drained:true,all_stages_oom:0,all_stages_swap_peak:0,instance_terminated:true,volume_absent:true,external_timer_inactive:true,default_changed:false,screen:$native[0].screen,native_summary:$native[0],quality_scope:"Exact recorded returned IDs/score bits/physical ordinals and logical coverage parity. No independently stored full scored-row roster; native qualification/parity falsifiers retained separately.",disposition:"Retain valid tradeoffs and immutable result; no stable split benefit demonstrated, do not promote default or repeat unchanged screen automatically",population_performance_claim:false,vendor_win:false,product_ready:false}' > "$out/root-verification.final.json"
cat > "$out/decision.final.md" <<'DECISION'
# SQ8 range-splitting screen: valid, inconclusive

Six frozen 32-query cells completed with the qualified native binaries. Seven independently owned stages passed original native/manager/time exits, CPU0/CPU100%/2GiB/noSwap/pids128 checks, zero OOM/swap and exact cgroup drain. Original SSM and all publication/shutdown exits were zero. Instance and volume are gone; external timer is inactive.

The native reducer authenticated the sealed results and confirmed ordered ID/score-bit/returned-physical-ordinal and logical-coverage parity. This does not prove an independently recorded full scored-row roster.

| Target query | Pair 1 fetch B−C | Pair 2 fetch B−C | Pair 1 fetch-to-rank B−C | Pair 2 fetch-to-rank B−C |
|---|---:|---:|---:|---:|
| 483 | −18.054145 ms | +4.748826 ms | −17.262858 ms | +4.360587 ms |
| 773 | −1.414203 ms | +7.812614 ms | −2.771184 ms | +10.455283 ms |

Positive means candidate faster. The candidate critical logical range was split at both queries in both pairs. Neither preregistered strong nor small-effect screen passed: native disposition **INCONCLUSIVE**. Preserve the valid observations; leave splitting opt-in and retain the unsplit default. No unchanged repeat or larger benchmark is automatically authorized.

This is a targeted, previously accessed panel with uncontrolled S3 cache, not a population p95/QPS estimate, network-causality proof or vendor win. Overall SSM runtime 58.424 seconds is not query latency. No recall improvement was introduced by this physical scheduling change.

Next: source-backed contextless specialist plan for one different causal Rust intervention, followed by the cheapest native quality/correctness falsifier. Retain useful quality and efficiency Pareto points even when strict screens miss. Existing full-population ANN results and competitor comparisons are unchanged.
DECISION
(cd "$out"; find collected -type f ! -name report.json -print0 | sort -z | xargs -0 sha256sum) > "$out/closed-metadata.sha256"
sha256sum "$out/root-verification.final.json" "$out/decision.final.md"
