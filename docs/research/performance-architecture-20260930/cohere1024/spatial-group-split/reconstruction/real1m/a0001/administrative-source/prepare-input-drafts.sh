#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
meta=$out/original-metadata
# This is a metadata-only draft. Actual native authentication remains mandatory on EC2.
jq -e '.schema=="borsuk-two-bit-generation-v8" and .generation==1 and .base_epoch==0 and .plane_manifest_sha256=="e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6" and .discovery.profile=="scale1m" and .discovery.input_schema=="borsuk-two-bit-plane-v3" and .discovery.centroids_sha256=="720b98899031e1aeeeabe0fb2aa0d938485b43141c2438c251b3383a6f0315d8"' "$meta/generation.json" > "$out/original-metadata-binding.log"
jq -e '.schema=="borsuk-two-bit-plane-v3" and .rows==1000000 and .dimensions==1024 and .seed==20260923 and .page_rows==32 and .query_or_truth_used==false' "$meta/plane.json" >> "$out/original-metadata-binding.log"
# serde_json in frozen Cargo.lock has no indexmap dependency: Value uses sorted object keys.
# Discovery contains only integer counts and ASCII string fields, no float formatting.
jq -j -S -c '.discovery' "$meta/generation.json" > "$meta/discovery.canonical.json"
sha=$(sha256sum "$meta/discovery.canonical.json"); sha=${sha%% *}
jq --arg discovery "$sha" '{schema:"borsuk-partition-reconstruction-approval-v1",generation_sha256:"cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307",generation:.generation,base_epoch:.base_epoch,plane_sha256:.plane_manifest_sha256,discovery_sha256:$discovery,input_schema:.discovery.input_schema,input_root_sha256:.discovery.input_root_sha256,centroids_sha256:.discovery.centroids_sha256,profile:"scale1m",rows:1000000,dimensions:1024}' "$meta/generation.json" > "$out/original-approval.draft.json"
jq '{generation_manifest,plane_manifest,leaves,router_root,membership} | with_entries(.value.path |= sub("/mnt/borsuk-neighborhood-data";"/mnt/borsuk-reconstruction-real1m"))' /data/target/borsuk-source-neighborhood-data-gates-a0002.pending/admission-config.pending.json > "$out/original-input-pins.draft.json"
for phase in admission smoke parity; do
 jq --arg phase "$phase" '{schema:"borsuk-partition-reconstruction-config-v1",generation_manifest,plane_manifest,leaves,historical:(if $phase=="parity" then {root:.router_root,membership:.membership} else null end),caller_pinned_bytes:0,max_payload_bytes:536870912,output_directory:("/mnt/borsuk-reconstruction-real1m/evidence/"+$phase+"/published")}' "$out/original-input-pins.draft.json" > "$out/$phase.config.draft.json"
done
jq '{schema:"borsuk-reconstruction-inputs-draft-v1",authority_pending:true,source_commit:"91107713f6f7bd700b5b6d92a83e438febac8069",binary:null,inputs:[.inputs[]|select(.role=="generation_manifest" or .role=="plane_manifest" or .role=="router_root" or .role=="membership" or .role=="leaves")|.pin.path |= sub("/mnt/borsuk-neighborhood-data";"/mnt/borsuk-reconstruction-real1m")]}' /data/target/borsuk-source-neighborhood-data-gates-a0002.pending/transport.pending.json > "$out/transport.draft.json"
(cd "$out" && sha256sum original-approval.draft.json original-input-pins.draft.json admission.config.draft.json smoke.config.draft.json parity.config.draft.json transport.draft.json) > "$out/input-drafts.sha256"
printf 'REAL1M_METADATA_DRAFT_ONLY_NATIVE_ADMISSION_NOT_RUN\n'
