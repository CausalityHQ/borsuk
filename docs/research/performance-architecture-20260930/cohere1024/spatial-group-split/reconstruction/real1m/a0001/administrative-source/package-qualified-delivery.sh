#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
dest=$out/delivery-closed-a0001
test ! -e "$dest"
mkdir "$dest"
for dir in package admission-collected smoke-collected parity-collected recovery-collected retained-derived-heads; do
 cp -a "$out/$dir" "$dest/$dir"
done
for file in decision.md protocol.frozen.md live-attempt.md launch-request.json launch-user-data.txt launch-inputs.sha256 launch.stdout instance.terminated.json volume-final.stdout volume-final.stderr volume-final.exit external-timer-stop.stdout external-timer-stop.stderr setup.ssm-original-terminal.json admission.ssm-original-terminal.json smoke.ssm-original-terminal.json parity.ssm-original-terminal.json parity-diagnosis.ssm-original.json parity-diagnosis-r2.ssm-original.json parity-closed-recovery-r2.ssm-observation.json recovery-sdk-metadata.ssm-observation.json admission.root-accepted.json smoke.root-accepted.json parity-native-original-receipt.json retained-derived-objects.json retained-derived-pins.tsv; do
 cp "$out/$file" "$dest/$file"
done
mkdir "$dest/administrative-source"
for file in "$out"/*.sh; do cp "$file" "$dest/administrative-source/"; done
for file in "$out"/*.ssm-request.json "$out"/*.ssm-send.json "$out"/*.ssm-send.claim "$out"/*.ssm-command.id; do
 cp "$file" "$dest/"
done
jq -n --slurpfile original "$out/parity-native-original-receipt.json" --slurpfile admission "$out/admission.root-accepted.json" --slurpfile canary "$out/smoke.root-accepted.json" '{schema:"borsuk-real1m-construction-qualified-v1",status:"REAL_RECONSTRUCTION_AND_HISTORICAL_RADIAL_PARITY_QUALIFIED",source_commit:"91107713f6f7bd700b5b6d92a83e438febac8069",published_compiler_qualification_commit:"319f8c4bbac69f522fc25a0ecd93f2e9232e9b84",elf_sha256:"739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52",original_generation_sha256:$original[0].generation.sha256,original_approval:$original[0].original,admission:$admission[0],canary:$canary[0],original_parity:{ssm_command_id:"dd1c3fb4-157c-479e-a2ca-11feb3e758cd",ssm_status:"Failed",manager_exit:1,native_exit:0,disposition:"INVALID_RECEIPT_VALIDATOR",disposition_preserved:true,receipt_sha256:"a85b49fa008dc3feb699abb96d498dafb872de870cd804bc77aff38785a33771"},closed_output_revalidation:{ssm_command_id:"c1f36a29-65a7-4927-980b-2fc0bef87922",status:"Success",exit:0,native_reexecuted:false,all_recovery_body_checksums_verified:true},historical_parity:$original[0].historical_parity,retained_derived_outputs:$original[0].radial_outputs,centroids:$original[0].centroids,retained_s3_prefix:"research/semantic-router/20261011/reconstruction-real1m-a0001/retained-derived/",requests_opened:false,truth_opened:false,instance_id:"i-0e7ec79ad22db80d1",terminated:true,volume_id:"vol-09f6fcc3c4f757a36",volume_deleted:true,watchdog_stopped:true,performance_claim:false,product_ready:false}' > "$dest/qualification.closed.final.json"
(cd "$dest" && find . -type f ! -name DELIVERY_SHA256SUMS -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) > "$dest/DELIVERY_SHA256SUMS"
(cd "$dest" && sha256sum -c DELIVERY_SHA256SUMS) > "$out/delivery-closed-verified.log"
du -sh "$dest"
