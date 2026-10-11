#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
proof=/data/target/borsuk-reconstruction-native-gates-a0002.pending/delivery-native-qualified-r2/qualification.closed.final.json
test ! -e "$out/transport.qualified-binary.draft.json"
jq -e '
 .status=="NATIVE_QUALIFIED"
 and .source_commit=="91107713f6f7bd700b5b6d92a83e438febac8069"
 and .native_source_identity_sha256=="bef40caf201f947718554128b544cb77feedf907b897faa59d32e8d36bf62772"
 and .full_source_before_after_root_match==true
 and (.stages|length)==6
 and all(.stages[];.native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true)
 and (.closure|length)==2
 and all(.closure[];.terminated==true and .volume_deleted==true)
 and .release_elf.bytes==8387384
 and .release_elf.sha256=="739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52"
' "$proof" > "$out/qualified-binary-binding.log"
jq --slurpfile q "$proof" '
 .binary={role:"compare_native_replay",key:$q[0].release_elf.key,
 pin:{path:"/mnt/borsuk-reconstruction-real1m/bin/compare_native_replay",
 bytes:$q[0].release_elf.bytes,sha256:$q[0].release_elf.sha256}}
 | .qualification={path:"qualification.closed.final.json"}
 | .authority_pending=true
' "$out/transport.draft.json" > "$out/transport.qualified-binary.draft.json"
sha256sum "$proof" "$out/transport.qualified-binary.draft.json" "$out/resource-envelope.draft.md" > "$out/qualified-next-drafts.sha256"
printf 'QUALIFIED_BINARY_BOUND_DRAFT_ONLY_NO_LAUNCH\n'
