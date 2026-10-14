# SOURCE budget96: verified cold regression, retain128

See [decision.md](decision.md) for the exact four-run table and limits. The candidate preserves97.80% recall and exact IDs/scorebits/semantictrace/SQ8parity but has ~80% higher p95 and6–8% lower sequentialQPS. SOURCEGETs drop ~14.5%, SOURCEbytes increase ~7.5%. Keep128default; retain this valid tradeoff. No matchedvendorwin orproduct-readiness claim.

Qualified Rust source8910ef50c1cce155eb396d71a2e05dd3b9e906a0 is already on publicmain3e534d57. This delivery changes evidence only. Native inputs/config/binary/scoring are identical between arms except budget128/96. Rust owns raw query/result reading and statistics; repository contains small native aggregates and receipts, not rawcorpus/query/truth or formalqueryrows.

The olda0001 is executionINVALID from a rootexpectedheader selected_count1 instead1000, preserved under prior-invalid. The one-field correction was validated against the retained1000-query result on a separate causalityEC2 reducer-only job; header-replay holds its accepted receipts. The fresha0002 used newcreate-only outputs/order/instance; no oldcell pooling orthreshold adjustment.

Immutable S3 evidence bucket borsuk-bench-453182569524-euc1:
- valid a0002: research/semantic-router/20261014/source-budget96-scale-cold-a0002/terminal/evidence.tar.gz
- original INVALID a0001: research/semantic-router/20261014/source-budget96-scale-cold-a0001/terminal/evidence.tar.gz
- corrected-header validation: research/semantic-router/20261014/source-budget96-header-replay-a0001/terminal/replay-evidence.tar.gz

ArchiveSHA receipts are retained. Full actual-input admission/source/compiler/review proof is in ../source-budget96-native-qualification-20261014. The17-stage rootverification checks native/manager exits, exactorder/frozenheaders/opaqueoutputhashes, full4000-query parity/recall/resources and instance+volume+timerclosure. The descriptive repeat-noise rule was written before measurements. Native performance_screen_passed=false. Sequentialmicrobenchmark excludesstartup/clientingress and does not measure concurrentservice throughput, servercacheeviction, billedwirebytes ortotallifecycledollars.

Stage receipts are consolidated in receipts/stage-receipts.json. Each entry retains its original path, SHA256 and exact UTF-8 body; JSON string decoding reconstructs the original bytes. The immutable S3 archive retains the original files.
