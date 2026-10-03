# Read-only input/control inventory

Consultation fcc12bafdcd649a7; completed exit 0, gpt-6.1-sol/high. No payload authentication or new performance measurement; root checks descriptors separately.

**Reusable inputs exist; a qualified 100k control on current HEAD is not established by these receipts.** Historical native 100k controls are authenticated separately from V282 and the current fixed48 profile. HEAD matched `6dda0e763291c7880fbcbb7dc232abaddb860a8a`; working tree was clean. No payloads were opened or experiments run.

Exact path prefixes below:

- `S = s3://borsuk-bench-453182569524-euc1/`
- `L = /tmp/borsuk-semantic-router-inputs/`
- `R = /tmp/borsuk-semantic-native-paired-relaion-a0002/`
- `C = /tmp/borsuk-semantic-native-paired-cohere-a0001/`

“Present” means filesystem presence/size only; payload hashes were taken from committed descriptors, not recomputed.

| Input | Exact path, using prefixes above | SHA-256 | Bytes | Rows / dimension / k | Availability |
|---|---|---|---:|---|---|
| ReLAION sealed corpus | `Sresearch/v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/100k-a0023/attempt/inputs/source-100k.parquet` | `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d` | 145121661 | 100000 / 768 / — | Remote descriptor; local original not established |
| ReLAION canonical records | `Rgraph/canonical.bin` | `c260b7fe0b6a54de98dfba3f2599d272242f724421064ed5ca18c0cd63e6283d` | 308000000 | 100000 / 768 / — | Present; remote key in source-completion config |
| ReLAION physical/source-order map | `Lrelaion/order.u64` | `22abeb087000ea2978c4fef8e85c2f7fda4e4b22c4e5126ba37a1c19816fab18` | 800000 | 100000 IDs | Present |
| ReLAION requests | `Lrelaion/requests` | `b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0` | 17714558 | 1000 / 768 / 100 | Present |
| ReLAION truth | `Lrelaion/truth` | `4bd3ac79fce3919f85359ce3e305491663991f6cc0890ab91682cda34247a24e` | 400000 | 1000 × GT100 | Present |
| CoHere first100k corpus | `Sresearch/v248-cohere-transfer-100k/7b66f9f7d306a0f1febc2091783de68a6ece72c7/runs/a0001/artifacts/vectors.raw` | `0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e` | 307200000 | 100000 / 768 / — | Remote descriptor; local original not established |
| CoHere canonical records | `Cgraph/canonical.bin` | `38304c3b448f2f62b32da8b443bbe52f20ccc63f5472d0e7885d5c268dd7a6df` | 308000000 | 100000 / 768 / — | Present; remote key in source-completion config |
| CoHere physical/source-order map | `Lcohere/order.u64` | `e2314ca14f5aa6f08b02cc328b863c921e55b1461a413ad6d3d66e781ff42d50` | 800000 | 100000 IDs | Present |
| CoHere requests | `Lcohere/requests` | `86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812` | 15369495 | 1000 / 768 / 100 | Present |
| CoHere truth | `Lcohere/truth` | `06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8` | 400000 | 1000 × GT100 | Present |

Descriptor authority is [source-completion-http-config.json](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/source-completion-http-config.json), cross-bound by the [ReLAION scorer config](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-router-scorer-relaion-config.json) and [CoHere scorer config](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-router-scorer-cohere-config.json). ReLAION’s decoded source identity is separately recorded as `0d55a09756f41361c89f0ca355a3bc98da6799b4dc7ea39ab34a905844a758c2`; it is not the parquet digest.

The historical V282 split is **development 0–255, validation 256–999**, cosine k100. Source-completion/scorer development uses **0–63**, k100. Closed native cold qualification uses the consumed **0–63** slice at **k10**, not k100. Its exact sliced request/truth descriptors are in [semantic-native-cold-input-derivation.json](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-native-cold-input-derivation.json).

| Control/artifact | Exact local path | SHA-256 | Bytes | Provenance and qualification boundary |
|---|---|---|---:|---|
| Native graph, ReLAION | `Rgraph/manifest.json` | `49de32e57db9e1f7692462d508d34ea1ba8e91c0e5a88dc7687278206d55340c` | 33980 | Present; native generation v7; closed a0004, 100k/D768/cosine/k10/0–63 |
| Native graph, CoHere | `Cgraph/manifest.json` | `668f019b1c44a8e09b7d0ebe7dc4ef8e00649ba456744fb73c113b9fb93e2368` | 34017 | Present; same qualification boundary |
| Native semantic, ReLAION | `Rsemantic/manifest.json` | `ef0b07a836a265e91184469906ff353469089e160893487a4921bc9a823a6e8d` | 21329 | Present; v7; subsequently reused unchanged in metadata-waves pairing |
| Native semantic, CoHere | `Csemantic/manifest.json` | `4e9ec30b0bb1ec464454d2331f5d8bd83e7513dd2d6290964c012497e7a85ae9` | 21426 | Present; same boundary |
| Original qualified HTTP binary | `/tmp/borsuk-centroid-portable-rounding-20260930/two_bit_http` | `c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533` | 16191384 | Source commit `a1bdcdfec1f73aa6da1b29202a68fe4723723fa8`; assurance source identity `92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520` |
| Qualified 100k metadata-waves winner | `/tmp/borsuk-semantic-metadata-waves-20261001/two_bit_http` | `e3516453797add1f8e76daddcc97a8fb5e4c1c3467749ae9cb732934b3bc2c0e` | 16259024 | Present; source commit `5dbdb44d0dbb8bec1f02b6e8c64f9e6bc1e14287`; identity `714794a10c2d886f7a53a9926a4bb63e093cec16858676268e31833aeead2681`; k10/0–63 |

The [a0004 receipt](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-cold/a0004/independent-verification.json) records 53 authenticated artifacts, 512 revalidated records and publication revalidation. The [metadata-waves closed receipt](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-cold/metadata-waves/a0001/decision.json) records source-order/physical parity and unchanged ordered results and charges. These establish historical qualification only.

**Format and input risks:**

- V282 is a PQ-primary/graph-versus-flat k100 diagnostic contract. Its gates and historical quality are not current native control authority.
- The scorer configs’ original roots are **v4 provenance** (`577a9c…` / `943055…`). Native packaging adds plane-v3 digest authority and produces distinct **v7 roots**. [Publication preparation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-native-publication-preparation.json) is explicitly unpublished; later closed publication receipts supply the qualification.
- The current fixed48 source manifest inspected records native commit `4d618ffa3289941c71887855f99ef2850925d8c5`, identity `addf62bce23ceee034f22e4d1dfc0b318564d924c6e0fcee34b9336d1532813e`. It is a separate source/profile authority, not the historical 100k binary above. Unsupported historical formats must be rejected by current admission, not silently loaded.
- V282 names ReLAION `original_ids.u64`, but the inspected inventory does not provide its exact authenticated descriptor. The historical stable-ID → ordinal-ID → physical-order → truth binding needs an explicit prospective receipt.

**One actionable plan, after operator approval:** freeze a next100k input manifest from these descriptors, resolve that ID binding, and qualify an unchanged current-library 100k control on the exact chosen source/profile/k/split. Add only the four loss-stage receipts required by the pending decision: router selection, boundary coverage, local nomination and final ranking, with actual fetch/resource accounting.

**Bounded falsifying test:** one preregistered paired replay on both 100k corpora, with fixed budgets and no tuning or second sweep. Reject execution on identity/format/accounting failure. Falsify the candidate if either corpus breaches its frozen paired-loss or resource threshold; stage receipts must locate the loss. Historical V282 thresholds must not become new thresholds implicitly.

A fresh panel is **not necessary for a consumed-panel diagnostic falsifier**. It **is necessary for an independent held-out qualification/publication claim**; ordinals 256–999 cannot be presumed fresh from their old “validation” label. No panel is selected here.

