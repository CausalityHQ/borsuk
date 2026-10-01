# Fresh ReLAION FIRST1M construction authority map

Audited exact base `bddd6db743f492c5c450616c3cfbdf6d9b935661`. Local files below match that committed tree byte for byte. Source commit means the last commit touching the path at this base; remote construction retains its separate historical source/archive identity.

## One unresolved authority

`refs.panel` is absent: no committed JSON or archived JSON under `docs/` has schema `borsuk-semantic-1m-fresh-panel-ids-v1`. The committed panel-tools verification says `real_panel_selected: false` and `queries_or_truth_generated: false`. The preregistration specifies the successor, but is not a selected-panel body.

`construction-config.draft.json` uses the existing helper schema and leaves only `refs.panel.path`, `refs.panel.bytes`, and `refs.panel.sha256` null. It is deliberately non-runnable: the helper requires an authenticated JSON file. No path, size, hash, or selected ID was invented. The root must perform the preregistered metadata-only selection, commit the resulting panel, fill these three fields from its exact bytes, and freeze the completed configuration before resource-heavy preparation.

Preserve reservoir ordinals 1000–1063 as query ordinals 0–63; consumed reservoir 0–999 remains excluded. Keep `quality_peek_allowed: false` and `complete_historical_coverage: false`. This audit does not select queries, decode vectors, open truth, establish historical vector coverage, or qualify the arm.

## Required repository references

| Helper ref | Committed path | Bytes | SHA256 | Source commit |
| --- | --- | ---: | --- | --- |
| panel | unresolved | unresolved | unresolved | unresolved |
| old_panel | `docs/research/native-union-20260928/fresh-rank16-provisional-ids.json` | 227817 | `a8bd97d6e6468e715c4b26d9df8c400f649e5428b2a85f151307f20cb030230c` | `13b2e883af3a6e60bb072b842aac0485626be3ae` |
| old_config | `docs/research/native-union-20260928/fresh-rank16-seal-config.json` | 1507 | `7885240547ca7791c2c7aca4d6905cafd8af8e651ee3c1c5925290c4bbb0ff16` | `8cf874115239098c37904aabe92b3c8d8a82e492` |
| old_verification | `docs/research/native-union-20260928/fresh-rank16-seal/a0001/verification.json` | 1614 | `c9616d1829956d80a73f066254368181b79bbc9ef010cb71d14f2d46cbab55e1` | `35b2fab4e6d1aaa80077034b38c3d20891b35761` |
| registry | `docs/research/v36-prefix-source-registry.json` | 726170 | `b9a19e2f142fd54983ed1db9f09862f2c5623b6b2105d66e538664f8adda9180` | `92862594c795bcf3b69d0bf5ccd2558f12f139ae` |
| population | `docs/research/native-union-20260928/fresh-identity-authorities/population-authority.json` | 9839 | `be6abb86e2930b38da572cb7daec3fc0e1a7adb93a53ce974ab038f1efa42fbe` | `c32479584dcb56af34e26fab127b2d621dcb0941` |
| overlap | `docs/research/native-union-20260928/fresh-relaion-physical-id-overlap.json` | 9422 | `888748ef1125416bfa47c6dc791280e8be1885eb7ea68f3145872985fe5801c6` | `6dff04dc529f8726f68de6d8c28165d6e0821b1c` |

The old configuration, verification and panel identities cross-bind exactly; the selector pins old_panel/registry/population/overlap to these same hashes. The old roster contains exactly query ordinals 0–999. Closed construction records `valid_construction: true`, `ann_quality_measured: false`, and terminated instance `i-0b38b829e7709b803`.

## Remote input identities copied without substitution

Bucket: `borsuk-bench-453182569524-euc1`. These are committed authorities, not fresh cloud readbacks or worker-authenticated local vector bodies. No cloud call was made.

| Helper field | Key / authority | Bytes | SHA256 |
| --- | --- | ---: | --- | --- |
| source_parquet | `research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet` from old_config | 1458450077 | `2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86` |
| source_raw_sha256 | Raw FIRST1M, from old_config; size = 1000000×768×4 | 3072000000 | `a3eac4dedae5006843ea2ad243ec590440fe5b983ca8cd969dbc92b3e3406c33` |
| consumed_queries | `research/native-union/20260928/fresh-rank16-seal-a0001/sealed/queries.raw` from old_verification.sealed_artifacts.queries.raw | 3072000 | `a1a2d8d0f37bafbb26e2f918a03c94330819be0ab41ec6cafc1c8b6e898a1758` |

The authenticated archived construction decision repeats both source hashes and the consumed query identity. Remote construction source base was `165aa6d0d795d77bc5363e1752ed538390ba094a`, with source archive SHA256 `2500a6c97a219c4e91576f32f77079268184946eb901ef38f8da2b81a5cb5afc`; these are historical provenance, not current helper code pins. The source parquet/raw and consumed query bodies must be authenticated by the root-owned preparation using the existing helper. Production source-order/application-ID mapping is a separate later gate, not an extra field in this helper schema.

## Supporting committed evidence

| Committed path | Bytes | SHA256 | Source commit |
| --- | ---: | --- | --- |
| `docs/research/performance-architecture-20260930/semantic-1m/panel-tools/verification.json` | 1843 | `2d94860821cbefa25d5188a143490d41307d29293fa26cb03884f73d4e0c2908` | `b364e9ff38779373bd4b0ffc268bc5c25899bce2` |
| `docs/research/performance-architecture-20260930/semantic-1m-fresh-panel-preregister.md` | 3930 | `4f2185b551077c2f2cddf0e269555add02f81ebd16dcbe44a2df7b55ad8fde14` | `0c427fa39a3eb5cd22b25c7cd59dc6b4c25b29d3` |
| `docs/research/native-union-20260928/fresh-rank16-seal/a0001/aws-terminal.json` | 1064 | `c22cea7e147632511873a0cbc0350d39d691f181ad3281bd9042ff21c0e81e88` | `35b2fab4e6d1aaa80077034b38c3d20891b35761` |
| `docs/research/native-union-20260928/fresh-rank16-seal/a0001/aws-reservation.json` | 1018 | `a7465fc4b8c6bfc2de614fa3bdb81c010c564a68591caa189987fa2007b41a55` | `35b2fab4e6d1aaa80077034b38c3d20891b35761` |
| `docs/research/native-union-20260928/fresh-rank16-seal/a0001/aws-closeout.json` | 209 | `73dad496ae0c6705abb0f724d90c9bcc76f34c59d0167ba2beccba633f40c5e3` | `35b2fab4e6d1aaa80077034b38c3d20891b35761` |
| `docs/research/native-union-20260928/fresh-rank16-seal/a0001/screen/decision.json.gz` | 1663 | `66bfb3fb35dc72eb521828a9beb0b5da5867a807f2eb909d54338d85cb8d9c4b` | `35b2fab4e6d1aaa80077034b38c3d20891b35761` |

The gzip decision decompresses to 3903 bytes, SHA256 `d093822f9f6e1d063bea5037c5fe19040e91bbccefe3b5f912f929c47dd8b261`, exactly matching `aws-terminal.json.artifacts["screen/decision.json"]`. Its decoded body is not an extra repository ref and was not written to disk.

The local formatted reservation SHA above differs from the original remote reservation body SHA `5231ce71c4fb2decd585f4bb74a8be5896fea9086664d7c0a8e21f2c357d9f9c` in old_verification. Local JSON values match the terminal's source/archive, configuration and panel bindings; no equality of their serialized bytes is claimed.

## Exact existing helper import closure

Static AST inspection of top-level `scripts` imports yields exactly these 20 files, including transitive imports of the existing oracle module. Every scripts import in the closure was checked for inclusion. No helper, selector, oracle or scorer was executed or imported for this audit; NumPy/PyArrow versions are copied from the helper requirements. `scripts` is a namespace package with no `scripts/__init__.py` at this base.

| Code path | Bytes | SHA256 | Source commit |
| --- | ---: | --- | --- |
| `scripts/audit_v36_ranked_physical_ids.py` | 5240 | `67af40bf3d505d45979f269218f4bfd426aaaa5f1e6705b6947bd32831d8bb2b` | `fbcc749cdb353224c5ea248e0f06e7c0738fe22c` |
| `scripts/native_geometric_layout_screen.py` | 78486 | `f12c50c2cb492c003f78dc208cdb3e39d5e73af9cd253db46ce2cc6872400bf6` | `d167368347a1e174ac21b45f7526ae7cdec0e19e` |
| `scripts/native_rotated_two_bit_codes.py` | 13090 | `8d7e52b78f8d8c44fe120a04cd146744ae6f5392867a806de4c528154aacdc33` | `e122d51fc54cd4fd2f215e03c6566cf2de35ddbf` |
| `scripts/native_row_score_code_artifacts.py` | 8265 | `e670519128fef19547fe4daae48518d94ef1b2658e6cd1b00bd7b1e62687ebf4` | `72400c494ea120bb399def38969deee1a24b7337` |
| `scripts/native_two_bit_cosine_development.py` | 6741 | `ad439e50a8a5e448d520136516e0e4a3813c51d0a01bb570a19c13ffb092244d` | `4f7c7dd19dcd15cef7924c3acf684a14e6ca6ce0` |
| `scripts/native_two_bit_topology.py` | 9693 | `142aaf45ba490bc62868e7f946d0ed43ad020511ca7cd6967233262217e2e1b3` | `73569e89b3d53f75ae022714cf80aecd99a7a058` |
| `scripts/prepare_semantic_1m_fresh_panel.py` | 33106 | `25093cbf96117ab126d6aac45869058e2f620210d34a76a008cb22683a4276b8` | `bcfa91030ec6db75f4c006371c01a0bc7dc90124` |
| `scripts/run_native_cold.py` | 7915 | `adc14ad6580b328e8bee9c75660493d6b7bb526865686f8e110e83feea5a262c` | `9813e37bcf66751109c40cf4dc1f3fafcc3a2186` |
| `scripts/run_native_source_frontier_1m.py` | 14781 | `fe91b3afbd95648fa2d6de371ae4fa3b31d549df873496010a643f365bff450d` | `98bdf1c3abeeeddeaf62eeaad6b3421c8f3b2c10` |
| `scripts/run_native_union_cold.py` | 13229 | `6a4bbd483c28432587b8ee9c92e72c679fea73bd65af1a773ee255e67606f7d9` | `0286df020589fd07aec07c1f59a8b3dc68b4abea` |
| `scripts/run_native_union_http.py` | 10532 | `886f84ea639b822668a848e8f085027aec12818e316d0839fbf607844d18f880` | `b2e3074aad5406a1052cc9e7c5562ece8b817294` |
| `scripts/seal_v36_rank16_fresh_1m.py` | 10807 | `8ce2eb3d68a051dda1d12483793cbf7e28ed1ff8d4f7f055900185f305a52145` | `bcfa91030ec6db75f4c006371c01a0bc7dc90124` |
| `scripts/select_v36_rank16_fresh_ids.py` | 6905 | `6388e274ba128d770bbadd8b02833af4f685214bc1f78c9796fd778471d30399` | `bcfa91030ec6db75f4c006371c01a0bc7dc90124` |
| `scripts/v102_two_wave_pq48_refinement.py` | 14187 | `2b702b08a49f9a325de8e2e6ef04b6c6489215aba9fb208bbd41253166afc27b` | `2c560a8e70c4c5cbc55d1e6b6eebdad7a070a2da` |
| `scripts/v284_page_primary_dev64.py` | 5016 | `7602ec6365995f73d6656ab59c246950225e507b79190e16cf523c4d9c076d1e` | `f99f35d8429e3ecdd16a8bc821955902cd672a8f` |
| `scripts/v285_exact_page_rank_bound.py` | 3827 | `f5ffd1b3798ec1ee19948206ea150734895abba6ab500ca5ec6a8834d8f60aa9` | `ac8ebf835265dd27c5d64139b19cbe4612c73da8` |
| `scripts/v291_two_stage_development.py` | 9655 | `2c55907696c22273f8adec826059969d4262fcfbb97de9cd5ef579cb75073916` | `bbdae616768c97e33347466b10bc2e41776c5929` |
| `scripts/v97_row_width_screen.py` | 50388 | `49307a79702dd297416950cf55d600f080da450ad217e9f8d9af1616b2e17477` | `25acd3be21524ee2b0ab4040121ccf8d7ff60902` |
| `scripts/v98_hierarchical_row_router.py` | 52964 | `ce70105ef2a5b75b25147f00dd3d6965e8d52c34a5278ca1267288c0e28e424c` | `b3db2b2d060eeb56a3890ab9607cdaea85bfa7e0` |
| `scripts/v99_ranked_gap_range_router.py` | 22008 | `ec8ffc2aaf1376a7d6c3f8228429ec6c37117d0223ebaf9a775ddb4249d29079` | `b3db2b2d060eeb56a3890ab9607cdaea85bfa7e0` |

## Verification boundary

A stdlib-only audit checked every cited local file against `git show` at the exact base, recomputed all byte lengths/SHA256 values and source commits, checked historical cross-bindings and the authenticated gzip decision, traversed the 20-file AST import closure, and scanned committed JSON/archived JSON for the missing panel schema. The final owned-file diff must pass `git diff --check`. No new construction, cloud readback, vector audit, oracle output or ANN measurement is claimed.
