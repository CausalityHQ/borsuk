# V21 Dual-Authority Feasibility Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the current V21 feasibility diagnostic against the exact completed historical Deep Image V20 index without rebuilding or misattributing that index.

**Architecture:** Add a V21-only base-index authority boundary that authenticates the historical build terminal and every referenced manifest/protocol/index artifact before any write or EC2 request. The current source is compiled on a short-lived build-class Spot host, while raw evidence names the historical index producer and the terminal receipt binds both authority chains.

**Tech Stack:** Python 3.12, unittest, Bash, AWS CLI/S3 Standard/EC2 Spot, Rust/Cargo, systemd cgroups.

**Spec:** `docs/superpowers/specs/2026-08-27-v21-dual-authority-diagnostic-design.md`

## Global Constraints

- Use S3 Standard only; no S3 Express, local serving tier, or query-populated persistent object cache.
- Reuse `index-41967870b0bff1a67f5e82af`; do not write or rebuild a V20/V21 index.
- Historical build terminal URI and SHA-256 are explicit CLI inputs with no ambient default.
- Validate historical authority before current-source S3 publication, attempt selection, or EC2 launch.
- Use the registered build-worker Spot host, `MemoryMax=34359738368`, `MemorySwapMax=0`, and terminate at the first terminal outcome.
- Result remains `claim_eligible:false`; selector, GET, byte, coverage, recall, and projected-RSS gates are unchanged.

---

### Task 1: Authenticate the historical base-index authority

**Files:**
- Modify: `scripts/publication_v3_controller.py`
- Test: `scripts/test_publication_v3_controller.py`

**Interfaces:**
- Consumes: canonical current manifest, exact historical build-terminal URI and SHA-256, `AwsCli` immutable reads.
- Produces: `BaseIndexAuthority` and `authenticate_v21_base_index_authority` for later controller/runtime tasks.

- [ ] **Step 1: Write failing authority tests**

Add tests whose fake AWS boundary records every method call and proves the historical manifest/source/protocol differ from current authority but derive the exact historical cell and index:

```python
def test_v21_base_authority_authenticates_distinct_historical_generation_before_writes(self):
    authority = authenticate_v21_base_index_authority(
        current_manifest=current_manifest,
        terminal_uri=HISTORICAL_BUILD_TERMINAL_URI,
        terminal_sha256=HISTORICAL_BUILD_TERMINAL_SHA256,
        aws=HistoricalAuthorityAws(),
        git_is_ancestor=lambda old, new: (old, new) == (BASE_COMMIT, CURRENT_COMMIT),
    )
    self.assertEqual(authority.cell_id, "r01-4b9cd157d24e8da94d9c8807")
    self.assertEqual(authority.index_id, "index-41967870b0bff1a67f5e82af")
    self.assertNotEqual(authority.source_archive_sha256, CURRENT_SOURCE_SHA256)

def test_v21_base_authority_mismatch_fails_before_any_write_or_ec2(self):
    aws = ReadOnlyAuthorityAwsWithMutatedIndexUri()
    with self.assertRaisesRegex(ValueError, "base-index authority"):
        authenticate_v21_base_index_authority(
            current_manifest=current_manifest,
            terminal_uri=HISTORICAL_BUILD_TERMINAL_URI,
            terminal_sha256=HISTORICAL_BUILD_TERMINAL_SHA256,
            aws=aws,
            git_is_ancestor=lambda _old, _new: True,
        )
    self.assertEqual(aws.mutating_calls, [])
```

Mutation cases cover terminal bytes, terminal digest, manifest digest, protocol digest, cell ID, index URI, source ancestry, Deep Image dataset authority, index profile, architecture, campaign, workload, receipt, roster, and inventory.

- [ ] **Step 2: Run the focused tests and confirm RED**

Run:

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_publication_v3_controller.PublicationV3ControllerTests.test_v21_base_authority_authenticates_distinct_historical_generation_before_writes \
  scripts.test_publication_v3_controller.PublicationV3ControllerTests.test_v21_base_authority_mismatch_fails_before_any_write_or_ec2
```

Expected: import or attribute failure for `authenticate_v21_base_index_authority`.

- [ ] **Step 3: Implement the immutable read and authority type**

Add these exact boundaries:

```python
@dataclasses.dataclass(frozen=True)
class BaseIndexAuthority:
    manifest: dict[str, object]
    manifest_uri: str
    manifest_sha256: str
    protocol_uri: str
    protocol_sha256: str
    source_uri: str
    source_archive_sha256: str
    source_git_commit: str
    build_terminal_uri: str
    build_terminal_sha256: str
    build_prefix: str
    build_cell_id: str
    build_attempt: int
    index_id: str
    index_uri: str
    index_receipt_sha256: str
    object_roster_sha256: str
    inventory_sha256: str

def AwsCli.read_immutable_bytes(self, uri: str, sha256: str) -> bytes:
    head = self._head(uri)
    if head is None:
        raise ValueError("publication authority object is missing")
    body = self._download_bytes(uri)
    digest = hashlib.sha256(body).digest()
    if (
        len(body) != head.get("ContentLength")
        or digest.hex() != sha256
        or head.get("Metadata", {}).get("borsuk-sha256") != sha256
        or head.get("ChecksumSHA256") != base64.b64encode(digest).decode("ascii")
    ):
        raise ValueError("publication authority object checksum differs")
    return body

```

`read_immutable_bytes` must reconcile HEAD content length, metadata SHA-256, checksum SHA-256, downloaded length, and downloaded digest. The authenticator derives every URI from canonical campaign structure, validates the historical manifest and protocol bytes, reconstructs `borsuk_cell(historical_manifest, workload_id="standard-ann-read", dataset_id="deep-image-96", repetition_id="r01", build_attempt=terminal["attempt"])`, validates the terminal using `completed_build_authority`, and authenticates INDEX_COMPLETE/INDEX_OBJECTS/INDEX_INVENTORY without accepting caller-provided derived values.

Its exact signature is `authenticate_v21_base_index_authority(*, current_manifest: dict[str, object], terminal_uri: str, terminal_sha256: str, aws: Any, git_is_ancestor: Any = _git_is_ancestor) -> BaseIndexAuthority`.

- [ ] **Step 4: Run focused authority tests and confirm GREEN**

Run the command from Step 2 plus all tests matching `v21_base_authority`.

- [ ] **Step 5: Commit the authority slice**

```bash
git add scripts/publication_v3_controller.py scripts/test_publication_v3_controller.py
git commit -m "Authenticate reused V21 base index"
```

### Task 2: Split current diagnostic execution from historical index identity

**Files:**
- Modify: `scripts/publication_v3_execution.py`
- Modify: `scripts/run_publication_v3_cell.py`
- Test: `scripts/test_publication_v3_execution.py`
- Test: `scripts/test_run_publication_v3_cell.py`

**Interfaces:**
- Consumes: `BaseIndexAuthority` serialized as canonical worker inputs.
- Produces: V21 runtime worker that compiles current source but validates and reads the historical index.

- [ ] **Step 1: Write failing split-authority worker and parser tests**

```python
def test_v21_worker_compiles_current_source_but_reads_historical_index_authority(self):
    script = make_v21_runtime_worker_for_test(v21_base_authority=BASE_AUTHORITY)
    self.assertIn("cargo build --locked --release --example production_bench", script)
    self.assertNotIn("cp $work/build/production_bench", script)
    self.assertIn(BASE_INDEX_URI, script)
    self.assertIn(BASE_SOURCE_SHA256, script)
    self.assertIn(CURRENT_SOURCE_SHA256, script)

def test_v21_summary_rejects_diagnostic_source_as_index_producer(self):
    with self.assertRaisesRegex(ValueError, "base-index identity"):
        summarize_v21_fixture(expected_index_id=CURRENT_INDEX_ID)
```

Also assert all three raw artifacts and canonical result use historical `index_id` and historical source archive, while the result authority object separately uses current source and compiled binary.

- [ ] **Step 2: Run the focused tests and confirm RED**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_publication_v3_execution.PublicationV3ExecutionTests.test_v21_worker_compiles_current_source_but_reads_historical_index_authority \
  scripts.test_run_publication_v3_cell.PublicationV3CellTests.test_v21_summary_rejects_diagnostic_source_as_index_producer
```

Expected: missing `v21_base_authority` and identity still taken from the current cell.

- [ ] **Step 3: Implement separate worker inputs and runtime validation**

Extend `runtime_worker_script` with:

```python
v21_base_authority: dict[str, object] | None = None,
v21_memory_max_bytes: int | None = None,
```

For V21 only, download/authenticate the historical manifest, protocol, build terminal, INDEX_COMPLETE, INDEX_OBJECTS, and INDEX_INVENTORY; compile `production_bench` from the current source; invoke `run_publication_v3_cell.py` with both current and base authority files. Extend the runner CLI with:

```text
--v21-base-manifest
--v21-base-protocol
--v21-base-build-terminal
--v21-base-index-receipt
--v21-base-object-roster
--v21-base-inventory
```

The V21 plan validates current cell authority first, separately validates the historical build cell and index receipt, and overrides only the index URI and V21 environment identity. Ordinary paths reject these arguments.

- [ ] **Step 4: Run focused worker/parser tests and shell syntax**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_publication_v3_execution scripts.test_run_publication_v3_cell
bash -n scripts/launch_aws_publication_v3.sh
```

- [ ] **Step 5: Commit the execution split**

```bash
git add scripts/publication_v3_execution.py scripts/run_publication_v3_cell.py \
  scripts/test_publication_v3_execution.py scripts/test_run_publication_v3_cell.py
git commit -m "Run V21 diagnostic over reused index"
```

### Task 3: Bind both authorities and actual resource enforcement in the controller

**Files:**
- Modify: `scripts/publication_v3_controller.py`
- Modify: `scripts/publication_v3_aws.py`
- Test: `scripts/test_publication_v3_controller.py`
- Test: `scripts/test_publication_v3_aws.py`

**Interfaces:**
- Consumes: `BaseIndexAuthority`, current diagnostic manifest/source/protocol, runtime attempt.
- Produces: exact build-class Spot request and dual-authority terminal expectations.

- [ ] **Step 1: Write failing controller/resource tests**

```python
def test_v21_execution_uses_build_class_spot_and_binds_dual_authority(self):
    prepared = prepare_v21_execution_for_test(v21_base_authority=BASE_AUTHORITY)
    self.assertEqual(prepared.request["InstanceType"], "r7g.8xlarge")
    self.assertEqual(prepared.expected["memory_max_bytes"], 34_359_738_368)
    self.assertEqual(prepared.expected["memory_swap_max_bytes"], 0)
    self.assertEqual(prepared.expected["base_index_id"], BASE_INDEX_ID)
    self.assertEqual(prepared.expected["diagnostic_source_archive_sha256"], CURRENT_SOURCE_SHA256)
```

Add terminal receipt mutations for every dual-authority SHA, host class, memory/swap cgroup value, binary SHA, index ID/URI, and four V21 artifact hashes.

- [ ] **Step 2: Run focused tests and confirm RED**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_publication_v3_controller -k v21 \
  scripts.test_publication_v3_aws -k v21
```

Expected: request still uses runtime resources and 3 GiB authority.

- [ ] **Step 3: Implement build-class diagnostic request and exact receipt fields**

Add a `diagnostic` resource role to `build_launch_request` that selects the manifest's build-worker instance/storage but retains runtime job tags and terminal paths. Set V21 expectations to:

```python
{
    "claim_eligible": False,
    "v21_feasibility": True,
    "base_build_terminal_sha256": authority.build_terminal_sha256,
    "base_manifest_sha256": authority.manifest_sha256,
    "base_protocol_sha256": authority.protocol_sha256,
    "base_source_archive_sha256": authority.source_archive_sha256,
    "base_index_receipt_sha256": authority.index_receipt_sha256,
    "base_object_roster_sha256": authority.object_roster_sha256,
    "base_inventory_sha256": authority.inventory_sha256,
    "base_index_id": authority.index_id,
    "base_index_uri": authority.index_uri,
    "diagnostic_source_archive_sha256": source_sha256,
    "memory_max_bytes": 34_359_738_368,
    "memory_swap_max_bytes": 0,
}
```

The worker obtains actual cgroup `memory.max`, `memory.swap.max`, and peak memory after the systemd scope exits and includes them in the receipt. Remove `requested-systemd-enforced`; receipt validation must compare actual configured values.

- [ ] **Step 4: Run controller/AWS tests and confirm GREEN**

Run the command from Step 2, then all controller and AWS test modules.

- [ ] **Step 5: Commit the orchestration slice**

```bash
git add scripts/publication_v3_controller.py scripts/publication_v3_aws.py \
  scripts/test_publication_v3_controller.py scripts/test_publication_v3_aws.py
git commit -m "Bind V21 diagnostic resource authority"
```

### Task 4: Add the fail-closed paid launcher entry point

**Files:**
- Modify: `scripts/launch_aws_publication_v3.sh`
- Test: `scripts/test_launch_aws_publication_v3.py`

**Interfaces:**
- Consumes: two explicit arguments, historical build-terminal URI and SHA-256.
- Produces: one exact `diagnose-v21-selector` controller invocation with automatic bounded attempts.

- [ ] **Step 1: Write failing launcher tests**

```python
def test_launcher_forwards_exact_v21_base_terminal_authority(self):
    completed = run_launcher(
        "--diagnose-v21-selector",
        HISTORICAL_BUILD_TERMINAL_URI,
        HISTORICAL_BUILD_TERMINAL_SHA256,
    )
    self.assert_controller_argv_contains(
        "diagnose-v21-selector",
        "--base-build-terminal-uri", HISTORICAL_BUILD_TERMINAL_URI,
        "--base-build-terminal-sha256", HISTORICAL_BUILD_TERMINAL_SHA256,
        "--purchase-option", "spot",
    )
```

Negative tests reject missing arguments, non-S3 URI, uppercase/non-64-hex digest, ambient attempt overrides, dirty/unpushed source, and any raw-generated dataset before controller execution.

- [ ] **Step 2: Run launcher test and confirm RED**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_launch_aws_publication_v3 -k v21
```

Expected: launcher usage rejection.

- [ ] **Step 3: Implement the launcher mode**

Add exact syntax:

```text
launch_aws_publication_v3.sh --diagnose-v21-selector <base-build-terminal-s3-uri> <base-build-terminal-sha256>
```

It freezes and authenticates current source as usual, sets both attempt sentinels to zero internally, forwards AWS profile `causality` by default, and refuses any non-Spot override.

- [ ] **Step 4: Run launcher tests and shell syntax**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_launch_aws_publication_v3
bash -n scripts/launch_aws_publication_v3.sh
```

- [ ] **Step 5: Commit the launcher slice**

```bash
git add scripts/launch_aws_publication_v3.sh scripts/test_launch_aws_publication_v3.py
git commit -m "Expose V21 diagnostic launch"
```

### Task 5: Final assurance, independent review, and one paid attempt

**Files:**
- Modify only if verification finds a defect.

**Interfaces:**
- Consumes: all prior committed slices.
- Produces: reviewed, pushed source and one terminal paid diagnostic receipt.

- [ ] **Step 1: Run focused and static assurance serially**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -p borsuk --lib v21_feasibility -- --nocapture
cargo test --locked -p borsuk --example production_bench v21 -- --nocapture
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_publication_v3_controller \
  scripts.test_publication_v3_execution scripts.test_run_publication_v3_cell \
  scripts.test_publication_v3_aws scripts.test_launch_aws_publication_v3
```

- [ ] **Step 2: Run repository assurance once**

```bash
cargo test --locked --workspace --all-targets
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python scripts/validate_research_docs.py
```

- [ ] **Step 3: Request a read-only cross-provider diff review**

Ask for Critical/Important provenance, lifecycle, idempotence, memory, evidence-attribution, or paid-launch defects. Repair any finding with a focused RED/GREEN before rerunning only the affected gate, then one final full gate.

- [ ] **Step 4: Deliver source by fast-forward**

```bash
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

- [ ] **Step 5: Launch one exact diagnostic attempt**

```bash
AWS_PROFILE=causality scripts/launch_aws_publication_v3.sh \
  --diagnose-v21-selector \
  s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-4b9cd157d24e8da94d9c8807/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
  265a6ee522c6cf7e42eb4bcb2278fafd71d7b311ce957377362154448313cc19
```

Monitor only terminal markers and infrastructure health. Stop on registered memory pressure, preserve the original attempt result, start no overlap, and terminate compute immediately after the terminal marker.

- [ ] **Step 6: Apply the immutable decision rule**

Qualify an arm only if selector bytes are at most 40,000,000, actual GETs at most four/query, actual bytes at most 1 MiB/query, minimum GT coverage at least 0.990, recall@10 at least 0.975, and projected production RSS at most 768 MiB. Otherwise record the terminal negative and start the selector-region fetch-unit design without relaxing any gate.
