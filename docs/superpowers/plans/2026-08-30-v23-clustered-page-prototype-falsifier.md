# V23 Clustered Page-Prototype Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, authority-strict, bounded-memory Python tool that streams the 28,282 immutable Revision-4 BVP2 pages and falsifies or preserves the K=32 spherical page-centroid hypothesis without launching D3 or new compute infrastructure.

**Architecture:** One evidence-only `Revision4Bvs2Authority` adapter authenticates the five explicit historical prerequisites with their role-specific writers, validates their complete dependency graph, and normalizes them into the existing `Authority`. The unchanged scientific path then streams content-addressed S3 page bodies through a four-object ordered buffer; each page is decoded, clustered, scored, and discarded, leaving only a `32 x 28,282` score matrix and compact counters. A separate test module owns the exact five-artifact no-page boundary, synthetic codecs, fake S3, six authority-mutation families, pressure stops, and canonical result validation.

**Tech Stack:** Python 3.12, stdlib `argparse`/`concurrent.futures`/`dataclasses`/`hashlib`/`json`/`resource`/`struct`, NumPy 2.4.2, PyArrow 24.0.0, boto3 1.34.46, Python blake3 1.0.8, stdlib unittest, Ruff.

**Spec:** `docs/superpowers/specs/2026-08-30-v23-clustered-page-prototype-falsifier-design.md`

## Global Constraints

- This is an evidence script, not a production BVP2 reader; production code must not import it or retain a legacy dispatch path.
- The full stream is forbidden without the literal `--execute-complete-stream` CLI flag and a separately coordinated lane/cost grant.
- Inputs are explicit immutable URIs and SHA-256/BLAKE3 digests; discovery of a latest object is forbidden.
- The scientific shape is exactly 28,282 pages, 32 queries, eight pages per query, K=32, and eight Lloyd iterations.
- At most four complete page bodies may be retained; pages are scientifically consumed in ascending ordinal order.
- A complete stream stops without restart and without partial quality output at RSS 768 MiB, PSI full avg10 0.50, swap growth 128 MiB, five minutes without page progress, or any authority failure.
- The result must recompute the exact 2,686,433,028-byte 100M serving projection and reject values above 3 GiB.
- No AWS compute, D3, production format, paid comparison, or performance claim is authorized by this plan.
- No page GET is authorized until the env-gated five-artifact integration reaches the registered ordinal-zero page through `PAGE_S3_ACCESS_FORBIDDEN`.
- Revision-4/BVS2 support is confined to this evidence adapter; it is not a legacy production reader or migration contract.

---

### Task 1: Strict BVP2 authority and deterministic page clustering

**Files:**
- Create: `scripts/v23_clustered_page_prototype_falsifier.py`
- Create: `scripts/test_v23_clustered_page_prototype_falsifier.py`
- Modify: `scripts/requirements-format-bench.txt`

**Interfaces:**
- Consumes: canonical Revision-4 page references with exact keys `generation_checksum`, `page_ordinal`, `metric`, `dimensions`, `family`, `code_width`, `path`, `checksum`, `encoded_bytes`, `primary_rows`, and `replicated_rows`.
- Produces: `PageRef`, `decode_bvp2_page(reference: PageRef, body: bytes) -> numpy.ndarray`, `SplitMix64`, `spherical_kmeans(vectors: numpy.ndarray, body_checksum: str, clusters: int = 32, iterations: int = 8) -> numpy.ndarray`, and `score_page_means(queries: numpy.ndarray, means: numpy.ndarray) -> numpy.ndarray`.

- [ ] **Step 1: Write failing strict-codec and clustering tests**

```python
class PageCodecAndClusteringTests(unittest.TestCase):
    def test_bvp2_f16_flat_decodes_primary_then_replicas(self):
        reference, body, expected = bvp2_fixture(primary=3, replicas=2)
        actual = subject.decode_bvp2_page(reference, body)
        numpy.testing.assert_array_equal(actual, expected)

    def test_bvp2_rejects_every_authority_and_byte_mutation(self):
        reference, body, _ = bvp2_fixture(primary=3, replicas=2)
        for name, mutant_ref, mutant_body in codec_mutations(reference, body):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    subject.decode_bvp2_page(mutant_ref, mutant_body)

    def test_spherical_kmeans_is_repeatable_and_roundtrips_f16(self):
        vectors = normalized_fixture_vectors()
        first = subject.spherical_kmeans(vectors, "12" * 32, clusters=4, iterations=8)
        second = subject.spherical_kmeans(vectors, "12" * 32, clusters=4, iterations=8)
        numpy.testing.assert_array_equal(first, second)
        self.assertEqual(first.dtype, numpy.float32)
        self.assertTrue(numpy.isfinite(first).all())
        numpy.testing.assert_array_equal(first, first.astype("<f2").astype("<f4"))

    def test_page_score_is_minimum_squared_distance(self):
        queries = numpy.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=numpy.float32)
        means = numpy.asarray([[1.0, 0.0], [-1.0, 0.0]], dtype=numpy.float32)
        numpy.testing.assert_array_equal(
            subject.score_page_means(queries, means),
            numpy.asarray([0.0, 2.0], dtype=numpy.float32),
        )
```

The mutation generator covers wrong magic/version/metric/family/reserved bytes/dimension/ordinal/generation/code width/counts/section lengths/path/length/BLAKE3, duplicate or unsorted IDs within either partition, cross-partition duplicate IDs, non-finite f16, short headers, invalid offsets, and trailing bytes. A separate repeated-vector fixture forces empty-cluster repair and asserts that every final cluster is populated.

- [ ] **Step 2: Run the focused tests and preserve RED**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_v23_clustered_page_prototype_falsifier.PageCodecAndClusteringTests
```

Expected: collection/import failure because `scripts.v23_clustered_page_prototype_falsifier` does not exist.

- [ ] **Step 3: Add the evidence dependency and minimal implementation**

Append exactly `blake3==1.0.8` to `scripts/requirements-format-bench.txt`. Set `OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, and `NUMEXPR_NUM_THREADS` to `1` before importing NumPy. Define a frozen `PageRef` and reject booleans anywhere an integer is required. Decode the historical 96-byte header and offsets with `struct.unpack_from`, compare `blake3.blake3(body).hexdigest()` before interpreting sections, and use `numpy.frombuffer(code_bytes, dtype="<f2").astype("<f4")` only after every structural check passes.

Implement SplitMix64 exactly:

```python
class SplitMix64:
    def __init__(self, state: int) -> None:
        self.state = state & 0xFFFF_FFFF_FFFF_FFFF

    def next_u64(self) -> int:
        self.state = (self.state + 0x9E37_79B9_7F4A_7C15) & 0xFFFF_FFFF_FFFF_FFFF
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58_476D_1CE4_E5B9) & 0xFFFF_FFFF_FFFF_FFFF
        value = ((value ^ (value >> 27)) * 0x94D0_49BB_1331_11EB) & 0xFFFF_FFFF_FFFF_FFFF
        return value ^ (value >> 31)
```

Seed from the first eight checksum bytes interpreted little-endian. For k-means++, turn `next_u64()` into a `[0,total)` f64 boundary with `(next_u64() / 2**64) * total`, scan masses in input order, and choose the first cumulative boundary strictly greater than the draw. Lloyd assignment is `vectors @ centers.T`, `argmax` supplies the lowest-center tie, f64 `numpy.add.at` supplies accumulation, and empty-cluster repair follows the spec before the final f16 encode/decode.

- [ ] **Step 4: Run the focused tests and require GREEN**

Run the Step 2 command. Expected: all `PageCodecAndClusteringTests` pass.

- [ ] **Step 5: Commit the codec/clustering slice**

```bash
git add scripts/requirements-format-bench.txt \
  scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git commit -m "Add deterministic V23 page clustering falsifier core"
```

### Task 2: Initial synthetic input authentication and quality recomputation

This completed historical checkpoint established the normalized `Authority`
and quality APIs against synthetic fixtures. Its partial historical parsing is
explicitly superseded by Task 4 before any page-stream execution.

**Files:**
- Modify: `scripts/v23_clustered_page_prototype_falsifier.py`
- Modify: `scripts/test_v23_clustered_page_prototype_falsifier.py`

**Interfaces:**
- Consumes: exact local report/roster/query bytes plus their explicit SHA-256 values and Task 1 page/scoring APIs.
- Produces: `ScientificShape`, `RegisteredAuthority`, `Authority`, `load_authority(terminal_path: Path, result_path: Path, report_path: Path, roster_path: Path, query_path: Path, registered: RegisteredAuthority = REGISTERED_AUTHORITY, shape: ScientificShape = REGISTERED_SHAPE) -> Authority`, `select_pages(score_matrix: numpy.ndarray, width: int = 8) -> numpy.ndarray`, `quality_metrics(authority: Authority, selections: numpy.ndarray) -> dict[str, object]`, and `projected_serving_bytes() -> int`.

- [x] **Step 1: Write failing authority, ordering, and arithmetic tests**

```python
class AuthorityAndQualityTests(unittest.TestCase):
    def test_load_authority_binds_exact_bytes_schema_and_shape(self):
        fixture = authority_fixture(pages=3, queries=2)
        authority = subject.load_authority(**fixture.arguments)
        self.assertEqual([page.page_ordinal for page in authority.pages], [0, 1, 2])
        self.assertEqual(authority.queries.shape, (2, 96))

    def test_authority_rejects_digest_schema_order_shape_and_type_drift(self):
        for fixture in authority_mutations():
            with self.subTest(name=fixture.name):
                with self.assertRaises(ValueError):
                    subject.load_authority(**fixture.arguments)

    def test_page_selection_uses_distance_then_page_ordinal(self):
        scores = numpy.asarray([[1.0, 0.5, 0.5, 0.2]], dtype=numpy.float32)
        numpy.testing.assert_array_equal(subject.select_pages(scores, 3), [[3, 1, 2]])

    def test_quality_is_recomputed_from_assignments_and_oracle(self):
        authority, selections = quality_fixture()
        result = subject.quality_metrics(authority, selections)
        self.assertEqual(result["aggregate_recall_ppm"], 750_000)
        self.assertEqual(result["minimum_query_recall_ppm"], 500_000)
        self.assertEqual(result["oracle_attainment_ppm"], 750_000)

    def test_projection_is_exact_and_within_three_gibibytes(self):
        self.assertEqual(subject.projected_serving_bytes(), 2_686_433_028)
        self.assertLessEqual(subject.projected_serving_bytes(), 3 * 1024**3)
```

Mutations cover noncanonical JSON, wrong exact key set, bool/int substitution, report or roster SHA drift, repeated/missing/out-of-order page ordinals, repeated page path/checksum, any page-reference drift from the report arm, wrong query-object hash, wrong query ordinals, non-finite or non-unit query vectors, ground-truth assignment outside the roster, wrong query count, wrong dimension, and wrong oracle hit counts.

- [x] **Step 2: Run the focused authority tests and preserve RED**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_v23_clustered_page_prototype_falsifier.AuthorityAndQualityTests
```

Expected: failures for the missing Task 2 interfaces.

- [x] **Step 3: Implement exact authority and recomputation**

Require exact registered SHA-256 values. Result, report, and roster use `scripts.publication_v3_protocol.canonical_json_bytes` plus one trailing newline; the historical terminal instead uses its shell writer's compact insertion order plus one newline. Select the only Revision-4 D2 arm and bind the frozen source/report/roster/query identities available to this initial fixture. `REGISTERED_AUTHORITY` and `REGISTERED_SHAPE` are immutable defaults; smaller values may be injected only through the pure Python API for synthetic tests, never through CLI flags. Read the registered query rows from the authenticated Parquet table, normalize once in f64, cast to f32, and reject zero/non-finite rows. `select_pages` uses `numpy.lexsort((page_ordinals, scores[row]))[:width]`. Recompute per-query hits from declared page assignments. Task 4 replaces every remaining trusted historical oracle/result value with exact Revision-4 recomputation and cross-binding before page access.

Implement the serving projection with checked nonnegative integer helpers and these exact terms: `9_050_240 * 196`, `282_820 * 320`, `65_536 * 96 * 2`, `(65_536 + 1) * 4`, `65_536 * 4_096`, `512 * 1024**2`, and `2 * 1_966_080`.

- [x] **Step 4: Run Task 1 and Task 2 tests and require GREEN**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_v23_clustered_page_prototype_falsifier.PageCodecAndClusteringTests \
  scripts.test_v23_clustered_page_prototype_falsifier.AuthorityAndQualityTests
```

Expected: all selected tests pass.

- [x] **Step 5: Commit the authority/quality slice**

```bash
git add scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git commit -m "Authenticate V23 clustered-page falsifier inputs"
```

### Task 3: Bounded ordered streaming, pressure stops, and canonical result

**Files:**
- Modify: `scripts/v23_clustered_page_prototype_falsifier.py`
- Modify: `scripts/test_v23_clustered_page_prototype_falsifier.py`

**Interfaces:**
- Consumes: Task 2 `Authority` and an injected S3-compatible client exposing `get_object(*, Bucket: str, Key: str) -> dict[str, object]`.
- Produces: `PressureSample`, `StreamStopped`, `ordered_page_bodies(client: object, bucket: str, prefix: str, pages: tuple[PageRef, ...], max_inflight: int = 4) -> Iterator[tuple[PageRef, bytes]]`, `run_falsifier(authority: Authority, client: object, pressure_probe: Callable[[], PressureSample], execute_complete_stream: bool) -> dict[str, object]`, `validate_result(value: object) -> dict[str, object]`, `canonical_result_bytes(value: dict[str, object]) -> bytes`, and `main(argv: Sequence[str] | None = None) -> int`.

- [x] **Step 1: Write failing bounded-stream and terminal-contract tests**

```python
class StreamingAndResultTests(unittest.TestCase):
    def test_ordered_fetch_never_retains_more_than_four_bodies(self):
        client, pages = delayed_s3_fixture(page_count=9)
        observed = list(subject.ordered_page_bodies(client, "bucket", "attempt/", pages, 4))
        self.assertEqual([page.page_ordinal for page, _ in observed], list(range(9)))
        self.assertLessEqual(client.peak_open_bodies, 4)

    def test_full_stream_requires_explicit_execution_flag(self):
        with self.assertRaises(ValueError):
            subject.run_falsifier(authority_fixture_object(), fake_s3(), safe_pressure(), False)

    def test_pressure_stop_emits_no_partial_quality(self):
        with self.assertRaises(subject.StreamStopped) as caught:
            subject.run_falsifier(
                authority_fixture_object(), fake_s3(), pressured_after_page(1), True
            )
        self.assertEqual(caught.exception.last_authenticated_page, 0)
        self.assertFalse(hasattr(caught.exception, "aggregate_recall_ppm"))

    def test_complete_result_is_canonical_strict_and_hash_stable(self):
        result = completed_stream_fixture()
        validated = subject.validate_result(result)
        payload = subject.canonical_result_bytes(validated)
        expected = json.dumps(validated, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.assertEqual(payload, expected)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), completed_stream_fixture_sha256())
```

Additional tests force GET failure, short or overlong `StreamingBody.read()`, out-of-order future completion, duplicate consumption, RSS/PSI/swap/progress stops, non-finite scores, result key/type mutation, threshold equality, query/page cardinality drift, and CLI refusal without the exact flag. Tests patch `open`, `Path.write_bytes`, `os.open`, and `tempfile` to prove no scientific scratch is written.

- [x] **Step 2: Run focused streaming tests and preserve RED**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_v23_clustered_page_prototype_falsifier.StreamingAndResultTests
```

Expected: failures for the missing streaming/result interfaces.

- [x] **Step 3: Implement bounded streaming and fail-closed execution**

`ordered_page_bodies` owns one `ThreadPoolExecutor(max_workers=4)`, submits at most four consecutive refs, and after yielding ordinal N submits only the next unsubmitted ref. A worker reads exactly `encoded_bytes`, reads one additional byte to prove EOF, closes the body, and returns immutable `bytes`. On any exception the coordinator cancels pending futures and closes every obtained body.

`run_falsifier` allocates `numpy.empty((32, page_count), dtype="<f4")`, samples pressure before the first GET and after every page, authenticates/decodes/clusters/scores one page, writes one score column, and deletes body/vectors/means. It computes peak RSS from `resource.getrusage`, PSI from cgroup `memory.pressure`, and swap from `memory.swap.current`, while accepting an injected probe in tests. A threshold equal to the cap stops. Only after every page authenticates may it select pages, recompute metrics, validate the exact-key result, print canonical JSON once to stdout, and print its SHA-256 once to stderr.

The CLI requires explicit local report/roster/query paths and hashes, exact bucket/prefix, AWS profile `causality`, region `eu-central-1`, and `--execute-complete-stream`. It rejects output paths and never writes result or scratch files.

- [x] **Step 4: Run the complete focused file and require GREEN**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_v23_clustered_page_prototype_falsifier
```

Expected: all tests pass and no AWS call occurs.

- [x] **Step 5: Run scoped static checks**

```bash
/home/rb/worktrees/sfora-ghc/.venv/bin/ruff check \
  scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
python3 -m py_compile \
  scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git diff --check
```

Expected: every command exits zero.

- [x] **Step 6: Commit the streaming/result slice**

```bash
git add scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git commit -m "Stream clustered V23 page evidence within fixed memory"
```

### Task 4: Coherent Revision-4/BVS2 authority adapter

**Files:**
- Modify: `scripts/v23_clustered_page_prototype_falsifier.py:44-291,296-397,745-1181`
- Modify: `scripts/test_v23_clustered_page_prototype_falsifier.py:273-545,547-757,1020-1105`

**Interfaces:**
- Consumes: the five exact local immutable files named by `Revision4Bvs2Paths`, `RegisteredAuthority`, and `ScientificShape`.
- Produces: `Revision4Bvs2Paths`, `Revision4Bvs2Authority`, `Revision4Bvs2Authority.load(paths: Revision4Bvs2Paths) -> Authority`, and the existing five-path `load_authority` signature as a thin wrapper returning `Authority`.
- Preserves: the existing `Authority`, page codec, clustering, streaming, pressure-stop, and result interfaces without a BVS2 branch.
- Private production methods: `_read_terminal(path: Path) -> dict[str, object]`, `_read_sorted_json(path: Path, expected_sha256: str, role: str) -> dict[str, object]`, `_validate_bundle(terminal: dict[str, object], result: dict[str, object], report: dict[str, object], roster: dict[str, object]) -> tuple[dict[str, object], tuple[PageRef, ...], tuple[int, ...]]`, `_validate_arm_evidence(arm: dict[str, object], pages: tuple[PageRef, ...]) -> tuple[tuple[tuple[tuple[int, ...], ...], ...], tuple[int, ...]]`, and `_read_query(path: Path, result: dict[str, object], query_ordinals: tuple[int, ...]) -> numpy.ndarray`.
- Test-only helpers: `exact_revision4_sha256s(paths: Revision4Bvs2Paths) -> dict[Path, str]`, `FirstPageForbiddenClient.get_object(*, Bucket: str, Key: str) -> object`, `session_for(client: object) -> object`, `revision4_cli_arguments(paths: Revision4Bvs2Paths) -> list[str]`, and `registered_ordinal_zero_location(paths: Revision4Bvs2Paths) -> tuple[str, str]`.

- [ ] **Step 1: Write the single exact-bundle no-page integration RED**

Add an environment-gated test with this exact public name:

```python
class HistoricalBundleIntegrationTests(unittest.TestCase):
    def test_revision4_bvs2_exact_bundle_reaches_first_registered_page_get(self):
        root_value = os.environ.get("BORSUK_REV4_BVS2_FIXTURE_DIR")
        if root_value is None:
            self.skipTest("BORSUK_REV4_BVS2_FIXTURE_DIR is not set")
        root = Path(root_value)
        paths = subject.Revision4Bvs2Paths(
            terminal=root / "TERMINAL.json",
            result=root / "RESULT_COMPLETE.json",
            report=root / "report.json",
            roster=root / "pages.json",
            query=root / "test.parquet",
        )
        for path, expected in exact_revision4_sha256s(paths).items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected)
        client = FirstPageForbiddenClient()
        with patch("boto3.Session", return_value=session_for(client)):
            with self.assertRaisesRegex(RuntimeError, "PAGE_S3_ACCESS_FORBIDDEN"):
                subject.main(revision4_cli_arguments(paths))
        self.assertEqual(client.calls, [registered_ordinal_zero_location(paths)])
```

The fixture helper maps exactly the registered terminal, result, report, roster, and query SHA-256 values. `FirstPageForbiddenClient.get_object` appends `(Bucket, Key)` before raising, so no page body can be read.

- [ ] **Step 2: Run the exact integration RED and preserve its boundary**

```bash
: "${BORSUK_REV4_BVS2_FIXTURE_DIR:?set exact five-file directory}" \
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_v23_clustered_page_prototype_falsifier.HistoricalBundleIntegrationTests.test_revision4_bvs2_exact_bundle_reaches_first_registered_page_get
```

Expected: FAIL before boto3 with `D2 selector authority differs`, proving the current dense-anchor assumption rejects the exact sparse historical selector. Any page request, pass, or different failure requires investigation before implementation.

- [ ] **Step 3: Write six focused synthetic mutation families and preserve RED**

Extend `AuthorityAndQualityTests` with one table-driven test per family. For a leaf-role semantic mutation, update only the minimum dependent hash bindings needed to pass earlier integrity gates and state that exact cascade in the subtest. Keep separate tests proving registered-SHA drift fails earlier. Do not create a negative test for a fully coherent report rewrite after re-rooting its registered SHA and complete dependent binding chain: without selector bytes, that rejection would be tautological or impossible.

1. Result receipt: exact key/concrete-type drift and report, roster, attempt, instance, source, index, D1, query, runtime-attestation, or reconstructed-summary binding drift.
2. Terminal: insertion-order bytes, exact keys/types, result/report/roster/source/index/attempt/instance/memory/pass binding drift.
3. Arm: historical sparse anchor bounds; selector byte formula `96 + coarse_cells * dimensions * 4 + (coarse_cells + 1) * 4 + anchor_count * (12 + dimensions * 2)`; routing `min(320, coarse_cells)`; rank cap 8192; target 384; assignments two; pages eight; primary/assignment sums; amplification; projected root/RAM/build.
4. Pages: primary or replica count above 65,535 and generation, ordinal, path, checksum, encoded length, family, metric, dimension, or report/roster equality drift.
5. Query evidence: the exact registered report SHA externally roots historical ranked IDs/distances, page choices, and CPU. Mutation-lock their exact schema, concrete types, finiteness, ordering, and cardinality. From authenticated primitives, recompute every derivable relation: selected/oracle membership, ground-truth hits, recall, encoded bytes, candidates, selector telemetry, aggregate/minimum/oracle/regret/p99/pass, and the frozen coverage-oracle tie break. Do not claim to regenerate selector outputs without selector bytes.
6. Parquet: extra physical column, nullability/list/dtype/dimension drift, row count other than 10,000, SHA/URI/materialization drift, NaN, or zero norm.

Run only the six new test methods. Expected: each derivable-invariant mutation currently either reaches the dense-anchor rejection or is accepted, preserving the missing adapter behavior as RED. A coherently re-rooted report rewrite is deliberately absent from the rejection matrix.

- [ ] **Step 4: Implement the minimal evidence-only adapter**

Add these exact frozen types:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class Revision4Bvs2Paths:
    terminal: Path
    result: Path
    report: Path
    roster: Path
    query: Path

@dataclasses.dataclass(frozen=True, slots=True)
class Revision4Bvs2Authority:
    registered: RegisteredAuthority
    shape: ScientificShape

    def load(self, paths: Revision4Bvs2Paths) -> Authority:
        terminal = self._read_terminal(paths.terminal)
        result = self._read_sorted_json(paths.result, self.registered.result_sha256, "result")
        report = self._read_sorted_json(paths.report, self.registered.report_sha256, "report")
        roster = self._read_sorted_json(paths.roster, self.registered.roster_sha256, "roster")
        arm, pages, query_ordinals = self._validate_bundle(terminal, result, report, roster)
        assignments, oracle_hits = self._validate_arm_evidence(arm, pages)
        queries = self._read_query(paths.query, result, query_ordinals)
        return Authority(self.registered, self.shape, pages, queries, query_ordinals, assignments, oracle_hits)
```

Keep terminal insertion-order compact JSON plus newline distinct from sorted canonical result/report/roster bytes. Move the current partial validators behind `_validate_bundle`; implement every frozen cross-object and within-report formula and relation available from authenticated primitives with concrete-type checks. Reconstruct the historical summary relationally before comparing it with the result receipt; do not claim regeneration of historical selector distances without selector bytes. Require the full physical Parquet schema and 10,000 rows before projecting `emb`. Replace dense selector equality with the historical bounded sparse relation. Do not add version dispatch, aliases, optional fields, or production imports.

- [ ] **Step 5: Run the six mutation families and exact no-page GREEN**

Run the six focused synthetic methods first. Only if they pass, run the exact Step 2 command. Expected: every registered-SHA or derivable-invariant mutation is rejected at its intended relation; no test asserts rejection of a coherently re-rooted report rewrite. The exact bundle reaches `PAGE_S3_ACCESS_FORBIDDEN`; the recorded request is exactly the ordinal-zero page; no S3 page body is read.

- [ ] **Step 6: Run complete focused and static verification**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_v23_clustered_page_prototype_falsifier
/home/rb/worktrees/sfora-ghc/.venv/bin/ruff check \
  scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
python3 -m py_compile \
  scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git diff --check
```

Expected: every command exits zero. Do not run the complete page stream, AWS compute, D3, or a broader repository suite in this checkpoint.

- [ ] **Step 7: Commit and push the adapter checkpoint**

```bash
git add scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git commit -m "Authenticate Revision-4 BVS2 falsifier evidence"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

Expected: the focused commit contains only the adapter and its tests, and the push is a fast-forward. Report the full 40-character commit SHA and retain the page-stream fence.

### Task 5: Review, assurance, and separately authorized evidence run

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md`
- Force-add: `docs/superpowers/specs/2026-08-30-v23-clustered-page-prototype-falsifier-design.md`
- Force-add: `docs/superpowers/plans/2026-08-30-v23-clustered-page-prototype-falsifier.md`

**Interfaces:**
- Consumes: the complete Task 3 module/test and immutable authority from the spec.
- Produces: reviewed source, green repository evidence, and—only after a separate execution grant—one terminal canonical falsifier receipt or one fail-closed stop receipt.

- [ ] **Step 1: Self-review spec coverage and placeholders**

```bash
rg -n 'T[B]D|T[O]DO|F[I]XME|implement l[a]ter|fill i[n]|similar to T[a]sk' \
  docs/superpowers/specs/2026-08-30-v23-clustered-page-prototype-falsifier-design.md \
  docs/superpowers/plans/2026-08-30-v23-clustered-page-prototype-falsifier.md
git diff --check
```

Expected: the placeholder scan prints nothing and diff check exits zero.

- [ ] **Step 2: Run the affected Python gate**

```bash
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_v23_clustered_page_prototype_falsifier \
  scripts.test_run_publication_v3_cell.V23DiagnosticWorkerTests
```

Expected: all tests pass.

- [ ] **Step 3: Run the repository's complete assurance once**

Coordinate the shared lane and run the repository-prescribed complete Rust/Python/static assurance exactly once. Preserve the original process result. If it fails, repair only the failing layer before one final complete rerun.

- [ ] **Step 4: Obtain a read-only cross-provider review**

Ask the other provider to inspect only the spec, plan, module, tests, and diff for authority gaps, accidental legacy support, memory-bound violations, outcome leakage, and invalid scientific inference. Independently reproduce every accepted finding with a focused RED before changing code.

If cross-provider OAuth or capacity is unavailable, record the review as blocked and continue only with locally reproducible verification. Never claim an independent review that did not reach a terminal result.

- [ ] **Step 5: Commit and push the reviewed implementation fast-forward**

```bash
git add docs/research/publication-v3-attempt-ledger.md \
  scripts/requirements-format-bench.txt \
  scripts/v23_clustered_page_prototype_falsifier.py \
  scripts/test_v23_clustered_page_prototype_falsifier.py
git add -f \
  docs/superpowers/specs/2026-08-30-v23-clustered-page-prototype-falsifier-design.md \
  docs/superpowers/plans/2026-08-30-v23-clustered-page-prototype-falsifier.md
git commit -m "Add bounded V23 clustered-page falsifier"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

Expected: the ancestry check and push succeed without force.

- [ ] **Step 6: Separately coordinate the complete immutable stream**

This step remains forbidden until Task 4's exact five-artifact no-page integration is GREEN and its adapter commit is pushed. Before executing, obtain a new explicit authorization and report the host/bucket region relationship, expected 28,282 GETs, approximately 3.7 GiB transfer path, clean shared lane, memory PSI, swap baseline, and exact command. Run one original process only with `--execute-complete-stream`; do not restart after failure or pressure stop. Preserve stdout canonical JSON, stderr SHA-256, PID/session terminal, peak pressure, and downloaded-byte count.

- [ ] **Step 7: Record the result without overclaiming**

If complete, independently validate the receipt and add its full SHA-256, metrics, timings, authority, and pass/fail conclusion to `docs/research/publication-v3-attempt-ledger.md`. A pass authorizes only the smaller-K ladder and a new BVS4 IVF design; a failure ends page-prototype work. In either case, do not launch D3 or paid infrastructure from this plan.
