"""Prepare the root-frozen CoHere FIRST1M fresh64 panel; construction only.

CLI (also supports python -m scripts.prepare_cohere_semantic_1m_panel):
  CONFIG CONFIGSHA REPO NEW_OUTPUT PREFIX
  --replay CONFIG CONFIGSHA REPO OUTPUT     (local exhaustive verification)
  --self-check                            (synthetic, no SDK/network/native)

FAIL-CLOSED CONFIG CONTRACT, schema borsuk-cohere-semantic-1m-preparation-v1:
Exactly these top-level fields, with no optional fields:
  schema, bucket, region="eu-central-1", rows=1000000, dimensions=768,
  k=100, queries=64, quality_peek_allowed=false,
  complete_historical_coverage=false, normalization=NORMALIZATION,
  versions={numpy:"2.3.3",pyarrow:"24.0.0"},
  limits={memory_bytes:2147483648,scratch_bytes:8589934592,cpu:2,threads:2,
          swap_bytes:0,max_requests:128},
  code_sha256={path:SHA256 for EXACTLY every path in CODE},
  refs={metadata_authority,panel,root_freeze,source_receipt,PLUS EXACTLY
        every name in metadata_authority.proofs},
  corpus=metadata_authority.corpus (entire original raw/SQ8/order/root binding),
  consumed_queries=metadata_authority.old_consumed_panel.artifacts[queries.raw],
  registered_test={key,bytes,sha256,rows}, taken from the original receipt's
                  sole role=query object (URI key, rows=1000),
  prior_unit_sha256={v271,v278,registered_test}, the historical f32-normalized
                    query identities from their authenticated proof bodies.
Every refs entry is exactly {path,bytes,sha256}; paths are repository-relative.
CONFIGSHA authenticates the entire config body. FIXED pins below authenticate
the four original metadata bodies independently of the caller. Every named
proof body and the exact transitive code roster is authenticated before use.
The root owns creation of CONFIG, controller, cgroup, and any paid launch.

Selection was frozen under CPython3.14.4. Python3.12 preparation consumes the
immutable64 locators (59 shards, population8997000); it NEVER samples again.
Any duplicate/mismatch stops the whole panel, with no replacement. Historical
coverage remains incomplete. Original FIRST1M raw is streamed from its original
key and authenticated, never re-encoded. SQ8 is bound as metadata only; no ANN
input is downloaded. Order/root bodies are authenticated separately. Original
raw rows are logical source ordinals, independent of physical SQ8 order.

Full authenticated CoHere parquet bodies are extracted one owned shard at a
time using iter_batches(1024), emb:fixed_size_list<float32>[768], and removed
even on failure. Audit compares raw/f64-normalized vectors against FIRST1M,
old consumed1000, train[1000000,1002000) from pinned shards45/46 (46 also
reconstructs/authenticates old1000), and original registered test1000. Historical
f32 normalization is checked ONLY for prior producer identity; duplicate audit
and GT use the existing f64 normalization/oracle. Within-index queries are
already covered by FIRST1M. All inputs authenticate before exhaustive GT100.

Linux cgroup admission requires memory.max<=2GiB, swap.max=0, cpu.max<=2
cores; numerical pools use <=2 threads. These are prospective PREPARATION
limits, not serving/build measurements. Admit disk headroom and scratch<=8GiB;
record actual scratch/RSS/stage durations and SDK/HTTP dispatch attempts from
debug records, received object bytes, retries/errors. Confirmed wire requests
and billed requests are UNMEASURED. AWS debug
logs (potential credentials) are temporary, never emitted or retained. S3 cp
in reused helpers is narrowed to one s3api get-object; retries disabled.

Outputs: queries.raw, requests.jsonl, truth.u32, truth.i64, panel.json,
duplicate-audit.json, oracle.json, resources.json, decision.json, and local
seal-readback.json. The first nine are conditionally PUT and HEAD/full GET
authenticated through the existing sealing helper. resources.json describes
pre-seal work; final-resources.json records sealing too. Authenticated raw,
order/root, old/prior/test queries remain locally for --replay; parquet/debug
scratch never remains. Failure removes owned heavy inputs/scratch and writes
failure.json/resources.json without a completion seal. Replay authenticates
all artifacts/inputs, reruns duplicate audit and exhaustive oracle, and checks
the pinned corpus/order/ledger; it opens no remote objects. No ANN is run.
"""

import copy
import ast
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time


SCHEMA = "borsuk-cohere-semantic-1m-preparation-v1"
NORMALIZATION = "original-le-f32-to-le-f64-divide-sqrt-sum-squares-axis1-zero-to-positive-v1"
LIMITS = dict(memory_bytes=2 << 30, scratch_bytes=8 << 30, cpu=2, threads=2,
              swap_bytes=0, max_requests=128)
VERSIONS = dict(numpy="2.3.3", pyarrow="24.0.0")
BASE = "docs/research/performance-architecture-20260930/semantic-1m/"
FIXED = {
    "metadata_authority": dict(path=BASE + "cohere-panel-tools/metadata-authority.json", bytes=176689,
        sha256="956a650eb34618d0d5fcab5d7776ea61e6875be64dd5245701aa521930ab703d"),
    "panel": dict(path=BASE + "cohere-panel-tools/panel.json", bytes=36744,
        sha256="979ad0f737ff045ab58b09dc9bc84105092c34238a5625558e2caa199e1b796e"),
    "root_freeze": dict(path=BASE + "cohere-panel-tools/root-freeze.json", bytes=868,
        sha256="c0af609473b8eee64ab0264c9458edffb7891557c830012aec7a7b37f39fd535"),
    "source_receipt": dict(path=BASE + "cohere-source-receipt.json", bytes=135298,
        sha256="0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87")}
CODE = tuple("scripts/" + name + ".py" for name in (
    "prepare_cohere_semantic_1m_panel", "prepare_semantic_1m_fresh_panel",
    "select_cohere_1m_fresh64", "build_cohere_1m_source", "v271_fresh_frontier",
    "audit_v36_ranked_physical_ids", "native_geometric_layout_screen",
    "native_rotated_two_bit_codes", "native_row_score_code_artifacts",
    "native_two_bit_cosine_development", "native_two_bit_topology",
    "run_native_cold", "run_native_source_frontier_1m", "run_native_union_cold",
    "run_native_union_http", "seal_v36_rank16_fresh_1m", "select_v36_rank16_fresh_ids",
    "v102_two_wave_pq48_refinement", "v284_page_primary_dev64",
    "v285_exact_page_rank_bound", "v291_two_stage_development", "v97_row_width_screen",
    "v98_hierarchical_row_router", "v99_ranked_gap_range_router"))
INPUT_FILES = ("source.raw", "source-order.u64", "source-root.json", "consumed-queries.raw",
               "prior-queries.raw", "test-queries.raw", "all-consumed.raw")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def value_sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def local_identity(path):
    with path.open("rb") as stream:
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    return dict(path=path.name, bytes=path.stat().st_size, sha256=sha)


def repo_path(repo, name):
    path = (repo / name).resolve()
    require(not Path(name).is_absolute() and path.is_relative_to(repo.resolve()),
            "pinned path escapes repository")
    return path


def dependencies(repo, code=None):
    """Authenticate the explicit closure before executing any repository imports."""
    global shared, selector, np, pa, pq, previous_unit
    require(__debug__, "optimized Python disables oracle checks")
    repo = repo.resolve()
    require(Path(__file__).resolve() == repo / CODE[0], "preparation helper origin differs")
    if code is not None:
        require(set(code) == set(CODE), "transitive code roster differs")
        for name in CODE:
            require(local_identity(repo_path(repo, name))["sha256"] == code[name],
                    f"pinned code identity differs: {name}")
    # Direct-file and -m invocation share the same explicit, checked import origins.
    for path in (repo, repo / "scripts"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
                 "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ[name] = "1" if sys.argv[1:] == ["--self-check"] else "2"
    from scripts import prepare_semantic_1m_fresh_panel as shared
    from scripts import select_cohere_1m_fresh64 as selector
    from scripts.v271_fresh_frontier import unit as previous_unit
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    require((np.__version__, pa.__version__) == ("2.3.3", "24.0.0"), "dependency versions differ")
    origins = {str(Path(__file__).resolve().relative_to(repo))}
    for name, module in list(sys.modules.items()):
        if name.startswith("scripts.") or name == "build_cohere_1m_source":
            path = Path(module.__file__).resolve()
            require(path.is_relative_to(repo / "scripts"), "imported code origin differs")
            origins.add(str(path.relative_to(repo)))
    require(origins == set(CODE), "executed transitive code roster differs")
    pa.set_cpu_count(1 if sys.argv[1:] == ["--self-check"] else 2)
    pa.set_io_thread_count(1)


def read_config(path, sha, repo):
    require(local_identity(path)["sha256"] == sha, "configuration body identity differs")
    config = json.loads(path.read_text())
    expected = dict(schema=SCHEMA, bucket="borsuk-bench-453182569524-euc1", region="eu-central-1",
                    rows=1_000_000, dimensions=768, k=100, queries=64,
                    quality_peek_allowed=False, complete_historical_coverage=False,
                    normalization=NORMALIZATION, versions=VERSIONS, limits=LIMITS)
    fields = set(expected) | {"code_sha256", "refs", "corpus", "consumed_queries",
                              "registered_test", "prior_unit_sha256"}
    require(set(config) == fields and all(canonical(config.get(k)) == canonical(v)
                                         for k, v in expected.items()), "configuration contract differs")
    dependencies(repo, config["code_sha256"])
    return config


def validate_fixed(authority, panel, freeze):
    require(authority["schema"] == "borsuk-cohere-first1m-fresh64-metadata-authority-v1"
            and authority["root_pending"] is False and authority["authority_pending"] is False
            and panel["schema"] == "borsuk-cohere-first1m-fresh64-locators-v1"
            and freeze["schema"] == "borsuk-cohere-fresh64-root-freeze-v1",
            "unfrozen metadata authority")
    for body in (authority, panel):
        require(body["metadata_only"] is True and body["vector_or_truth_bodies_opened"] is False
                and body["complete_historical_coverage"] is False and body["qualification"] is False
                and body["root_pending"] is False, "metadata-only scope differs")
    selected = panel["selected"]
    sha = "379b6421d794fb2d0956e7ed787478b019abfd599e0510be157cdcaa53205abf"
    require(freeze["authority_sha256"] == panel["authority_sha256"] == FIXED["metadata_authority"]["sha256"]
            and freeze["panel_sha256"] == FIXED["panel"]["sha256"]
            and freeze["selected_locators_sha256"] == panel["selected_sha256"] == value_sha(selected) == sha
            and freeze["selection_count"] == len(selected) == 64
            and freeze["selected_shards"] == len({r["shard_ordinal"] for r in selected}) == 59
            and freeze["independent_rank_mapping_equal"] is True and freeze["root_pending"] is False
            and freeze["vector_or_truth_bodies_opened"] is False
            and freeze["native_quality_measured"] is False
            and freeze["complete_historical_coverage"] is False, "fixed locator/root freeze binding differs")
    spec = authority["selection"]
    require(canonical(panel["selection"]) == canonical(spec)
            and spec["python"] == dict(implementation="CPython", version="3.14.4")
            and spec["population_size"] == 8_997_000
            and spec["eligible_intervals"] == [[1_002_000, 1_005_000], [1_006_000, 10_000_000]]
            and panel["consumed_query_ledger_sha256"] == value_sha(authority["consumed_query_ledger"])
            and canonical(panel["corpus"]) == canonical(authority["corpus"])
            and panel["source_receipt_sha256"] == FIXED["source_receipt"]["sha256"],
            "fixed population/corpus/ledger differs")
    require(len({r["source_ordinal"] for r in selected}) == 64, "duplicate source ordinal; no replacement")
    # No validate_population(), sampled_ranks(), select_panel(), or host-version check.
    for ordinal, row in enumerate(selected):
        rank = row["eligible_rank"]
        source = selector.rank_to_ordinal(rank, spec["eligible_intervals"])
        index, local = selector.locate(source, authority["ordered_train_shards"])
        shard = authority["ordered_train_shards"][index]
        expected = dict(query_ordinal=ordinal, eligible_rank=rank, source_ordinal=source,
                        shard_ordinal=index, shard_key=shard["key"], shard_sha256=shard["sha256"],
                        shard_bytes=shard["bytes"], local_row=local, embedding_column="emb")
        require(canonical(row) == canonical(expected) and not any(
            e["split"] == "train" and e["interval"][0] <= source < e["interval"][1]
            for e in authority["consumed_query_ledger"]), "consumed/changed locator; no replacement")


def historical_pins(proofs):
    test = {p["query_artifact_identity"]["sha256"]
            for p in proofs["registered_test"]["descendant_terminal_proofs"]}
    require(len(test) == 1, "registered test producer identities disagree")
    return dict(v271=proofs["v271"]["query_identity"]["sha256"],
                v278=proofs["v278"]["query_sha256"], registered_test=test.pop())


def load_inputs(config, repo):
    refs = config["refs"]
    require(all(refs.get(name) == ident for name, ident in FIXED.items()), "original metadata pins differ")
    data = {}
    for name in FIXED:
        path = repo_path(repo, refs[name]["path"])
        shared.authenticate(path, refs[name]); data[name] = json.loads(path.read_text())
    a = data["metadata_authority"]
    require(set(refs) == set(FIXED) | set(a["proofs"])
            and all(refs[name] == pin for name, pin in a["proofs"].items()),
            "exact named source/consumption proof roster differs")
    for name, pin in a["proofs"].items():
        path = repo_path(repo, pin["path"])
        shared.authenticate(path, pin); data[name] = json.loads(path.read_text())
    require(a["source_receipt"] == dict(FIXED["source_receipt"],
            key=selector.PUBLICATION + "/STAGING_COMPLETE.json"), "original receipt binding differs")
    roster = selector.receipt_roster(data["source_receipt"])
    require(canonical(roster) == canonical(a["ordered_train_shards"]), "original train roster differs")
    selector.authenticate_bindings(a, data)
    validate_fixed(a, data["panel"], data["root_freeze"])
    tests = [o for o in data["source_receipt"]["objects"] if o["role"] == "query"]
    require(len(tests) == 1 and tests[0]["format"] == "parquet" and tests[0]["rows"] == 1000
            and tests[0]["uri"].startswith("s3://" + config["bucket"] + "/"), "registered test source differs")
    t = tests[0]
    test = dict(key=t["uri"].split("/", 3)[3], bytes=t["bytes"], sha256=t["sha256"], rows=t["rows"])
    require(canonical(config["corpus"]) == canonical(a["corpus"])
            and config["consumed_queries"] == a["old_consumed_panel"]["artifacts"]["queries.raw"]
            and config["consumed_queries"]["bytes"] == 3_072_000
            and config["registered_test"] == test
            and test["sha256"] == data["registered_test"]["test_sha256"]
            and config["prior_unit_sha256"] == historical_pins(data), "current corpus/consumed input pins differ")
    return data


def extract(path, rows, local_rows):
    """Decode only bounded batches, preserving the supplied locator order."""
    require(len(set(local_rows)) == len(local_rows)
            and all(type(r) is int and 0 <= r < rows for r in local_rows), "invalid/duplicate shard locator")
    result = np.empty((len(local_rows), 768), dtype="<f4")
    order = np.argsort(local_rows); sorted_rows = np.asarray(local_rows)[order]
    count = filled = 0
    with pq.ParquetFile(path) as source:
        field = source.schema_arrow.field("emb")
        require(source.metadata.num_rows == rows and pa.types.is_fixed_size_list(field.type)
                and field.type.list_size == 768 and field.type.value_type == pa.float32(),
                "CoHere emb schema/row geometry differs")
        for batch in source.iter_batches(batch_size=1024, columns=["emb"], use_threads=False):
            col = batch.column(0)
            require(not col.null_count and not col.values.null_count, "null CoHere embedding")
            values = np.asarray(col.values.slice(col.offset * 768, len(col) * 768)
                                .to_numpy(zero_copy_only=False), dtype="<f4").reshape(len(col), 768)
            shared.vector_hashes(values)  # finite/nonzero f32, including unselected batch rows
            left, right = np.searchsorted(sorted_rows, [count, count + len(col)])
            result[order[left:right]] = values[sorted_rows[left:right] - count]
            filled += right - left; count += len(col)
        require(count == rows and filled == len(local_rows), "CoHere locator extraction incomplete")
    shared.vector_hashes(result)
    return result


def cgroup_limits(group, limits=None):
    limits = LIMITS if limits is None else limits
    memory = (group / "memory.max").read_text().strip()
    quota, period = (group / "cpu.max").read_text().split()
    require(memory != "max" and 0 < int(memory) <= limits["memory_bytes"]
            and (group / "memory.swap.max").read_text().strip() == str(limits["swap_bytes"])
            and quota != "max" and 0 < int(quota) <= limits["cpu"] * int(period),
            "preparation cgroup memory/CPU/swap limits differ")


def scratch_bytes(out):
    total = 0
    for path in out.rglob("*"):
        require(not path.is_symlink(), "owned scratch contains a symlink")
        if path.is_file():
            total += path.stat().st_size
    return total


class Accounting:
    """One AWS command at a time; reuse seal helpers through a narrow proxy."""
    DEVNULL = subprocess.DEVNULL

    def __init__(self, config, out, group, *, limits=None):
        self.config, self.out, self.group = config, out, group
        self.limits = dict(LIMITS if limits is None else limits)
        self.started = time.monotonic(); self.stages = {}; self.peak_scratch = 0
        self.requests = dict(GET=0, HEAD=0, PUT=0)
        self.received_bytes = self.response_lengths = self.errors = 0
        self.sizes = {}
        self.admission = None
        self.accounting_complete = True
        self.native_run = subprocess.run

    def checkpoint(self, reserve=0):
        current = scratch_bytes(self.out); self.peak_scratch = max(self.peak_scratch, current)
        require(current + reserve <= self.limits["scratch_bytes"], "preparation scratch limit exceeded")
        require(shutil.disk_usage(self.out).free >= reserve, "insufficient preparation scratch space")
        require(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 <= self.limits["memory_bytes"],
                "preparation RSS limit exceeded")
        require(next(line.split()[1] for line in Path("/proc/self/status").read_text().splitlines()
                     if line.startswith("VmSwap:")) == "0", "preparation process has swapped")
        if self.group is not None:
            cgroup_limits(self.group, self.limits)

    @contextmanager
    def stage(self, name):
        started = time.monotonic()
        try:
            self.checkpoint(); yield; self.checkpoint()
        finally:
            self.stages[name] = time.monotonic() - started

    def run(self, args, **kwargs):
        args = list(map(str, args)); target = None
        require(args[0] == "aws", "unexpected helper subprocess")
        if args[1:3] == ["s3", "cp"]:
            bucket_key = args[3].removeprefix("s3://").split("/", 1)
            require(bucket_key[0] == self.config["bucket"], "download bucket differs")
            target = Path(args[4]); key = bucket_key[1]
            args = ["aws", "s3api", "get-object", "--bucket", bucket_key[0], "--key", key, str(target)]
            require(key in self.sizes, "unregistered object download")
            self.checkpoint(self.sizes[key] + (4 << 20))
        require(sum(self.requests.values()) < self.limits["max_requests"], "preparation request limit exceeded")
        env = dict(os.environ, AWS_MAX_ATTEMPTS="1", AWS_RETRY_MODE="standard",
                   AWS_REGION=self.config["region"], AWS_DEFAULT_REGION=self.config["region"])
        log = self.out / "aws-debug.tmp"
        require(not log.exists(), "overlapping AWS operation")
        result = None
        self.accounting_complete = False
        try:
            with log.open("xb") as stream:
                result = self.native_run(args + ["--debug", "--region", self.config["region"]],
                                         stdout=subprocess.PIPE, stderr=stream, env=env)
            require(log.stat().st_size <= 4 << 20, "AWS trace exceeds bounded log limit")
            counts = dict(GET=0, HEAD=0, PUT=0)
            with log.open() as stream:
                for line in stream:
                    if "Sending http request:" in line:
                        match = re.search(r"method=(GET|HEAD|PUT)[, >]", line)
                        require(match is not None, "unrecognized AWS transport method")
                        counts[match[1]] += 1
                    if "Response headers:" in line and counts["GET"]:
                        headers = ast.literal_eval(line.split("Response headers:", 1)[1].strip())
                        self.response_lengths += int(next((v for k, v in headers.items()
                                                          if k.lower() == "content-length"), 0))
            for method, count in counts.items():
                self.requests[method] += count
            require(sum(counts.values()) > 0, "AWS operation has no auditable transport record")
            self.accounting_complete = True
            require(sum(self.requests.values()) <= self.limits["max_requests"], "HTTP dispatch attempt cap exceeded")
            if result.returncode:
                self.errors += 1
                raise ValueError(f"AWS {args[2]} failed (exit {result.returncode}); debug log discarded")
            self.checkpoint()
            return result
        finally:
            if target is not None and target.exists():
                self.received_bytes += target.stat().st_size
                if result is None or result.returncode:
                    target.unlink()
            log.unlink(missing_ok=True)

    def check_output(self, args, **kwargs):
        return self.run(args, **kwargs).stdout

    def report(self, passed, cleanup=None):
        self.peak_scratch = max(self.peak_scratch, scratch_bytes(self.out))
        return dict(schema="borsuk-cohere-preparation-resources-v1", passed=passed,
                    prospective_preparation_limits=self.limits.copy(), serving_or_build_measurement=False,
                    wall_seconds=time.monotonic() - self.started, stage_seconds=self.stages,
                    process_max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    child_max_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                    actual_scratch_bytes=scratch_bytes(self.out), peak_scratch_bytes=self.peak_scratch,
                    http_request_dispatch_attempts=self.requests.copy(),
                    confirmed_wire_requests="UNMEASURED", billed_requests="UNMEASURED",
                    received_object_body_bytes=self.received_bytes,
                    transport_accounting_complete=self.accounting_complete, admission=self.admission,
                    get_response_content_length_bytes=self.response_lengths, aws_errors=self.errors,
                    aws_max_attempts=1, cgroup=None if self.group is None else {
                        name: (self.group / name).read_text().strip() for name in
                        ("memory.max", "memory.peak", "memory.swap.max", "memory.swap.peak", "cpu.max", "cpu.stat")
                        if (self.group / name).exists()}, failure_cleanup=cleanup,
                    python=platform.python_version(), ann_quality_measured=False)


@contextmanager
def accounted_helpers(accounting):
    original = shared.subprocess, shared.seal.subprocess
    shared.subprocess = shared.seal.subprocess = accounting
    try:
        yield
    finally:
        shared.subprocess, shared.seal.subprocess = original


def download(config, ident, path, accounting):
    accounting.sizes[ident["key"]] = ident["bytes"]
    require(not path.exists() and not path.is_symlink(), "owned download already exists")
    try:
        shared.seal.checked_s3(config["bucket"], ident, path)
        shared.authenticate(path, ident)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def acquire_vectors(config, data, out, accounting):
    a, panel = data["metadata_authority"], data["panel"]
    roster = a["ordered_train_shards"]
    targets = dict(selected=np.empty((64, 768), dtype="<f4"), prior=np.empty((2000, 768), dtype="<f4"),
                   old=np.empty((1000, 768), dtype="<f4"))
    jobs = {}
    for row in panel["selected"]:
        jobs.setdefault(row["shard_ordinal"], []).append(("selected", row["query_ordinal"], row["local_row"]))
    for scope, low, high in (("prior", 1_000_000, 1_002_000), ("old", 1_005_000, 1_006_000)):
        for ordinal in range(low, high):
            index, local = selector.locate(ordinal, roster)
            jobs.setdefault(index, []).append((scope, ordinal - low, local))
    sources = []
    scratch = out / "shard-scratch"; scratch.mkdir()
    try:
        for index, requests in sorted(jobs.items()):
            shard = roster[index]; path = scratch / "owned.parquet"
            try:
                download(config, shard, path, accounting)
                decoded = extract(path, shard["rows"], [row[2] for row in requests])
                for vector, (scope, ordinal, _) in zip(decoded, requests):
                    targets[scope][ordinal] = vector
                sources.append(dict(shard, extracted_rows=len(requests), authenticated_full_body=True))
            finally:
                path.unlink(missing_ok=True)
        test = config["registered_test"]; path = scratch / "owned.parquet"
        try:
            download(config, test, path, accounting)
            targets["test"] = extract(path, 1000, list(range(1000)))
            sources.append(dict(test, authenticated_full_body=True))
        finally:
            path.unlink(missing_ok=True)
    finally:
        scratch.rmdir()
    require(hashlib.sha256(targets["old"].tobytes()).hexdigest() == config["consumed_queries"]["sha256"],
            "shard46 extraction differs from sealed old1000")
    verify_prior(targets["prior"], targets["test"], config)
    for name, scope in (("prior-queries.raw", "prior"), ("test-queries.raw", "test")):
        with (out / name).open("xb") as stream:
            stream.write(targets[scope].tobytes())
    return targets["selected"], sources


def verify_prior(prior, test, config):
    for name, values in (("v271", prior[:1000]), ("v278", prior[1000:]), ("registered_test", test)):
        shared.vector_hashes(values)
        require(hashlib.sha256(previous_unit(values).tobytes()).hexdigest() == config["prior_unit_sha256"][name],
                f"historical producer query identity differs: {name}")


def audit(queries, out, rows=1_000_000, counts=(1000, 2000, 1000), accounting=None):
    combined = out / "all-consumed.raw"
    try:
        with combined.open("xb") as stream:
            for name in ("consumed-queries.raw", "prior-queries.raw", "test-queries.raw"):
                with (out / name).open("rb") as source:
                    shutil.copyfileobj(source, stream, length=1 << 20)
        if accounting is not None:
            accounting.checkpoint()
        try:
            report = shared.audit_duplicates(queries, out / "source.raw", combined,
                                             rows=rows, consumed_rows=sum(counts))
        except ValueError as error:
            raise ValueError(str(error).replace("consumed rank16", "known consumed CoHere vectors")) from error
    finally:
        combined.unlink(missing_ok=True)
    report.update(schema="borsuk-cohere-semantic-1m-vector-audit-v1",
                  consumed_scopes=dict(old_sealed1000=counts[0], source_holdout_prior=counts[1],
                                       registered_test=counts[2]), within_index_prior_covered_by_first1m=True)
    return report


def verify_order(out, config):
    shared.authenticate(out / "source-order.u64", config["corpus"]["order"])
    order = np.fromfile(out / "source-order.u64", dtype="<u8")
    require(np.array_equal(np.sort(order), np.arange(config["rows"], dtype="<u8")), "order is not FIRST1M permutation")
    shared.authenticate(out / "source-root.json", config["corpus"]["root_manifest"])


def input_identities(out):
    return {name: shared.identity(out / name) for name in INPUT_FILES if name != "all-consumed.raw"}


def verify_local(config, sha, out):
    decision = json.loads((out / "decision.json").read_text())
    require(decision["schema"] == "borsuk-cohere-semantic-1m-construction-v1"
            and decision["config_sha256"] == sha and decision["refs"] == config["refs"]
            and decision["code_sha256"] == config["code_sha256"]
            and decision["corpus"] == config["corpus"] and decision["consumed_queries"] == config["consumed_queries"]
            and decision["registered_test"] == config["registered_test"]
            and decision["complete_historical_coverage"] is False
            and decision["ann_quality_measured"] is False and decision["qualification"] is False
            and decision["truth_id_space"] == "source ordinal"
            and set(decision["artifacts"]) == set(shared.OUTPUTS), "construction decision binding differs")
    require(set(decision["inputs"]) == set(INPUT_FILES) - {"all-consumed.raw"}, "local input roster differs")
    for name, ident in dict(decision["artifacts"], **decision["inputs"]).items():
        require(ident["path"] == name, "artifact path differs")
        shared.authenticate(out / name, ident)
    shared.authenticate(out / "source.raw", config["corpus"]["raw"])
    shared.authenticate(out / "consumed-queries.raw", config["consumed_queries"])
    shared.authenticate(out / "panel.json", config["refs"]["panel"])
    verify_order(out, config); shared.check_outputs(out)
    readback = json.loads((out / "seal-readback.json").read_text())
    expected = dict(decision["artifacts"], **{"decision.json": shared.identity(out / "decision.json")})
    require(readback["schema"] == "borsuk-semantic-1m-seal-readback-v1"
            and set(readback["artifacts"]) == set(expected), "seal completion roster differs")
    require(readback["final_resources"]["path"] == "final-resources.json", "final resource pointer differs")
    shared.authenticate(out / "final-resources.json", readback["final_resources"])
    for name, ident in expected.items():
        sealed = readback["artifacts"][name]
        require(sealed["authenticated_readback"] is True
                and sealed["key"] == f"{decision['prefix']}/sealed/{name}"
                and (sealed["bytes"], sealed["sha256"]) == (ident["bytes"], ident["sha256"]),
                "full-body seal readback identity differs")
    return decision


def replay(config_path, sha, repo, out):
    config = read_config(config_path, sha, repo); load_inputs(config, repo)
    verify_local(config, sha, out)
    prior = np.fromfile(out / "prior-queries.raw", dtype="<f4").reshape(2000, 768)
    test = np.fromfile(out / "test-queries.raw", dtype="<f4").reshape(1000, 768)
    verify_prior(prior, test, config)
    queries = np.fromfile(out / "queries.raw", dtype="<f4").reshape(64, 768)
    recorded = json.loads((out / "duplicate-audit.json").read_text())
    require(all(recorded.get(k) == v for k, v in audit(queries, out).items()), "replayed duplicate audit differs")
    shared.oracle_self_check()
    require(shared.oracle(out / "source.raw", queries, 1_000_000).tobytes() == (out / "truth.u32").read_bytes(),
            "replayed exhaustive oracle differs")
    read_config(config_path, sha, repo); load_inputs(config, repo)
    verify_local(config, sha, out)
    return dict(passed=True, remote_objects_reopened=False, ann_quality_measured=False)


def cleanup_failure(out):
    removed = []
    for name in INPUT_FILES:
        path = out / name
        if path.exists():
            path.unlink(); removed.append(name)
    for name in ("shard-scratch", "aws-debug.tmp", "seal-readback.json"):
        path = out / name
        if path.is_dir():
            shutil.rmtree(path); removed.append(name)
        elif path.exists():
            path.unlink(); removed.append(name)
    return dict(removed=removed, owned_heavy_inputs_remaining=False,
                remote_conditional_objects_may_remain=True, replacement_allowed=False)


def prepare(config_path, sha, repo, out, prefix):
    started = time.monotonic()
    config = read_config(config_path, sha, repo); data = load_inputs(config, repo)
    require(prefix and prefix == prefix.strip("/") and ".." not in prefix.split("/")
            and not any(c.isspace() for c in prefix), "invalid new output prefix")
    group = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("0::")[-1].lstrip("/")
    cgroup_limits(group)
    out.mkdir()
    accounting = Accounting(config, out, group)
    accounting.started = started
    accounting.stages["configuration_and_authority_authentication"] = time.monotonic() - started
    try:
        largest = max(s["bytes"] for s in data["metadata_authority"]["ordered_train_shards"])
        admit = 3_072_000_000 + 3_072_000 + 6_144_000 + 3_072_000 + 8_000_000 + largest + (64 << 20)
        shards = {r["shard_ordinal"] for r in data["panel"]["selected"]} | {45, 46}
        gets = 4 + len(shards) + 1
        baseline_requests = gets + 3 * (len(shared.OUTPUTS) + 1)
        input_bytes = sum(data["metadata_authority"]["ordered_train_shards"][i]["bytes"] for i in shards)
        input_bytes += sum(config["corpus"][name]["bytes"] for name in ("raw", "order", "root_manifest"))
        input_bytes += config["consumed_queries"]["bytes"] + config["registered_test"]["bytes"]
        accounting.admission = dict(scratch_bytes=admit, input_gets=gets, input_body_bytes=input_bytes,
                                    baseline_http_dispatch_attempts_including_seal=baseline_requests)
        require(admit <= LIMITS["scratch_bytes"], "declared working scratch exceeds preparation cap")
        require(baseline_requests <= LIMITS["max_requests"], "declared requests exceed preparation cap")
        accounting.checkpoint(admit)
        with accounted_helpers(accounting):
            with accounting.stage("authenticated_corpus_and_old_queries"):
                for ident, name in ((config["corpus"]["raw"], "source.raw"),
                                    (config["corpus"]["order"], "source-order.u64"),
                                    (config["corpus"]["root_manifest"], "source-root.json"),
                                    (config["consumed_queries"], "consumed-queries.raw")):
                    download(config, ident, out / name, accounting)
                verify_order(out, config)
            with accounting.stage("fixed_locator_and_consumed_extraction"):
                queries, sources = acquire_vectors(config, data, out, accounting)
            with accounting.stage("raw_and_f64_duplicate_audit"):
                report = audit(queries, out, accounting=accounting)
                report.update(config_sha256=sha, selected_locators_sha256=data["panel"]["selected_sha256"],
                              authenticated_query_sources=sources, inputs=input_identities(out),
                              consumed_query_ledger=data["metadata_authority"]["consumed_query_ledger"])
                shared.write_json(out / "duplicate-audit.json", report)
            # Recheck ALL input/code identities at the boundary before GT can open.
            read_config(config_path, sha, repo); load_inputs(config, repo)
            for name, ident in report["inputs"].items():
                shared.authenticate(out / name, ident)
            shared.oracle_self_check()
            with accounting.stage("exhaustive_f64_gt100"):
                truth = shared.oracle(out / "source.raw", queries, 1_000_000)
                shared.write_requests_truth(out, queries, truth)
                with (out / "panel.json").open("xb") as stream:
                    stream.write(repo_path(repo, config["refs"]["panel"]["path"]).read_bytes())
                shared.write_json(out / "oracle.json", dict(schema="borsuk-cohere-semantic-1m-oracle-v1",
                    passed=True, oracle_self_check=True, rows=1_000_000, queries=64, k=100,
                    distance="1-dot of f64-normalized original f32", tie="signed source ordinal ascending",
                    exhaustive_block_sort_top100_merge=True, truth_id_space="source ordinal",
                    source_raw=config["corpus"]["raw"], truth_u32=shared.identity(out / "truth.u32"),
                    truth_i64=shared.identity(out / "truth.i64"), widening="LEu32 to LEi64, unchanged values"))
            shared.write_json(out / "resources.json", accounting.report(True))
            decision = dict(schema="borsuk-cohere-semantic-1m-construction-v1", config_sha256=sha,
                decision="PASS fixed fresh64 construction only", qualification=False,
                complete_historical_coverage=False, ann_quality_measured=False,
                refs=config["refs"], code_sha256=config["code_sha256"], corpus=config["corpus"],
                consumed_queries=config["consumed_queries"], registered_test=config["registered_test"],
                selected_locators_sha256=data["panel"]["selected_sha256"], prefix=prefix,
                queries=64, gt_k=100, truth_id_space="source ordinal", inputs=input_identities(out),
                artifacts={name: shared.identity(out / name) for name in shared.OUTPUTS})
            shared.write_json(out / "decision.json", decision)
            expected = dict(decision["artifacts"], **{"decision.json": shared.identity(out / "decision.json")})
            accounting.sizes.update({f"{prefix}/sealed/{name}": ident["bytes"] for name, ident in expected.items()})
            with accounting.stage("conditional_seal_and_full_readback"):
                readback = shared.seal_readback(config["bucket"], prefix, out, expected)
            read_config(config_path, sha, repo); load_inputs(config, repo)
            shared.write_json(out / "final-resources.json", accounting.report(True))
            readback["final_resources"] = shared.identity(out / "final-resources.json")
            shared.write_json(out / "seal-readback.json", readback)
            with accounting.stage("final_local_verification"):
                verify_local(config, sha, out)
            # These two local operational receipts include the completed verification stage.
            (out / "final-resources.json").write_bytes(canonical(accounting.report(True)) + b"\n")
            readback["final_resources"] = shared.identity(out / "final-resources.json")
            (out / "seal-readback.json").write_bytes(canonical(readback) + b"\n")
            return decision
    except BaseException as error:
        cleanup = cleanup_failure(out)
        (out / "resources.json").unlink(missing_ok=True)
        shared.write_json(out / "resources.json", accounting.report(False, cleanup))
        shared.write_json(out / "failure.json", dict(error_type=type(error).__name__,
                          config_sha256=sha, replacement_allowed=False, cleanup=cleanup))
        raise


def self_check():
    started = time.monotonic()
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    root = Path(__file__).resolve().parent.parent
    dependencies(root)
    from unittest.mock import patch
    directory = root / "docs/research/performance-architecture-20260930/semantic-1m/cohere-panel-tools"
    authority, panel, freeze = [json.loads((directory / name).read_text()) for name in
                                ("metadata-authority.json", "panel.json", "root-freeze.json")]
    validate_fixed(authority, panel, freeze)
    def rejects(action):
        try:
            action()
        except (ValueError, AssertionError, KeyError):
            return
        raise AssertionError("invalid immutable input accepted")
    changed = copy.deepcopy(panel); changed["selected"][0]["local_row"] += 1
    rejects(lambda: validate_fixed(authority, changed, freeze))
    changed = copy.deepcopy(panel); changed["selected"][1] = changed["selected"][0].copy()
    rejects(lambda: validate_fixed(authority, changed, freeze))
    changed = copy.deepcopy(authority); changed["root_pending"] = True
    rejects(lambda: validate_fixed(changed, panel, freeze))
    changed = copy.deepcopy(authority); changed["corpus"]["order"]["sha256"] = "0" * 64
    rejects(lambda: validate_fixed(changed, panel, freeze))
    with patch.object(selector, "sampled_ranks", side_effect=AssertionError("remote resampling")), \
            patch.object(platform, "python_version", return_value="3.12.10"):
        validate_fixed(authority, panel, freeze)

    def vectors(count, offset):
        values = np.zeros((count, 768), dtype="<f4")
        values[:, 0] = 1; values[:, 1] = np.arange(offset, offset + count)
        return values

    def arrow_vectors(values):
        return pa.FixedSizeListArray.from_arrays(pa.array(values.reshape(-1), type=pa.float32()), 768)

    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        proofs = {name: json.loads((root / pin["path"]).read_text()) for name, pin in authority["proofs"].items()}
        receipt = json.loads((root / FIXED["source_receipt"]["path"]).read_text())
        test = next(o for o in receipt["objects"] if o["role"] == "query")
        config = dict(schema=SCHEMA, bucket=authority["bucket"], region="eu-central-1", rows=1_000_000,
                      dimensions=768, k=100, queries=64, quality_peek_allowed=False,
                      complete_historical_coverage=False, normalization=NORMALIZATION, versions=VERSIONS,
                      limits=LIMITS, code_sha256={name: local_identity(root / name)["sha256"] for name in CODE},
                      refs=dict(FIXED, **authority["proofs"]), corpus=authority["corpus"],
                      consumed_queries=authority["old_consumed_panel"]["artifacts"]["queries.raw"],
                      registered_test=dict(key=test["uri"].split("/", 3)[3], bytes=test["bytes"],
                                           sha256=test["sha256"], rows=1000), prior_unit_sha256=historical_pins(proofs))
        config_path = work / "config.json"; config_path.write_bytes(canonical(config))
        sha = local_identity(config_path)["sha256"]
        assert read_config(config_path, sha, root) == config
        data = load_inputs(config, root)
        assert len(data["panel"]["selected"]) == 64
        rejects(lambda: read_config(config_path, "0" * 64, root))
        for field in ("consumed_queries", "corpus", "refs"):
            changed = copy.deepcopy(config)
            if field == "consumed_queries": changed[field]["sha256"] = "0" * 64
            elif field == "corpus": changed[field]["sq8"]["sha256"] = "0" * 64
            else: changed[field].pop("v278")
            rejects(lambda: load_inputs(changed, root))
        changed = copy.deepcopy(config); changed["code_sha256"].pop("scripts/run_native_source_frontier_1m.py")
        config_path.write_bytes(canonical(changed))
        rejects(lambda: read_config(config_path, local_identity(config_path)["sha256"], root))
        changed = copy.deepcopy(config); changed["limits"]["swap_bytes"] = 1
        config_path.write_bytes(canonical(changed))
        rejects(lambda: read_config(config_path, local_identity(config_path)["sha256"], root))
        config_path.write_bytes(canonical(config))

        # Wrong emb schema, locator order, null/zero/nonfinite rows, and a partial last batch.
        values = vectors(1025, 100)
        parquet = work / "synthetic.parquet"
        pq.write_table(pa.table({"emb": arrow_vectors(values)}),
                       parquet, row_group_size=1024)
        assert extract(parquet, 1025, [1024, 0, 1]).tobytes() == values[[1024, 0, 1]].tobytes()
        rejects(lambda: extract(parquet, 1024, [0]))
        rejects(lambda: extract(parquet, 1025, [1, 1]))
        rejects(lambda: extract(parquet, 1025, [-1]))
        for i, table in enumerate((pa.table({"embedding": pa.array([[1.] * 768], type=pa.list_(pa.float32(), 768))}),
                                    pa.table({"emb": pa.array([[1.] * 767], type=pa.list_(pa.float32(), 767))}),
                                    pa.table({"emb": pa.array([[1.] * 768], type=pa.list_(pa.float64(), 768))}),
                                    pa.table({"emb": pa.array([None], type=pa.list_(pa.float32(), 768))}))):
            bad = work / f"bad-schema-{i}.parquet"; pq.write_table(table, bad)
            rejects(lambda: extract(bad, 1, [0]))
        for i, invalid in enumerate((np.zeros((1, 768), dtype="<f4"),
                                     np.full((1, 768), np.nan, dtype="<f4"),
                                     np.full((1, 768), np.inf, dtype="<f4"))):
            bad = work / f"bad-vector-{i}.parquet"
            pq.write_table(pa.table({"emb": arrow_vectors(invalid)}), bad)
            rejects(lambda: extract(bad, 1, [0]))

        # Real acquisition/extraction with only the external object download mocked.
        fixture = work / "acquisition"; fixture.mkdir()
        prior, old, registered = vectors(2000, 2000), vectors(1000, 5000), vectors(1000, 7000)
        selected = vectors(64, 9000)
        bodies, roster = {}, []
        for ordinal, start, values in ((0, 0, vectors(1, 1)), (1, 1_000_000, prior),
                                        (2, 1_005_000, np.concatenate((old, selected)))):
            path = work / f"fixture-{ordinal}.parquet"
            pq.write_table(pa.table({"emb": arrow_vectors(values)}), path)
            pin = local_identity(path); key = f"synthetic/train-{ordinal}"
            bodies[key] = path
            roster.append(dict(ordinal=ordinal, source_start=start, source_end_exclusive=start + len(values),
                               rows=len(values), key=key, bytes=pin["bytes"], sha256=pin["sha256"]))
        path = work / "fixture-test.parquet"
        pq.write_table(pa.table({"emb": arrow_vectors(registered)}), path)
        ident = local_identity(path); bodies["synthetic/test"] = path
        synthetic = dict(config, registered_test=dict(key="synthetic/test", rows=1000,
                         bytes=ident["bytes"], sha256=ident["sha256"]),
                         consumed_queries=dict(config["consumed_queries"], sha256=hashlib.sha256(old.tobytes()).hexdigest()),
                         prior_unit_sha256={name: hashlib.sha256(previous_unit(v).tobytes()).hexdigest()
                             for name, v in (("v271", prior[:1000]), ("v278", prior[1000:]), ("registered_test", registered))})
        synthetic_data = dict(metadata_authority=dict(ordered_train_shards=roster),
                              panel=dict(selected=[dict(shard_ordinal=2, query_ordinal=i, local_row=1000 + i)
                                                   for i in range(64)]))
        accounting = Accounting(synthetic, fixture, None)
        def local_download(bucket, pin, destination):
            shutil.copyfile(bodies[pin["key"]], destination)
            shared.authenticate(destination, pin)
        with patch.object(shared.seal, "checked_s3", local_download):
            decoded, sources = acquire_vectors(synthetic, synthetic_data, fixture, accounting)
            assert decoded.tobytes() == selected.tobytes() and len(sources) == 3
            assert not (fixture / "shard-scratch").exists()
            # Wrong full-body hash fails before extraction and always removes owned scratch.
            failed = work / "failed-acquisition"; failed.mkdir()
            changed_data = copy.deepcopy(synthetic_data)
            changed_data["metadata_authority"]["ordered_train_shards"][1]["sha256"] = "0" * 64
            rejects(lambda: acquire_vectors(synthetic, changed_data, failed, Accounting(synthetic, failed, None)))
            assert list(failed.iterdir()) == []
        changed_prior = prior.copy(); changed_prior[0, 0] = 2
        rejects(lambda: verify_prior(changed_prior, registered, synthetic))
        raw = vectors(128, 10000); raw.tofile(fixture / "source.raw"); old.tofile(fixture / "consumed-queries.raw")
        result = audit(selected, fixture, rows=128)
        assert result["consumed_rows_audited"] == 4000 and result["complete_historical_coverage"] is False
        for name in ("source.raw", "consumed-queries.raw", "prior-queries.raw", "test-queries.raw"):
            original = (fixture / name).read_bytes()
            for duplicate in (selected[:1], selected[:1] * 2):
                body = duplicate.tobytes() + original[768 * 4:]
                (fixture / name).write_bytes(body)
                rejects(lambda: audit(selected, fixture, rows=128))
                assert not (fixture / "all-consumed.raw").exists()
            (fixture / name).write_bytes(original)
        duplicate = selected.copy(); duplicate[1] = duplicate[0]
        rejects(lambda: audit(duplicate, fixture, rows=128))
        assert panel["selected_sha256"] == "379b6421d794fb2d0956e7ed787478b019abfd599e0510be157cdcaa53205abf"

        # Same source ordinal ties, deterministic bytes, i64 widening and replay tamper rejection.
        shared.oracle_self_check()
        tied = np.zeros((128, 768), dtype="<f4"); tied[:, 0] = 1
        tied.tofile(fixture / "source.raw")
        truth = shared.oracle(fixture / "source.raw", selected, 128)
        assert truth.tolist() == [list(range(100))] * 64
        second = work / "repeated"; second.mkdir()
        for out in (fixture, second): shared.write_requests_truth(out, selected, truth)
        for name in ("queries.raw", "requests.jsonl", "truth.u32", "truth.i64"):
            assert (fixture / name).read_bytes() == (second / name).read_bytes()
        (fixture / "panel.json").write_bytes((root / FIXED["panel"]["path"]).read_bytes())
        for name in ("duplicate-audit.json", "oracle.json", "resources.json", "final-resources.json"):
            shared.write_json(fixture / name, dict(synthetic=True))
        np.arange(127, -1, -1, dtype="<u8").tofile(fixture / "source-order.u64")
        (fixture / "source-root.json").write_text("{}\n")
        local = dict(synthetic, rows=128, corpus=dict(synthetic["corpus"],
            raw=shared.identity(fixture / "source.raw"), order=shared.identity(fixture / "source-order.u64"),
            root_manifest=shared.identity(fixture / "source-root.json")),
            consumed_queries=shared.identity(fixture / "consumed-queries.raw"))
        decision = dict(schema="borsuk-cohere-semantic-1m-construction-v1", config_sha256=sha,
                        refs=local["refs"], code_sha256=local["code_sha256"], corpus=local["corpus"],
                        consumed_queries=local["consumed_queries"], registered_test=local["registered_test"],
                        complete_historical_coverage=False, ann_quality_measured=False, qualification=False,
                        truth_id_space="source ordinal", prefix="synthetic",
                        inputs=input_identities(fixture), artifacts={name: shared.identity(fixture / name) for name in shared.OUTPUTS})
        shared.write_json(fixture / "decision.json", decision)
        sealed = dict(decision["artifacts"], **{"decision.json": shared.identity(fixture / "decision.json")})
        shared.write_json(fixture / "seal-readback.json", dict(schema="borsuk-semantic-1m-seal-readback-v1",
            final_resources=shared.identity(fixture / "final-resources.json"), artifacts={name: dict(pin,
            key="synthetic/sealed/" + name, authenticated_readback=True) for name, pin in sealed.items()}))
        verify_local(local, sha, fixture)
        original = (fixture / "queries.raw").read_bytes(); (fixture / "queries.raw").write_bytes(original + b"x")
        with patch.dict(globals(), read_config=lambda *args: local, load_inputs=lambda *args: None):
            rejects(lambda: replay(config_path, sha, root, fixture))
        (fixture / "queries.raw").write_bytes(original)
        original = (fixture / "source-order.u64").read_bytes()
        (fixture / "source-order.u64").write_bytes(original[:8] * 128)
        rejects(lambda: verify_order(fixture, local))
        (fixture / "source-order.u64").write_bytes(original)

        # Cgroup admission and transport accounting without spawning any process or SDK.
        group = work / "cgroup"; group.mkdir()
        for name, value in (("memory.max", str(2 << 30)), ("memory.swap.max", "0"), ("cpu.max", "200000 100000")):
            (group / name).write_text(value)
        cgroup_limits(group)
        (group / "memory.swap.max").write_text("1"); rejects(lambda: cgroup_limits(group))
        (group / "memory.swap.max").write_text("0")
        (group / "cpu.max").write_text("max 100000"); rejects(lambda: cgroup_limits(group))
        transport = work / "transport"; transport.mkdir()
        account = Accounting(config, transport, None); account.sizes["synthetic/object"] = 4
        def aws_run(args, **kwargs):
            assert args[:3] == ["aws", "s3api", "get-object"]
            assert kwargs["env"]["AWS_MAX_ATTEMPTS"] == "1"
            kwargs["stderr"].write(b"Sending http request: <AWSPreparedRequest method=GET, url=synthetic>\nResponse headers: {'Content-Length': '4'}\n")
            Path(args[7]).write_bytes(b"body")
            return subprocess.CompletedProcess(args, 0, stdout=b"{}")
        account.native_run = aws_run
        account.run(["aws", "s3", "cp", "s3://" + config["bucket"] + "/synthetic/object", str(transport / "body"), "--only-show-errors"])
        assert account.requests == dict(GET=1, HEAD=0, PUT=0) and account.received_bytes == 4
        assert account.response_lengths == 4 and not (transport / "aws-debug.tmp").exists()
        def failed_run(args, **kwargs):
            kwargs["stderr"].write(b"Sending http request: <AWSPreparedRequest method=GET, url=synthetic>\n")
            Path(args[7]).write_bytes(b"partial")
            return subprocess.CompletedProcess(args, 1, stdout=b"")
        account.native_run = failed_run
        rejects(lambda: account.run(["aws", "s3", "cp", "s3://" + config["bucket"] + "/synthetic/object", str(transport / "partial")]))
        assert account.requests["GET"] == 2 and account.errors == 1
        assert not (transport / "partial").exists() and not (transport / "aws-debug.tmp").exists()

        # Exercise the actual reused conditional PUT/HEAD/full-body readback helpers.
        sealing = work / "sealing"; sealing.mkdir(); (sealing / "small.raw").write_bytes(b"body")
        account = Accounting(config, sealing, None); account.sizes["synthetic/sealed/small.raw"] = 4
        objects = {}; corrupt_readback = False
        def seal_aws(args, **kwargs):
            operation = args[2]; key = args[args.index("--key") + 1]
            method = {"put-object": "PUT", "head-object": "HEAD", "get-object": "GET"}[operation]
            kwargs["stderr"].write(f"Sending http request: <AWSPreparedRequest method={method}, url=synthetic>\n".encode())
            if operation == "put-object":
                assert args[args.index("--if-none-match") + 1] == "*"
                if key in objects:
                    return subprocess.CompletedProcess(args, 1, stdout=b"")
                objects[key] = Path(args[args.index("--body") + 1]).read_bytes()
                return subprocess.CompletedProcess(args, 0, stdout=b"{}")
            sha = hashlib.sha256(objects[key]).hexdigest()
            if operation == "head-object":
                return subprocess.CompletedProcess(args, 0, stdout=canonical(dict(
                    ContentLength=len(objects[key]), Metadata=dict(sha256=sha), ETag="synthetic")))
            body = objects[key] + (b"x" if corrupt_readback else b"")
            kwargs["stderr"].write(f"Response headers: {{'Content-Length': '{len(body)}'}}\n".encode())
            Path(args[7]).write_bytes(body)
            return subprocess.CompletedProcess(args, 0, stdout=b"{}")
        account.native_run = seal_aws
        with accounted_helpers(account):
            sealed = shared.seal_readback(config["bucket"], "synthetic", sealing,
                                          {"small.raw": shared.identity(sealing / "small.raw")})
            assert sealed["artifacts"]["small.raw"]["authenticated_readback"] is True
            rejects(lambda: shared.seal_readback(config["bucket"], "synthetic", sealing,
                                                {"small.raw": shared.identity(sealing / "small.raw")}))
            objects.clear(); corrupt_readback = True
            rejects(lambda: shared.seal_readback(config["bucket"], "synthetic", sealing,
                                                {"small.raw": shared.identity(sealing / "small.raw")}))
        assert account.requests == dict(GET=2, HEAD=2, PUT=3)
        report = account.report(True)
        assert report["http_request_dispatch_attempts"] == dict(GET=2, HEAD=2, PUT=3)
        assert report["confirmed_wire_requests"] == report["billed_requests"] == "UNMEASURED"
        assert list(sealing.iterdir()) == [sealing / "small.raw"]
        cleanup = cleanup_failure(fixture)
        assert cleanup["owned_heavy_inputs_remaining"] is False
        assert all(not (fixture / name).exists() for name in INPUT_FILES)
    require(not {"boto3", "botocore", "faiss"} & set(sys.modules), "self-check loaded a cloud/ANN SDK")
    require(time.monotonic() - started < 55
            and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 <= 200 << 20,
            "synthetic self-check exceeds 55s/200MiB")


def terminate(signum, frame):
    raise SystemExit(128 + signum)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, terminate)
    if sys.argv[1:] == ["--self-check"]:
        self_check()
        print("self-check PASS")
    elif len(sys.argv) == 6 and sys.argv[1] == "--replay":
        print(json.dumps(replay(Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]), Path(sys.argv[5]))))
    elif len(sys.argv) == 6:
        prepare(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5])
    else:
        raise SystemExit(__doc__)
