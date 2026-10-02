#!/usr/bin/env python3
"""Source-only fixed48 geometry replay; no ANN, truth, native execution or network.

Run --self-check, then --output PATH. Input authority is pinned at BASE_COMMIT.
SOURCE ranges are reconstructed: the historical trace only recorded their
counts/bytes. Candidate parity and latency still require a native falsifier.
"""
import argparse
import base64
import hashlib
import itertools
import json
from pathlib import Path
import resource
import signal
import tempfile

BASE_COMMIT = "334313ce0957f2bbd6108964800f0e9e6351f78f"
BASE = Path("docs/research/performance-architecture-20260930/semantic-1m/fixed48")
OFFERED = BASE / "offered-http/a0002"
ANCHOR = dict(bytes=7034, sha256="3e8c655726b5eacf346e75ec29be7ac71b4e8ba7aa50a1f432c7f90b98624a11")
SOURCE_FILES = (
    "crates/borsuk/src/budgeted_page_rank.rs",
    "crates/borsuk/src/two_bit_generation.rs",
    "crates/borsuk/src/sq8_s3_range.rs",
    "crates/borsuk/src/semantic_unit_router.rs",
    "crates/borsuk/src/bin/check_semantic_router_scorer.rs",
    "crates/borsuk/examples/two_bit_http.rs",
)
SOURCE_BUDGET = 64 << 20


def require(ok, context):
    if not ok:
        raise ValueError(context)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def identity(body):
    return dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())


def read_body(path, pin):
    require(type(pin["bytes"]) is int and 0 <= pin["bytes"] <= 16 << 20, f"input size cap: {path}")
    require(not path.is_symlink() and path.is_file(), f"missing/invalid authenticated body: {path}")
    require(path.stat().st_size == pin["bytes"], f"byte length mismatch: {path}")
    body = path.read_bytes()
    require(identity(body) == {k: pin[k] for k in ("bytes", "sha256")}, f"SHA256 mismatch: {path}")
    return body


def cover_pages(selected, rows, row_bytes, max_gets, max_bytes):
    """Rust cover_pages with page_rows=256, then plan_two_bit_source_cover's cap."""
    require(all(type(n) is int and n > 0 for n in (rows, row_bytes, max_gets)), "invalid geometry")
    require(selected and all(type(p) is int and 0 <= p < (rows + 255) // 256 for p in selected),
            "invalid selected pages")
    runs = []
    for page in sorted(set(selected)):
        if runs and runs[-1][1] == page:
            runs[-1][1] += 1
        else:
            runs.append([page, page + 1])
    # Rust sorts the (gap_length, run_index) tuple, including the tie breaker.
    gaps = sorted((runs[i + 1][0] - runs[i][1], i) for i in range(len(runs) - 1))
    joined = {i for _, i in gaps[:max(0, len(runs) - max_gets)]}
    merged = [runs[0][:]]
    for i, run in enumerate(runs[1:]):
        if i in joined:
            merged[-1][1] = run[1]
        else:
            merged.append(run[:])
    ranges = [[a * 256 * row_bytes, min(b * 256, rows) * row_bytes] for a, b in merged]
    charged = sum(b - a for a, b in ranges)
    require(charged <= max_bytes, "insufficient byte budget")
    return ranges, charged


def recorded_parity(query, response):
    ordinal = query["ordinal"]
    expected = dict(ids=query["returned_ids"][:10],
                    ranges=[[r["start"], r["end"]] for r in query["ranges"]],
                    submitted_gets=query["sq8_gets"], verified_bytes=query["sq8_bytes"],
                    planned_bytes=query["sq8_bytes"], failed_gets=0,
                    source_submitted_gets=query["source_gets"], source_verified_bytes=query["source_bytes"],
                    source_failed_gets=0, router_submitted_gets=query["leaf_gets"],
                    router_verified_bytes=query["leaf_bytes"], router_failed_gets=0)
    for key, value in expected.items():
        require(response[key] == value, f"query {ordinal} recorded parity: {key}")


def replay(repo):
    authenticated = {}

    def read(name, pin, *, json_body=True):
        path = repo / name
        require(path.resolve().is_relative_to(repo.resolve()), f"input outside repository: {name}")
        body = read_body(path, pin)
        authenticated[str(name)] = identity(body)
        return json.loads(body) if json_body else body

    def pointer(pin):
        return read(pin["path"], pin)

    audit = read(OFFERED / "root-audit.json", ANCHOR)
    require(audit["actual_closed_validator_passed"] is True and
            audit["all_declared_artifacts_authenticated"] is True and
            audit["scientific_status"] == "PASS", "offered audit is not closed PASS")
    config = read(OFFERED / "config.json", audit["authenticated_artifacts"]["config.json"])
    rate_body = read(OFFERED / "screen/rate5-records.jsonl",
                     audit["authenticated_artifacts"]["screen/rate5-records.jsonl"], json_body=False)
    cold_files = config["cold_run"]["files"]
    cold = pointer(cold_files["config.json"])
    publication = pointer(cold_files["screen/publication.json"])
    native = pointer(cold_files["native/native-source-manifest.json"])
    require(identity(encoded(native["source_sha256"]))["sha256"] == native["source_identity_sha256"],
            "native source manifest identity")
    source_pins = {}
    for name in SOURCE_FILES:
        # The historical source manifest pins hashes only; retain observed lengths.
        pin = dict(bytes=(repo / name).stat().st_size, sha256=native["source_sha256"][name])
        read(name, pin, json_body=False)
        source_pins[name] = pin

    proofs = cold["proofs"]
    terminal = pointer(proofs["scientific_terminal"])
    closed = pointer(proofs["historical_validation"])
    require(closed["closed_validation_passed"] is True and closed["scientific_status"] == "GO" and
            closed["state"] == "terminated" and closed["measurement_rerun"] is False,
            "scientific measurement not independently closed")
    require(closed["terminal"] == {k: proofs["scientific_terminal"][k] for k in ("bytes", "sha256")},
            "scientific terminal binding")
    require(terminal["phase"] == terminal["status"] == "complete" and
            terminal["exit_code"] == terminal["original_exit_code"] == 0, "scientific terminal status")
    science = Path(proofs["scientific_terminal"]["path"]).parent

    def artifact(name, *, json_body=True):
        return read(science / name, terminal["artifacts"][name], json_body=json_body)

    receipt = artifact("screen/measurement-receipt.json")
    marker = artifact("screen/COMPLETE.json")
    scorer = artifact("screen/scorer-config.json")
    trace_body = artifact("screen/records.jsonl", json_body=False)
    request_body = artifact("screen/requests.jsonl", json_body=False)
    require(receipt["exit_status"] == 0 and all(receipt[k] is True for k in
            ("cleanup_complete", "measurement_sealed_before_truth", "native_qualification_passed", "resource_gate_passed")),
            "unsealed/failed measurement receipt")
    require(marker["execution_status"] == "SUCCESS" and marker["scientific_status"] == "GO", "scientific seal status")
    require(identity(encoded(marker["files"]))["sha256"] == marker["roster_sha256"], "seal roster digest")
    for name, field, body in (("records.jsonl", "measurements", trace_body), ("requests.jsonl", "requests", request_body)):
        require(identity(body) == marker["files"][name] == {k: receipt[field][k] for k in ("bytes", "sha256")},
                f"receipt/seal/body binding: {name}")
    require({k: receipt["scorer_config"][k] for k in ("bytes", "sha256")} ==
            terminal["artifacts"]["screen/scorer-config.json"], "scorer config byte binding")
    require((scorer["rows"], scorer["dimensions"], scorer["first"], scorer["count"], scorer["profile"]) ==
            (1_000_000, 768, 0, 64, "fresh1m"), "sealed panel geometry")
    store = "screen/store/" + scorer["generation_prefix"] + "/"
    root = artifact(store + "manifest.json")
    plane = artifact(store + "plane/manifest.json")
    page = artifact(store + "page_manifest.json")
    require(scorer["generation_root_sha256"] == receipt["generation_root_sha256"] ==
            terminal["artifacts"][store + "manifest.json"]["sha256"], "sealed generation root")
    for field, name in (("plane_manifest_sha256", "plane/manifest.json"), ("page_manifest_sha256", "page_manifest.json")):
        require(root[field] == terminal["artifacts"][store + name]["sha256"], f"generation binding: {field}")
        require(publication["manifest"][field] == root[field], f"offered generation binding: {field}")
    require(root["discovery"] == publication["manifest"]["discovery"], "offered discovery identity")
    require(plane["rows"] == page["rows"] == scorer["rows"] and
            plane["dimensions"] == page["dimensions"] == scorer["dimensions"] and
            plane["page_rows"] == 32 and page["page_rows"] == 256 and plane["record_bytes"] == 200,
            "source/SQ8 geometry binding")
    require(plane["source_order_sha256"] == receipt["order"]["sha256"] == scorer["order"]["sha256"], "order identity")

    events = [json.loads(line) for line in trace_body.splitlines()]
    require([e["phase"] for e in events] == ["identity", "startup"] + ["frozen_query"] * 64 +
            ["all_queries_frozen", "terminal"], "exact closed scientific trace roster")
    summary = events[-1]["summary"]
    require(events[-2]["summary"] == summary and summary["complete"] is True and
            summary["truth_opened"] is False and summary["status"] == "FROZEN" and
            events[-2]["count"] == summary["queries"] == 64, "all64 frozen terminal")
    for key in ("binary_sha256", "router_source_sha256", "scorer_source_sha256"):
        require(events[0][key] == summary[key] == receipt[key], f"native trace identity: {key}")
    require(summary["config_sha256"] == events[0]["config_sha256"] == receipt["scorer_config"]["sha256"], "trace config identity")
    require(receipt["scorer_source_sha256"] == native["source_sha256"][SOURCE_FILES[4]], "scorer source identity")
    require(receipt["router_source_sha256"] == native["source_sha256"][SOURCE_FILES[3]], "router source identity")
    for field in ("requests", "order"):
        require(summary[field + "_sha256"] == receipt[field]["sha256"] and
                summary[field + "_bytes"] == receipt[field]["bytes"], f"trace {field} identity")
    require(summary["generation_root_sha256"] == scorer["generation_root_sha256"], "trace generation identity")
    queries = events[2:-2]
    requests = [json.loads(line) for line in request_body.splitlines()]
    offered = [json.loads(line) for line in rate_body.splitlines()]
    require([q["ordinal"] for q in queries] == [q["ordinal"] for q in requests] ==
            [q["query_ordinal"] for q in offered] == list(range(64)), "ordered all64 panel")
    results = []
    for query, request, row in zip(queries, requests, offered):
        ordinal = query["ordinal"]
        response, trace = row["response"], query["trace"]
        require(query["truth_opened"] is False and row["rate_index"] == 5 and row["offered_qps"] == 8 and
                row["outcome"] == "success" and row["http_status"] == 200, f"query {ordinal} status")
        require(response == json.loads(base64.b64decode(row["raw_response_base64"], validate=True)), "raw response parity")
        require(response["authority"] == row["expected_authority"] == publication["arm"]["authority"], "offered authority")
        body = encoded(dict(query=request["query"], k=10, **response["authority"]))
        require(identity(body) == dict(bytes=row["request_bytes"], sha256=row["request_sha256"]), f"query {ordinal} request identity")
        recorded_parity(query, response)
        units = trace["semantic_units"] + trace["semantic_seed_additions"]
        nominations = trace["nomination_evaluated_units"]
        require(len(set(units)) == len(units) and len(set(nominations)) == len(nominations) <= 4096,
                f"query {ordinal} duplicate/oversized units")
        require(all(type(u) is int and 0 <= u < (scorer["rows"] + 31) // 32 for u in units + nominations), "unit bounds")
        closure = {u // 8 for u in units}
        require(set(units) <= set(nominations) and closure == {u // 8 for u in nominations} ==
                set(trace["ranked_candidate_pages"]) and len(closure) <= 512, f"query {ordinal} nomination closure")
        require(len(trace["semantic_leaves"]) == len(set(trace["semantic_leaves"])) == query["leaf_gets"] == 48 and
                len(trace["semantic_units"]) * (4 + scorer["dimensions"] * 2) == query["leaf_bytes"], "leaf accounting")
        require(len(set(query["selected_pages"])) == len(query["selected_pages"]) and
                set(query["selected_pages"]) <= closure, "SQ8 page duplicates/outside nomination closure")
        sq8_ranges, sq8_bytes = cover_pages(query["selected_pages"], scorer["rows"], scorer["dimensions"] + 12, 32, 16773120)
        require(sq8_ranges == response["ranges"] and sq8_bytes == query["sq8_bytes"] and
                len(sq8_ranges) == query["sq8_gets"], f"query {ordinal} SQ8 range replay")
        arms = {}
        for cap in (128, 32):
            ranges, charged = cover_pages(closure, scorer["rows"], plane["record_bytes"], cap, SOURCE_BUDGET)
            require(all(any(a <= p * 256 * plane["record_bytes"] and
                            min((p + 1) * 256, scorer["rows"]) * plane["record_bytes"] <= b
                            for a, b in ranges) for p in closure), f"query {ordinal} missing closure bytes")
            arms[str(cap)] = dict(gets=len(ranges), bytes=charged, ranges=ranges,
                                  largest_range_bytes=max(b - a for a, b in ranges))
        require((arms["128"]["gets"], arms["128"]["bytes"]) == (query["source_gets"], query["source_bytes"]),
                f"query {ordinal} native SOURCE counts/bytes")
        results.append(dict(ordinal=ordinal, closure_pages=sorted(closure), nomination_units=len(nominations),
                            recorded_top10_leaf_source_sq8_parity=True, sq8_recorded_ranges_replayed=True,
                            source_recorded_ranges_available=False, source=arms,
                            extra_bytes=arms["32"]["bytes"] - arms["128"]["bytes"],
                            saved_gets=arms["128"]["gets"] - arms["32"]["gets"]))
    totals = {cap: dict(gets=sum(r["source"][cap]["gets"] for r in results),
                        bytes=sum(r["source"][cap]["bytes"] for r in results),
                        max_query_bytes=max(r["source"][cap]["bytes"] for r in results),
                        largest_range_bytes=max(r["source"][cap]["largest_range_bytes"] for r in results)) for cap in ("128", "32")}
    return dict(schema="borsuk-fixed48-source-get-cap-geometry-v1", status="PASS_CONDITIONAL_GEOMETRY",
                base_commit=BASE_COMMIT, script=identity(Path(__file__).read_bytes()),
                authenticated_inputs=authenticated, native_source_identity_sha256=native["source_identity_sha256"],
                source_files=source_pins, source_byte_cap=SOURCE_BUDGET, query_count=64, totals=totals,
                worst_extra_bytes_ordinal=max(results, key=lambda r: r["extra_bytes"])["ordinal"],
                queries=results, interpretation="Fixed nomination closure model only; no candidate native execution, "
                "quality computation, or latency gain established. Historical SOURCE ranges are absent; only native "
                "SOURCE counts/bytes and exact SQ8 ranges are recorded and checked. Current candidate remains viable.")


def self_check():
    # Catch reversed equal-gap ties, untrimmed tails, undercharging and lost pages.
    assert cover_pages({0, 2}, 513, 9, 2, 2313) == ([[0, 2304], [4608, 4617]], 2313)
    assert cover_pages({0, 2}, 513, 9, 1, 4617) == ([[0, 4617]], 4617)
    assert cover_pages({0, 2, 4}, 1025, 9, 2, 10000) == ([[0, 6912], [9216, 9225]], 6921)
    cases = 0
    for bits in range(1, 1 << 7):
        pages = [p for p in range(7) if bits & (1 << p)]
        boundaries = [i for i in range(len(pages) - 1) if pages[i + 1] > pages[i] + 1]
        for cap in range(1, 8):
            choices = []
            for splits in itertools.combinations(boundaries, min(cap - 1, len(boundaries))):
                cuts = [-1, *splits, len(pages) - 1]
                ranges = [[pages[a + 1] * 256 * 9, min((pages[b] + 1) * 256, 1553) * 9]
                          for a, b in zip(cuts, cuts[1:])]
                joined = tuple(i for i in boundaries if i not in splits)
                choices.append((sum(b - a for a, b in ranges), joined, ranges))
            cost, _, want = min(choices)
            assert cover_pages(set(pages), 1553, 9, cap, cost) == (want, cost)
            cases += 1
    for selected, rows, width, cap, budget in (({0, 2}, 513, 9, 1, 4616), ({3}, 513, 9, 2, 9999),
                                              (set(), 513, 9, 2, 9999), ({0}, 513, 9, 0, 9999)):
        try:
            cover_pages(selected, rows, width, cap, budget)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid geometry/byte budget accepted")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "body"
        pin = identity(b"authentic")
        path.write_bytes(b"authentic")
        assert read_body(path, pin) == b"authentic"
        for tampered in (b"authentic!", b"Authentic"):
            path.write_bytes(tampered)
            try:
                read_body(path, pin)
            except ValueError:
                pass
            else:
                raise AssertionError("tampered body accepted")
    query = dict(ordinal=0, returned_ids=list(range(10)), ranges=[dict(start=0, end=780)],
                 sq8_gets=1, sq8_bytes=780, source_gets=1, source_bytes=200, leaf_gets=1, leaf_bytes=1540)
    response = dict(ids=list(range(10)), ranges=[[0, 780]], submitted_gets=1, verified_bytes=780,
                    planned_bytes=780, failed_gets=0, source_submitted_gets=1, source_verified_bytes=200,
                    source_failed_gets=0, router_submitted_gets=1, router_verified_bytes=1540, router_failed_gets=0)
    recorded_parity(query, response)
    for key in response:
        bad = dict(response)
        bad[key] = list(reversed(bad[key])) if key == "ids" else ([[0, 779]] if key == "ranges" else bad[key] + 1)
        try:
            recorded_parity(query, bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"recorded parity tamper accepted: {key}")
    return dict(self_check="PASS", exhaustive_cases=cases, recorded_parity_negatives=len(response),
                negatives="geometry/byte cap/length/SHA256/ordered top10/leaf/SOURCE/SQ8")


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("BLOCKED: self-check requires assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    resource.setrlimit(resource.RLIMIT_CPU, (120, 120))
    signal.alarm(120)
    try:
        checks = self_check()
        if args.self_check:
            print(json.dumps(checks, sort_keys=True))
        else:
            require(args.output is not None, "--output is required for replay")
            result = replay(args.repo)
            result["verification"] = checks
            # An existing evidence body is immutable; choose a fresh output path.
            with args.output.open("x") as out:
                json.dump(result, out, sort_keys=True, indent=2)
                out.write("\n")
            print(json.dumps(dict(status=result["status"], totals=result["totals"]), sort_keys=True))
    except (ValueError, OSError, KeyError, TypeError, AssertionError) as error:
        raise SystemExit(f"BLOCKED: {type(error).__name__}: {error}") from error
