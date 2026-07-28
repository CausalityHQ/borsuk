#!/usr/bin/env python3
"""Validate BORSUK's production/research documentation evidence boundary."""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

STANDARD_DATASETS = (
    "fashion-mnist-784",
    "glove-100",
    "sift-128",
    "nytimes-256",
    "gist-960",
    "deep-image-96",
)
LEAF_METHODS = (
    "exact",
    "flat-scan",
    "sq-scan",
    "pq-scan",
    "graph",
    "vamana-pq",
    "hybrid",
)
RESEARCH_PAGES = (
    "README.md",
    "standard-datasets.md",
    "methods.md",
    "configuration-ablation.md",
    "scale-and-workloads.md",
    "systems-comparison.md",
    "cost-and-deployment.md",
    "reproducibility.md",
)
RESOURCE_COLUMNS = (
    "elapsed_ms",
    "cpu_percent",
    "rss_bytes",
    "vms_bytes",
    "process_read_bytes",
    "process_write_bytes",
    "cache_disk_bytes",
)
DEFAULT_DOCS = (
    "README.md",
    "docs/api.md",
    "docs/architecture.md",
    "docs/web/docs.html",
)
RESEARCH_ONLY_PATTERNS = (
    "uncapped research ceiling",
    "direct s3 vectors comparison",
    "aws production profiles (full public corpora)",
    "recall/latency curves and resources",
    "dimension-aware cell-layout ablation",
)
OBSOLETE_PRODUCTION_CLAIMS = (
    "near-zero ram",
    "few hundred bytes of resident memory",
    "perfect recall without a full scan",
)
MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
CLAIM_ID = re.compile(r'data-claim-id\s*=\s*["\']([^"\']+)["\']')
RESULT_STATUS = re.compile(r'data-result-status\s*=\s*["\']([^"\']+)["\']')
RESULT_TAG = re.compile(r"<[^>]*data-(?:claim-id|result-status)[^>]*>", re.IGNORECASE)
CURRENT_RESULT_COLUMNS = (
    "claim_id",
    "dataset",
    "method",
    "index_capability",
    "cache_state",
    "queries",
    "source_artifact",
    "status",
)


def read_rows(path: Path, errors: list[str]) -> list[dict[str, str]]:
    if not path.is_file():
        errors.append(f"missing artifact: {path}")
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def require_columns(path: Path, columns: tuple[str, ...], errors: list[str]) -> None:
    if not path.is_file():
        errors.append(f"missing artifact: {path}")
        return
    with path.open(newline="") as handle:
        present = tuple(csv.DictReader(handle).fieldnames or ())
    for column in columns:
        if column not in present:
            errors.append(f"{path}: missing required column {column}")


def require_dataset_coverage(path: Path, errors: list[str]) -> None:
    rows = read_rows(path, errors)
    present = {row.get("dataset", "") for row in rows}
    for dataset in STANDARD_DATASETS:
        if dataset not in present:
            errors.append(f"{path}: missing standard dataset {dataset}")


def validate_links(research_root: Path, errors: list[str]) -> None:
    for page in research_root.glob("*.md"):
        for target in MARKDOWN_LINK.findall(page.read_text()):
            if target.startswith(("http://", "https://", "#", "mailto:")):
                continue
            local = target.split("#", 1)[0]
            if local and not (page.parent / local).resolve().exists():
                errors.append(f"{page}: broken local link {target}")


def publication_documents(root: Path) -> list[Path]:
    documents = [root / "README.md"]
    documents.extend(sorted((root / "docs/research").glob("*.md")))
    documents.extend(sorted((root / "docs/web").glob("*.html")))
    return [path for path in documents if path.is_file()]


def validate_current_results(root: Path, assets: Path, errors: list[str]) -> None:
    manifest = assets / "current-results.csv"
    require_columns(manifest, CURRENT_RESULT_COLUMNS, errors)
    rows = read_rows(manifest, errors)
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        claim_id = row.get("claim_id", "").strip()
        if not claim_id:
            errors.append(f"{manifest}: empty claim_id")
            continue
        if claim_id in by_id:
            errors.append(f"{manifest}: duplicate claim_id `{claim_id}`")
            continue
        by_id[claim_id] = row
        status = row.get("status", "")
        if status not in {"current", "historical"}:
            errors.append(f"{manifest}: {claim_id} has invalid status `{status}`")
        if status == "current":
            try:
                queries = int(row.get("queries", ""))
            except ValueError:
                queries = 0
            if queries < 100:
                errors.append(f"{manifest}: {claim_id} requires at least 100 queries")
        source = Path(row.get("source_artifact", ""))
        if not source.is_absolute():
            source = root / source
        if not source.is_file():
            errors.append(f"{manifest}: {claim_id} missing source artifact {source}")

    referenced: set[str] = set()
    for document in publication_documents(root):
        body = document.read_text()
        lowered = body.lower()
        for line_number, line in enumerate(body.splitlines(), start=1):
            line_lower = line.lower()
            stale_glove = (
                "glove" in line_lower
                and re.search(r"\b0\.98\d*\b", line_lower)
                and re.search(r"\b256\s*(?:cand|candidate)", line_lower)
                and re.search(r"\b28(?:\.0+)?\s*(?:mb|mib)\b", line_lower)
                and re.search(r"\b2(?:\.0+)?\s*(?:s|sec|seconds)\b", line_lower)
                and "historical" not in line_lower
            )
            if stale_glove:
                errors.append(
                    f"{document}:{line_number}: stale GloVe result signature must be removed or labeled historical"
                )
        for match in CLAIM_ID.finditer(body):
            claim_id = match.group(1)
            referenced.add(claim_id)
            if claim_id not in by_id:
                errors.append(f"{document}: unknown claim id `{claim_id}`")
                continue
            row = by_id[claim_id]
            context = lowered[max(0, match.start() - 240) : match.end() + 240]
            if row.get("status") == "historical" and "historical" not in context:
                errors.append(
                    f"{document}: historical claim `{claim_id}` lacks a visible historical label"
                )
        for tag in RESULT_TAG.findall(body):
            status_match = RESULT_STATUS.search(tag)
            claim_match = CLAIM_ID.search(tag)
            if status_match and status_match.group(1) == "current":
                if not claim_match:
                    errors.append(
                        f"{document}: rendered current result has no data-claim-id"
                    )
                elif by_id.get(claim_match.group(1), {}).get("status") != "current":
                    errors.append(
                        f"{document}: claim `{claim_match.group(1)}` is rendered current but manifest status is not current"
                    )
    for claim_id, row in by_id.items():
        if row.get("status") == "current" and claim_id not in referenced:
            errors.append(
                f"{manifest}: current claim `{claim_id}` is not rendered in publication docs"
            )


def validate_repository(root: Path) -> list[str]:
    root = root.resolve()
    errors: list[str] = []
    research_root = root / "docs/research"
    assets = root / "docs/web/assets/benchmarks"

    validate_current_results(root, assets, errors)

    for name in RESEARCH_PAGES:
        path = research_root / name
        if not path.is_file():
            errors.append(f"missing research page: {path}")

    production = assets / "aws-production-profiles.csv"
    recall = assets / "aws-recall-latency-2026-07-20.csv"
    uncapped = assets / "aws-uncapped-research-2026-07-20.csv"
    for path in (production, recall, uncapped):
        require_dataset_coverage(path, errors)

    require_columns(
        production,
        (
            "dataset",
            "leaf_mode",
            "global_cap",
            "width",
            "recall_at_10",
            "uncached_p95_ms",
            "disk_cached_p95_ms",
            "experiment_peak_rss_mib",
        ),
        errors,
    )
    for row in read_rows(production, errors):
        dataset = row.get("dataset", "<unknown>")
        if row.get("leaf_mode") != "pq-scan":
            errors.append(
                f"{production}: {dataset} production leaf mode must be pq-scan"
            )
        try:
            global_cap = int(row.get("global_cap", ""))
        except ValueError:
            global_cap = 0
        if not 1 <= global_cap <= 32:
            errors.append(
                f"{production}: {dataset} must use a bounded production global decode cap in 1..32"
            )
    require_columns(
        recall,
        (
            "dataset",
            "nprobe",
            "max_candidates",
            "recall_at_10",
            "p95_ms",
            "cache_state",
        ),
        errors,
    )

    cost = assets / "aws-cost-model-2026-07-20.csv"
    require_dataset_coverage(cost, errors)
    require_columns(
        cost,
        (
            "dataset",
            "index_bytes",
            "storage_usd_per_month",
            "uncached_gets_per_query",
            "uncached_get_usd_per_million_queries",
            "disk_cached_backing_gets_per_query",
            "price_region",
            "price_snapshot_date",
        ),
        errors,
    )
    require_columns(
        uncapped,
        ("dataset", "workers", "qps", "p95_ms", "experiment_peak_rss_mib"),
        errors,
    )

    sequential = assets / "sequential.csv"
    method_rows = read_rows(sequential, errors)
    present_methods = {row.get("mode", "") for row in method_rows}
    for method in LEAF_METHODS:
        if method not in present_methods:
            errors.append(f"{sequential}: missing leaf method {method}")

    matrix = assets / "standard-method-matrix/coverage.csv"
    matrix_rows = read_rows(matrix, errors)
    matrix_cells = {
        (row.get("dataset", ""), row.get("method", "")) for row in matrix_rows
    }
    for dataset in STANDARD_DATASETS:
        for method in LEAF_METHODS:
            if (dataset, method) not in matrix_cells:
                errors.append(f"{matrix}: missing matrix cell {dataset}/{method}")
    for row in matrix_rows:
        if not row.get("status", "").startswith("measured"):
            continue
        resource = Path(row.get("resource_path", ""))
        if not resource.is_absolute():
            resource = root / resource
        result = resource.parent / "bench_recall_latency.csv"
        if not resource.is_file() or not result.is_file():
            errors.append(
                "measured matrix cell missing result/resources: "
                f"{row.get('dataset')}/{row.get('method')}"
            )

    resource_schema = assets / "resource-schema.csv"
    if not resource_schema.is_file():
        resource_schema = next(
            iter(sorted((assets / "raw").rglob("resources.csv"))),
            resource_schema,
        )
    require_columns(resource_schema, RESOURCE_COLUMNS, errors)

    resource_root = assets / "resources"
    for dataset in STANDARD_DATASETS:
        matches = tuple(resource_root.glob(f"*{dataset}*.svg"))
        if not matches:
            errors.append(f"{resource_root}: missing resource graph for {dataset}")
        recall_chart = assets / "recall-latency" / f"recall-latency-{dataset}.svg"
        if not recall_chart.is_file():
            errors.append(f"missing recall/latency chart: {recall_chart}")

    documented_artifacts = "\n".join(
        (research_root / name).read_text()
        for name in RESEARCH_PAGES
        if (research_root / name).is_file()
    )
    for artifact in sorted(assets.glob("*.csv")):
        if artifact.name == "resource-schema.csv":
            continue
        if artifact.name not in documented_artifacts:
            errors.append(
                f"{artifact}: top-level artifact is not documented under docs/research"
            )

    validate_links(research_root, errors)

    prose_contracts = {
        "docs/research/README.md": ("pq-scan", "bounded rerank-read gate"),
        "docs/research/cost-and-deployment.md": (
            "common application/client compute",
            "US $100,000",
            "2030-07-02",
            "BUSL",
            "MIT",
        ),
        "docs/deployment-and-integrations.md": (
            "file://",
            "s3://",
            "cache_dir",
            "LangChain",
            "TypeScript",
            "S3-compatible",
        ),
        "docs/publication-notes.md": (
            "TurboQuant-4b",
            "SRHT",
            "SIMD",
            "scalar fallback",
            "novelty",
            "prior art",
        ),
    }
    for relative, markers in prose_contracts.items():
        path = root / relative
        if not path.is_file():
            errors.append(f"missing required document: {path}")
            continue
        body = path.read_text()
        for marker in markers:
            if marker not in body:
                errors.append(
                    f"{path}: missing required documentation marker `{marker}`"
                )

    for relative in DEFAULT_DOCS:
        path = root / relative
        if not path.is_file():
            errors.append(f"missing default document: {path}")
            continue
        lowered = path.read_text().lower()
        for claim in OBSOLETE_PRODUCTION_CLAIMS:
            if claim in lowered:
                errors.append(f"{path}: obsolete production claim `{claim}`")
        for pattern in RESEARCH_ONLY_PATTERNS:
            if pattern in lowered:
                errors.append(
                    f"{path}: research-only section `{pattern}` belongs under docs/research"
                )
    return errors


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    errors = validate_repository(root)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(
        "research evidence valid: "
        f"datasets={len(STANDARD_DATASETS)} methods={len(LEAF_METHODS)} "
        f"pages={len(RESEARCH_PAGES)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
