#!/usr/bin/env python3
"""Download checksum-pinned public BEIR archives from the official mirror."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import stat
import tempfile
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

BASE_URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets"
OFFICIAL_MD5 = {
    "msmarco": "444067daf65d982533ea17ebd59501e4",
    "trec-covid": "ce62140cb23feb9becf6270d0d1fe6d1",
    "nfcorpus": "a89dba18a62ef92f7d323ec890a0d38d",
    "nq": "d4d3d2e48787a744b6f6e691ff534307",
    "hotpotqa": "f412724f78b0d91183a0e86805e16114",
    "fiqa": "17918ed23cd04fb15047f73e6c3bd9d9",
    "arguana": "8ad3e3c2a5867cdced806d6503f29b99",
    "webis-touche2020": "46f650ba5a527fc69e0a6521c5a23563",
    "cqadupstack": "4e41456d7df8ee7760a7f866133bda78",
    "quora": "18fb154900ba42a600f84b839c173167",
    "dbpedia-entity": "c2a39eb420a3164af735795df012ac2c",
    "scidocs": "38121350fc3a4d2f48850f6aff52e4a9",
    "fever": "5a818580227bfb4b35bb6fa46d9b6c03",
    "climate-fever": "8b66f0a9126c521bae2bde127b4dc99d",
    "scifact": "5f7d1de60b170fc8027bb7898e2efca1",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--url")
    parser.add_argument("--md5")
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def hash_file(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def is_symlink(info: zipfile.ZipInfo) -> bool:
    mode = info.external_attr >> 16
    return stat.S_ISLNK(mode)


def safe_member_path(info: zipfile.ZipInfo) -> PurePosixPath:
    member = PurePosixPath(info.filename)
    if (
        member.is_absolute()
        or ".." in member.parts
        or not member.parts
        or is_symlink(info)
    ):
        fail(f"unsafe ZIP member: {info.filename!r}")
    return member


def extract_safely(archive: Path, root: Path) -> None:
    with zipfile.ZipFile(archive) as bundle:
        members = [(info, safe_member_path(info)) for info in bundle.infolist()]
        for info, member in members:
            target = root.joinpath(*member.parts)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(info) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)


def validate_dataset_root(path: Path) -> None:
    required = [
        path / "corpus.jsonl",
        path / "queries.jsonl",
        path / "qrels",
    ]
    missing = [str(item) for item in required if not item.exists()]
    if missing:
        fail(f"BEIR archive is missing required paths: {', '.join(missing)}")


def fetch(args: argparse.Namespace) -> Path:
    dataset = args.dataset
    if bool(args.url) != bool(args.md5):
        fail("--url and --md5 must be provided together")
    if args.url:
        url = args.url
        expected_md5 = args.md5.lower()
    else:
        expected_md5 = OFFICIAL_MD5.get(dataset)
        if expected_md5 is None:
            fail(
                f"unknown public BEIR dataset {dataset!r}; "
                "use an explicit --url and --md5"
            )
        url = f"{BASE_URL}/{dataset}.zip"

    output = args.output
    target = output / dataset
    if target.exists():
        validate_dataset_root(target)
        return target

    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".beir-fetch-", dir=output) as temporary:
        temporary_root = Path(temporary)
        archive = temporary_root / f"{dataset}.zip"
        with urllib.request.urlopen(url) as source, archive.open("wb") as destination:
            shutil.copyfileobj(source, destination)
        actual_md5 = hash_file(archive, "md5")
        if actual_md5 != expected_md5:
            fail(
                "BEIR archive checksum mismatch: "
                f"expected {expected_md5}, got {actual_md5}"
            )

        extracted = temporary_root / "extracted"
        extracted.mkdir()
        extract_safely(archive, extracted)
        candidate = extracted / dataset
        if not candidate.is_dir():
            children = [child for child in extracted.iterdir() if child.is_dir()]
            if len(children) != 1:
                fail("BEIR archive does not contain exactly one dataset directory")
            candidate = children[0]
        validate_dataset_root(candidate)
        (candidate / ".borsuk-source.json").write_text(
            json.dumps(
                {
                    "dataset": dataset,
                    "url": url,
                    "archive_md5": actual_md5,
                    "archive_sha256": hash_file(archive, "sha256"),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        candidate.rename(target)
    return target


def main() -> None:
    target = fetch(parse_args())
    print(target.resolve())


if __name__ == "__main__":
    main()
