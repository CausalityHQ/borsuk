"""Local-file BORSUK example using the native Python API."""

from __future__ import annotations

import tempfile

import borsuk


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="borsuk-py-index-") as root:
        index = borsuk.create(
            uri=f"file://{root}",
            metric="cosine",
            dimensions=3,
            segment_size=2,
        )

        index.add(
            ["alpha", "beta", "gamma"],
            [
                [1.0, 0.0, 0.0],
                [0.9, 0.1, 0.0],
                [0.0, 1.0, 0.0],
            ],
        )

        report = index.search_with_report(
            [1.0, 0.0, 0.0],
            k=2,
            mode="approx",
            max_candidates_per_segment=2,
        )
        ids = [hit.id for hit in report.hits]
        assert ids == ["alpha", "beta"], ids
        assert report.bytes_read > 0

        cosine = borsuk.vector_distance("cosine", [1.0, 0.0], [1.0, 0.0])
        edit = borsuk.string_distance("jaro-winkler", "segment", "segments")
        assert cosine == 0.0
        assert 0.0 < edit < 0.2

        print(
            f"hits={ids} bytes_read={report.bytes_read} "
            f"records_scored={report.records_scored}"
        )


if __name__ == "__main__":
    main()
