"""Evaluate a sealed data-range plan against physical truth-owner pages."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from scripts.native_one_million_data_ranges import admit_ranked_pages


def evaluate_query_pages(
    plan: Mapping[str, object],
    truth_pages: Sequence[int],
    page_lengths: Mapping[str, Sequence[int]],
    *,
    maximum_gets: int = 32,
    maximum_bytes: int = 16_777_216,
) -> dict[str, object]:
    """Recount bridged pages too; reject a plan whose priority replay differs."""
    if set(page_lengths) != {"base", "delta"} or set(plan) != {
        "priority_pages", "target_pages", "ranges", "included_pages", "gets", "encoded_bytes"
    }:
        raise ValueError("data-range evaluation contract differs")
    ranked = tuple(tuple(page) for page in plan["priority_pages"])
    expected = admit_ranked_pages(page_lengths, ranked, maximum_gets, maximum_bytes)
    if plan != {
        "priority_pages": [list(page) for page in ranked],
        "target_pages": [list(page) for page in expected.targets],
        "ranges": [list(interval) for interval in expected.cover.intervals],
        "included_pages": [list(page) for page in expected.cover.included_pages],
        "gets": len(expected.cover.intervals),
        "encoded_bytes": expected.cover.bytes,
    }:
        raise ValueError("data-range sealed plan replay differs")
    base_pages = len(page_lengths["base"])
    total_pages = base_pages + len(page_lengths["delta"])
    if any(type(page) is not int or not 0 <= page < total_pages for page in truth_pages):
        raise ValueError("data-range truth page differs")
    chosen = set(expected.cover.included_pages)
    targets = set(expected.targets)
    priority = {page: index + 1 for index, page in enumerate(ranked)}
    owners = [
        ("base", page) if page < base_pages else ("delta", page - base_pages)
        for page in truth_pages
    ]
    mask = "".join("1" if owner in chosen else "0" for owner in owners)
    return {
        "hit_mask": mask,
        "hits_at_10": mask[:10].count("1"),
        "hits_at_100": mask.count("1"),
        "priority_owner_ranks": [priority.get(owner) for owner in owners],
        "hit_kinds": [
            "target" if owner in targets else "bridge" if owner in chosen else "miss"
            for owner in owners
        ],
    }
