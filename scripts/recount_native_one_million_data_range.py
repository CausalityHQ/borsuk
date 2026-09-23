"""Independent arithmetic recount of sealed 1M range geometry and hit masks."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence


def _geometry(
    plan: Mapping[str, object], page_lengths: Mapping[str, Sequence[int]],
) -> set[tuple[str, int]]:
    if set(plan) != {
        "priority_pages", "target_pages", "ranges", "included_pages", "gets", "encoded_bytes"
    }:
        raise ValueError("data-range geometry differs")
    if set(page_lengths) not in ({"base"}, {"base", "delta"}):
        raise ValueError("data-range geometry differs")
    roles = tuple(page_lengths)
    included: list[tuple[str, int]] = []
    total_bytes = 0
    last: tuple[str, int] | None = None
    for item in plan["ranges"]:
        if len(item) != 3:
            raise ValueError("data-range geometry differs")
        role, start, end = item
        if (
            role not in roles or type(start) is not int or type(end) is not int
            or not 0 <= start < end <= len(page_lengths[role])
            or (last is not None and (role, start) <= last)
        ):
            raise ValueError("data-range geometry differs")
        included.extend((role, page) for page in range(start, end))
        total_bytes += sum(page_lengths[role][start:end])
        last = role, end - 1
    priority = [tuple(item) for item in plan["priority_pages"]]
    targets = [tuple(item) for item in plan["target_pages"]]
    if (
        len(plan["ranges"]) != plan["gets"] or plan["gets"] > 32
        or total_bytes != plan["encoded_bytes"] or total_bytes > 16_777_216
        or [list(item) for item in included] != plan["included_pages"]
        or len(priority) != len(set(priority))
        or len(targets) != len(set(targets))
        or not set(targets).issubset(priority)
        or not set(targets).issubset(included)
    ):
        raise ValueError("data-range geometry differs")
    return set(included)


def recount_range_masks(
    plan_samples: Sequence[dict[str, object]],
    truth_pages: Sequence[Sequence[int]],
    page_lengths: Mapping[str, Sequence[int]],
    *, arms: tuple[str, str] = ("candidate", "control"),
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Recount every planned byte and truth-owner hit without scorer code."""
    if (
        not plan_samples or len(plan_samples) != len(truth_pages)
        or len(arms) != 2 or len(set(arms)) != 2
        or any(type(arm) is not str or not arm for arm in arms)
    ):
        raise ValueError("data-range recount cohort differs")
    base_count = len(page_lengths["base"])
    total_pages = base_count + len(page_lengths.get("delta", ()))
    samples: list[dict[str, object]] = []
    for ordinal, (plan, owners) in enumerate(zip(plan_samples, truth_pages, strict=True)):
        if plan.get("query_ordinal") != ordinal or any(
            type(page) is not int or not 0 <= page < total_pages for page in owners
        ):
            raise ValueError("data-range recount truth differs")
        sample: dict[str, object] = {"query_ordinal": ordinal}
        for arm in arms:
            arm_plan = plan[arm]
            chosen = _geometry(arm_plan, page_lengths)
            targets = {tuple(page) for page in arm_plan["target_pages"]}
            priority = {
                tuple(page): index + 1
                for index, page in enumerate(arm_plan["priority_pages"])
            }
            owner_pairs = [
                ("base", page) if page < base_count else ("delta", page - base_count)
                for page in owners
            ]
            mask = "".join(
                "1" if owner in chosen else "0" for owner in owner_pairs
            )
            sample[arm] = {
                "hit_mask": mask,
                "hits_at_10": mask[:10].count("1"),
                "hits_at_100": mask.count("1"),
                "priority_owner_ranks": [priority.get(owner) for owner in owner_pairs],
                "hit_kinds": [
                    "target" if owner in targets else "bridge" if owner in chosen else "miss"
                    for owner in owner_pairs
                ],
            }
        samples.append(sample)
    count = len(samples)
    metrics: dict[str, object] = {}
    for arm in arms:
        hits = [int(item[arm]["hits_at_100"]) for item in samples]
        planned_arms = [item[arm] for item in plan_samples]
        metrics[arm] = {
            "gt100_hits": sum(hits),
            "gt10_hits": sum(int(item[arm]["hits_at_10"]) for item in samples),
            "target_gt100_hits": sum(item[arm]["hit_kinds"].count("target") for item in samples),
            "bridge_gt100_hits": sum(item[arm]["hit_kinds"].count("bridge") for item in samples),
            "p05_gt100_hits": sorted(hits)[math.ceil(count * .05) - 1],
            "sub90_queries": sum(value < 90 for value in hits),
            "maximum_gets": max(int(item["gets"]) for item in planned_arms),
            "maximum_bytes": max(int(item["encoded_bytes"]) for item in planned_arms),
            "total_gets": sum(int(item["gets"]) for item in planned_arms),
            "total_bytes": sum(int(item["encoded_bytes"]) for item in planned_arms),
        }
    first, second = arms
    metrics["paired_gt100"] = {
        f"{first}_better": sum(item[first]["hits_at_100"] > item[second]["hits_at_100"] for item in samples),
        f"{second}_better": sum(item[first]["hits_at_100"] < item[second]["hits_at_100"] for item in samples),
        "tied": sum(item[first]["hits_at_100"] == item[second]["hits_at_100"] for item in samples),
    }
    return samples, metrics
