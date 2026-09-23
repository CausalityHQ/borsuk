"""Truth-free page geometry for concurrent 104/96-byte two-bit planes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from scripts.native_one_million_data_range_query import plan_ranked_pages

SIGN_BYTES = 104
MAGNITUDE_BYTES = 96
MAXIMUM_GETS = 32
MAXIMUM_BYTES = 16_777_216


def page_lengths(
    page_row_counts: Sequence[int], *, base_pages: int,
) -> tuple[dict[str, tuple[int, ...]], dict[str, tuple[int, ...]]]:
    counts = tuple(page_row_counts)
    if (
        not counts or type(base_pages) is not int
        or not 0 < base_pages <= len(counts)
        or any(type(count) is not int or count <= 0 for count in counts)
    ):
        raise ValueError("progressive code page rows differ")
    sign = {"base": tuple(SIGN_BYTES * count for count in counts[:base_pages])}
    magnitude = {"base": tuple(MAGNITUDE_BYTES * count for count in counts[:base_pages])}
    if base_pages < len(counts):
        sign["delta"] = tuple(SIGN_BYTES * count for count in counts[base_pages:])
        magnitude["delta"] = tuple(MAGNITUDE_BYTES * count for count in counts[base_pages:])
    return sign, magnitude


def plan_mirrored_wave(
    ranked_pages: Sequence[int],
    sign_lengths: Mapping[str, Sequence[int]],
    magnitude_lengths: Mapping[str, Sequence[int]],
    *, maximum_gets: int = MAXIMUM_GETS,
    maximum_bytes: int = MAXIMUM_BYTES,
) -> dict[str, dict[str, object]]:
    """Admit on the wider plane, then mirror its physical page intervals."""
    if (
        set(sign_lengths) != set(magnitude_lengths)
        or set(sign_lengths) not in ({"base"}, {"base", "delta"})
        or any(
            len(sign_lengths[role]) != len(magnitude_lengths[role])
            or any(
                type(sign) is not int or type(magnitude) is not int
                or sign <= 0 or magnitude <= 0
                or sign * MAGNITUDE_BYTES != magnitude * SIGN_BYTES
                for sign, magnitude in zip(
                    sign_lengths[role], magnitude_lengths[role], strict=True
                )
            )
            for role in sign_lengths
        )
    ):
        raise ValueError("progressive code plane geometry differs")
    sign = plan_ranked_pages(
        ranked_pages, sign_lengths,
        maximum_gets=maximum_gets, maximum_bytes=maximum_bytes,
    )
    magnitude_bytes = sum(
        sum(magnitude_lengths[role][start:end])
        for role, start, end in sign["ranges"]
    )
    if magnitude_bytes > sign["encoded_bytes"] or magnitude_bytes > maximum_bytes:
        raise ValueError("progressive mirror exceeds sign wave")
    magnitude = {**sign, "encoded_bytes": magnitude_bytes}
    return {"sign": sign, "magnitude": magnitude}
