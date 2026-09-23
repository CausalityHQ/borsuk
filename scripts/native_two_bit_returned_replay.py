"""Offline replay of the terminal-closed two-bit code-wave candidate roster."""

from __future__ import annotations


def selected_positions(sample: object, codes: object) -> tuple[int, ...]:
    """Map each sealed selected group to physical code-record positions."""
    selected = getattr(sample, "group_ranges", None)
    sealed = getattr(codes, "group_ranges", None)
    counts = getattr(codes, "page_row_counts", None)
    if (
        type(selected) is not tuple or type(sealed) is not tuple
        or type(counts) is not tuple or not counts
        or any(type(count) is not int or count <= 0 for count in counts)
        or not selected or len(selected) > 32
    ):
        raise ValueError("returned group authority differs")
    offsets = [0]
    for count in counts:
        offsets.append(offsets[-1] + count)
    seen: set[int] = set()
    positions: list[int] = []
    encoded = 0
    for group in selected:
        if (
            type(group) is not tuple or len(group) != 5
            or group not in sealed
            or any(type(value) is not int for value in group[:4])
            or not 0 <= group[0] < group[1] <= len(counts)
        ):
            raise ValueError("returned group identity differs")
        first, end, _, length, _ = group
        if seen.intersection(range(first, end)):
            raise ValueError("returned group overlap differs")
        seen.update(range(first, end))
        encoded += length
        positions.extend(range(offsets[first], offsets[end]))
    if (
        type(getattr(sample, "code_gets", None)) is not int
        or sample.code_gets != len(selected)
        or type(getattr(sample, "code_bytes", None)) is not int
        or sample.code_bytes != encoded
        or encoded > 16_777_216
    ):
        raise ValueError("returned code-wave budget differs")
    return tuple(positions)
