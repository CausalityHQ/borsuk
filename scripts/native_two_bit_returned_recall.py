"""Count returned truth IDs after ranking a fixed fetched-row roster."""

from __future__ import annotations

import math
from collections.abc import Sequence


def ranked_hits(
    scores: Sequence[float], source_ordinals: Sequence[int],
    stable_ids: Sequence[bytes], truth_ids: Sequence[bytes], *, top_k: int = 100,
) -> int:
    """Break float-score ties by source ordinal, as in the historical scorer."""
    if (
        type(top_k) is not int or not 0 < top_k <= len(scores)
        or len(source_ordinals) != len(scores)
        or len(set(source_ordinals)) != len(source_ordinals)
        or any(type(source) is not int or not 0 <= source < len(stable_ids)
               for source in source_ordinals)
        or any(not math.isfinite(float(score)) for score in scores)
    ):
        raise ValueError("returned candidate authority differs")
    if (
        not truth_ids or len(set(truth_ids)) != len(truth_ids)
        or any(type(value) is not bytes for value in truth_ids)
        or any(type(stable_ids[source]) is not bytes for source in source_ordinals)
    ):
        raise ValueError("returned truth authority differs")
    ordered = sorted(
        range(len(scores)), key=lambda index: (float(scores[index]), source_ordinals[index])
    )
    chosen = {stable_ids[source_ordinals[index]] for index in ordered[:top_k]}
    if len(chosen) != top_k:
        raise ValueError("returned stable ID authority differs")
    return sum(value in chosen for value in truth_ids)
