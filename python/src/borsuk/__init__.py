"""Native Python API for BORSUK.

The implementation is provided by the Rust/PyO3 extension module
``borsuk._borsuk``. There is intentionally no subprocess or CLI fallback in the
runtime API.
"""

from ._borsuk import (
    CompactionReport,
    BorsukError,
    GarbageCollectionReport,
    Hit,
    Index,
    SearchReport,
    create,
    open,
    string_distance,
    vector_distance,
)

__all__ = [
    "BorsukError",
    "CompactionReport",
    "GarbageCollectionReport",
    "Hit",
    "Index",
    "SearchReport",
    "create",
    "open",
    "string_distance",
    "vector_distance",
]
