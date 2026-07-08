"""Drop-in compatibility adapters that emulate popular vector-database SDKs on
top of BORSUK.

- :mod:`borsuk.compat.pinecone` — Pinecone client surface
- :mod:`borsuk.compat.s3vectors` — Amazon S3 Vectors client surface
- :mod:`borsuk.compat.turbopuffer` — turbopuffer client surface
- :mod:`borsuk.compat.chroma` — Chroma client surface
- :mod:`borsuk.compat.qdrant` — Qdrant client surface

Each maps a namespace (or S3 Vectors index) to its own BORSUK index under a
shared storage root, so switching backends is an import change. These are local,
embedded backends — not network services.
"""

from __future__ import annotations

__all__ = ["pinecone", "s3vectors", "turbopuffer", "chroma", "qdrant"]
