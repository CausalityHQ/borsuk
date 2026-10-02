# BORSUK Python — Experimental

The Python package loads a compiled Rust extension through PyO3. Its
create/add/search API is a separate experimental surface from the current
native generation route. Native generation measurements do not describe this
binding's performance or supported operations.

## Local example

Run with the compiled package installed, using a new index directory:

```python
import borsuk

index = borsuk.create(uri="file:///tmp/borsuk-python-example", metric="euclidean", dimensions=2)
index.add([[0.0, 0.0], [1.0, 0.0]], ids=["a", "b"])
ids = index.search_ids([0.2, 0.0], k=2, mode="exact")
assert ids == ["a", "b"]
```

See [API tests](tests/test_api.py), [the local example](examples/local_index.py),
and [Python source](src/borsuk/__init__.py) for the surface in this checkout.
Build instructions and dependencies are in [pyproject.toml](pyproject.toml).
For the current Rust generation path, read the [native API guide](../docs/api.md).

BORSUK is unreleased; APIs and stored formats can change. The package is
source-available under [Business Source License 1.1](../LICENSE), with the
Additional Use Grant defined there.
