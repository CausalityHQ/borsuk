# Experimental adapters

The repository contains compatibility adapter code alongside the
[Python](../python/README.md) and [TypeScript](../packages/borsuk/README.md) bindings.
It does not establish a drop-in replacement for a managed vector service.

Evaluate adapter signatures and tests in the pinned checkout for the specific
calls your application uses. They do not expose the current native generation
route or establish equivalent service behavior, filtering, consistency,
performance, or control-plane operations.

Use the [native API guide](api.md) for the current Rust path.
