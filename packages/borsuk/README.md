# BORSUK TypeScript — Experimental

The TypeScript package loads a Rust N-API addon. Its create/add/search API is a
separate experimental surface from the current native generation route. Native
generation measurements do not describe this binding's performance or
supported operations.

## Local example

Run with the compiled package installed, using a new index directory:

```ts
import { create } from "borsuk";

const index = await create({
  uri: "file:///tmp/borsuk-typescript-example",
  metric: "euclidean",
  dimensions: 2,
});
await index.add([[0, 0], [1, 0]], { ids: ["a", "b"] });
const ids = await index.searchIds([0.2, 0], { k: 2, mode: "exact" });
console.log(ids); // ["a", "b"]
```

See [API tests](test/api.test.ts) and [package source](src/index.ts) for the
surface in this checkout. Build scripts and dependencies are in
[package.json](package.json). For the current Rust generation path, read the
[native API guide](../../docs/api.md).

BORSUK is unreleased; APIs and stored formats can change. The package is
source-available under [Business Source License 1.1](../../LICENSE), with the
Additional Use Grant defined there.
