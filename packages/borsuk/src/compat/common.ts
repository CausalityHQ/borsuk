// Shared plumbing for the drop-in compatibility adapters. Each adapter emulates
// a target SDK surface and stores each namespace (or S3 Vectors index) in its
// own BORSUK index under a shared base URI, so switching backends is an import
// change with no engine support required.
import { BorsukError, Index, create, open } from "../index.js";
import type { VectorMetric } from "../index.js";

function sanitize(segment: string): string {
  const value = segment === "" ? "__default__" : String(segment);
  return encodeURIComponent(value);
}

/** Lazily creates/opens one BORSUK index per namespace under `baseUri`. */
export class NamespaceStore {
  readonly #baseUri: string;
  readonly #metric: VectorMetric;
  readonly #dimensions: number;
  readonly #handles = new Map<string, Index>();

  constructor(baseUri: string, metric: VectorMetric, dimensions: number) {
    this.#baseUri = baseUri.replace(/\/+$/, "");
    this.#metric = metric;
    this.#dimensions = dimensions;
  }

  uriFor(namespace: string): string {
    return `${this.#baseUri}/${sanitize(namespace)}`;
  }

  /** Return the index for `namespace`, creating it on first use. */
  async get(namespace: string, create_ = true): Promise<Index> {
    const key = sanitize(namespace);
    const cached = this.#handles.get(key);
    if (cached) {
      return cached;
    }
    const uri = this.uriFor(namespace);
    let handle: Index;
    try {
      handle = open(uri);
    } catch (error) {
      if (!create_ && error instanceof BorsukError) {
        throw error;
      }
      handle = await create({
        uri,
        metric: this.#metric,
        dimensions: this.#dimensions
      });
    }
    this.#handles.set(key, handle);
    return handle;
  }

  get dimensions(): number {
    return this.#dimensions;
  }

  namespaces(): string[] {
    return [...this.#handles.keys()];
  }
}

// ---- Metric mapping -------------------------------------------------------

const METRIC_MAPS: Record<string, Record<string, VectorMetric>> = {
  pinecone: { cosine: "cosine", euclidean: "euclidean", dotproduct: "inner-product" },
  turbopuffer: { cosine_distance: "cosine", euclidean_squared: "squared-euclidean" },
  s3vectors: { cosine: "cosine", euclidean: "euclidean" }
};

/** Translate a target service's metric name to a BORSUK metric. */
export function mapMetric(service: keyof typeof METRIC_MAPS | string, metric: string): VectorMetric {
  const table = METRIC_MAPS[service];
  const mapped = table?.[metric];
  if (!mapped) {
    const supported = Object.keys(table ?? {}).sort().join(", ");
    throw new BorsukError(`${service} metric '${metric}' is not supported; use one of: ${supported}`);
  }
  return mapped;
}

// ---- Filter translation (turbopuffer tuple syntax -> $-operator dict) ------

const TPUF_LEAF_OPS: Record<string, string> = {
  Eq: "$eq",
  NotEq: "$ne",
  Gt: "$gt",
  Gte: "$gte",
  Lt: "$lt",
  Lte: "$lte",
  In: "$in",
  NotIn: "$nin",
  Contains: "$contains"
};
const TPUF_LOGICAL: Record<string, string> = { And: "$and", Or: "$or" };

export type TurbopufferFilter = unknown;

/** Convert a turbopuffer tuple filter into a BORSUK `$`-operator dict. */
export function translateTurbopufferFilter(node: TurbopufferFilter): Record<string, unknown> {
  if (node === null || node === undefined) {
    return {};
  }
  if (!Array.isArray(node) || node.length === 0) {
    throw new BorsukError(`invalid turbopuffer filter: ${JSON.stringify(node)}`);
  }
  const [head] = node as unknown[];
  if (typeof head === "string" && head in TPUF_LOGICAL) {
    const children = (node[1] as TurbopufferFilter[]).map(translateTurbopufferFilter);
    return { [TPUF_LOGICAL[head]]: children };
  }
  if (head === "Not") {
    return { $not: translateTurbopufferFilter(node[1]) };
  }
  if (node.length !== 3) {
    throw new BorsukError(`invalid turbopuffer leaf filter: ${JSON.stringify(node)}`);
  }
  const [attr, op, value] = node as [string, string, unknown];
  const mapped = TPUF_LEAF_OPS[op];
  if (!mapped) {
    const supported = [...Object.keys(TPUF_LEAF_OPS), ...Object.keys(TPUF_LOGICAL), "Not"].sort().join(", ");
    throw new BorsukError(`turbopuffer operator '${op}' is not supported; use one of: ${supported}`);
  }
  return { [String(attr)]: { [mapped]: value } };
}

/** Split a turbopuffer-style row into { id, vector, metadata } parts. */
export function splitRow(
  row: Record<string, unknown>,
  idKey: string,
  vectorKey: string
): { id: string; vector: number[]; metadata: Record<string, unknown> } {
  const rest = { ...row };
  if (!(idKey in rest)) {
    throw new BorsukError(`row is missing required '${idKey}'`);
  }
  if (!(vectorKey in rest)) {
    throw new BorsukError(`row is missing required '${vectorKey}'`);
  }
  const id = String(rest[idKey]);
  const vector = rest[vectorKey] as number[];
  delete rest[idKey];
  delete rest[vectorKey];
  return { id, vector: [...vector], metadata: rest };
}
