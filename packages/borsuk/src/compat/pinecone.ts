// Drop-in Pinecone client backed by BORSUK.
//
//   // before: import { Pinecone } from "@pinecone-database/pinecone";
//   import { Pinecone } from "borsuk/compat/pinecone";
//   const pc = new Pinecone({ baseUri: "file:///data/vectors", dimension: 768, metric: "cosine" });
//   const index = pc.Index("products");
//   await index.upsert([{ id: "a", values: [/*…*/], metadata: { genre: "rock" } }], "store-1");
//   const res = await index.query({ vector: [/*…*/], topK: 10,
//       filter: { genre: { $eq: "rock" } }, includeMetadata: true, namespace: "store-1" });
//
// The backend is a local/embedded BORSUK index — no network service, auth, or
// server-side consistency — and `score` carries BORSUK's distance.
import type { Index as BorsukIndex } from "../index.js";
import { NamespaceStore, mapMetric } from "./common.js";

const DEFAULT_NAMESPACE = "__default__";

export interface PineconeOptions {
  baseUri: string;
  dimension: number;
  metric?: string;
  apiKey?: string;
}

export interface PineconeVector {
  id: string;
  values: number[];
  metadata?: Record<string, unknown>;
}

export type UpsertVector = PineconeVector | [string, number[], Record<string, unknown>?];

export interface QueryOptions {
  vector?: number[];
  id?: string;
  topK?: number;
  filter?: Record<string, unknown>;
  includeValues?: boolean;
  includeMetadata?: boolean;
  namespace?: string;
}

export interface QueryMatch {
  id: string;
  score: number;
  values?: number[];
  metadata?: Record<string, unknown>;
}

export class Pinecone {
  readonly #baseUri: string;
  readonly #dimension: number;
  readonly #metric: string;
  readonly #indexes = new Map<string, PineconeIndex>();

  constructor(options: PineconeOptions) {
    this.#baseUri = options.baseUri.replace(/\/+$/, "");
    this.#dimension = options.dimension;
    this.#metric = options.metric ?? "cosine";
  }

  createIndex(name: string, dimension?: number, metric?: string): PineconeIndex {
    return this.Index(name, { dimension, metric });
  }

  // Matches the Pinecone SDK's method name.
  Index(name: string, options: { dimension?: number; metric?: string } = {}): PineconeIndex {
    const existing = this.#indexes.get(name);
    if (existing) {
      return existing;
    }
    const store = new NamespaceStore(
      `${this.#baseUri}/${name}`,
      mapMetric("pinecone", options.metric ?? this.#metric),
      options.dimension ?? this.#dimension,
    );
    const index = new PineconeIndex(store);
    this.#indexes.set(name, index);
    return index;
  }
}

function coerceVector(entry: UpsertVector): {
  id: string;
  values: number[];
  metadata: Record<string, unknown>;
} {
  if (Array.isArray(entry)) {
    return { id: String(entry[0]), values: [...entry[1]], metadata: { ...(entry[2] ?? {}) } };
  }
  return {
    id: String(entry.id),
    values: [...entry.values],
    metadata: { ...(entry.metadata ?? {}) },
  };
}

export class PineconeIndex {
  readonly #store: NamespaceStore;

  constructor(store: NamespaceStore) {
    this.#store = store;
  }

  async upsert(
    vectors: UpsertVector[],
    namespace: string = DEFAULT_NAMESPACE,
  ): Promise<{ upsertedCount: number }> {
    const ids: string[] = [];
    const values: number[][] = [];
    const metadata: Record<string, unknown>[] = [];
    for (const entry of vectors) {
      const coerced = coerceVector(entry);
      ids.push(coerced.id);
      values.push(coerced.values);
      metadata.push(coerced.metadata);
    }
    const index = await this.#store.get(namespace);
    await deleteExisting(index, ids);
    await index.add(values, { ids, metadata });
    return { upsertedCount: ids.length };
  }

  async query(options: QueryOptions): Promise<{ matches: QueryMatch[]; namespace: string }> {
    const namespace = options.namespace ?? DEFAULT_NAMESPACE;
    const index = await this.#store.get(namespace);
    let vector = options.vector;
    if (vector === undefined) {
      if (options.id === undefined) {
        throw new Error("query requires either vector or id");
      }
      const record = await index.getRecord(options.id);
      if (!record) {
        return { matches: [], namespace };
      }
      vector = record.vector;
    }
    const report = await index.searchWithReport(vector, {
      k: options.topK ?? 10,
      filter: options.filter,
      includeMetadata: Boolean(options.includeMetadata),
    });
    const matches: QueryMatch[] = [];
    for (const hit of report.hits) {
      const match: QueryMatch = { id: hit.id, score: hit.distance };
      if (options.includeMetadata) {
        match.metadata = hit.metadata ?? {};
      }
      if (options.includeValues) {
        const fetched = await index.getRecord(hit.id);
        match.values = fetched ? fetched.vector : [];
      }
      matches.push(match);
    }
    return { matches, namespace };
  }

  async fetch(
    ids: string[],
    namespace: string = DEFAULT_NAMESPACE,
  ): Promise<{ vectors: Record<string, PineconeVector>; namespace: string }> {
    const index = await this.#store.get(namespace);
    const vectors: Record<string, PineconeVector> = {};
    for (const id of ids) {
      const record = await index.getRecord(id);
      if (!record) {
        continue;
      }
      vectors[id] = { id, values: record.vector, metadata: record.metadata };
    }
    return { vectors, namespace };
  }

  async delete(options: {
    ids?: string[];
    deleteAll?: boolean;
    filter?: Record<string, unknown>;
    namespace?: string;
  }): Promise<Record<string, never>> {
    const namespace = options.namespace ?? DEFAULT_NAMESPACE;
    const index = await this.#store.get(namespace);
    if (options.filter) {
      throw new Error("delete by metadata filter is not supported yet; pass ids");
    }
    if (options.deleteAll) {
      throw new Error("deleteAll requires enumerating all ids; delete by ids for now");
    }
    if (options.ids && options.ids.length > 0) {
      await index.delete(options.ids.map(String));
    }
    return {};
  }

  async describeIndexStats(): Promise<{
    dimension: number;
    totalVectorCount: number;
    namespaces: Record<string, { vectorCount: number }>;
  }> {
    const namespaces: Record<string, { vectorCount: number }> = {};
    let total = 0;
    for (const namespace of this.#store.namespaces()) {
      const index = await this.#store.get(namespace, false);
      const count = (await index.stats()).records;
      namespaces[namespace] = { vectorCount: count };
      total += count;
    }
    return { dimension: this.#store.dimensions, totalVectorCount: total, namespaces };
  }
}

async function deleteExisting(index: BorsukIndex, ids: string[]): Promise<void> {
  const present: string[] = [];
  for (const id of ids) {
    if (await index.getRecord(id)) {
      present.push(id);
    }
  }
  if (present.length > 0) {
    await index.delete(present);
    await index.purge();
  }
}
