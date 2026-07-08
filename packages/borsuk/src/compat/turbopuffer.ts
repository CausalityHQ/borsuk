// Drop-in turbopuffer client backed by BORSUK.
//
//   // before: import { Turbopuffer } from "@turbopuffer/turbopuffer";
//   import { Turbopuffer } from "borsuk/compat/turbopuffer";
//   const tpuf = new Turbopuffer({ baseUri: "file:///data/vectors", dimension: 768 });
//   const ns = tpuf.namespace("products");
//   await ns.write({ upsertRows: [{ id: "1", vector: [/*…*/], genre: "rock" }],
//                    distanceMetric: "cosine_distance" });
//   const rows = await ns.query({ rankBy: ["vector", "ANN", [/*…*/]], topK: 10,
//       filters: ["And", [["genre", "Eq", "rock"]]], includeAttributes: ["genre"] });
//
// turbopuffer stores the vector inline as `vector` and every other row key as a
// filterable attribute; those map to BORSUK metadata. Tuple filters are
// translated to BORSUK's operator dict.
import type { Index } from "../index.js";
import { NamespaceStore, mapMetric, splitRow, translateTurbopufferFilter } from "./common.js";
import type { TurbopufferFilter } from "./common.js";

export interface TurbopufferOptions {
  baseUri: string;
  dimension: number;
  region?: string;
  apiKey?: string;
  defaultDistanceMetric?: string;
}

export class Turbopuffer {
  readonly #baseUri: string;
  readonly #dimension: number;
  readonly #defaultMetric: string;
  readonly #namespaces = new Map<string, Namespace>();

  constructor(options: TurbopufferOptions) {
    this.#baseUri = options.baseUri.replace(/\/+$/, "");
    this.#dimension = options.dimension;
    this.#defaultMetric = options.defaultDistanceMetric ?? "cosine_distance";
  }

  namespace(name: string): Namespace {
    const existing = this.#namespaces.get(name);
    if (existing) {
      return existing;
    }
    const namespace = new Namespace(this.#baseUri, name, this.#dimension, this.#defaultMetric);
    this.#namespaces.set(name, namespace);
    return namespace;
  }
}

export interface WriteArgs {
  upsertRows?: Record<string, unknown>[];
  deletes?: string[];
  deleteByFilter?: TurbopufferFilter;
  distanceMetric?: string;
}

export interface QueryArgs {
  rankBy: [string, string, number[]];
  topK?: number;
  filters?: TurbopufferFilter;
  includeAttributes?: string[] | boolean;
}

export class Namespace {
  readonly #baseUri: string;
  readonly #name: string;
  readonly #dimension: number;
  readonly #defaultMetric: string;
  #store: NamespaceStore | undefined;

  constructor(baseUri: string, name: string, dimension: number, defaultMetric: string) {
    this.#baseUri = baseUri;
    this.#name = name;
    this.#dimension = dimension;
    this.#defaultMetric = defaultMetric;
  }

  async #index(distanceMetric?: string): Promise<Index> {
    if (!this.#store) {
      this.#store = new NamespaceStore(
        `${this.#baseUri}/${this.#name}`,
        mapMetric("turbopuffer", distanceMetric ?? this.#defaultMetric),
        this.#dimension
      );
    }
    return this.#store.get("");
  }

  async write(args: WriteArgs): Promise<{ rowsAffected: number }> {
    const index = await this.#index(args.distanceMetric);
    if (args.deleteByFilter !== undefined) {
      throw new Error("deleteByFilter is not supported yet; pass deletes");
    }
    if (args.deletes && args.deletes.length > 0) {
      await index.delete(args.deletes.map(String));
    }
    if (args.upsertRows && args.upsertRows.length > 0) {
      const ids: string[] = [];
      const values: number[][] = [];
      const metadata: Record<string, unknown>[] = [];
      for (const row of args.upsertRows) {
        const split = splitRow(row, "id", "vector");
        ids.push(split.id);
        values.push(split.vector);
        metadata.push(split.metadata);
      }
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
      await index.add(values, { ids, metadata });
      return { rowsAffected: ids.length };
    }
    return { rowsAffected: 0 };
  }

  async query(args: QueryArgs): Promise<Record<string, unknown>[]> {
    const rankBy = args.rankBy;
    if (!Array.isArray(rankBy) || rankBy.length !== 3 || rankBy[1] !== "ANN") {
      throw new Error('rankBy must be ["vector", "ANN", <query vector>]; other ranks are unsupported');
    }
    const index = await this.#index();
    const includeMetadata = args.includeAttributes !== undefined && args.includeAttributes !== false;
    const filter =
      args.filters === undefined || args.filters === null
        ? undefined
        : translateTurbopufferFilter(args.filters);
    const report = await index.searchWithReport([...rankBy[2]], {
      k: args.topK ?? 10,
      filter,
      includeMetadata
    });
    const wanted = Array.isArray(args.includeAttributes) ? new Set(args.includeAttributes.map(String)) : null;
    return report.hits.map((hit) => {
      const row: Record<string, unknown> = { id: hit.id, dist: hit.distance };
      for (const [attr, value] of Object.entries(hit.metadata ?? {})) {
        if (wanted === null || wanted.has(attr)) {
          row[attr] = value;
        }
      }
      return row;
    });
  }
}
