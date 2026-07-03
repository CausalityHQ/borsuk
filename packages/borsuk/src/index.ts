import { Buffer } from "node:buffer";
import { createRequire } from "node:module";

export enum VectorMetricName {
  Euclidean = "euclidean",
  SquaredEuclidean = "squared-euclidean",
  Cosine = "cosine",
  InnerProduct = "inner-product",
  Angular = "angular",
  Manhattan = "manhattan",
  Gower = "gower",
  Chebyshev = "chebyshev",
  Canberra = "canberra",
  BrayCurtis = "bray-curtis",
  Correlation = "correlation",
  Hamming = "hamming",
  Jaccard = "jaccard",
  Dice = "dice",
  SimpleMatching = "simple-matching",
  RussellRao = "russell-rao",
  RogersTanimoto = "rogers-tanimoto",
  SokalSneath = "sokal-sneath",
  Yule = "yule",
  Hellinger = "hellinger",
  ChiSquare = "chi-square",
  KullbackLeibler = "kullback-leibler",
  Jeffreys = "jeffreys",
  JensenShannon = "jensen-shannon",
  Bhattacharyya = "bhattacharyya",
  Wasserstein = "wasserstein",
  DynamicTimeWarping = "dynamic-time-warping",
  Ruzicka = "ruzicka",
  SquaredChord = "squared-chord",
  WaveHedges = "wave-hedges",
  Lorentzian = "lorentzian",
  Clark = "clark"
}

export enum SearchMode {
  Exact = "exact",
  Approx = "approx"
}

export enum LeafModeName {
  FlatScan = "flat-scan",
  SqScan = "sq-scan",
  PqScan = "pq-scan",
  Graph = "graph",
  VamanaPq = "vamana-pq",
  Hybrid = "hybrid"
}

export type CanonicalVectorMetricName = `${VectorMetricName}`;
export type VectorMetricAlias =
  | "l2"
  | "sqeuclidean"
  | "l2-squared"
  | "innerproduct"
  | "ip"
  | "dot"
  | "dot-product"
  | "angle"
  | "l1"
  | "gower-distance"
  | "linf"
  | "l-infinity"
  | "braycurtis"
  | "simplematching"
  | "matching"
  | "smc"
  | "russellrao"
  | "rogerstanimoto"
  | "sokalsneath"
  | "chisquare"
  | "chi2"
  | "kullbackleibler"
  | "kl"
  | "kl-divergence"
  | "jeffreys-divergence"
  | "jensenshannon"
  | "js"
  | "js-distance"
  | "bhattacharyya-distance"
  | "earth-mover"
  | "earthmover"
  | "emd"
  | "dynamictimewarping"
  | "dtw"
  | "weighted-jaccard"
  | "weightedjaccard"
  | "squaredchord"
  | "wavehedges";
export type MinkowskiMetricName = `minkowski:${number}` | `lp:${number}`;
export type VectorMetric = CanonicalVectorMetricName | VectorMetricAlias | MinkowskiMetricName;

export type SearchModeName = `${SearchMode}`;
export type CanonicalLeafModeName = `${LeafModeName}`;
export type SearchTerminationReason =
  | "complete"
  | "exact-pruned"
  | "epsilon"
  | "max-segments"
  | "max-bytes"
  | "max-latency";
export type LeafModeAlias =
  | "flat"
  | "flatscan"
  | "sq"
  | "sqscan"
  | "scalar-scan"
  | "scalar-quantized-scan"
  | "pq"
  | "pqscan"
  | "product-quantized-scan"
  | "local-graph"
  | "segment-graph"
  | "vamana"
  | "vamanapq"
  | "vamana_pq"
  | "diskann"
  | "diskann-pq"
  | "auto"
  | "stored"
  | "stored-leaf"
  | "segment-leaf";
export type LeafMode = CanonicalLeafModeName | LeafModeAlias;

export interface Hit {
  id: string;
  idBytes: Uint8Array;
  distance: number;
}

export interface IndexStats {
  metric: CanonicalVectorMetricName | MinkowskiMetricName;
  dimensions: number;
  segmentMaxVectors: number;
  ramBudgetBytes?: number | null;
  manifestVersion: number;
  segments: number;
  records: number;
  segmentBytes: number;
  graphBytes: number;
  residentBytesEstimate: number;
}

export interface SearchReport {
  hits: Hit[];
  leafMode: CanonicalLeafModeName;
  terminationReason: SearchTerminationReason;
  segmentsTotal: number;
  segmentsSearched: number;
  segmentsSkipped: number;
  bytesRead: number;
  graphBytesRead: number;
  objectCacheHits: number;
  objectCacheMisses: number;
  recordsConsidered: number;
  recordsScored: number;
  graphCandidatesAdded: number;
  residentBytesEstimate: number;
  elapsedMs: number;
}

export interface CompactionOptions {
  sourceLevel?: number;
  targetLevel?: number;
  maxSegments?: number;
  allMatching?: boolean;
  minSegments?: number;
  targetSegmentMaxVectors?: number;
}

export interface CompactionReport {
  compacted: boolean;
  sourceLevel: number;
  targetLevel: number;
  segmentsRead: number;
  segmentsWritten: number;
  recordsRewritten: number;
  routingPageIndexesRead: number;
  routingPagesRead: number;
  routingPageIndexesWritten: number;
  routingPagesWritten: number;
  graphPayloadsRead: number;
  graphBytesRead: number;
  bytesRead: number;
  bytesWritten: number;
  objectCacheHits: number;
  objectCacheMisses: number;
  manifestVersion: number;
}

export interface GarbageCollectionOptions {
  dryRun?: boolean;
}

export interface GarbageCollectionReport {
  dryRun: boolean;
  objectsScanned: number;
  objectsDeleted: number;
  bytesReclaimable: number;
  bytesReclaimed: number;
  candidates: string[];
}

export interface RebuildOptions {
  sourceLevel?: number;
  targetLevel?: number;
  minSegments?: number;
  targetSegmentMaxVectors?: number;
  deleteObsolete?: boolean;
}

export interface RebuildReport {
  compaction: CompactionReport;
  garbageCollection: GarbageCollectionReport;
}

export interface CreateOptions {
  uri: string;
  metric: VectorMetric;
  dim?: number;
  dimensions?: number;
  segmentSize?: number;
  segmentMaxVectors?: number;
  ramBudget?: string;
  cacheDir?: string;
}

export interface SearchOptions {
  k?: number;
  mode?: SearchModeName;
  leafMode?: LeafMode;
  eps?: number;
  maxSegments?: number;
  maxBytes?: number | string;
  maxLatencyMs?: number;
  maxCandidatesPerSegment?: number;
}

export type VectorInput = readonly number[];
export type VectorBatchInput = readonly VectorInput[];
export type RecordId = string | Uint8Array | number | bigint;
export type IdsInput = readonly RecordId[];

export interface AddOptions<TId extends RecordId = RecordId> {
  ids?: readonly TId[];
}

interface NativeModule {
  Index: new (uri: string) => NativeIndex;
  create(options: NativeCreateOptions): NativeIndex;
  open(uri: string, options?: NativeOpenOptions): NativeIndex;
  leafModeNames(): string[];
  recallAtK(exactIds: string[], actualIds: string[], k: number): number;
  vectorDistance(metric: string, left: number[], right: number[]): number;
  vectorMetricNames(): string[];
}

interface NativeIndex {
  add(vectors: number[][], ids?: string[] | null): string[];
  addIdBytes(vectors: number[][], ids: Uint8Array[]): Uint8Array[];
  addBuffer(vectors: Float32Array, ids?: string[] | null): string[];
  addBufferIdBytes(vectors: Float32Array, ids: Uint8Array[]): Uint8Array[];
  stats(): IndexStats;
  searchIds(query: number[], options?: NativeSearchOptions): string[];
  searchIdBytes(query: number[], options?: NativeSearchOptions): Uint8Array[];
  searchVectors(query: number[], options?: NativeSearchOptions): number[][];
  getVector(id: string): number[] | null;
  getVectorById(id: Uint8Array): number[] | null;
  searchIdsBuffer(query: Float32Array, options?: NativeSearchOptions): string[];
  searchIdBytesBuffer(query: Float32Array, options?: NativeSearchOptions): Uint8Array[];
  searchVectorsBuffer(query: Float32Array, options?: NativeSearchOptions): number[][];
  searchWithReportBuffer(query: Float32Array, options?: NativeSearchOptions): NativeSearchReport;
  searchIdsBatch(queries: number[][], options?: NativeSearchOptions): string[][];
  searchIdBytesBatch(queries: number[][], options?: NativeSearchOptions): Uint8Array[][];
  searchVectorsBatch(queries: number[][], options?: NativeSearchOptions): number[][][];
  searchIdsBatchBuffer(queries: Float32Array, options?: NativeSearchOptions): string[][];
  searchIdBytesBatchBuffer(queries: Float32Array, options?: NativeSearchOptions): Uint8Array[][];
  searchVectorsBatchBuffer(queries: Float32Array, options?: NativeSearchOptions): number[][][];
  searchWithReport(query: number[], options?: NativeSearchOptions): NativeSearchReport;
  searchBatchWithReport(queries: number[][], options?: NativeSearchOptions): NativeSearchReport[];
  searchBatchWithReportBuffer(
    queries: Float32Array,
    options?: NativeSearchOptions
  ): NativeSearchReport[];
  compact(options?: NativeCompactionOptions): CompactionReport;
  rebuild(options?: NativeRebuildOptions): RebuildReport;
  gcObsoleteSegments(options?: NativeGarbageCollectionOptions): GarbageCollectionReport;
}

interface NativeHit {
  id: string;
  idBytes?: Uint8Array;
  id_bytes?: Uint8Array;
  distance: number;
}

interface NativeSearchReport extends Omit<SearchReport, "hits"> {
  hits: NativeHit[];
}

interface NativeCreateOptions {
  uri: string;
  metric: string;
  dim?: number;
  dimensions?: number;
  segmentSize?: number;
  segmentMaxVectors?: number;
  segment_size?: number;
  segment_max_vectors?: number;
  ramBudget?: string;
  ram_budget?: string;
  cacheDir?: string;
  cache_dir?: string;
}

export interface OpenOptions {
  cacheDir?: string;
  ramBudget?: string;
  residentRouting?: boolean;
}

interface NativeOpenOptions {
  cacheDir?: string;
  cache_dir?: string;
  ramBudget?: string;
  ram_budget?: string;
  residentRouting?: boolean;
  resident_routing?: boolean;
}

interface NativeSearchOptions {
  k?: number;
  mode?: string;
  leafMode?: string;
  leaf_mode?: string;
  eps?: number;
  maxSegments?: number;
  max_segments?: number;
  maxBytes?: number;
  max_bytes?: number;
  maxBytesText?: string;
  max_bytes_text?: string;
  maxLatencyMs?: number;
  max_latency_ms?: number;
  maxCandidatesPerSegment?: number;
  max_candidates_per_segment?: number;
}

interface NativeCompactionOptions {
  sourceLevel?: number;
  source_level?: number;
  targetLevel?: number;
  target_level?: number;
  maxSegments?: number;
  max_segments?: number;
  allMatching?: boolean;
  all_matching?: boolean;
  minSegments?: number;
  min_segments?: number;
  targetSegmentMaxVectors?: number;
  target_segment_max_vectors?: number;
}

interface NativeGarbageCollectionOptions {
  dryRun?: boolean;
  dry_run?: boolean;
}

interface NativeRebuildOptions {
  sourceLevel?: number;
  source_level?: number;
  targetLevel?: number;
  target_level?: number;
  minSegments?: number;
  min_segments?: number;
  targetSegmentMaxVectors?: number;
  target_segment_max_vectors?: number;
  deleteObsolete?: boolean;
  delete_obsolete?: boolean;
}

const require = createRequire(import.meta.url);
const native = require("../../index.cjs") as NativeModule;

export class BorsukError extends Error {
  readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "BorsukError";
    this.cause = cause;
  }
}

export class Index {
  readonly #inner: NativeIndex;

  constructor(uri: string);
  /** @internal */
  constructor(uri: string, inner: NativeIndex);
  constructor(uri: string, inner?: NativeIndex) {
    this.#inner = inner ?? wrapNativeError(() => new native.Index(uri));
  }

  async add(vectors: VectorBatchInput): Promise<string[]>;
  async add(vectors: VectorBatchInput, ids: readonly string[]): Promise<string[]>;
  async add(vectors: VectorBatchInput, ids: readonly Uint8Array[]): Promise<Uint8Array[]>;
  async add(vectors: VectorBatchInput, ids: readonly number[]): Promise<number[]>;
  async add(vectors: VectorBatchInput, ids: readonly bigint[]): Promise<bigint[]>;
  async add(vectors: VectorBatchInput, options: AddOptions<string>): Promise<string[]>;
  async add(vectors: VectorBatchInput, options: AddOptions<Uint8Array>): Promise<Uint8Array[]>;
  async add(vectors: VectorBatchInput, options: AddOptions<number>): Promise<number[]>;
  async add(vectors: VectorBatchInput, options: AddOptions<bigint>): Promise<bigint[]>;
  async add(vectors: VectorBatchInput, ids: IdsInput): Promise<RecordId[]>;
  async add(vectors: VectorBatchInput, options: AddOptions): Promise<RecordId[]>;
  async add(
    vectors: VectorBatchInput,
    idsOrOptions: AddOptions | IdsInput = {}
  ): Promise<RecordId[]> {
    return wrapNativeError(() => {
      const ids = addIds(idsOrOptions);
      const nativeVectorsValue = nativeVectors(vectors);
      if (ids === null || idsAreAllStrings(ids)) {
        return this.#inner.add(nativeVectorsValue, nativeStringIds(ids));
      }
      const added = this.#inner.addIdBytes(nativeVectorsValue, nativeIdBytes(ids));
      return idsContainIntegers(ids) ? [...ids] : added;
    });
  }

  async addBuffer(vectors: Float32Array): Promise<string[]>;
  async addBuffer(vectors: Float32Array, ids: readonly string[]): Promise<string[]>;
  async addBuffer(vectors: Float32Array, ids: readonly Uint8Array[]): Promise<Uint8Array[]>;
  async addBuffer(vectors: Float32Array, ids: readonly number[]): Promise<number[]>;
  async addBuffer(vectors: Float32Array, ids: readonly bigint[]): Promise<bigint[]>;
  async addBuffer(vectors: Float32Array, options: AddOptions<string>): Promise<string[]>;
  async addBuffer(vectors: Float32Array, options: AddOptions<Uint8Array>): Promise<Uint8Array[]>;
  async addBuffer(vectors: Float32Array, options: AddOptions<number>): Promise<number[]>;
  async addBuffer(vectors: Float32Array, options: AddOptions<bigint>): Promise<bigint[]>;
  async addBuffer(vectors: Float32Array, ids: IdsInput): Promise<RecordId[]>;
  async addBuffer(vectors: Float32Array, options: AddOptions): Promise<RecordId[]>;
  async addBuffer(
    vectors: Float32Array,
    idsOrOptions: AddOptions | IdsInput = {}
  ): Promise<RecordId[]> {
    return wrapNativeError(() => {
      const ids = addIds(idsOrOptions);
      if (ids === null || idsAreAllStrings(ids)) {
        return this.#inner.addBuffer(vectors, nativeStringIds(ids));
      }
      const added = this.#inner.addBufferIdBytes(vectors, nativeIdBytes(ids));
      return idsContainIntegers(ids) ? [...ids] : added;
    });
  }

  async stats(): Promise<IndexStats> {
    return wrapNativeError(() => this.#inner.stats());
  }

  async searchIds(query: VectorInput, options: SearchOptions = {}): Promise<string[]> {
    return wrapNativeError(() => this.#inner.searchIds(nativeVector(query), nativeSearchOptions(options)));
  }

  async searchIdBytes(query: VectorInput, options: SearchOptions = {}): Promise<Uint8Array[]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytes(nativeVector(query), nativeSearchOptions(options))
    );
  }

  async searchVectors(query: VectorInput, options: SearchOptions = {}): Promise<number[][]> {
    return wrapNativeError(() => this.#inner.searchVectors(nativeVector(query), nativeSearchOptions(options)));
  }

  async getVector(id: RecordId): Promise<number[] | null> {
    return wrapNativeError(() =>
      typeof id === "string" ? this.#inner.getVector(id) : this.#inner.getVectorById(nativeIdByte(id))
    );
  }

  async searchIdsBuffer(query: Float32Array, options: SearchOptions = {}): Promise<string[]> {
    return wrapNativeError(() => this.#inner.searchIdsBuffer(query, nativeSearchOptions(options)));
  }

  async searchIdBytesBuffer(query: Float32Array, options: SearchOptions = {}): Promise<Uint8Array[]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytesBuffer(query, nativeSearchOptions(options))
    );
  }

  async searchVectorsBuffer(query: Float32Array, options: SearchOptions = {}): Promise<number[][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectorsBuffer(query, nativeSearchOptions(options))
    );
  }

  async searchWithReportBuffer(
    query: Float32Array,
    options: SearchOptions = {}
  ): Promise<SearchReport> {
    return wrapNativeError(() =>
      normalizeSearchReport(this.#inner.searchWithReportBuffer(query, nativeSearchOptions(options)))
    );
  }

  async searchIdsBatch(queries: VectorBatchInput, options: SearchOptions = {}): Promise<string[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdsBatch(nativeVectors(queries), nativeSearchOptions(options))
    );
  }

  async searchIdBytesBatch(
    queries: VectorBatchInput,
    options: SearchOptions = {}
  ): Promise<Uint8Array[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytesBatch(nativeVectors(queries), nativeSearchOptions(options))
    );
  }

  async searchVectorsBatch(
    queries: VectorBatchInput,
    options: SearchOptions = {}
  ): Promise<number[][][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectorsBatch(nativeVectors(queries), nativeSearchOptions(options))
    );
  }

  async searchIdsBatchBuffer(
    queries: Float32Array,
    options: SearchOptions = {}
  ): Promise<string[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdsBatchBuffer(queries, nativeSearchOptions(options))
    );
  }

  async searchIdBytesBatchBuffer(
    queries: Float32Array,
    options: SearchOptions = {}
  ): Promise<Uint8Array[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytesBatchBuffer(queries, nativeSearchOptions(options))
    );
  }

  async searchVectorsBatchBuffer(
    queries: Float32Array,
    options: SearchOptions = {}
  ): Promise<number[][][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectorsBatchBuffer(queries, nativeSearchOptions(options))
    );
  }

  async searchWithReport(query: VectorInput, options: SearchOptions = {}): Promise<SearchReport> {
    return wrapNativeError(() =>
      normalizeSearchReport(this.#inner.searchWithReport(nativeVector(query), nativeSearchOptions(options)))
    );
  }

  async searchBatchWithReport(
    queries: VectorBatchInput,
    options: SearchOptions = {}
  ): Promise<SearchReport[]> {
    return wrapNativeError(() =>
      this.#inner
        .searchBatchWithReport(nativeVectors(queries), nativeSearchOptions(options))
        .map(normalizeSearchReport)
    );
  }

  async searchBatchWithReportBuffer(
    queries: Float32Array,
    options: SearchOptions = {}
  ): Promise<SearchReport[]> {
    return wrapNativeError(() =>
      this.#inner
        .searchBatchWithReportBuffer(queries, nativeSearchOptions(options))
        .map(normalizeSearchReport)
    );
  }

  async compact(options: CompactionOptions = {}): Promise<CompactionReport> {
    return wrapNativeError(() => this.#inner.compact({
      sourceLevel: options.sourceLevel,
      source_level: options.sourceLevel,
      targetLevel: options.targetLevel,
      target_level: options.targetLevel,
      maxSegments: options.maxSegments,
      max_segments: options.maxSegments,
      allMatching: options.allMatching,
      all_matching: options.allMatching,
      minSegments: options.minSegments,
      min_segments: options.minSegments,
      targetSegmentMaxVectors: options.targetSegmentMaxVectors,
      target_segment_max_vectors: options.targetSegmentMaxVectors
    }));
  }

  async rebuild(options: RebuildOptions = {}): Promise<RebuildReport> {
    return wrapNativeError(() => this.#inner.rebuild({
      sourceLevel: options.sourceLevel,
      source_level: options.sourceLevel,
      targetLevel: options.targetLevel,
      target_level: options.targetLevel,
      minSegments: options.minSegments,
      min_segments: options.minSegments,
      targetSegmentMaxVectors: options.targetSegmentMaxVectors,
      target_segment_max_vectors: options.targetSegmentMaxVectors,
      deleteObsolete: options.deleteObsolete,
      delete_obsolete: options.deleteObsolete
    }));
  }

  async gcObsoleteSegments(
    options: GarbageCollectionOptions = {}
  ): Promise<GarbageCollectionReport> {
    return wrapNativeError(() => this.#inner.gcObsoleteSegments({
      dryRun: options.dryRun,
      dry_run: options.dryRun
    }));
  }
}

function normalizeHit(hit: NativeHit): Hit {
  const idBytes = hit.idBytes ?? hit.id_bytes;
  if (!idBytes) {
    throw new BorsukError("native search hit did not include idBytes");
  }
  return {
    id: hit.id,
    idBytes,
    distance: hit.distance
  };
}

function normalizeHits(hits: NativeHit[]): Hit[] {
  return hits.map(normalizeHit);
}

function addIds(idsOrOptions: AddOptions | IdsInput): IdsInput | null {
  if (Array.isArray(idsOrOptions)) {
    return idsOrOptions;
  }
  return (idsOrOptions as AddOptions).ids ?? null;
}

function idsAreAllStrings(ids: IdsInput): ids is readonly string[] {
  return ids.every((id) => typeof id === "string");
}

function idsContainIntegers(ids: IdsInput): boolean {
  return ids.some((id) => typeof id === "number" || typeof id === "bigint");
}

function nativeStringIds(ids: readonly string[] | null): string[] | null {
  return ids === null ? null : [...ids];
}

function nativeIdByte(id: RecordId): Uint8Array {
  if (typeof id === "string") {
    return Buffer.from(id, "utf8");
  }
  if (typeof id === "number" || typeof id === "bigint") {
    return integerIdBytes(id);
  }
  return new Uint8Array(id);
}

function integerIdBytes(id: number | bigint): Uint8Array {
  let value: bigint;
  if (typeof id === "number") {
    if (!Number.isSafeInteger(id)) {
      throw new BorsukError("integer record ids must be safe integers");
    }
    value = BigInt(id);
  } else {
    value = id;
  }
  if (value < 0n) {
    throw new BorsukError("integer record ids must be non-negative");
  }

  const bytes: number[] = [];
  do {
    let byte = Number(value & 0x7fn);
    value >>= 7n;
    if (value !== 0n) {
      byte |= 0x80;
    }
    bytes.push(byte);
  } while (value !== 0n);

  return Uint8Array.from(bytes);
}

function nativeIdBytes(ids: IdsInput): Uint8Array[] {
  return ids.map(nativeIdByte);
}

function nativeVector(vector: VectorInput): number[] {
  return [...vector];
}

function nativeVectors(vectors: VectorBatchInput): number[][] {
  return vectors.map(nativeVector);
}

function normalizeSearchReport(report: NativeSearchReport): SearchReport {
  return {
    ...report,
    hits: normalizeHits(report.hits)
  };
}

function nativeSearchOptions(options: SearchOptions): NativeSearchOptions {
  const maxBytesNumber = typeof options.maxBytes === "number" ? options.maxBytes : undefined;
  const maxBytesText = typeof options.maxBytes === "string" ? options.maxBytes : undefined;

  return {
      k: options.k,
      mode: options.mode,
      leafMode: options.leafMode,
      leaf_mode: options.leafMode,
      eps: options.eps,
      maxSegments: options.maxSegments,
      max_segments: options.maxSegments,
      maxBytes: maxBytesNumber,
      max_bytes: maxBytesNumber,
      maxBytesText: maxBytesText,
      max_bytes_text: maxBytesText,
      maxLatencyMs: options.maxLatencyMs,
      max_latency_ms: options.maxLatencyMs,
      maxCandidatesPerSegment: options.maxCandidatesPerSegment,
      max_candidates_per_segment: options.maxCandidatesPerSegment
  };
}

export async function create(options: CreateOptions): Promise<Index> {
  const inner = wrapNativeError(() => native.create({
    uri: options.uri,
    metric: options.metric,
    dim: options.dim,
    dimensions: options.dimensions,
    segmentSize: options.segmentSize,
    segmentMaxVectors: options.segmentMaxVectors,
    segment_size: options.segmentSize,
    segment_max_vectors: options.segmentMaxVectors,
    ramBudget: options.ramBudget,
    ram_budget: options.ramBudget,
    cacheDir: options.cacheDir,
    cache_dir: options.cacheDir
  }));
  return new Index(options.uri, inner);
}

export function open(uri: string, options: OpenOptions = {}): Index {
  return new Index(uri, wrapNativeError(() => native.open(uri, {
    cacheDir: options.cacheDir,
    cache_dir: options.cacheDir,
    ramBudget: options.ramBudget,
    ram_budget: options.ramBudget,
    residentRouting: options.residentRouting,
    resident_routing: options.residentRouting
  })));
}

export function recallAtK(
  exactIds: readonly string[],
  actualIds: readonly string[],
  k: number
): number {
  return wrapNativeError(() => native.recallAtK([...exactIds], [...actualIds], k));
}

export function leafModeNames(): CanonicalLeafModeName[] {
  return wrapNativeError(() => native.leafModeNames() as CanonicalLeafModeName[]);
}

export function vectorDistance(
  metric: VectorMetric,
  left: readonly number[],
  right: readonly number[]
): number {
  return wrapNativeError(() => native.vectorDistance(metric, [...left], [...right]));
}

export function minkowskiMetric(p: number): MinkowskiMetricName {
  if (!Number.isFinite(p) || p < 1) {
    throw new TypeError("Minkowski power must be greater than or equal to 1");
  }
  return `minkowski:${p}` as MinkowskiMetricName;
}

export function vectorMetricNames(): CanonicalVectorMetricName[] {
  return wrapNativeError(() => native.vectorMetricNames() as CanonicalVectorMetricName[]);
}

function wrapNativeError<T>(operation: () => T): T {
  try {
    return operation();
  } catch (error) {
    throw toBorsukError(error);
  }
}

function toBorsukError(error: unknown): BorsukError {
  if (error instanceof BorsukError) {
    return error;
  }

  if (error instanceof Error) {
    return new BorsukError(error.message, error);
  }

  return new BorsukError(String(error), error);
}
