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

export enum StringMetricName {
  Levenshtein = "levenshtein",
  NormalizedLevenshtein = "normalized-levenshtein",
  DamerauLevenshtein = "damerau-levenshtein",
  NormalizedDamerauLevenshtein = "normalized-damerau-levenshtein",
  OptimalStringAlignment = "optimal-string-alignment",
  Hamming = "hamming",
  Jaro = "jaro",
  JaroWinkler = "jaro-winkler",
  SorensenDice = "sorensen-dice"
}

export enum SearchMode {
  Exact = "exact",
  Approx = "approx"
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

export type CanonicalStringMetricName = `${StringMetricName}`;
export type StringMetricAlias =
  | "edit"
  | "edit-distance"
  | "normalized-edit"
  | "normalized-edit-distance"
  | "damerau"
  | "normalized-damerau"
  | "osa"
  | "jarowinkler"
  | "sorensendice"
  | "dice";
export type StringMetric = CanonicalStringMetricName | StringMetricAlias;
export type SearchModeName = `${SearchMode}`;

export interface Hit {
  id: string;
  distance: number;
  payloadRef: string | null;
}

export interface IndexStats {
  metric: VectorMetric;
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
  eps?: number;
  maxSegments?: number;
  maxBytes?: number | string;
  maxLatencyMs?: number;
  maxCandidatesPerSegment?: number;
}

export interface AddOptions {
  payloadRefs?: Array<string | null | undefined>;
}

interface NativeModule {
  Index: new (uri: string) => NativeIndex;
  create(options: NativeCreateOptions): NativeIndex;
  open(uri: string, options?: NativeOpenOptions): NativeIndex;
  recallAtK(exactIds: string[], actualIds: string[], k: number): number;
  stringDistance(metric: string, left: string, right: string): number;
  stringMetricNames(): string[];
  vectorDistance(metric: string, left: number[], right: number[]): number;
  vectorMetricNames(): string[];
}

interface NativeIndex {
  add(ids: string[], vectors: number[][], payloadRefs?: Array<string | null | undefined>): void;
  addBuffer(
    ids: string[],
    vectors: Float32Array,
    payloadRefs?: Array<string | null | undefined>
  ): void;
  stats(): IndexStats;
  search(query: number[], options?: NativeSearchOptions): NativeHit[];
  searchBuffer(query: Float32Array, options?: NativeSearchOptions): NativeHit[];
  searchWithReportBuffer(query: Float32Array, options?: NativeSearchOptions): NativeSearchReport;
  searchBatch(queries: number[][], options?: NativeSearchOptions): NativeHit[][];
  searchBatchBuffer(queries: Float32Array, options?: NativeSearchOptions): NativeHit[][];
  searchWithReport(query: number[], options?: NativeSearchOptions): NativeSearchReport;
  searchBatchWithReport(queries: number[][], options?: NativeSearchOptions): NativeSearchReport[];
  searchBatchWithReportBuffer(
    queries: Float32Array,
    options?: NativeSearchOptions
  ): NativeSearchReport[];
  compact(options?: NativeCompactionOptions): CompactionReport;
  gcObsoleteSegments(options?: NativeGarbageCollectionOptions): GarbageCollectionReport;
}

interface NativeHit {
  id: string;
  distance: number;
  payloadRef?: string | null;
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

interface OpenOptions {
  cacheDir?: string;
  ramBudget?: string;
}

interface NativeOpenOptions {
  cacheDir?: string;
  cache_dir?: string;
  ramBudget?: string;
  ram_budget?: string;
}

interface NativeSearchOptions {
  k?: number;
  mode?: string;
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
  minSegments?: number;
  min_segments?: number;
  targetSegmentMaxVectors?: number;
  target_segment_max_vectors?: number;
}

interface NativeGarbageCollectionOptions {
  dryRun?: boolean;
  dry_run?: boolean;
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

  constructor(uri: string, inner?: NativeIndex) {
    this.#inner = inner ?? wrapNativeError(() => new native.Index(uri));
  }

  async add(ids: string[], vectors: number[][], options: AddOptions = {}): Promise<void> {
    return wrapNativeError(() => this.#inner.add(ids, vectors, options.payloadRefs));
  }

  async addBuffer(ids: string[], vectors: Float32Array, options: AddOptions = {}): Promise<void> {
    return wrapNativeError(() => this.#inner.addBuffer(ids, vectors, options.payloadRefs));
  }

  async stats(): Promise<IndexStats> {
    return wrapNativeError(() => this.#inner.stats());
  }

  async search(query: number[], options: SearchOptions = {}): Promise<Hit[]> {
    return wrapNativeError(() =>
      normalizeHits(this.#inner.search(query, nativeSearchOptions(options)))
    );
  }

  async searchBuffer(query: Float32Array, options: SearchOptions = {}): Promise<Hit[]> {
    return wrapNativeError(() =>
      normalizeHits(this.#inner.searchBuffer(query, nativeSearchOptions(options)))
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

  async searchBatch(queries: number[][], options: SearchOptions = {}): Promise<Hit[][]> {
    return wrapNativeError(() =>
      this.#inner.searchBatch(queries, nativeSearchOptions(options)).map(normalizeHits)
    );
  }

  async searchBatchBuffer(queries: Float32Array, options: SearchOptions = {}): Promise<Hit[][]> {
    return wrapNativeError(() =>
      this.#inner.searchBatchBuffer(queries, nativeSearchOptions(options)).map(normalizeHits)
    );
  }

  async searchWithReport(query: number[], options: SearchOptions = {}): Promise<SearchReport> {
    return wrapNativeError(() =>
      normalizeSearchReport(this.#inner.searchWithReport(query, nativeSearchOptions(options)))
    );
  }

  async searchBatchWithReport(
    queries: number[][],
    options: SearchOptions = {}
  ): Promise<SearchReport[]> {
    return wrapNativeError(() =>
      this.#inner
        .searchBatchWithReport(queries, nativeSearchOptions(options))
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
      minSegments: options.minSegments,
      min_segments: options.minSegments,
      targetSegmentMaxVectors: options.targetSegmentMaxVectors,
      target_segment_max_vectors: options.targetSegmentMaxVectors
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
  return {
    ...hit,
    payloadRef: hit.payloadRef ?? null
  };
}

function normalizeHits(hits: NativeHit[]): Hit[] {
  return hits.map(normalizeHit);
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
    ram_budget: options.ramBudget
  })));
}

export function recallAtK(exactIds: string[], actualIds: string[], k: number): number {
  return wrapNativeError(() => native.recallAtK(exactIds, actualIds, k));
}

export function stringDistance(metric: StringMetric, left: string, right: string): number {
  return wrapNativeError(() => native.stringDistance(metric, left, right));
}

export function stringMetricNames(): CanonicalStringMetricName[] {
  return wrapNativeError(() => native.stringMetricNames() as CanonicalStringMetricName[]);
}

export function vectorDistance(metric: VectorMetric, left: number[], right: number[]): number {
  return wrapNativeError(() => native.vectorDistance(metric, left, right));
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
