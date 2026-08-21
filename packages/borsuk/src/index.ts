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
  Clark = "clark",
}

export enum SearchMode {
  Exact = "exact",
  Approx = "approx",
}

export enum LeafModeName {
  FlatScan = "flat-scan",
  SqScan = "sq-scan",
  PqScan = "pq-scan",
  SrhtPqScan = "srht-pq-scan",
  FastTurboQuantMseScan = "fast-turboquant-mse-scan",
  FastTurboQuantScan = "fast-turboquant-scan",
  Graph = "graph",
  VamanaPq = "vamana-pq",
  Hybrid = "hybrid",
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
  "complete" | "exact-pruned" | "epsilon" | "max-segments" | "max-bytes" | "max-latency";
export type RecallGuarantee = "exact" | "budget-complete" | "degraded";
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
export type CacheExecutionPolicy = "scan" | "graph" | "auto";
export type GlobalScanCodec =
  "pq-scan" | "srht-pq-scan" | "fast-turboquant-mse-scan" | "fast-turboquant-scan";

export interface Hit {
  id: string;
  idBytes: Uint8Array;
  distance: number;
  /** Present only when the search requested `includeMetadata: true`. */
  metadata?: Record<string, unknown> | null;
}

/** A stored vector together with its metadata, returned by {@link Index.getRecord}. */
export interface GetRecord {
  vector: number[];
  metadata: Record<string, unknown>;
}

/** A record enumerated by {@link Index.listRecords}: id plus its stored value. */
export interface ListedRecord {
  id: string;
  vector: number[];
  metadata: Record<string, unknown>;
}

export interface IndexStats {
  metric: CanonicalVectorMetricName | MinkowskiMetricName;
  dimensions: number;
  segmentMaxVectors: number;
  ramBudgetBytes?: number | null;
  text: boolean;
  namedVectors: string[];
  sparseEncodedVectors: number;
  denseEncodedVectors: number;
  manifestVersion: number;
  routingMaxLevel: number;
  routingPageFanout: number;
  routingLeafPages: number;
  routingPages: number;
  segments: number;
  records: number;
  segmentBytes: number;
  graphBytes: number;
  residentBytesEstimate: number;
  preparedPositionedBytes: number;
  collectionResidentBytes: number;
  retainedBytes: number;
  retainedCapacityBytes: number;
  retainedPeakBytes: number;
  transientBytes: number;
  transientCapacityBytes: number;
  transientPeakBytes: number;
}

export interface WarmReport {
  segmentsLoaded: number;
  segmentsTotal: number;
  segmentsResident: number;
  graphsResident: number;
  coverageComplete: boolean;
  bytesResident: number;
}

/** Object-store requests issued while executing an operation, including retries. */
export interface RequestCounts {
  gets: number;
  puts: number;
  deletes: number;
  heads: number;
  lists: number;
  total: number;
}

export interface SearchReport {
  hits: Hit[];
  leafMode: CanonicalLeafModeName;
  terminationReason: SearchTerminationReason;
  recallGuarantee: RecallGuarantee;
  segmentsTotal: number;
  segmentsSearched: number;
  segmentsSkipped: number;
  routingPageIndexesRead: number;
  routingPagesRead: number;
  bytesRead: number;
  prefetchedBytesUnused: number;
  graphBytesRead: number;
  decodedCacheHits: number;
  decodedCacheBytesRead: number;
  objectCacheHits: number;
  objectCacheMisses: number;
  diskCacheBytesRead: number;
  backingBytesRead: number;
  diskCacheReads: number;
  backingReads: number;
  cacheRepairs: number;
  recordsConsidered: number;
  recordsScored: number;
  graphCandidatesAdded: number;
  globalGraphChunksSearched: number;
  globalScanChunksSearched: number;
  /** Aggregate global routing/PQ candidate work across merged immutable runs. */
  globalBaseApproximateUs: number;
  /** Time waiting for head-stage transient-memory admission. */
  globalBaseHeadAdmissionUs: number;
  /** Time completing authenticated head fetch waves, including leaf-read admission. */
  globalBaseHeadFetchUs: number;
  /** Time waiting for bounded head decode/rank admission. */
  globalBaseHeadDecodeAdmissionUs: number;
  /** Time decoding authenticated global routing heads. */
  globalBaseHeadDecodeUs: number;
  /** Time waiting for exact-wave transient-memory admission. */
  globalBaseExactAdmissionUs: number;
  /** Time fetching exact-vector ranges. */
  globalBaseExactFetchUs: number;
  /** Time decoding, resolving, and exact-scoring fetched candidates. */
  globalBaseExactCpuUs: number;
  /** Aggregate global exact-fetch/rerank work across merged immutable runs. */
  globalBaseExactRerankUs: number;
  residentBytesEstimate: number;
  preparedPositionedBytes: number;
  collectionResidentBytes: number;
  retainedBytes: number;
  retainedCapacityBytes: number;
  retainedPeakBytes: number;
  transientBytes: number;
  transientCapacityBytes: number;
  transientPeakBytes: number;
  elapsedMs: number;
  requests: RequestCounts;
  /** Candidate records inspected by the metadata filter (0 when no filter is set). */
  rowsEvaluated: number;
  /** Records that satisfied the metadata filter and were eligible for ranking. */
  rowsPassedFilter: number;
  /** Segments skipped entirely because their metadata statistics ruled out the filter. */
  segmentsPrunedByFilter: number;
}

export interface AddReport {
  segmentsWritten: number;
  graphPayloadsWritten: number;
  manifestTablesWritten: number;
  routingPagesWritten: number;
  totalBytesWritten: number;
  bytesPerVector: number;
  requests: RequestCounts;
}

export interface AddWithReportResult<TId extends string = string> {
  ids: TId[];
  report: AddReport;
}

export interface CompactionOptions {
  sourceLevel?: number;
  targetLevel?: number;
  maxSegments?: number;
  allMatching?: boolean;
  minSegments?: number;
  targetSegmentMaxVectors?: number;
  targetSegmentMaxRadius?: number;
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
  minAgeMs?: number;
}

export interface GarbageCollectionReport {
  dryRun: boolean;
  objectsScanned: number;
  objectsDeleted: number;
  routingObjectsDeleted: number;
  tablesDeleted: number;
  routingPageIndexesRead: number;
  routingPagesRead: number;
  bytesRead: number;
  bytesReclaimable: number;
  bytesReclaimed: number;
  objectCacheHits: number;
  objectCacheMisses: number;
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

export interface DeleteReport {
  /** Unique ids in this request after canonical deduplication. */
  idsSubmitted: number;
  /** Whether this handle emitted a positioned mutation. */
  published: boolean;
  requests: RequestCounts;
}

export interface PurgeReport {
  segmentsRewritten: number;
  recordsPurged: number;
  tombstonesCleared: number;
  published: boolean;
  requests: RequestCounts;
}

export interface IncrementalOptions {
  maxSegmentVectors?: number;
  maxSegmentRadius?: number;
  minSegmentVectors?: number;
  maxOperations?: number;
}

export interface IncrementalReport {
  splits: number;
  merges: number;
  segmentsCreated: number;
  segmentsRemoved: number;
  recordsMoved: number;
  published: boolean;
  requests: RequestCounts;
}

export type VectorElementType =
  "float32" | "float16" | "bfloat16" | "float8-e4m3fn" | "float8-e5m2" | "fp8" | "int8" | "binary";

export interface CreateOptions {
  uri: string;
  metric: VectorMetric;
  vectorElementType?: VectorElementType;
  dim?: number;
  dimensions?: number;
  segmentSize?: number;
  segmentMaxVectors?: number;
  routingPageFanout?: number;
  graphNeighbors?: number;
  leafCapability?: "pq-scan-only" | "graph-enabled";
  globalPqLayout?: "adaptive" | "flat-256" | "product-2x64" | `hierarchical-${number}`;
  globalPqCodeBytes?: 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 256;
  globalScanCodec?: GlobalScanCodec;
  /** Zero or omitted selects the codec-specific qualified default. */
  globalTurboquantBits?: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;
  globalTurboquantQjlBits?: number;
  globalTurboquantShards?: number;
  ramBudget?: ByteSize;
  cacheDir?: string;
  text?: boolean;
  namedVectors?: Record<string, NamedVectorSpecInput>;
}

export interface SearchOptions {
  k?: number;
  mode?: SearchModeName;
  leafMode?: LeafMode;
  cacheExecution?: CacheExecutionPolicy;
  eps?: number;
  maxSegments?: number;
  maxBytes?: ByteSize;
  maxLatencyMs?: number;
  routingPageOverfetch?: number;
  maxCandidatesPerSegment?: number;
  guaranteedRecall?: boolean;
  prefetchDepth?: number;
  /**
   * Metadata filter applied before ranking. Accepts a Pinecone-style operator
   * dictionary, e.g. `{ genre: "rock", year: { $gte: 1990 } }`. Records whose
   * metadata does not satisfy the filter are never returned.
   */
  filter?: Record<string, unknown>;
  /** Return each hit's stored metadata under {@link Hit.metadata} (default false). */
  includeMetadata?: boolean;
  /** Named vector to search; empty string selects the primary vector. */
  vector?: string;
}

export type VectorInput = readonly number[];
export type VectorBatchInput = readonly VectorInput[];
export type RecordId = string | Uint8Array | number | bigint;
export type IdsInput = readonly RecordId[];
export type ByteSize = number | string;

export interface SparseVectorInput {
  indices: readonly number[];
  values: readonly number[];
}

export interface NamedVectorSpecInput {
  dimensions: number;
  metric: VectorMetric;
  /**
   * `"dense"` (default), `"sparse"` (inverted index), or
   * `"late-interaction"` (flattened token ANN plus exact MaxSim).
   */
  kind?: "dense" | "sparse" | "late-interaction";
  elementType?: VectorElementType;
}

export type LateInteractionVectorInput = readonly VectorInput[];
export type NamedVectorInput = VectorInput | SparseVectorInput | LateInteractionVectorInput;
export type NamedVectorRecordInput = Record<string, NamedVectorInput>;
export type HybridVectorInput = Record<string, NamedVectorInput>;

export interface AddRecordOptions {
  /**
   * Per-vector metadata, aligned positionally with `vectors`. Each entry is a
   * JSON-like object (`null` or omitted entries store empty metadata). Only
   * supported alongside string ids.
   */
  metadata?: readonly (Record<string, unknown> | null | undefined)[];
  /** Per-vector sparse vector inputs, aligned positionally with `vectors`; non-null entries replace that row's dense vector input. */
  sparse?: readonly (SparseVectorInput | null | undefined)[];
  /** Per-vector text payloads, aligned positionally with `vectors`; `null` stores no text. */
  text?: readonly (string | null | undefined)[];
  /** Per-vector named vectors, aligned positionally with `vectors`; `null` stores no named vectors. */
  namedVectors?: readonly (NamedVectorRecordInput | null | undefined)[];
}

export interface AddOptions<TId extends RecordId = RecordId> extends AddRecordOptions {
  ids?: readonly TId[];
}

export interface KSearchOptions {
  k?: number;
}

export type HybridFusion = "rrf" | "weighted";

export interface HybridQuery {
  vectors?: HybridVectorInput;
  text?: string;
}

export interface HybridSearchOptions {
  k?: number;
  fusion?: HybridFusion;
  rrfK?: number;
  weights?: Record<string, number>;
}

interface NativeModule {
  Index: new (uri: string) => NativeIndex;
  create(options: NativeCreateOptions): Promise<NativeIndex>;
  open(uri: string, options?: NativeOpenOptions): Promise<NativeIndex>;
  leafModeNames(): string[];
  recallAtK(exactIds: string[], actualIds: string[], k: number): number;
  tieAwareRecallAtK(exactDistances: number[], actualDistances: number[], k: number): number;
  vectorDistance(metric: string, left: number[], right: number[]): number;
  vectorMetricNames(): string[];
}

interface NativeSparseVectorInput {
  indices: number[];
  values: number[];
}

interface NativeNamedVectorSpecInput {
  name: string;
  dimensions: number;
  metric: string;
  kind?: string;
  elementType?: string;
  element_type?: string;
}

/** A query's plan and estimated object-storage cost, returned by {@link Index.explain}. */
export interface ExplainReport {
  hits: string[];
  getRequests: number;
  bytesRead: number;
  estimatedCostUsd: number;
  cacheHitRatio: number;
  elapsedMs: number;
  segmentsTotal: number;
  segmentsSearched: number;
  segmentsSkipped: number;
  segmentsPrunedByFilter: number;
}

interface NativeNamedVectorEntryInput {
  name: string;
  vector?: number[];
  vectors?: number[][];
  sparse?: NativeSparseVectorInput;
}

interface NativeHybridQuery {
  vectors?: NativeNamedVectorEntryInput[];
  text?: string;
}

interface NativeKSearchOptions {
  k?: number;
}

interface NativeHybridOptions {
  k?: number;
  fusion?: string;
  rrfK?: number;
  rrf_k?: number;
  weights?: NativeNamedWeightInput[];
}

interface NativeNamedWeightInput {
  name: string;
  weight: number;
}

type NativeSparseRows = (NativeSparseVectorInput | null)[] | null;
type NativeTextRows = (string | null)[] | null;
type NativeNamedVectorRows = (NativeNamedVectorEntryInput[] | null)[] | null;

interface NativeIndex {
  add(
    vectors: number[][],
    ids?: string[] | null,
    metadata?: unknown[] | null,
    sparse?: NativeSparseRows,
    text?: NativeTextRows,
    namedVectors?: NativeNamedVectorRows,
  ): Promise<string[]>;
  upsert(
    vectors: number[][],
    ids: string[],
    metadata?: unknown[] | null,
    sparse?: NativeSparseRows,
    text?: NativeTextRows,
    namedVectors?: NativeNamedVectorRows,
  ): Promise<string[]>;
  addWithReport(vectors: number[][], ids?: string[] | null): Promise<AddWithReportResult>;
  addIdBytes(vectors: number[][], ids: Uint8Array[]): Promise<Uint8Array[]>;
  addBuffer(vectors: Float32Array, ids?: string[] | null): Promise<string[]>;
  addBufferIdBytes(vectors: Float32Array, ids: Uint8Array[]): Promise<Uint8Array[]>;
  stats(): Promise<IndexStats>;
  refresh(): Promise<boolean>;
  flush(): Promise<void>;
  warm(): Promise<WarmReport>;
  searchIds(query: number[], options?: NativeSearchOptions): Promise<string[]>;
  explain(
    query: number[],
    options?: NativeSearchOptions,
    requestPricePerMillion?: number,
    dataPricePerGib?: number,
  ): Promise<ExplainReport>;
  searchSparseNamed(
    name: string,
    indices: number[],
    values: number[],
    k?: number,
  ): Promise<string[]>;
  searchLateInteraction(
    name: string,
    queryTokens: number[][],
    options?: NativeKSearchOptions,
  ): Promise<Hit[]>;
  searchIdBytes(query: number[], options?: NativeSearchOptions): Promise<Uint8Array[]>;
  searchVectors(query: number[], options?: NativeSearchOptions): Promise<number[][]>;
  searchText(text: string, options?: NativeKSearchOptions): Promise<string[]>;
  searchTextWithReport(text: string, options?: NativeKSearchOptions): Promise<NativeSearchReport>;
  searchHybrid(query: NativeHybridQuery, options?: NativeHybridOptions): Promise<string[]>;
  searchHybridWithReport(
    query: NativeHybridQuery,
    options?: NativeHybridOptions,
  ): Promise<NativeSearchReport>;
  getVector(id: string): Promise<number[] | null>;
  getVectorById(id: Uint8Array): Promise<number[] | null>;
  getRecord(id: string): Promise<NativeGetRecord | null>;
  listRecords(offset: number, limit: number): Promise<NativeListedRecord[]>;
  searchIdsBuffer(query: Float32Array, options?: NativeSearchOptions): Promise<string[]>;
  searchIdBytesBuffer(query: Float32Array, options?: NativeSearchOptions): Promise<Uint8Array[]>;
  searchVectorsBuffer(query: Float32Array, options?: NativeSearchOptions): Promise<number[][]>;
  searchWithReportBuffer(
    query: Float32Array,
    options?: NativeSearchOptions,
  ): Promise<NativeSearchReport>;
  searchIdsBatch(queries: number[][], options?: NativeSearchOptions): Promise<string[][]>;
  searchIdBytesBatch(queries: number[][], options?: NativeSearchOptions): Promise<Uint8Array[][]>;
  searchVectorsBatch(queries: number[][], options?: NativeSearchOptions): Promise<number[][][]>;
  searchIdsBatchBuffer(queries: Float32Array, options?: NativeSearchOptions): Promise<string[][]>;
  searchIdBytesBatchBuffer(
    queries: Float32Array,
    options?: NativeSearchOptions,
  ): Promise<Uint8Array[][]>;
  searchVectorsBatchBuffer(
    queries: Float32Array,
    options?: NativeSearchOptions,
  ): Promise<number[][][]>;
  searchWithReport(query: number[], options?: NativeSearchOptions): Promise<NativeSearchReport>;
  searchBatchWithReport(
    queries: number[][],
    options?: NativeSearchOptions,
  ): Promise<NativeSearchReport[]>;
  searchBatchWithReportBuffer(
    queries: Float32Array,
    options?: NativeSearchOptions,
  ): Promise<NativeSearchReport[]>;
  compact(options?: NativeCompactionOptions): Promise<CompactionReport>;
  rebuild(options?: NativeRebuildOptions): Promise<RebuildReport>;
  gcObsoleteSegments(options?: NativeGarbageCollectionOptions): Promise<GarbageCollectionReport>;
  delete(ids: string[]): Promise<DeleteReport>;
  purge(): Promise<PurgeReport>;
  maintain(options?: IncrementalOptions): Promise<IncrementalReport>;
}

interface NativeHit {
  id: string;
  idBytes?: Uint8Array;
  id_bytes?: Uint8Array;
  distance: number;
  metadata?: Record<string, unknown> | null;
}

interface NativeGetRecord {
  vector: number[];
  metadata?: Record<string, unknown> | null;
}

interface NativeListedRecord {
  id: string;
  vector: number[];
  metadata?: Record<string, unknown> | null;
}

interface NativeSearchReport extends Omit<SearchReport, "hits"> {
  hits: NativeHit[];
}

interface NativeCreateOptions {
  uri: string;
  metric: string;
  vectorElementType?: string;
  vector_element_type?: string;
  dim?: number;
  dimensions?: number;
  segmentSize?: number;
  segmentMaxVectors?: number;
  routingPageFanout?: number;
  graphNeighbors?: number;
  leafCapability?: "pq-scan-only" | "graph-enabled";
  globalPqLayout?: string;
  globalPqCodeBytes?: number;
  globalScanCodec?: string;
  /** Zero or omitted selects the codec-specific qualified default. */
  globalTurboquantBits?: number;
  globalTurboquantQjlBits?: number;
  globalTurboquantShards?: number;
  segment_size?: number;
  segment_max_vectors?: number;
  routing_page_fanout?: number;
  graph_neighbors?: number;
  leaf_capability?: "pq-scan-only" | "graph-enabled";
  global_pq_layout?: string;
  global_pq_code_bytes?: number;
  global_scan_codec?: string;
  /** Zero or omitted selects the codec-specific qualified default. */
  global_turboquant_bits?: number;
  global_turboquant_qjl_bits?: number;
  global_turboquant_shards?: number;
  ramBudget?: string;
  ram_budget?: string;
  cacheDir?: string;
  cache_dir?: string;
  text?: boolean;
  namedVectors?: NativeNamedVectorSpecInput[];
  named_vectors?: NativeNamedVectorSpecInput[];
}

export interface OpenOptions {
  cacheDir?: string;
  cacheMaxBytes?: ByteSize;
  ramBudget?: ByteSize;
  residentRouting?: boolean;
  preload?: boolean;
  maxActiveSearches?: number;
  maxWaitingSearches?: number;
  leafReadWidth?: number;
  maxInflightLeafReads?: number;
  maxParallelDecodeRankTasks?: number;
  exactReadMaxPhysicalAmplification?: number;
}

interface NativeOpenOptions {
  cacheDir?: string;
  cache_dir?: string;
  cacheMaxBytes?: string;
  cache_max_bytes?: string;
  ramBudget?: string;
  ram_budget?: string;
  residentRouting?: boolean;
  resident_routing?: boolean;
  preload?: boolean;
  maxActiveSearches?: number;
  max_active_searches?: number;
  maxWaitingSearches?: number;
  max_waiting_searches?: number;
  leafReadWidth?: number;
  leaf_read_width?: number;
  maxInflightLeafReads?: number;
  max_inflight_leaf_reads?: number;
  maxParallelDecodeRankTasks?: number;
  max_parallel_decode_rank_tasks?: number;
  exactReadMaxPhysicalAmplification?: number;
  exact_read_max_physical_amplification?: number;
}

interface NativeSearchOptions {
  k?: number;
  mode?: string;
  leafMode?: string;
  leaf_mode?: string;
  cacheExecution?: string;
  cache_execution?: string;
  eps?: number;
  maxSegments?: number;
  max_segments?: number;
  maxBytes?: number;
  max_bytes?: number;
  maxBytesText?: string;
  max_bytes_text?: string;
  maxLatencyMs?: number;
  max_latency_ms?: number;
  routingPageOverfetch?: number;
  routing_page_overfetch?: number;
  maxCandidatesPerSegment?: number;
  max_candidates_per_segment?: number;
  guaranteedRecall?: boolean;
  guaranteed_recall?: boolean;
  prefetchDepth?: number;
  prefetch_depth?: number;
  filter?: unknown;
  includeMetadata?: boolean;
  include_metadata?: boolean;
  vector?: string;
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
  targetSegmentMaxRadius?: number;
  target_segment_max_radius?: number;
}

interface NativeGarbageCollectionOptions {
  dryRun?: boolean;
  dry_run?: boolean;
  minAgeMs?: number;
  min_age_ms?: number;
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
const nativeBorsukErrorPattern = /^\[borsuk:([a-z_]+)\] ([\s\S]*)$/;

export type BorsukErrorCode =
  | "arrow_error"
  | "checksum_mismatch"
  | "concurrent_modification"
  | "dimension_mismatch"
  | "index_not_found"
  | "invalid_compaction_input"
  | "invalid_metric_input"
  | "invalid_record_input"
  | "invalid_search_options"
  | "invalid_storage"
  | "io_error"
  | "object_store_error"
  | "parquet_error"
  | "ram_budget_exceeded"
  | "recall_guarantee_violated"
  | "runtime_error";

export class BorsukError extends Error {
  readonly cause?: unknown;
  readonly code: BorsukErrorCode;

  constructor(message: string, cause?: unknown, code: BorsukErrorCode = "runtime_error") {
    super(message);
    this.name = "BorsukError";
    this.cause = cause;
    this.code = code;
  }
}

export class Index {
  readonly #inner: NativeIndex;

  /** @internal */
  // Constructed by create() and open(); kept public only for module-local wiring.
  constructor(uri: string, inner: NativeIndex);
  constructor(_uri: string, inner: NativeIndex) {
    this.#inner = inner;
  }

  async add(vectors: VectorBatchInput): Promise<string[]>;
  async add(
    vectors: VectorBatchInput,
    ids: readonly string[],
    options?: AddRecordOptions,
  ): Promise<string[]>;
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
    idsOrOptions: AddOptions | IdsInput = {},
    options: AddRecordOptions = {},
  ): Promise<RecordId[]> {
    return wrapNativeError(() => {
      const ids = addIds(idsOrOptions);
      const metadata = addMetadata(idsOrOptions, options);
      const sparse = addSparse(idsOrOptions, options);
      const text = addText(idsOrOptions, options);
      const namedVectors = addNamedVectors(idsOrOptions, options);
      const nativeVectorsValue = nativeVectors(vectors);
      if (ids === null || idsAreAllStrings(ids)) {
        return this.#inner.add(
          nativeVectorsValue,
          nativeStringIds(ids),
          metadata,
          sparse,
          text,
          namedVectors,
        );
      }
      if (metadata !== null) {
        throw new BorsukError("metadata is only supported with string ids");
      }
      if (sparse !== null) {
        throw new BorsukError("sparse is only supported with string ids");
      }
      if (text !== null) {
        throw new BorsukError("text is only supported with string ids");
      }
      if (namedVectors !== null) {
        throw new BorsukError("namedVectors is only supported with string ids");
      }
      const added = this.#inner.addIdBytes(nativeVectorsValue, nativeIdBytes(ids));
      return idsContainIntegers(ids) ? [...ids] : added;
    });
  }

  /**
   * Insert or replace records by id (MVCC upsert). Existing ids are overwritten
   * atomically — reads immediately see the new record and the superseded version
   * is reclaimed by the next compaction. Ids are required.
   */
  async upsert(
    vectors: VectorBatchInput,
    ids: readonly string[],
    options: AddRecordOptions = {},
  ): Promise<string[]> {
    return wrapNativeError(() => {
      const metadata = addMetadata(ids, options);
      const sparse = addSparse(ids, options);
      const text = addText(ids, options);
      const namedVectors = addNamedVectors(ids, options);
      const nativeVectorsValue = nativeVectors(vectors);
      return this.#inner.upsert(nativeVectorsValue, [...ids], metadata, sparse, text, namedVectors);
    });
  }

  async addWithReport(vectors: VectorBatchInput): Promise<AddWithReportResult>;
  async addWithReport(
    vectors: VectorBatchInput,
    ids: readonly string[],
  ): Promise<AddWithReportResult<string>>;
  async addWithReport(
    vectors: VectorBatchInput,
    options: AddOptions<string>,
  ): Promise<AddWithReportResult<string>>;
  async addWithReport(
    vectors: VectorBatchInput,
    idsOrOptions: AddOptions<string> | readonly string[] = {},
  ): Promise<AddWithReportResult<string>> {
    return wrapNativeError(() => {
      const ids = addIds(idsOrOptions);
      const nativeVectorsValue = nativeVectors(vectors);
      if (ids !== null && !idsAreAllStrings(ids)) {
        throw new BorsukError("addWithReport ids must be strings");
      }
      return this.#inner.addWithReport(nativeVectorsValue, nativeStringIds(ids));
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
    idsOrOptions: AddOptions | IdsInput = {},
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

  /**
   * Advance this handle to the latest atomically published snapshot.
   * Returns `true` when the snapshot advanced and `false` when already current.
   * Until refresh succeeds, reads keep using the previously pinned snapshot.
   */
  async refresh(): Promise<boolean> {
    return wrapNativeError(() => this.#inner.refresh());
  }

  /**
   * Materialize the current WAL tail into immutable cells. Reads already see
   * unflushed records; call this before cell-level administration such as
   * compaction, or when an external workflow requires immutable objects.
   */
  async flush(): Promise<void> {
    return wrapNativeError(() => this.#inner.flush());
  }

  async warm(): Promise<WarmReport> {
    return wrapNativeError(() => this.#inner.warm());
  }

  async searchIds(query: VectorInput, options: SearchOptions = {}): Promise<string[]> {
    return wrapNativeError(() =>
      this.#inner.searchIds(nativeVector(query), nativeSearchOptions(options)),
    );
  }

  /**
   * Run a query and return its plan and estimated object-storage cost: GET/HEAD
   * requests, bytes read, routing pruning, cache hit ratio, latency, and a
   * dollar estimate under an S3-style cost model.
   */
  async explain(
    query: VectorInput,
    options: SearchOptions = {},
    cost: { requestPricePerMillion?: number; dataPricePerGib?: number } = {},
  ): Promise<ExplainReport> {
    return wrapNativeError(() =>
      this.#inner.explain(
        nativeVector(query),
        nativeSearchOptions(options),
        cost.requestPricePerMillion,
        cost.dataPricePerGib,
      ),
    );
  }

  /**
   * Search a sparse (inverted-index) named vector for the top `k` record ids by
   * inner-product similarity. Nothing is densified, so it scales to huge lexical
   * vocabularies.
   */
  async searchSparseNamed(
    name: string,
    indices: number[],
    values: number[],
    k = 10,
  ): Promise<string[]> {
    return wrapNativeError(() => this.#inner.searchSparseNamed(name, indices, values, k));
  }

  async searchLateInteraction(
    name: string,
    queryTokens: LateInteractionVectorInput,
    options: KSearchOptions = {},
  ): Promise<Hit[]> {
    return wrapNativeError(() =>
      this.#inner.searchLateInteraction(
        name,
        nativeVectors(queryTokens),
        nativeKSearchOptions(options),
      ),
    );
  }

  async searchIdBytes(query: VectorInput, options: SearchOptions = {}): Promise<Uint8Array[]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytes(nativeVector(query), nativeSearchOptions(options)),
    );
  }

  async searchVectors(query: VectorInput, options: SearchOptions = {}): Promise<number[][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectors(nativeVector(query), nativeSearchOptions(options)),
    );
  }

  async searchText(text: string, options: KSearchOptions = {}): Promise<string[]> {
    return wrapNativeError(() => this.#inner.searchText(text, nativeKSearchOptions(options)));
  }

  async searchTextWithReport(text: string, options: KSearchOptions = {}): Promise<SearchReport> {
    const report = await wrapNativeError(() =>
      this.#inner.searchTextWithReport(text, nativeKSearchOptions(options)),
    );
    return normalizeSearchReport(report);
  }

  async searchHybrid(query: HybridQuery, options: HybridSearchOptions = {}): Promise<string[]> {
    return wrapNativeError(() =>
      this.#inner.searchHybrid(nativeHybridQuery(query), nativeHybridOptions(options)),
    );
  }

  async searchHybridWithReport(
    query: HybridQuery,
    options: HybridSearchOptions = {},
  ): Promise<SearchReport> {
    const report = await wrapNativeError(() =>
      this.#inner.searchHybridWithReport(nativeHybridQuery(query), nativeHybridOptions(options)),
    );
    return normalizeSearchReport(report);
  }

  async getVector(id: RecordId): Promise<number[] | null> {
    return wrapNativeError(() =>
      typeof id === "string"
        ? this.#inner.getVector(id)
        : this.#inner.getVectorById(nativeIdByte(id)),
    );
  }

  /** Fetch a stored vector together with its metadata, or `null` when the id is absent. */
  async getRecord(id: string): Promise<GetRecord | null> {
    if (typeof id !== "string") {
      throw new TypeError("getRecord expects a string id");
    }
    return wrapNativeError(async () => {
      const record = await this.#inner.getRecord(id);
      if (record === null || record === undefined) {
        return null;
      }
      return { vector: record.vector, metadata: record.metadata ?? {} };
    });
  }

  async listRecords(offset: number, limit: number): Promise<ListedRecord[]> {
    return wrapNativeError(async () =>
      (await this.#inner.listRecords(offset, limit)).map((record) => ({
        id: record.id,
        vector: record.vector,
        metadata: record.metadata ?? {},
      })),
    );
  }

  async searchIdsBuffer(query: Float32Array, options: SearchOptions = {}): Promise<string[]> {
    return wrapNativeError(() => this.#inner.searchIdsBuffer(query, nativeSearchOptions(options)));
  }

  async searchIdBytesBuffer(
    query: Float32Array,
    options: SearchOptions = {},
  ): Promise<Uint8Array[]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytesBuffer(query, nativeSearchOptions(options)),
    );
  }

  async searchVectorsBuffer(query: Float32Array, options: SearchOptions = {}): Promise<number[][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectorsBuffer(query, nativeSearchOptions(options)),
    );
  }

  async searchWithReportBuffer(
    query: Float32Array,
    options: SearchOptions = {},
  ): Promise<SearchReport> {
    const report = await wrapNativeError(() =>
      this.#inner.searchWithReportBuffer(query, nativeSearchOptions(options)),
    );
    return normalizeSearchReport(report);
  }

  async searchIdsBatch(
    queries: VectorBatchInput,
    options: SearchOptions = {},
  ): Promise<string[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdsBatch(nativeVectors(queries), nativeSearchOptions(options)),
    );
  }

  async searchIdBytesBatch(
    queries: VectorBatchInput,
    options: SearchOptions = {},
  ): Promise<Uint8Array[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytesBatch(nativeVectors(queries), nativeSearchOptions(options)),
    );
  }

  async searchVectorsBatch(
    queries: VectorBatchInput,
    options: SearchOptions = {},
  ): Promise<number[][][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectorsBatch(nativeVectors(queries), nativeSearchOptions(options)),
    );
  }

  async searchIdsBatchBuffer(
    queries: Float32Array,
    options: SearchOptions = {},
  ): Promise<string[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdsBatchBuffer(queries, nativeSearchOptions(options)),
    );
  }

  async searchIdBytesBatchBuffer(
    queries: Float32Array,
    options: SearchOptions = {},
  ): Promise<Uint8Array[][]> {
    return wrapNativeError(() =>
      this.#inner.searchIdBytesBatchBuffer(queries, nativeSearchOptions(options)),
    );
  }

  async searchVectorsBatchBuffer(
    queries: Float32Array,
    options: SearchOptions = {},
  ): Promise<number[][][]> {
    return wrapNativeError(() =>
      this.#inner.searchVectorsBatchBuffer(queries, nativeSearchOptions(options)),
    );
  }

  async searchWithReport(query: VectorInput, options: SearchOptions = {}): Promise<SearchReport> {
    const report = await wrapNativeError(() =>
      this.#inner.searchWithReport(nativeVector(query), nativeSearchOptions(options)),
    );
    return normalizeSearchReport(report);
  }

  async searchBatchWithReport(
    queries: VectorBatchInput,
    options: SearchOptions = {},
  ): Promise<SearchReport[]> {
    return wrapNativeError(async () =>
      (
        await this.#inner.searchBatchWithReport(
          nativeVectors(queries),
          nativeSearchOptions(options),
        )
      ).map(normalizeSearchReport),
    );
  }

  async searchBatchWithReportBuffer(
    queries: Float32Array,
    options: SearchOptions = {},
  ): Promise<SearchReport[]> {
    return wrapNativeError(async () =>
      (await this.#inner.searchBatchWithReportBuffer(queries, nativeSearchOptions(options))).map(
        normalizeSearchReport,
      ),
    );
  }

  async delete(ids: string[]): Promise<DeleteReport> {
    if (!Array.isArray(ids) || ids.some((id) => typeof id !== "string")) {
      throw new TypeError("delete expects an array of string ids");
    }
    return wrapNativeError(() => this.#inner.delete(ids));
  }

  async purge(): Promise<PurgeReport> {
    return wrapNativeError(() => this.#inner.purge());
  }

  async maintain(options: IncrementalOptions = {}): Promise<IncrementalReport> {
    return wrapNativeError(() =>
      this.#inner.maintain({
        maxSegmentVectors: options.maxSegmentVectors,
        maxSegmentRadius: options.maxSegmentRadius,
        minSegmentVectors: options.minSegmentVectors,
        maxOperations: options.maxOperations,
      }),
    );
  }

  async compact(options: CompactionOptions = {}): Promise<CompactionReport> {
    const sourceLevel = validateOptionalIntegerOption(options.sourceLevel, "source_level");
    const targetLevel = validateOptionalIntegerOption(options.targetLevel, "target_level");
    const maxSegments = validateOptionalIntegerOption(options.maxSegments, "max_segments");
    const minSegments = validateOptionalIntegerOption(options.minSegments, "min_segments");
    const targetSegmentMaxVectors = validateOptionalIntegerOption(
      options.targetSegmentMaxVectors,
      "target_segment_max_vectors",
    );
    const allMatching = validateOptionalBooleanOption(options.allMatching, "all_matching");
    const targetSegmentMaxRadius = options.targetSegmentMaxRadius;
    if (
      targetSegmentMaxRadius !== undefined &&
      !(typeof targetSegmentMaxRadius === "number" && targetSegmentMaxRadius > 0)
    ) {
      throw new TypeError("targetSegmentMaxRadius must be a positive number");
    }
    return wrapNativeError(() =>
      this.#inner.compact({
        sourceLevel: sourceLevel,
        source_level: sourceLevel,
        targetLevel: targetLevel,
        target_level: targetLevel,
        maxSegments: maxSegments,
        max_segments: maxSegments,
        allMatching: allMatching,
        all_matching: allMatching,
        minSegments: minSegments,
        min_segments: minSegments,
        targetSegmentMaxVectors: targetSegmentMaxVectors,
        target_segment_max_vectors: targetSegmentMaxVectors,
        targetSegmentMaxRadius: targetSegmentMaxRadius,
        target_segment_max_radius: targetSegmentMaxRadius,
      }),
    );
  }

  async rebuild(options: RebuildOptions = {}): Promise<RebuildReport> {
    const sourceLevel = validateOptionalIntegerOption(options.sourceLevel, "source_level");
    const targetLevel = validateOptionalIntegerOption(options.targetLevel, "target_level");
    const minSegments = validateOptionalIntegerOption(options.minSegments, "min_segments");
    const targetSegmentMaxVectors = validateOptionalIntegerOption(
      options.targetSegmentMaxVectors,
      "target_segment_max_vectors",
    );
    const deleteObsolete = validateOptionalBooleanOption(options.deleteObsolete, "delete_obsolete");
    return wrapNativeError(() =>
      this.#inner.rebuild({
        sourceLevel: sourceLevel,
        source_level: sourceLevel,
        targetLevel: targetLevel,
        target_level: targetLevel,
        minSegments: minSegments,
        min_segments: minSegments,
        targetSegmentMaxVectors: targetSegmentMaxVectors,
        target_segment_max_vectors: targetSegmentMaxVectors,
        deleteObsolete: deleteObsolete,
        delete_obsolete: deleteObsolete,
      }),
    );
  }

  async gcObsoleteSegments(
    options: GarbageCollectionOptions = {},
  ): Promise<GarbageCollectionReport> {
    const dryRun = validateOptionalBooleanOption(options.dryRun, "dry_run");
    const minAgeMs = validateOptionalNonNegativeNumberOption(options.minAgeMs, "min_age_ms");
    return wrapNativeError(() =>
      this.#inner.gcObsoleteSegments({
        dryRun: dryRun,
        dry_run: dryRun,
        minAgeMs: minAgeMs,
        min_age_ms: minAgeMs,
      }),
    );
  }
}

function normalizeHit(hit: NativeHit): Hit {
  const idBytes = hit.idBytes ?? hit.id_bytes;
  if (!idBytes) {
    throw new BorsukError("native search hit did not include idBytes");
  }
  const normalized: Hit = {
    id: hit.id,
    idBytes,
    distance: hit.distance,
  };
  if (hit.metadata !== undefined && hit.metadata !== null) {
    normalized.metadata = hit.metadata;
  }
  return normalized;
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

function addRecordOptions(
  idsOrOptions: AddOptions | IdsInput,
  options: AddRecordOptions,
): AddRecordOptions {
  return Array.isArray(idsOrOptions) ? options : (idsOrOptions as AddOptions);
}

function addMetadata(
  idsOrOptions: AddOptions | IdsInput,
  options: AddRecordOptions,
): unknown[] | null {
  const metadata = addRecordOptions(idsOrOptions, options).metadata;
  if (metadata === undefined) {
    return null;
  }
  return metadata.map((entry) => entry ?? {});
}

function addSparse(
  idsOrOptions: AddOptions | IdsInput,
  options: AddRecordOptions,
): (NativeSparseVectorInput | null)[] | null {
  const sparse = addRecordOptions(idsOrOptions, options).sparse;
  if (sparse === undefined) {
    return null;
  }
  return sparse.map((entry) => (entry == null ? null : nativeSparseVector(entry)));
}

function addText(
  idsOrOptions: AddOptions | IdsInput,
  options: AddRecordOptions,
): (string | null)[] | null {
  const text = addRecordOptions(idsOrOptions, options).text;
  if (text === undefined) {
    return null;
  }
  return text.map((entry) => entry ?? null);
}

function addNamedVectors(
  idsOrOptions: AddOptions | IdsInput,
  options: AddRecordOptions,
): (NativeNamedVectorEntryInput[] | null)[] | null {
  const namedVectors = addRecordOptions(idsOrOptions, options).namedVectors;
  if (namedVectors === undefined) {
    return null;
  }
  return namedVectors.map((entry) => (entry == null ? null : nativeNamedVectorEntries(entry)));
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
  if (!(id instanceof Uint8Array)) {
    throw new BorsukError("record ids must be strings, Uint8Array values, or integers");
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

function recordIdKey(id: RecordId): string {
  return Buffer.from(nativeIdByte(id)).toString("base64");
}

function nativeVector(vector: VectorInput): number[] {
  return [...vector];
}

function nativeVectors(vectors: VectorBatchInput): number[][] {
  return vectors.map(nativeVector);
}

function nativeSparseVector(sparse: SparseVectorInput): NativeSparseVectorInput {
  return {
    indices: [...sparse.indices],
    values: [...sparse.values],
  };
}

function isSparseVectorInput(vector: NamedVectorInput): vector is SparseVectorInput {
  return (
    typeof vector === "object" &&
    vector !== null &&
    !Array.isArray(vector) &&
    "indices" in vector &&
    "values" in vector
  );
}

function isLateInteractionVectorInput(
  vector: NamedVectorInput,
): vector is LateInteractionVectorInput {
  return Array.isArray(vector) && vector.length > 0 && Array.isArray(vector[0]);
}

function nativeNamedVectorEntries(record: NamedVectorRecordInput): NativeNamedVectorEntryInput[] {
  return Object.entries(record).map(([name, vector]) => nativeNamedVectorEntry(name, vector));
}

function nativeNamedVectorEntry(
  name: string,
  vector: NamedVectorInput,
): NativeNamedVectorEntryInput {
  if (isSparseVectorInput(vector)) {
    return { name, sparse: nativeSparseVector(vector) };
  }
  if (isLateInteractionVectorInput(vector)) {
    return { name, vectors: nativeVectors(vector) };
  }
  return { name, vector: nativeVector(vector) };
}

function nativeHybridQuery(query: HybridQuery): NativeHybridQuery {
  return {
    vectors: query.vectors === undefined ? undefined : nativeNamedVectorEntries(query.vectors),
    text: validateOptionalStringOption(query.text, "text"),
  };
}

function normalizeSearchReport(report: NativeSearchReport): SearchReport {
  return {
    ...report,
    hits: normalizeHits(report.hits),
  };
}

function nativeKSearchOptions(options: KSearchOptions): NativeKSearchOptions {
  return {
    k: validateSearchK(options.k),
  };
}

function nativeHybridOptions(options: HybridSearchOptions): NativeHybridOptions {
  const fusion = validateOptionalStringOption(options.fusion, "fusion");
  if (fusion !== undefined && fusion !== "rrf" && fusion !== "weighted") {
    throw new BorsukError(`unknown hybrid fusion \`${fusion}\`; expected 'rrf' or 'weighted'`);
  }
  const rrfK = validateOptionalIntegerOption(options.rrfK, "rrf_k");
  return {
    k: validateSearchK(options.k),
    fusion,
    rrfK,
    rrf_k: rrfK,
    weights: nativeHybridWeights(options.weights),
  };
}

function nativeHybridWeights(
  weights: Record<string, number> | undefined,
): NativeNamedWeightInput[] | undefined {
  if (weights === undefined) {
    return undefined;
  }
  return Object.entries(weights).map(([name, weight]) => {
    if (typeof weight !== "number" || !Number.isFinite(weight)) {
      throw new BorsukError("weights must contain finite numbers");
    }
    return { name, weight };
  });
}

function nativeNamedVectorSpecs(
  namedVectors: Record<string, NamedVectorSpecInput> | undefined,
): NativeNamedVectorSpecInput[] | undefined {
  if (namedVectors === undefined) {
    return undefined;
  }
  return Object.entries(namedVectors).map(([name, spec]) => ({
    name,
    dimensions: validateOptionalIntegerOption(spec.dimensions, "dimensions") ?? spec.dimensions,
    metric: spec.metric,
    kind: spec.kind,
    elementType: spec.elementType,
    element_type: spec.elementType,
  }));
}

function nativeSearchOptions(options: SearchOptions): NativeSearchOptions {
  let maxBytesNumber: number | undefined;
  let maxBytesText: string | undefined;
  if (options.maxBytes !== undefined) {
    if (typeof options.maxBytes === "number") {
      maxBytesNumber = validateOptionalIntegerOption(options.maxBytes, "max_bytes");
    } else if (typeof options.maxBytes === "string") {
      maxBytesText = options.maxBytes;
    } else {
      throw new BorsukError("max_bytes must be an integer when set");
    }
  }
  const maxSegments = validateOptionalIntegerOption(options.maxSegments, "max_segments");
  const maxLatencyMs = validateOptionalIntegerOption(options.maxLatencyMs, "max_latency_ms");
  const routingPageOverfetch = validateOptionalIntegerOption(
    options.routingPageOverfetch,
    "routing_page_overfetch",
  );
  const maxCandidatesPerSegment = validateOptionalIntegerOption(
    options.maxCandidatesPerSegment,
    "max_candidates_per_segment",
  );
  const guaranteedRecall = validateOptionalBooleanOption(
    options.guaranteedRecall,
    "guaranteed_recall",
  );
  const prefetchDepth = validateOptionalIntegerOption(options.prefetchDepth, "prefetch_depth");
  const mode = validateOptionalStringOption(options.mode, "mode");
  const leafMode = validateOptionalStringOption(options.leafMode, "leaf_mode");
  const cacheExecution = validateOptionalStringOption(options.cacheExecution, "cache_execution");
  const vector = validateOptionalStringOption(options.vector, "vector");

  return {
    k: validateSearchK(options.k),
    mode,
    leafMode,
    leaf_mode: leafMode,
    cacheExecution,
    cache_execution: cacheExecution,
    eps: options.eps,
    maxSegments: maxSegments,
    max_segments: maxSegments,
    maxBytes: maxBytesNumber,
    max_bytes: maxBytesNumber,
    maxBytesText: maxBytesText,
    max_bytes_text: maxBytesText,
    maxLatencyMs: maxLatencyMs,
    max_latency_ms: maxLatencyMs,
    routingPageOverfetch: routingPageOverfetch,
    routing_page_overfetch: routingPageOverfetch,
    maxCandidatesPerSegment: maxCandidatesPerSegment,
    max_candidates_per_segment: maxCandidatesPerSegment,
    guaranteedRecall: guaranteedRecall,
    guaranteed_recall: guaranteedRecall,
    prefetchDepth: prefetchDepth,
    prefetch_depth: prefetchDepth,
    filter: options.filter,
    includeMetadata: options.includeMetadata,
    include_metadata: options.includeMetadata,
    vector,
  };
}

function validateSearchK(k: number | undefined): number | undefined {
  if (k !== undefined && !Number.isSafeInteger(k)) {
    throw new BorsukError("k must be an integer");
  }
  return k;
}

function validateOptionalIntegerOption(
  value: number | undefined,
  field: string,
): number | undefined {
  if (value !== undefined && !Number.isSafeInteger(value)) {
    throw new BorsukError(`${field} must be an integer when set`);
  }
  return value;
}

function validateOptionalPositiveIntegerOption(
  value: number | undefined,
  field: string,
): number | undefined {
  const validated = validateOptionalIntegerOption(value, field);
  if (validated !== undefined && validated <= 0) {
    throw new BorsukError(`${field} must be greater than zero when set`);
  }
  return validated;
}

function validateOptionalNonNegativeNumberOption(
  value: number | undefined,
  field: string,
): number | undefined {
  if (value !== undefined && (typeof value !== "number" || !Number.isFinite(value) || value < 0)) {
    throw new BorsukError(`${field} must be a non-negative finite number when set`);
  }
  return value;
}

function validateOptionalStringOption(
  value: string | undefined,
  field: string,
): string | undefined {
  if (value !== undefined && typeof value !== "string") {
    throw new BorsukError(`${field} must be a string when set`);
  }
  return value;
}

function nativeByteSizeOption(value: ByteSize | undefined, field: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return `${validateOptionalIntegerOption(value, field)}B`;
  }
  throw new BorsukError(`${field} must be an integer byte count or byte-size string when set`);
}

function validateOptionalBooleanOption(
  value: boolean | undefined,
  field: string,
): boolean | undefined {
  if (value !== undefined && typeof value !== "boolean") {
    throw new BorsukError(`${field} must be a boolean when set`);
  }
  return value;
}

export async function create(options: CreateOptions): Promise<Index> {
  const dim = validateOptionalIntegerOption(options.dim, "dim");
  const dimensions = validateOptionalIntegerOption(options.dimensions, "dimensions");
  const segmentSize = validateOptionalIntegerOption(options.segmentSize, "segment_size");
  const segmentMaxVectors = validateOptionalIntegerOption(
    options.segmentMaxVectors,
    "segment_max_vectors",
  );
  const routingPageFanout = validateOptionalIntegerOption(
    options.routingPageFanout,
    "routing_page_fanout",
  );
  const graphNeighbors = validateOptionalIntegerOption(options.graphNeighbors, "graph_neighbors");
  const globalPqCodeBytes = validateOptionalIntegerOption(
    options.globalPqCodeBytes,
    "global_pq_code_bytes",
  );
  const globalTurboquantBits = validateOptionalIntegerOption(
    options.globalTurboquantBits,
    "global_turboquant_bits",
  );
  const globalTurboquantQjlBits = validateOptionalIntegerOption(
    options.globalTurboquantQjlBits,
    "global_turboquant_qjl_bits",
  );
  const globalTurboquantShards = validateOptionalIntegerOption(
    options.globalTurboquantShards,
    "global_turboquant_shards",
  );
  const ramBudget = nativeByteSizeOption(options.ramBudget, "ram_budget");
  const text = validateOptionalBooleanOption(options.text, "text");
  const namedVectors = nativeNamedVectorSpecs(options.namedVectors);
  const inner = await wrapNativeError(() =>
    native.create({
      uri: options.uri,
      metric: options.metric,
      vectorElementType: options.vectorElementType,
      vector_element_type: options.vectorElementType,
      dim: dim,
      dimensions: dimensions,
      segmentSize: segmentSize,
      segmentMaxVectors: segmentMaxVectors,
      routingPageFanout: routingPageFanout,
      graphNeighbors: graphNeighbors,
      leafCapability: options.leafCapability,
      leaf_capability: options.leafCapability,
      globalPqLayout: options.globalPqLayout,
      global_pq_layout: options.globalPqLayout,
      globalPqCodeBytes,
      global_pq_code_bytes: globalPqCodeBytes,
      globalScanCodec: options.globalScanCodec,
      global_scan_codec: options.globalScanCodec,
      globalTurboquantBits,
      global_turboquant_bits: globalTurboquantBits,
      globalTurboquantQjlBits,
      global_turboquant_qjl_bits: globalTurboquantQjlBits,
      globalTurboquantShards,
      global_turboquant_shards: globalTurboquantShards,
      segment_size: segmentSize,
      segment_max_vectors: segmentMaxVectors,
      routing_page_fanout: routingPageFanout,
      graph_neighbors: graphNeighbors,
      ramBudget: ramBudget,
      ram_budget: ramBudget,
      cacheDir: options.cacheDir,
      cache_dir: options.cacheDir,
      text,
      namedVectors,
      named_vectors: namedVectors,
    }),
  );
  return new Index(options.uri, inner);
}

export async function open(uri: string, options: OpenOptions = {}): Promise<Index> {
  const residentRouting = validateOptionalBooleanOption(
    options.residentRouting,
    "resident_routing",
  );
  const ramBudget = nativeByteSizeOption(options.ramBudget, "ram_budget");
  const cacheMaxBytes = nativeByteSizeOption(options.cacheMaxBytes, "cache_max_bytes");
  const preload = validateOptionalBooleanOption(options.preload, "preload");
  const maxActiveSearches = validateOptionalPositiveIntegerOption(
    options.maxActiveSearches,
    "max_active_searches",
  );
  const maxWaitingSearches = validateOptionalIntegerOption(
    options.maxWaitingSearches,
    "max_waiting_searches",
  );
  if (maxWaitingSearches !== undefined && maxWaitingSearches < 0) {
    throw new BorsukError("max_waiting_searches must be non-negative when set");
  }
  const leafReadWidth = validateOptionalPositiveIntegerOption(
    options.leafReadWidth,
    "leaf_read_width",
  );
  const maxInflightLeafReads = validateOptionalPositiveIntegerOption(
    options.maxInflightLeafReads,
    "max_inflight_leaf_reads",
  );
  const maxParallelDecodeRankTasks = validateOptionalPositiveIntegerOption(
    options.maxParallelDecodeRankTasks,
    "max_parallel_decode_rank_tasks",
  );
  const exactReadMaxPhysicalAmplification = validateOptionalIntegerOption(
    options.exactReadMaxPhysicalAmplification,
    "exact_read_max_physical_amplification",
  );
  if (
    exactReadMaxPhysicalAmplification !== undefined &&
    (exactReadMaxPhysicalAmplification < 1 || exactReadMaxPhysicalAmplification > 5)
  ) {
    throw new BorsukError(
      "exact_read_max_physical_amplification must be between 1 and 5 when set",
    );
  }
  const inner = await wrapNativeError(() =>
    native.open(uri, {
      cacheDir: options.cacheDir,
      cache_dir: options.cacheDir,
      cacheMaxBytes: cacheMaxBytes,
      cache_max_bytes: cacheMaxBytes,
      ramBudget: ramBudget,
      ram_budget: ramBudget,
      residentRouting: residentRouting,
      resident_routing: residentRouting,
      preload,
      maxActiveSearches,
      max_active_searches: maxActiveSearches,
      maxWaitingSearches,
      max_waiting_searches: maxWaitingSearches,
      leafReadWidth,
      leaf_read_width: leafReadWidth,
      maxInflightLeafReads,
      max_inflight_leaf_reads: maxInflightLeafReads,
      maxParallelDecodeRankTasks,
      max_parallel_decode_rank_tasks: maxParallelDecodeRankTasks,
      exactReadMaxPhysicalAmplification,
      exact_read_max_physical_amplification: exactReadMaxPhysicalAmplification,
    }),
  );
  return new Index(uri, inner);
}

export function recallAtK(
  exactIds: readonly RecordId[],
  actualIds: readonly RecordId[],
  k: number,
): number {
  return wrapNativeError(() => {
    validateRecallK(k);
    if (k <= 0) {
      throw new BorsukError("k must be greater than zero");
    }

    const exactTop = new Set(exactIds.slice(0, k).map(recordIdKey));
    if (exactTop.size === 0) {
      return 0;
    }

    const actualTop = new Set(actualIds.slice(0, k).map(recordIdKey));
    let overlap = 0;
    for (const id of actualTop) {
      if (exactTop.has(id)) {
        overlap += 1;
      }
    }
    return overlap / exactTop.size;
  });
}

function validateRecallK(k: number): void {
  if (!Number.isSafeInteger(k)) {
    throw new BorsukError("k must be an integer");
  }
}

export function tieAwareRecallAtK(
  exactDistances: readonly number[],
  actualDistances: readonly number[],
  k: number,
): number {
  return wrapNativeError(() => {
    validateRecallK(k);
    if (k <= 0) {
      throw new BorsukError("k must be greater than zero");
    }
    return native.tieAwareRecallAtK([...exactDistances], [...actualDistances], k);
  });
}

export function leafModeNames(): CanonicalLeafModeName[] {
  return wrapNativeError(() => native.leafModeNames() as CanonicalLeafModeName[]);
}

export function vectorDistance(
  metric: VectorMetric,
  left: readonly number[],
  right: readonly number[],
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
    const value = operation();
    if (value instanceof Promise) {
      return value.catch((error: unknown) => {
        throw toBorsukError(error);
      }) as T;
    }
    return value;
  } catch (error) {
    throw toBorsukError(error);
  }
}

function toBorsukError(error: unknown): BorsukError {
  if (error instanceof BorsukError) {
    return error;
  }

  if (error instanceof Error) {
    const details = nativeBorsukErrorDetails(error);
    return new BorsukError(details.message, error, details.code);
  }

  return new BorsukError(String(error), error);
}

function nativeBorsukErrorDetails(error: Error): { message: string; code: BorsukErrorCode } {
  const match = nativeBorsukErrorPattern.exec(error.message);
  if (!match) {
    return { message: error.message, code: "runtime_error" };
  }

  return {
    message: match[2],
    code: match[1] as BorsukErrorCode,
  };
}
