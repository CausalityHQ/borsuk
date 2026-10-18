crates/borsuk/src/bin/check_cohere_native_baseline.rs:1212: fn scratch_bytes(rows: usize) -> Result<usize> {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1213:     // RotatedTwoBitCodec::prepare_query: (packed_bytes * 256 + padded_D) * sizeof(f64).
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1214:     let codec = (D / 4)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1215:         .checked_mul(256)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1216:         .and_then(|n| n.checked_add(D))
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1217:         .and_then(|n| n.checked_mul(size_of::<f64>()))
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1218:         .ok_or("codec scratch overflow")?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1219:     require(codec == 532_480, "D1024 codec formula")?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1220:     codec
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1221:         .checked_add(TwoBitPlanTrace::scratch_bytes(rows))
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1222:         .ok_or_else(|| "trace scratch overflow".into())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1223: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1224: fn caller_bytes(c: &Config) -> usize {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1225:     let backend = match &c.backend {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1226:         Backend::Local { store_root } => store_root.capacity(),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1227:         Backend::S3 {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1228:             bucket,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1229:             region,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1230:             physical_prefix,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1231:             sq8_object_key,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1232:             sq8_etag,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1233:         } => [bucket, region, physical_prefix, sq8_object_key, sq8_etag]
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1234:             .iter()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1235:             .map(|s| s.capacity())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1236:             .sum(),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1237:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1238:     let layout_bytes = match &c.source_layout {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1239:         SourceLayout::FlatRecords => 0,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1240:         SourceLayout::LeafCapsulesV2 {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1241:             blob_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1242:             directory_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1243:             ..
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1244:         } => blob_sha256.capacity() + directory_sha256.capacity(),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1245:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1246:     let descriptors = size_of::<Config>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1247:         + layout_bytes
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1248:         + [
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1249:             &c.schema,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1250:             &c.dataset,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1251:             &c.revision,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1252:             &c.metric,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1253:             &c.tie_rule,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1254:             &c.generation_prefix,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1255:             &c.generation_root_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1256:             &c.requests.sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1257:             &c.truth.sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1258:             &c.native_source.source_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1259:             &c.native_source.sq8_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1260:             &c.native_source.source_order_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1261:             &c.cohort_receipt.sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1262:             &c.derivation_receipt.sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1263:             &c.producer_authority.source_commit,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1264:             &c.producer_authority.executable_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1265:             &c.producer_authority.producer_source_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1266:             &c.producer_authority.sq8_source_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1267:             &c.producer_authority.source_order_source_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1268:         ]
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1269:         .iter()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1270:         .map(|s| s.capacity())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1271:         .sum::<usize>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1272:         + [&c.scratch_parent, &c.requests.path, &c.truth.path]
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1273:             .iter()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1274:             .map(|p| p.capacity())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1275:             .sum::<usize>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1276:         + 2 * backend
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1277:         + match &c.execution {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1278:             Execution::Full {} => 0,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1279:             Execution::DiagnosticPanel { ordinals, .. } => ordinals.capacity() * size_of::<usize>(),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1280:         }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1281:         + c.corpus_intervals.capacity() * size_of::<SourceInterval>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1282:         + c.cohort_receipt.path.capacity()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1283:         + c.derivation_receipt.path.capacity()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1284:         + size_of::<Reader>();
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1285:     // Returned trace/result remain caller-owned while streaming, then drop before the next query.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1286:     // Vec growth is bounded by twice the entire page roster; ranking retains only k entries.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1287:     let returned = TwoBitPlanTrace::scratch_bytes(c.rows)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1288:         + 2 * c.rows.div_ceil(256) * (size_of::<usize>() + size_of::<std::ops::Range<usize>>())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1289:         + K * (size_of::<ScoredNominee>() + size_of::<Hit>())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1290:         + size_of::<[usize; K]>();
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1291:     let query = c.requests.bytes + size_of::<[f32; D]>() + returned;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1292:     // Reduction drops generation/requests first: one capped JSONL line, one truth body,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1293:     // 8KiB reader, k hits. Authentication uses one 64KiB stack block.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1294:     let reduction = SOURCE_BINDING_LINE_CAP + 1 + c.truth.bytes + 8192 + 2 * K * size_of::<Hit>();
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1295:     // Capped receipt/config bodies cover sequential authentication/decoding; identity,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1296:     // descriptors duplicated by store/path/runtime setup fit another CONFIG_CAP.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1297:     // Native histogram has 900 possible codes; allow doubled Vec capacity and a
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1298:     // transient snapshot alongside the retained before/after pair. The fixed process
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1299:     // histogram is also charged. SDK/TLS RSS still requires an external cgroup gate.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1300:     let transport = if matches!(&c.backend, Backend::S3 { .. }) {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1301:         3 * (size_of::<NativeTransportStats>() + 2 * 900 * size_of::<(u16, u64)>())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1302:             + 900 * size_of::<u64>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1303:     } else {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1304:         0
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1305:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1306:     // Producer bit vectors and bounded manifest f32 vectors coexist during source binding.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1307:     descriptors
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1308:         + transport
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1309:         + size_of::<Progress>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1310:         + 8 * CONFIG_CAP
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1311:         // Binding serialization and its coexisting record tree are startup-only,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1312:         // outside the unchanged per-query diagnostic reservation.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1313:         + 2 * SOURCE_BINDING_LINE_CAP
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1314:         + BLOCK
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1315:         + 4 * D * size_of::<u32>()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1316:         + query.max(reduction)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1317: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1318: const ACTIVE_QUERIES: usize = 1;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1319: // SQ8 ranges one traced query can plan: the baseline SQ8 cap or the explicit direct cap.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1320: fn trace_ranges(c: &Config) -> usize {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1321:     if let Some(split) = c.sq8_range_split {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1322:         return split.max_physical_gets;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1323:     }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1324:     c.serving
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1325:         .direct()
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1326:         .map_or(c.sq8_get_budget, |direct| direct.max_sq8_gets)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1327: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1328: fn source_trace_ranges(c: &Config) -> usize {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1329:     if c.serving.direct().is_some() {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1330:         1
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1331:     } else {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1332:         c.source_get_budget
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1333:     }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1334: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1335: // The library retains the ordinary bound even for direct queries; direct adds only excess.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1336: fn range_scheduler_bytes(c: &Config) -> Result<usize> {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1337:     let pending = |ranges| {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1338:         Sq8RangeTrace::checked_pending_futures_bytes(ranges, c.fetch_parallelism)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1339:             .ok_or("range scheduler memory")
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1340:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1341:     let sq8 = c
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1342:         .sq8_range_split
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1343:         .map_or(c.sq8_get_budget.max(trace_ranges(c)), |split| {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1344:             split.max_physical_gets
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1345:         });
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1346:     Ok(pending(c.source_get_budget)?.max(pending(sq8)?))
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1347: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1348: fn source_authentication_bytes(c: &Config) -> Result<u64> {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1349:     c.source_authentication
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1350:         .additional_memory_bytes(c.fetch_parallelism, c.source_get_budget, ACTIVE_QUERIES)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1351:         .ok_or_else(|| "SOURCE authentication memory".into())
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1352: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1403: // Every diagnostic byte the runner itself holds: `diagnostic_bytes` minus the trace peak.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1404: fn diagnostic_pinned_bytes(c: &Config) -> Result<usize> {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1405:     Ok(diagnostic_bytes(c)? - trace_peak_bytes(c))
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1406: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1407: // Scheduler state belongs to the library; the runner pins only its own buffers.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1408: fn uninstrumented_pinned_bytes(c: &Config) -> u64 {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1409:     caller_bytes(c) as u64
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1410: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1411: fn pinned_bytes(c: &Config) -> Result<u64> {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1412:     Ok(caller_bytes(c) as u64 + (diagnostic_pinned_bytes(c)? as u64) * ACTIVE_QUERIES as u64)
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1413: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1414: fn limits(c: &Config) -> Result<TwoBitGenerationLimits> {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1415:     Ok(TwoBitGenerationLimits {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1416:         max_memory_bytes: c.max_memory_bytes,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1417:         max_active_queries: ACTIVE_QUERIES,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1418:         max_query_bytes: c.sq8_byte_budget,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1419:         max_query_gets: c.sq8_get_budget,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1420:         max_parallel_gets: c.fetch_parallelism,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1421:         max_source_bytes: c.source_byte_budget,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1422:         max_source_gets: c.source_get_budget,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1423:         max_parallel_source_gets: c.fetch_parallelism,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1424:         max_query_scratch_bytes: scratch_bytes(c.rows)?,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1425:         already_pinned_bytes: pinned_bytes(c)?,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1426:     })
crates/borsuk/src/bin/check_cohere_native_baseline.rs:1427: }
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2215:                 "host_counters":"host_wide_not_per_flow_null_means_unavailable",
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2216:                 "unset":"a boundary never reached is null, never a fabricated timestamp"},
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2217:             "truth_opened":false});
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2218:         // Three different figures, never interchangeable: what the trace RETAINS between queries,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2219:         // the cumulative PEAK while one drains, and the range state both modes carry.
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2220:         let traced = c.execution.trace().then(|| trace_ranges(c));
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2221:         admission_record["trace_retained_bytes"] =
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2222:             json!(traced.map(|n| QueryProfile::retained_bytes(source_trace_ranges(c), n)));
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2223:         admission_record["trace_peak_bytes"] = json!(traced.map(|_| trace_peak_bytes(c)));
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2224:         admission_record["diagnostic_pinned_bytes"] = json!(diagnostic_pinned_bytes(c)?);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2225:         admission_record["trace_additional_bytes_charged_by_library"] =
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2226:             json!(library_trace_bytes(c));
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2227:         admission_record["range_scheduler_owner"] = json!("library");
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2228:         admission_record["range_state_charged_by_library_bytes"] = json!(range_scheduler_bytes(c)?);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2229:         admission_record["range_state_both_paths_bytes"] = json!(range_scheduler_bytes(c)?);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2230:         admission_record["source_authentication_extra_library_bytes"] =
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2231:             json!(source_authentication_bytes(c)?);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2232:         out.emit(&admission_record)?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2244:         "generation_authority":binding.as_ref().ok(),"truth_opened":false}),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2245:     )?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2246:     binding?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2247:     p.stage = "generation_open";
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2248:     p.transport.begin(&reader, p.stage, None);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2249:     let generation = TwoBitGeneration::open_remote(
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2250:         store,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2251:         &ObjectPath::from(c.generation_prefix.clone()),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2252:         &c.generation_root_sha256,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2253:         admission,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2254:         &c.scratch_parent,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2255:     )
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2256:     .await;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2257:     p.transport.finish(&reader);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2258:     out.emit(&json!({"phase":"generation_open","transport":p.transport,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2259:         "success":generation.is_ok(),"truth_opened":false}))?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2260:     let generation = generation?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2261:     let generation = match c.semantic_leaf_limit {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2262:         Some(limit) => generation.with_semantic_leaf_limit(limit)?,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2263:         None => generation,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2264:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2265:     let generation = generation.with_source_authentication(c.source_authentication)?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2266:     let generation = match c.shared_byte_window {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2267:         Some(window) => generation.with_byte_window_limits(
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2268:             window.max_bytes,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2269:             window.max_waiters,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2270:             window.max_state_bytes,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2271:         )?,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2272:         None => generation,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2273:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2274:     let generation = match c.sq8_range_split {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2275:         Some(split) => generation.with_sq8_range_split(split)?,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2276:         None => generation,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2277:     };
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2278:     require(
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2279:         generation.rows() == c.rows
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2280:             && generation.discovery_mode() == DiscoveryMode::Semantic
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2281:             && generation.semantic_profile() == Some(c.profile),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2282:         "generation geometry/profile",
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2283:     )?;
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2284:     let mut startup = json!({"phase":"startup","metadata":generation.remote_open_stats(),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2285:         "serving":c.serving,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2286:         "library_cap_bytes":c.max_memory_bytes,"caller_pinned_bytes":admission.already_pinned_bytes,
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2287:         "codec_scratch_bytes":532480,"trace_scratch_bytes":TwoBitPlanTrace::scratch_bytes(c.rows),
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2288:         "query_scratch_bytes":admission.max_query_scratch_bytes,"truth_opened":false});
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2289:     startup["source_layout"] = json!(c.source_layout);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2290:     startup["range_state_charged_by_library_bytes"] = json!(range_scheduler_bytes(c)?);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2291:     startup["source_authentication_extra_library_bytes"] = json!(source_authentication_bytes(c)?);
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2292:     if let Some(bytes) = generation.byte_window_state_bytes() {
crates/borsuk/src/bin/check_cohere_native_baseline.rs:2293:         startup["shared_byte_window_state_bytes"] = json!(bytes);
