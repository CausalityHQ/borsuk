crates/borsuk/src/two_bit_generation.rs:1597: fn range_scheduler_memory(
crates/borsuk/src/two_bit_generation.rs:1598:     limits: TwoBitGenerationLimits,
crates/borsuk/src/two_bit_generation.rs:1599:     paged: bool,
crates/borsuk/src/two_bit_generation.rs:1600:     sq8_gets: usize,
crates/borsuk/src/two_bit_generation.rs:1601: ) -> Result<u64> {
crates/borsuk/src/two_bit_generation.rs:1602:     let bytes = || {
crates/borsuk/src/two_bit_generation.rs:1603:         let source = if paged {
crates/borsuk/src/two_bit_generation.rs:1604:             Sq8RangeTrace::checked_pending_futures_bytes(
crates/borsuk/src/two_bit_generation.rs:1605:                 limits.max_source_gets,
crates/borsuk/src/two_bit_generation.rs:1606:                 limits.max_parallel_source_gets,
crates/borsuk/src/two_bit_generation.rs:1607:             )?
crates/borsuk/src/two_bit_generation.rs:1608:         } else {
crates/borsuk/src/two_bit_generation.rs:1609:             0
crates/borsuk/src/two_bit_generation.rs:1610:         };
crates/borsuk/src/two_bit_generation.rs:1611:         let sq8 = Sq8RangeTrace::checked_pending_futures_bytes(sq8_gets, limits.max_parallel_gets)?;
crates/borsuk/src/two_bit_generation.rs:1612:         u64::try_from(source.max(sq8))
crates/borsuk/src/two_bit_generation.rs:1613:             .ok()?
crates/borsuk/src/two_bit_generation.rs:1614:             .checked_mul(limits.max_active_queries as u64)
crates/borsuk/src/two_bit_generation.rs:1615:     };
crates/borsuk/src/two_bit_generation.rs:1616:     bytes().ok_or(TwoBitGenerationError::Invalid("range scheduler memory"))
crates/borsuk/src/two_bit_generation.rs:1617: }
crates/borsuk/src/two_bit_generation.rs:1618:
crates/borsuk/src/two_bit_generation.rs:1619: impl TwoBitGeneration {
crates/borsuk/src/two_bit_generation.rs:1620:     /// Physical row count of this immutable nonempty base.
crates/borsuk/src/two_bit_generation.rs:1621:     pub fn rows(&self) -> usize {
crates/borsuk/src/two_bit_generation.rs:1622:         self.pages.rows()
crates/borsuk/src/two_bit_generation.rs:1623:     }
crates/borsuk/src/two_bit_generation.rs:1624:     /// Explicit semantic profile, absent for graph discovery.
crates/borsuk/src/two_bit_generation.rs:1625:     pub fn semantic_profile(&self) -> Option<SemanticProfile> {
crates/borsuk/src/two_bit_generation.rs:1626:         self.manifest.discovery.semantic_profile()
crates/borsuk/src/two_bit_generation.rs:1627:     }
crates/borsuk/src/two_bit_generation.rs:1628:     /// Limit frozen squared-Euclidean leaf selection to an ordered prefix.
crates/borsuk/src/two_bit_generation.rs:1629:     /// Only authenticated Scale1m semantic generations admit explicit limits 1..=48.
crates/borsuk/src/two_bit_generation.rs:1630:     /// Omission retains the profile default; admission reserves remain unchanged.
crates/borsuk/src/two_bit_generation.rs:1631:     pub fn with_semantic_leaf_limit(mut self, limit: usize) -> Result<Self> {
crates/borsuk/src/two_bit_generation.rs:1632:         let limit = SemanticLeafLimit::new(limit, self.semantic_profile())?;
crates/borsuk/src/two_bit_generation.rs:1633:         self.semantic_leaf_limit = Some(limit);
crates/borsuk/src/two_bit_generation.rs:1634:         Ok(self)
crates/borsuk/src/two_bit_generation.rs:1635:     }
crates/borsuk/src/two_bit_generation.rs:1636:
crates/borsuk/src/two_bit_generation.rs:1637:     pub(crate) fn modeled_memory_bytes(&self) -> u64 {
crates/borsuk/src/two_bit_generation.rs:1638:         self.modeled_memory_bytes
crates/borsuk/src/two_bit_generation.rs:1639:     }
crates/borsuk/src/two_bit_generation.rs:1640:
crates/borsuk/src/two_bit_generation.rs:1641:     /// Select SOURCE authentication before sharing the generation. Inline is the default.
crates/borsuk/src/two_bit_generation.rs:1642:     /// Worker stacks, bounded jobs and the enlarged drain are admitted before any query I/O.
crates/borsuk/src/two_bit_generation.rs:1643:     pub fn with_source_authentication(
crates/borsuk/src/two_bit_generation.rs:1644:         mut self,
crates/borsuk/src/two_bit_generation.rs:1645:         authentication: SourceAuthentication,
crates/borsuk/src/two_bit_generation.rs:1646:     ) -> Result<Self> {
crates/borsuk/src/two_bit_generation.rs:1647:         let bad = TwoBitGenerationError::Invalid;
crates/borsuk/src/two_bit_generation.rs:1648:         if self.source_authentication != SourceAuthentication::Inline {
crates/borsuk/src/two_bit_generation.rs:1649:             return Err(bad("SOURCE authentication already configured"));
crates/borsuk/src/two_bit_generation.rs:1650:         }
crates/borsuk/src/two_bit_generation.rs:1651:         if authentication == SourceAuthentication::Inline {
crates/borsuk/src/two_bit_generation.rs:1652:             return Ok(self);
crates/borsuk/src/two_bit_generation.rs:1653:         }
crates/borsuk/src/two_bit_generation.rs:1654:         if self.source.is_none() {
crates/borsuk/src/two_bit_generation.rs:1655:             return Err(bad("SOURCE authentication requires remote source"));
crates/borsuk/src/two_bit_generation.rs:1656:         }
crates/borsuk/src/two_bit_generation.rs:1657:         let bytes = authentication
crates/borsuk/src/two_bit_generation.rs:1658:             .additional_memory_bytes(
crates/borsuk/src/two_bit_generation.rs:1659:                 self.limits.max_parallel_source_gets,
crates/borsuk/src/two_bit_generation.rs:1660:                 self.limits.max_source_gets,
crates/borsuk/src/two_bit_generation.rs:1661:                 self.limits.max_active_queries,
crates/borsuk/src/two_bit_generation.rs:1662:             )
crates/borsuk/src/two_bit_generation.rs:1663:             .ok_or(bad("SOURCE authentication memory"))?;
crates/borsuk/src/two_bit_generation.rs:1664:         self.modeled_memory_bytes = self
crates/borsuk/src/two_bit_generation.rs:1665:             .modeled_memory_bytes
crates/borsuk/src/two_bit_generation.rs:1666:             .checked_add(bytes)
crates/borsuk/src/two_bit_generation.rs:1667:             .filter(|&bytes| bytes <= self.limits.max_memory_bytes)
crates/borsuk/src/two_bit_generation.rs:1668:             .ok_or(bad("SOURCE authentication memory"))?;
crates/borsuk/src/two_bit_generation.rs:1669:         self.source_authentication = authentication;
crates/borsuk/src/two_bit_generation.rs:1670:         Ok(self)
crates/borsuk/src/two_bit_generation.rs:1671:     }
crates/borsuk/src/two_bit_generation.rs:1672:
crates/borsuk/src/two_bit_generation.rs:1673:     /// Opt in once to transport-only splitting. Reserve all additional per-query
crates/borsuk/src/two_bit_generation.rs:1674:     /// buffers before any query can allocate or perform I/O; logical budgets stay fixed.
crates/borsuk/src/two_bit_generation.rs:1675:     pub fn with_sq8_range_split(mut self, split: Sq8RangeSplit) -> Result<Self> {
crates/borsuk/src/two_bit_generation.rs:2290:             },
crates/borsuk/src/two_bit_generation.rs:2291:             limits.max_query_bytes as u64,
crates/borsuk/src/two_bit_generation.rs:2292:             planner_bytes,
crates/borsuk/src/two_bit_generation.rs:2293:             limits.max_active_queries as u64,
crates/borsuk/src/two_bit_generation.rs:2294:         )
crates/borsuk/src/two_bit_generation.rs:2295:         .map_err(|_| bad("query memory"))?;
crates/borsuk/src/two_bit_generation.rs:2296:         // Mean is 4D bytes. 64 times its admitted length conservatively covers
crates/borsuk/src/two_bit_generation.rs:2297:         // padded codec metadata and its temporary copies, including tiny-N/high-D.
crates/borsuk/src/two_bit_generation.rs:2298:         let codec_memory = (admitted_size("plane/mean.bin")? as u64)
crates/borsuk/src/two_bit_generation.rs:2299:             .checked_mul(64)
crates/borsuk/src/two_bit_generation.rs:2300:             .ok_or(bad("codec memory"))?;
crates/borsuk/src/two_bit_generation.rs:2301:         let source_payload_bytes = match &manifest.source_layout {
crates/borsuk/src/two_bit_generation.rs:2302:             SourceLayout::FlatRecords => expected_records,
crates/borsuk/src/two_bit_generation.rs:2303:             SourceLayout::LeafCapsulesV2 { blob_bytes, .. } => {
crates/borsuk/src/two_bit_generation.rs:2304:                 usize::try_from(*blob_bytes).map_err(|_| bad("capsule blob length"))?
crates/borsuk/src/two_bit_generation.rs:2305:             }
crates/borsuk/src/two_bit_generation.rs:2306:         };
crates/borsuk/src/two_bit_generation.rs:2307:         // Page references, selected descriptors and their actual capacities are
crates/borsuk/src/two_bit_generation.rs:2308:         // separate from the two framed transport copies. Flat models stay exact.
crates/borsuk/src/two_bit_generation.rs:2309:         let capsule_query_memory = if let Some(profile) = manifest.discovery.semantic_profile()
crates/borsuk/src/two_bit_generation.rs:2310:             && manifest.source_layout.is_capsules()
crates/borsuk/src/two_bit_generation.rs:2311:         {
crates/borsuk/src/two_bit_generation.rs:2312:             geometry
crates/borsuk/src/two_bit_generation.rs:2313:                 .rows
crates/borsuk/src/two_bit_generation.rs:2314:                 .div_ceil(32)
crates/borsuk/src/two_bit_generation.rs:2315:                 .min(profile.selected_leaf_limit() * 64)
crates/borsuk/src/two_bit_generation.rs:2316:                 .checked_mul(128)
crates/borsuk/src/two_bit_generation.rs:2317:                 .and_then(|n| {
crates/borsuk/src/two_bit_generation.rs:2318:                     n.checked_add(profile.selected_leaf_limit().checked_mul(
crates/borsuk/src/two_bit_generation.rs:2319:                         512 + std::mem::size_of::<usize>()
crates/borsuk/src/two_bit_generation.rs:2320:                             + std::mem::size_of::<std::ops::Range<usize>>(),
crates/borsuk/src/two_bit_generation.rs:2321:                     )?)
crates/borsuk/src/two_bit_generation.rs:2322:                 })
crates/borsuk/src/two_bit_generation.rs:2323:                 .and_then(|n| {
crates/borsuk/src/two_bit_generation.rs:2324:                     n.checked_add(
crates/borsuk/src/two_bit_generation.rs:2325:                         profile
crates/borsuk/src/two_bit_generation.rs:2326:                             .walk_unit_limit()
crates/borsuk/src/two_bit_generation.rs:2327:                             .checked_mul(std::mem::size_of::<usize>())?,
crates/borsuk/src/two_bit_generation.rs:2328:                     )
crates/borsuk/src/two_bit_generation.rs:2329:                 })
crates/borsuk/src/two_bit_generation.rs:2330:                 .and_then(|n| {
crates/borsuk/src/two_bit_generation.rs:2331:                     n.checked_add(crate::sq8_s3_range::capsule_fetch_descriptor_bytes::<
crates/borsuk/src/two_bit_generation.rs:2332:                         crate::source_capsule::VerifiedPageSpan<'_, '_>,
crates/borsuk/src/two_bit_generation.rs:2333:                     >(profile.selected_leaf_limit())?)
crates/borsuk/src/two_bit_generation.rs:2334:                 })
crates/borsuk/src/two_bit_generation.rs:2335:                 .and_then(|n| n.checked_add(4096))
crates/borsuk/src/two_bit_generation.rs:2336:                 .ok_or(bad("capsule query memory"))?
crates/borsuk/src/two_bit_generation.rs:2337:         } else {
crates/borsuk/src/two_bit_generation.rs:2338:             0
crates/borsuk/src/two_bit_generation.rs:2339:         };
crates/borsuk/src/two_bit_generation.rs:2340:         let source_query_memory = if paged {
crates/borsuk/src/two_bit_generation.rs:2341:             limits
crates/borsuk/src/two_bit_generation.rs:2342:                 .max_source_bytes
crates/borsuk/src/two_bit_generation.rs:2343:                 .min(source_payload_bytes)
crates/borsuk/src/two_bit_generation.rs:2344:                 .checked_mul(2)
crates/borsuk/src/two_bit_generation.rs:2345:                 .and_then(|n| n.checked_mul(limits.max_active_queries))
crates/borsuk/src/two_bit_generation.rs:2346:                 .ok_or(bad("source query memory"))? as u64
crates/borsuk/src/two_bit_generation.rs:2347:         } else {
crates/borsuk/src/two_bit_generation.rs:2348:             0
crates/borsuk/src/two_bit_generation.rs:2349:         };
crates/borsuk/src/two_bit_generation.rs:2350:         let validation_page_bytes = if validate_local_records {
crates/borsuk/src/two_bit_generation.rs:2351:             geometry
crates/borsuk/src/two_bit_generation.rs:2352:                 .rows
crates/borsuk/src/two_bit_generation.rs:2353:                 .min(32)
crates/borsuk/src/two_bit_generation.rs:2354:                 .checked_mul(padded.div_ceil(4) + 8)
crates/borsuk/src/two_bit_generation.rs:2355:                 .ok_or(bad("validation page memory"))? as u64
crates/borsuk/src/two_bit_generation.rs:2356:         } else {
crates/borsuk/src/two_bit_generation.rs:2357:             0
crates/borsuk/src/two_bit_generation.rs:2358:         };
crates/borsuk/src/two_bit_generation.rs:2359:         let modeled = disk
crates/borsuk/src/two_bit_generation.rs:2360:             .checked_mul(3)
crates/borsuk/src/two_bit_generation.rs:2361:             .and_then(|n| n.checked_add(codec_memory))
crates/borsuk/src/two_bit_generation.rs:2362:             .and_then(|n| n.checked_add(131072))
crates/borsuk/src/two_bit_generation.rs:2363:             // Shared SOURCE authority/directory allocation headers; payloads are already charged.
crates/borsuk/src/two_bit_generation.rs:2364:             .and_then(|n| n.checked_add(if paged { 128 } else { 0 }))
crates/borsuk/src/two_bit_generation.rs:2365:             .and_then(|n| n.checked_add(manifest.discovery.graph_memory()?))
crates/borsuk/src/two_bit_generation.rs:2366:             .and_then(|n| {
crates/borsuk/src/two_bit_generation.rs:2367:                 n.checked_add(if manifest.discovery.mode() == DiscoveryMode::Semantic {
crates/borsuk/src/two_bit_generation.rs:2368:                     // Root geometry is authenticated by the generation descriptor.
crates/borsuk/src/two_bit_generation.rs:2369:                     // Charge directory capacities, including construction scratch,
crates/borsuk/src/two_bit_generation.rs:2370:                     // before open allocates them. Retired generations are added below.
crates/borsuk/src/two_bit_generation.rs:2371:                     let root = admitted_size("router/root.bin").ok()? as u64;
crates/borsuk/src/two_bit_generation.rs:2372:                     let Discovery::Semantic { root_bytes, .. } = &manifest.discovery else {
crates/borsuk/src/two_bit_generation.rs:2373:                         unreachable!()
crates/borsuk/src/two_bit_generation.rs:2374:                     };
crates/borsuk/src/two_bit_generation.rs:2375:                     let leaves = (*root_bytes - 512) / (64 + 4 * geometry.dimensions);
crates/borsuk/src/two_bit_generation.rs:2376:                     let (_, directory_peak) =
crates/borsuk/src/two_bit_generation.rs:2377:                         crate::semantic_unit_router::directory_allocation_bytes(
crates/borsuk/src/two_bit_generation.rs:2378:                             geometry.rows.div_ceil(32),
crates/borsuk/src/two_bit_generation.rs:2379:                             leaves,
crates/borsuk/src/two_bit_generation.rs:2380:                         )
crates/borsuk/src/two_bit_generation.rs:2381:                         .ok()?;
crates/borsuk/src/two_bit_generation.rs:2382:                     root.checked_mul(32)?
crates/borsuk/src/two_bit_generation.rs:2383:                         .checked_add(directory_peak as u64)?
crates/borsuk/src/two_bit_generation.rs:2384:                         .checked_add(
crates/borsuk/src/two_bit_generation.rs:2385:                             (1024 * 1024_u64).checked_mul(limits.max_active_queries as u64)?,
crates/borsuk/src/two_bit_generation.rs:2386:                         )?
crates/borsuk/src/two_bit_generation.rs:2387:                 } else {
crates/borsuk/src/two_bit_generation.rs:2388:                     0
crates/borsuk/src/two_bit_generation.rs:2389:                 })
crates/borsuk/src/two_bit_generation.rs:2390:             })
crates/borsuk/src/two_bit_generation.rs:2391:             .and_then(|n| n.checked_add(query_memory))
crates/borsuk/src/two_bit_generation.rs:2392:             .and_then(|n| n.checked_add(scheduler_memory))
crates/borsuk/src/two_bit_generation.rs:2393:             .and_then(|n| n.checked_add(source_query_memory))
crates/borsuk/src/two_bit_generation.rs:2394:             .and_then(|n| n.checked_add(capsule_metadata as u64))
crates/borsuk/src/two_bit_generation.rs:2395:             .and_then(|n| {
