crates/borsuk/src/sq8_s3_range.rs:22: /// SOURCE response authentication policy; SQ8 authentication stays inline.
crates/borsuk/src/sq8_s3_range.rs:23: #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
crates/borsuk/src/sq8_s3_range.rs:24: #[serde(rename_all = "snake_case")]
crates/borsuk/src/sq8_s3_range.rs:25: pub enum SourceAuthentication {
crates/borsuk/src/sq8_s3_range.rs:26:     /// Production control, with no worker allocation.
crates/borsuk/src/sq8_s3_range.rs:27:     #[default]
crates/borsuk/src/sq8_s3_range.rs:28:     Inline,
crates/borsuk/src/sq8_s3_range.rs:29:     /// Experimental two-worker compute bound, independent of download concurrency.
crates/borsuk/src/sq8_s3_range.rs:30:     Offload2,
crates/borsuk/src/sq8_s3_range.rs:31: }
crates/borsuk/src/sq8_s3_range.rs:32:
crates/borsuk/src/sq8_s3_range.rs:33: impl SourceAuthentication {
crates/borsuk/src/sq8_s3_range.rs:34:     /// Extra library reservation for all active queries: worker stacks, jobs and scheduler
crates/borsuk/src/sq8_s3_range.rs:35:     /// state. Existing full payload and ordinary range-state reservations stay charged.
crates/borsuk/src/sq8_s3_range.rs:36:     pub fn additional_memory_bytes(
crates/borsuk/src/sq8_s3_range.rs:37:         self,
crates/borsuk/src/sq8_s3_range.rs:38:         downloads: usize,
crates/borsuk/src/sq8_s3_range.rs:39:         ranges: usize,
crates/borsuk/src/sq8_s3_range.rs:40:         active_queries: usize,
crates/borsuk/src/sq8_s3_range.rs:41:     ) -> Option<u64> {
crates/borsuk/src/sq8_s3_range.rs:42:         if self == Self::Inline {
crates/borsuk/src/sq8_s3_range.rs:43:             return Some(0);
crates/borsuk/src/sq8_s3_range.rs:44:         }
crates/borsuk/src/sq8_s3_range.rs:45:         u64::try_from(source_authentication_bytes(downloads, ranges)?)
crates/borsuk/src/sq8_s3_range.rs:46:             .ok()?
crates/borsuk/src/sq8_s3_range.rs:47:             .checked_mul(u64::try_from(active_queries).ok()?)?
crates/borsuk/src/sq8_s3_range.rs:48:             .checked_add(256)
crates/borsuk/src/sq8_s3_range.rs:49:     }
crates/borsuk/src/sq8_s3_range.rs:50: }
crates/borsuk/src/sq8_s3_range.rs:51:
crates/borsuk/src/sq8_s3_range.rs:52: const SOURCE_AUTH_WORKERS: usize = 2;
crates/borsuk/src/sq8_s3_range.rs:53: const SOURCE_AUTH_STACK_BYTES: usize = 1024 * 1024;
crates/borsuk/src/sq8_s3_range.rs:54: const SOURCE_AUTH_FUTURE_BYTES: usize = 4096;
crates/borsuk/src/sq8_s3_range.rs:55:
crates/borsuk/src/sq8_s3_range.rs:56: /// Extra per-query state; payloads move into jobs and remain in the existing full-body budget.
crates/borsuk/src/sq8_s3_range.rs:57: pub(crate) fn source_authentication_bytes(downloads: usize, ranges: usize) -> Option<usize> {
crates/borsuk/src/sq8_s3_range.rs:58:     let pending = ranges.min(downloads.checked_add(SOURCE_AUTH_WORKERS)?);
crates/borsuk/src/sq8_s3_range.rs:59:     pending
crates/borsuk/src/sq8_s3_range.rs:60:         .checked_mul(SOURCE_AUTH_FUTURE_BYTES)?
crates/borsuk/src/sq8_s3_range.rs:61:         .checked_add(SOURCE_AUTH_WORKERS * (SOURCE_AUTH_STACK_BYTES + 4096))?
crates/borsuk/src/sq8_s3_range.rs:62:         .checked_add(4096)
crates/borsuk/src/sq8_s3_range.rs:63: }
crates/borsuk/src/sq8_s3_range.rs:64:
crates/borsuk/src/sq8_s3_range.rs:77: type SourceAuthJob = Box<dyn FnOnce() + Send + 'static>;
crates/borsuk/src/sq8_s3_range.rs:78:
crates/borsuk/src/sq8_s3_range.rs:79: // Weak ownership lets the last query close and join the pool. The worker lease also
crates/borsuk/src/sq8_s3_range.rs:80: // covers that join, so reinitialization cannot overlap old and new worker threads.
crates/borsuk/src/sq8_s3_range.rs:81: static SOURCE_AUTH_POOL: LazyLock<Mutex<std::sync::Weak<SourceAuthPool>>> =
crates/borsuk/src/sq8_s3_range.rs:82:     LazyLock::new(|| Mutex::new(std::sync::Weak::new()));
crates/borsuk/src/sq8_s3_range.rs:83: static SOURCE_AUTH_INITIALIZE: LazyLock<Arc<tokio::sync::Semaphore>> =
crates/borsuk/src/sq8_s3_range.rs:84:     LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(1)));
crates/borsuk/src/sq8_s3_range.rs:85: static SOURCE_AUTH_THREADS: LazyLock<Arc<tokio::sync::Semaphore>> =
crates/borsuk/src/sq8_s3_range.rs:86:     LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(SOURCE_AUTH_WORKERS)));
crates/borsuk/src/sq8_s3_range.rs:87:
crates/borsuk/src/sq8_s3_range.rs:88: struct SourceAuthPool {
crates/borsuk/src/sq8_s3_range.rs:89:     sender: Option<std::sync::mpsc::SyncSender<SourceAuthJob>>,
crates/borsuk/src/sq8_s3_range.rs:90:     workers: Vec<std::thread::JoinHandle<()>>,
crates/borsuk/src/sq8_s3_range.rs:91:     slots: Arc<tokio::sync::Semaphore>,
crates/borsuk/src/sq8_s3_range.rs:92:     _worker_lease: tokio::sync::OwnedSemaphorePermit,
crates/borsuk/src/sq8_s3_range.rs:93: }
crates/borsuk/src/sq8_s3_range.rs:94:
crates/borsuk/src/sq8_s3_range.rs:95: impl SourceAuthPool {
crates/borsuk/src/sq8_s3_range.rs:96:     async fn acquire() -> Result<Arc<Self>, RangeFetchError> {
crates/borsuk/src/sq8_s3_range.rs:97:         let current = SOURCE_AUTH_POOL
crates/borsuk/src/sq8_s3_range.rs:98:             .lock()
crates/borsuk/src/sq8_s3_range.rs:99:             .unwrap_or_else(std::sync::PoisonError::into_inner)
crates/borsuk/src/sq8_s3_range.rs:100:             .upgrade();
crates/borsuk/src/sq8_s3_range.rs:101:         if let Some(current) = current {
crates/borsuk/src/sq8_s3_range.rs:102:             return Ok(current);
crates/borsuk/src/sq8_s3_range.rs:103:         }
crates/borsuk/src/sq8_s3_range.rs:104:         let _initialize = Arc::clone(&SOURCE_AUTH_INITIALIZE)
crates/borsuk/src/sq8_s3_range.rs:105:             .acquire_owned()
crates/borsuk/src/sq8_s3_range.rs:106:             .await
crates/borsuk/src/sq8_s3_range.rs:107:             .map_err(|_| RangeFetchError::UnexpectedMetadata)?;
crates/borsuk/src/sq8_s3_range.rs:108:         let current = SOURCE_AUTH_POOL
crates/borsuk/src/sq8_s3_range.rs:109:             .lock()
crates/borsuk/src/sq8_s3_range.rs:110:             .unwrap_or_else(std::sync::PoisonError::into_inner)
crates/borsuk/src/sq8_s3_range.rs:111:             .upgrade();
crates/borsuk/src/sq8_s3_range.rs:112:         if let Some(current) = current {
crates/borsuk/src/sq8_s3_range.rs:113:             return Ok(current);
crates/borsuk/src/sq8_s3_range.rs:114:         }
crates/borsuk/src/sq8_s3_range.rs:115:         let worker_lease = Arc::clone(&SOURCE_AUTH_THREADS)
crates/borsuk/src/sq8_s3_range.rs:116:             .acquire_many_owned(SOURCE_AUTH_WORKERS as u32)
crates/borsuk/src/sq8_s3_range.rs:117:             .await
crates/borsuk/src/sq8_s3_range.rs:118:             .map_err(|_| RangeFetchError::UnexpectedMetadata)?;
crates/borsuk/src/sq8_s3_range.rs:119:         let (sender, receiver) =
crates/borsuk/src/sq8_s3_range.rs:120:             std::sync::mpsc::sync_channel::<SourceAuthJob>(SOURCE_AUTH_WORKERS);
crates/borsuk/src/sq8_s3_range.rs:121:         let receiver = Arc::new(Mutex::new(receiver));
crates/borsuk/src/sq8_s3_range.rs:122:         let mut pool = Self {
crates/borsuk/src/sq8_s3_range.rs:123:             sender: Some(sender),
crates/borsuk/src/sq8_s3_range.rs:124:             workers: Vec::with_capacity(SOURCE_AUTH_WORKERS),
crates/borsuk/src/sq8_s3_range.rs:125:             slots: Arc::new(tokio::sync::Semaphore::new(SOURCE_AUTH_WORKERS)),
crates/borsuk/src/sq8_s3_range.rs:126:             _worker_lease: worker_lease,
crates/borsuk/src/sq8_s3_range.rs:127:         };
crates/borsuk/src/sq8_s3_range.rs:128:         for _ in 0..SOURCE_AUTH_WORKERS {
crates/borsuk/src/sq8_s3_range.rs:129:             let receiver = Arc::clone(&receiver);
crates/borsuk/src/sq8_s3_range.rs:130:             let worker = std::thread::Builder::new()
crates/borsuk/src/sq8_s3_range.rs:131:                 .name("borsuk-source-auth".into())
crates/borsuk/src/sq8_s3_range.rs:132:                 .stack_size(SOURCE_AUTH_STACK_BYTES)
crates/borsuk/src/sq8_s3_range.rs:133:                 .spawn(move || {
crates/borsuk/src/sq8_s3_range.rs:134:                     loop {
crates/borsuk/src/sq8_s3_range.rs:135:                         let next = receiver
crates/borsuk/src/sq8_s3_range.rs:136:                             .lock()
crates/borsuk/src/sq8_s3_range.rs:137:                             .unwrap_or_else(std::sync::PoisonError::into_inner)
crates/borsuk/src/sq8_s3_range.rs:138:                             .recv();
crates/borsuk/src/sq8_s3_range.rs:139:                         let Ok(job) = next else { break };
crates/borsuk/src/sq8_s3_range.rs:140:                         job();
crates/borsuk/src/sq8_s3_range.rs:141:                     }
crates/borsuk/src/sq8_s3_range.rs:142:                 })
crates/borsuk/src/sq8_s3_range.rs:143:                 .map_err(|_| RangeFetchError::UnexpectedMetadata)?;
crates/borsuk/src/sq8_s3_range.rs:144:             pool.workers.push(worker);
crates/borsuk/src/sq8_s3_range.rs:145:         }
crates/borsuk/src/sq8_s3_range.rs:146:         let pool = Arc::new(pool);
crates/borsuk/src/sq8_s3_range.rs:147:         *SOURCE_AUTH_POOL
crates/borsuk/src/sq8_s3_range.rs:148:             .lock()
crates/borsuk/src/sq8_s3_range.rs:149:             .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&pool);
crates/borsuk/src/sq8_s3_range.rs:150:         Ok(pool)
crates/borsuk/src/sq8_s3_range.rs:151:     }
crates/borsuk/src/sq8_s3_range.rs:152: }
crates/borsuk/src/sq8_s3_range.rs:153:
crates/borsuk/src/sq8_s3_range.rs:154: impl Drop for SourceAuthPool {
crates/borsuk/src/sq8_s3_range.rs:155:     fn drop(&mut self) {
crates/borsuk/src/sq8_s3_range.rs:156:         self.sender.take();
crates/borsuk/src/sq8_s3_range.rs:157:         for worker in self.workers.drain(..) {
crates/borsuk/src/sq8_s3_range.rs:158:             let _ = worker.join();
crates/borsuk/src/sq8_s3_range.rs:159:         }
crates/borsuk/src/sq8_s3_range.rs:160:     }
crates/borsuk/src/sq8_s3_range.rs:161: }
crates/borsuk/src/sq8_s3_range.rs:162:
crates/borsuk/src/sq8_s3_range.rs:163: #[derive(Default)]
crates/borsuk/src/sq8_s3_range.rs:164: struct SourceAuthGroup {
crates/borsuk/src/sq8_s3_range.rs:165:     pending: Mutex<usize>,
crates/borsuk/src/sq8_s3_range.rs:166:     changed: std::sync::Condvar,
crates/borsuk/src/sq8_s3_range.rs:167: }
crates/borsuk/src/sq8_s3_range.rs:168:
crates/borsuk/src/sq8_s3_range.rs:169: struct SourceAuthPending {
crates/borsuk/src/sq8_s3_range.rs:170:     group: Arc<SourceAuthGroup>,
crates/borsuk/src/sq8_s3_range.rs:171:     #[cfg(test)]
crates/borsuk/src/sq8_s3_range.rs:172:     _lifetime: Arc<()>,
crates/borsuk/src/sq8_s3_range.rs:173: }
crates/borsuk/src/sq8_s3_range.rs:174:
crates/borsuk/src/sq8_s3_range.rs:175: impl Drop for SourceAuthPending {
crates/borsuk/src/sq8_s3_range.rs:176:     fn drop(&mut self) {
crates/borsuk/src/sq8_s3_range.rs:177:         let mut pending = self
crates/borsuk/src/sq8_s3_range.rs:178:             .group
crates/borsuk/src/sq8_s3_range.rs:179:             .pending
crates/borsuk/src/sq8_s3_range.rs:180:             .lock()
crates/borsuk/src/sq8_s3_range.rs:181:             .unwrap_or_else(std::sync::PoisonError::into_inner);
crates/borsuk/src/sq8_s3_range.rs:182:         *pending -= 1;
crates/borsuk/src/sq8_s3_range.rs:183:         if *pending == 0 {
crates/borsuk/src/sq8_s3_range.rs:184:             self.group.changed.notify_all();
crates/borsuk/src/sq8_s3_range.rs:185:         }
crates/borsuk/src/sq8_s3_range.rs:186:     }
crates/borsuk/src/sq8_s3_range.rs:187: }
crates/borsuk/src/sq8_s3_range.rs:188:
crates/borsuk/src/sq8_s3_range.rs:189: /// Per-query download admission and drain, with two shared process-wide compute workers.
crates/borsuk/src/sq8_s3_range.rs:190: pub(crate) struct SourceAuthWorkers {
crates/borsuk/src/sq8_s3_range.rs:191:     pool: Arc<SourceAuthPool>,
crates/borsuk/src/sq8_s3_range.rs:192:     group: Arc<SourceAuthGroup>,
crates/borsuk/src/sq8_s3_range.rs:193:     slots: Arc<tokio::sync::Semaphore>,
crates/borsuk/src/sq8_s3_range.rs:194:     downloads: tokio::sync::Semaphore,
crates/borsuk/src/sq8_s3_range.rs:195:     #[cfg(test)]
crates/borsuk/src/sq8_s3_range.rs:196:     lifetime: Arc<()>,
crates/borsuk/src/sq8_s3_range.rs:197: }
crates/borsuk/src/sq8_s3_range.rs:198:
crates/borsuk/src/sq8_s3_range.rs:199: impl SourceAuthWorkers {
crates/borsuk/src/sq8_s3_range.rs:200:     pub(crate) async fn new(downloads: usize) -> Result<Self, RangeFetchError> {
crates/borsuk/src/sq8_s3_range.rs:201:         if downloads == 0 || downloads > tokio::sync::Semaphore::MAX_PERMITS {
crates/borsuk/src/sq8_s3_range.rs:202:             return Err(RangeFetchError::UnexpectedMetadata);
crates/borsuk/src/sq8_s3_range.rs:203:         }
crates/borsuk/src/sq8_s3_range.rs:204:         let pool = SourceAuthPool::acquire().await?;
crates/borsuk/src/sq8_s3_range.rs:205:         Ok(Self {
crates/borsuk/src/sq8_s3_range.rs:206:             slots: Arc::clone(&pool.slots),
crates/borsuk/src/sq8_s3_range.rs:207:             pool,
crates/borsuk/src/sq8_s3_range.rs:208:             group: Arc::new(SourceAuthGroup::default()),
crates/borsuk/src/sq8_s3_range.rs:209:             downloads: tokio::sync::Semaphore::new(downloads),
crates/borsuk/src/sq8_s3_range.rs:210:             #[cfg(test)]
crates/borsuk/src/sq8_s3_range.rs:211:             lifetime: Arc::new(()),
crates/borsuk/src/sq8_s3_range.rs:212:         })
crates/borsuk/src/sq8_s3_range.rs:213:     }
crates/borsuk/src/sq8_s3_range.rs:214:
crates/borsuk/src/sq8_s3_range.rs:215:     pub(crate) async fn authenticate<T: Send + 'static>(
crates/borsuk/src/sq8_s3_range.rs:216:         &self,
crates/borsuk/src/sq8_s3_range.rs:217:         mut probe: Option<Box<Probe>>,
crates/borsuk/src/sq8_s3_range.rs:218:         authenticate: impl FnOnce() -> Result<T, RangeFetchError> + Send + 'static,
crates/borsuk/src/sq8_s3_range.rs:219:     ) -> (Result<T, RangeFetchError>, Option<Box<Probe>>) {
crates/borsuk/src/sq8_s3_range.rs:220:         let slot = Arc::clone(&self.slots)
crates/borsuk/src/sq8_s3_range.rs:221:             .acquire_owned()
crates/borsuk/src/sq8_s3_range.rs:222:             .await
crates/borsuk/src/sq8_s3_range.rs:223:             .expect("SOURCE worker slots stay open");
crates/borsuk/src/sq8_s3_range.rs:224:         *self
crates/borsuk/src/sq8_s3_range.rs:225:             .group
crates/borsuk/src/sq8_s3_range.rs:226:             .pending
crates/borsuk/src/sq8_s3_range.rs:227:             .lock()
crates/borsuk/src/sq8_s3_range.rs:228:             .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
crates/borsuk/src/sq8_s3_range.rs:229:         let pending = SourceAuthPending {
crates/borsuk/src/sq8_s3_range.rs:230:             group: Arc::clone(&self.group),
crates/borsuk/src/sq8_s3_range.rs:231:             #[cfg(test)]
crates/borsuk/src/sq8_s3_range.rs:232:             _lifetime: Arc::clone(&self.lifetime),
crates/borsuk/src/sq8_s3_range.rs:233:         };
crates/borsuk/src/sq8_s3_range.rs:234:         let (send, receive) = tokio::sync::oneshot::channel();
crates/borsuk/src/sq8_s3_range.rs:235:         let job = Box::new(move || {
crates/borsuk/src/sq8_s3_range.rs:236:             let _pending = pending;
crates/borsuk/src/sq8_s3_range.rs:237:             let _slot = slot;
crates/borsuk/src/sq8_s3_range.rs:238:             let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
crates/borsuk/src/sq8_s3_range.rs:239:                 measured_auth(&mut probe.as_deref_mut(), authenticate)
crates/borsuk/src/sq8_s3_range.rs:240:             }))
crates/borsuk/src/sq8_s3_range.rs:241:             .unwrap_or(Err(RangeFetchError::UnexpectedMetadata));
crates/borsuk/src/sq8_s3_range.rs:242:             let _ = send.send((result, probe));
crates/borsuk/src/sq8_s3_range.rs:243:         });
crates/borsuk/src/sq8_s3_range.rs:244:         if self
crates/borsuk/src/sq8_s3_range.rs:245:             .pool
crates/borsuk/src/sq8_s3_range.rs:246:             .sender
crates/borsuk/src/sq8_s3_range.rs:247:             .as_ref()
crates/borsuk/src/sq8_s3_range.rs:248:             .expect("live SOURCE pool")
crates/borsuk/src/sq8_s3_range.rs:249:             .try_send(job)
crates/borsuk/src/sq8_s3_range.rs:250:             .is_err()
crates/borsuk/src/sq8_s3_range.rs:251:         {
crates/borsuk/src/sq8_s3_range.rs:252:             return (Err(RangeFetchError::UnexpectedMetadata), None);
crates/borsuk/src/sq8_s3_range.rs:253:         }
crates/borsuk/src/sq8_s3_range.rs:254:         receive
crates/borsuk/src/sq8_s3_range.rs:255:             .await
crates/borsuk/src/sq8_s3_range.rs:256:             .unwrap_or((Err(RangeFetchError::UnexpectedMetadata), None))
crates/borsuk/src/sq8_s3_range.rs:257:     }
crates/borsuk/src/sq8_s3_range.rs:258: }
crates/borsuk/src/sq8_s3_range.rs:259:
crates/borsuk/src/sq8_s3_range.rs:260: impl Drop for SourceAuthWorkers {
crates/borsuk/src/sq8_s3_range.rs:261:     fn drop(&mut self) {
crates/borsuk/src/sq8_s3_range.rs:262:         let pending = self
crates/borsuk/src/sq8_s3_range.rs:263:             .group
crates/borsuk/src/sq8_s3_range.rs:264:             .pending
crates/borsuk/src/sq8_s3_range.rs:265:             .lock()
crates/borsuk/src/sq8_s3_range.rs:266:             .unwrap_or_else(std::sync::PoisonError::into_inner);
crates/borsuk/src/sq8_s3_range.rs:267:         drop(
crates/borsuk/src/sq8_s3_range.rs:268:             self.group
crates/borsuk/src/sq8_s3_range.rs:269:                 .changed
crates/borsuk/src/sq8_s3_range.rs:270:                 .wait_while(pending, |pending| *pending != 0)
crates/borsuk/src/sq8_s3_range.rs:271:                 .unwrap_or_else(std::sync::PoisonError::into_inner),
crates/borsuk/src/sq8_s3_range.rs:272:         );
crates/borsuk/src/sq8_s3_range.rs:273:     }
crates/borsuk/src/sq8_s3_range.rs:274: }
