//! Encode and immutable-object publication throughput for the V73 row format.
//!
//! Every write number so far has been a bulk build: encode a whole corpus,
//! upload once, divide. That is not what turbopuffer's ~10,000 vectors/s or
//! S3 Vectors' 2,500 vectors/s per index describe, which is sustained ingest
//! where the written vectors become queryable. This diagnostic does not meet
//! that stronger definition: it has no generation manifest or reader-side
//! delta merge, so it must not be reported as query-visible ingest.
//!
//! A batch is encoded into an experimental combined row - an SQ8 row with its
//! precomputed squared norm plus the router's per-row code - and published as
//! one immutable object. The production reader stores row codes in its
//! manifest rather than appending them to the S3 row, so even the byte layout
//! is not directly consumable. The acknowledged PUT measures object
//! availability, not query visibility.
//!
//! Encode cost is data-independent at fixed dimension - quantisation is a clip
//! and a round, and code assignment is a fixed-size argmin over the codebooks
//! whatever the vector contains - so vectors are generated deterministically.

use std::{env, error::Error, sync::Arc, time::Instant};

use futures_util::stream::{self, StreamExt};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, parse_url_opts, path::Path as ObjectPath,
};
use rayon::prelude::*;
use url::Url;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const DIMENSIONS: usize = 768;
const SUBSPACES: usize = 64;
const WIDTH: usize = DIMENSIONS / SUBSPACES;
const ROW_BYTES: usize = 8 + 4 + DIMENSIONS + SUBSPACES;

fn required(name: &str) -> BenchResult<String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn optional_usize(name: &str, fallback: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(fallback),
    }
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * quantile).round() as usize]
}

fn synthesise(count: usize, seed: u64) -> Vec<f32> {
    let mut state = seed | 1;
    let mut out = vec![0.0f32; count * DIMENSIONS];
    for slot in out.iter_mut() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *slot = ((state >> 40) as u32 as f32 / 16_777_216.0) - 0.5;
    }
    out
}

fn encode_batch(
    vectors: &[f32],
    first_id: i64,
    low: &[f32],
    step: &[f32],
    codebooks: &[f32],
) -> Vec<u8> {
    let rows = vectors.len() / DIMENSIONS;
    let mut out = vec![0u8; rows * ROW_BYTES];
    out.par_chunks_exact_mut(ROW_BYTES)
        .enumerate()
        .for_each(|(row, slot)| {
            let vector = &vectors[row * DIMENSIONS..(row + 1) * DIMENSIONS];
            slot[..8].copy_from_slice(&(first_id + row as i64).to_le_bytes());
            let mut norm = 0.0f32;
            for index in 0..DIMENSIONS {
                let level =
                    (((vector[index] - low[index]) / step[index]).round()).clamp(0.0, 255.0);
                slot[12 + index] = level as u8;
                let restored = low[index] + level * step[index];
                norm += restored * restored;
            }
            slot[8..12].copy_from_slice(&norm.to_le_bytes());
            for subspace in 0..SUBSPACES {
                let piece = &vector[subspace * WIDTH..(subspace + 1) * WIDTH];
                let mut best = 0usize;
                let mut best_distance = f32::INFINITY;
                for codeword in 0..256 {
                    let base = (subspace * 256 + codeword) * WIDTH;
                    let mut distance = 0.0f32;
                    for index in 0..WIDTH {
                        let delta = codebooks[base + index] - piece[index];
                        distance += delta * delta;
                    }
                    if distance < best_distance {
                        best_distance = distance;
                        best = codeword;
                    }
                }
                slot[12 + DIMENSIONS + subspace] = best as u8;
            }
        });
    out
}

fn self_test() -> BenchResult<()> {
    let low = vec![-1.0f32; DIMENSIONS];
    let step = vec![2.0f32 / 255.0; DIMENSIONS];
    let codebooks = synthesise(SUBSPACES * 256 * WIDTH / DIMENSIONS + 1, 7);
    let codebooks = codebooks[..SUBSPACES * 256 * WIDTH].to_vec();
    let vectors = synthesise(4, 11);
    let encoded = encode_batch(&vectors, 500, &low, &step, &codebooks);
    if encoded.len() != 4 * ROW_BYTES {
        return Err("encoded batch length differs".into());
    }
    for row in 0..4 {
        let slot = &encoded[row * ROW_BYTES..(row + 1) * ROW_BYTES];
        let identifier = i64::from_le_bytes(slot[..8].try_into().expect("8 bytes"));
        if identifier != 500 + row as i64 {
            return Err(format!("row {row} carries identifier {identifier}").into());
        }
        // The stored norm must be the norm of the DEQUANTISED row, which is what
        // the reader scores against - not the norm of the input vector.
        let stored = f32::from_le_bytes(slot[8..12].try_into().expect("4 bytes"));
        let mut direct = 0.0f32;
        for index in 0..DIMENSIONS {
            let restored = low[index] + f32::from(slot[12 + index]) * step[index];
            direct += restored * restored;
        }
        if (stored - direct).abs() > direct.max(1.0) * 1e-4 {
            return Err(format!("stored norm {stored} differs from {direct}").into());
        }
    }
    println!("{{\"self_test\":\"passed\"}}");
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> BenchResult<()> {
    if env::args().any(|argument| argument == "--self-test") {
        return self_test();
    }
    let uri = required("BORSUK_V74_URI")?;
    let region = required("BORSUK_V74_REGION")?;
    let batch = optional_usize("BORSUK_V74_BATCH", 10_000)?;
    let batches = optional_usize("BORSUK_V74_BATCHES", 40)?;
    let concurrency = optional_usize("BORSUK_V74_CONCURRENCY", 16)?;

    let url = Url::parse(&uri)?;
    let (store, prefix) = parse_url_opts(&url, [("region".to_string(), region)])?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);

    let low = vec![-1.0f32; DIMENSIONS];
    let step = vec![2.0f32 / 255.0; DIMENSIONS];
    let codebooks = synthesise(SUBSPACES * 256, 99)[..SUBSPACES * 256 * WIDTH].to_vec();

    let sample = synthesise(batch, 5);
    let started = Instant::now();
    let _ = encode_batch(&sample, 0, &low, &step, &codebooks);
    let encode_seconds = started.elapsed().as_secs_f64();

    let payloads: Vec<Vec<u8>> = (0..batches)
        .map(|index| {
            encode_batch(
                &synthesise(batch, 17 + index as u64),
                (index * batch) as i64,
                &low,
                &step,
                &codebooks,
            )
        })
        .collect();

    let started = Instant::now();
    let mut visibility: Vec<f64> =
        stream::iter(payloads.into_iter().enumerate().map(|(index, body)| {
            let store = Arc::clone(&store);
            let key = ObjectPath::from(format!("{prefix}/delta-{index:06}.bin"));
            async move {
                let at = Instant::now();
                store.put(&key, PutPayload::from(body)).await?;
                Ok::<f64, object_store::Error>(at.elapsed().as_secs_f64() * 1000.0)
            }
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let publish_seconds = started.elapsed().as_secs_f64();
    visibility.sort_by(f64::total_cmp);

    let vectors = batch * batches;
    let encode_rate = batch as f64 / encode_seconds;
    let publish_rate = vectors as f64 / publish_seconds;
    // Overlapped, the slower stage sets the rate; serialised is the
    // pessimistic bound where a batch is encoded before any of it is sent.
    let pipelined = encode_rate.min(publish_rate);
    let serialised = 1.0 / (1.0 / encode_rate + 1.0 / publish_rate);
    let report = serde_json::json!({
        "schema": "borsuk-v74-object-publication-result-v2",
        "claim_eligible": false,
        "evidence_kind": "measured-synthetic-row-encode-and-object-put-not-query-visible-ingest",
        "vector_source": "deterministic-synthetic-encode-cost-is-data-independent",
        "dimensions": DIMENSIONS,
        "row_bytes": ROW_BYTES,
        "batch_vectors": batch,
        "batches": batches,
        "concurrency": concurrency,
        "total_vectors": vectors,
        "encode_only_vectors_per_second": encode_rate.round() as u64,
        "publish_seconds": publish_seconds,
        // Publish alone is a storage ceiling, never ingest throughput: the
        // payloads were encoded before its clock started.
        "publish_only_vectors_per_second": publish_rate.round() as u64,
        "pipelined_vectors_per_second": pipelined.round() as u64,
        "serialised_vectors_per_second": serialised.round() as u64,
        "published_mib_per_second":
            (vectors * ROW_BYTES) as f64 / publish_seconds / (1024.0 * 1024.0),
        "put_acknowledgement_ms": {
            "p50": percentile(&visibility, 0.50),
            "p95": percentile(&visibility, 0.95),
            "p99": percentile(&visibility, 0.99),
        },
    });
    println!("{report}");
    Ok(())
}
