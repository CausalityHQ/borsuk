#![allow(missing_docs)]

use std::{env, fs, path::PathBuf, str::FromStr, time::Instant};

use borsuk::{
    BorsukIndex, BuildConfig, IndexConfig, LeafCapability, PhysicalFormat, PhysicalLayoutPolicy,
    PhysicalObjectRole, SearchOptions, VectorElementType, VectorMetric, VectorRecord, WalConfig,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("wal_layout_bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> borsuk::Result<()> {
    let uri = if let Some(uri) = env::var_os("BORSUK_WAL_AB_URI").filter(|value| !value.is_empty())
    {
        uri.to_string_lossy().into_owned()
    } else {
        let root = env::var_os("BORSUK_WAL_AB_ROOT")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                borsuk::BorsukError::InvalidStorage(
                    "set BORSUK_WAL_AB_URI or BORSUK_WAL_AB_ROOT".to_string(),
                )
            })?;
        if root.exists() {
            return Err(borsuk::BorsukError::InvalidStorage(format!(
                "refusing to reuse WAL A/B root `{}`",
                root.display()
            )));
        }
        root.to_string_lossy().into_owned()
    };
    let policy_name = env::var("BORSUK_WAL_AB_FORMAT").unwrap_or_else(|_| "parquet".to_string());
    let policy = if policy_name == "adaptive" {
        PhysicalLayoutPolicy::production_baseline().with_wal_vortex_candidate()
    } else {
        let format = PhysicalFormat::from_str(&policy_name)?;
        if !matches!(format, PhysicalFormat::Parquet | PhysicalFormat::Vortex) {
            return Err(borsuk::BorsukError::InvalidStorage(
                "WAL A/B format must be parquet, vortex, or adaptive".to_string(),
            ));
        }
        PhysicalLayoutPolicy::production_baseline()
            .with_role_format(PhysicalObjectRole::WalRun, format)
    };
    let rows = env_usize("BORSUK_WAL_AB_ROWS", 20_000)?;
    let dimensions = env_usize("BORSUK_WAL_AB_DIMENSIONS", 96)?;
    let batch_rows = env_usize("BORSUK_WAL_AB_BATCH_ROWS", rows)?;
    let queries = env_usize("BORSUK_WAL_AB_QUERIES", 100)?;
    let element_type = VectorElementType::from_str(
        &env::var("BORSUK_WAL_AB_ELEMENT_TYPE").unwrap_or_else(|_| "float32".to_string()),
    )?;
    let metric = VectorMetric::from_str(&env::var("BORSUK_WAL_AB_METRIC").unwrap_or_else(|_| {
        if element_type == VectorElementType::Binary {
            "hamming".to_string()
        } else {
            "euclidean".to_string()
        }
    }))?;
    let repetition =
        env::var("BORSUK_WAL_AB_REPETITION").unwrap_or_else(|_| "unspecified".to_string());
    if rows == 0 || dimensions == 0 || batch_rows == 0 || queries == 0 {
        return Err(borsuk::BorsukError::InvalidStorage(
            "WAL A/B rows, dimensions, batch rows, and queries must be positive".to_string(),
        ));
    }
    if element_type != VectorElementType::Float32
        && dimensions < usize::BITS as usize
        && rows > (1_usize << dimensions)
    {
        return Err(borsuk::BorsukError::InvalidStorage(format!(
            "WAL A/B {element_type} fixture needs at least ceil(log2(rows)) dimensions for unique records"
        )));
    }

    let records = if let Some(dataset) =
        env::var_os("BORSUK_WAL_AB_DATASET").filter(|value| !value.is_empty())
    {
        load_dataset_records(PathBuf::from(dataset), rows, dimensions, element_type)?
    } else {
        (0..rows)
            .map(|row| {
                VectorRecord::new(format!("r{row:08}"), vector(row, dimensions, element_type))
            })
            .collect::<Vec<_>>()
    };
    let wal = WalConfig {
        enabled: true,
        flush_threshold_runs: usize::MAX,
        flush_threshold_records: usize::MAX,
        flush_threshold_bytes: u64::MAX,
        collection_flush_threshold_bytes: u64::MAX,
    };
    let mut index = BorsukIndex::create_with_wal_capability_and_build_config(
        IndexConfig {
            uri: uri.clone(),
            metric: metric.clone(),
            dimensions,
            segment_max_vectors: rows.max(1),
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
        wal,
        LeafCapability::PqScanOnly,
        BuildConfig {
            vector_element_type: element_type,
            physical_layout: policy,
            ..BuildConfig::default()
        },
    )?;

    let mut batch_ms = Vec::new();
    let mut ingest_bytes_written = 0_u64;
    let mut ingest_gets = 0_u64;
    let mut ingest_puts = 0_u64;
    let mut ingest_heads = 0_u64;
    let mut ingest_lists = 0_u64;
    let ingest_started = Instant::now();
    for batch in records.chunks(batch_rows) {
        let started = Instant::now();
        let (_, report) = index.add_with_report(
            batch.iter().map(|record| record.vector.clone()).collect(),
            Some(batch.iter().map(|record| record.id.to_string()).collect()),
        )?;
        batch_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
        ingest_bytes_written = ingest_bytes_written.saturating_add(report.total_bytes_written);
        ingest_gets = ingest_gets.saturating_add(report.requests.gets);
        ingest_puts = ingest_puts.saturating_add(report.requests.puts);
        ingest_heads = ingest_heads.saturating_add(report.requests.heads);
        ingest_lists = ingest_lists.saturating_add(report.requests.lists);
    }
    let ingest_ms = ingest_started.elapsed().as_secs_f64() * 1_000.0;
    if index.stats().records != rows {
        return Err(borsuk::BorsukError::InvalidStorage(
            "WAL A/B live-record count mismatch after ingest".to_string(),
        ));
    }
    drop(index);

    let open_started = Instant::now();
    let mut reopened = BorsukIndex::open(&uri)?;
    let open_ms = open_started.elapsed().as_secs_f64() * 1_000.0;

    let first_query_started = Instant::now();
    let first_report = reopened.search_with_report(&records[0].vector, SearchOptions::exact(10))?;
    let first_query_ms = first_query_started.elapsed().as_secs_f64() * 1_000.0;
    if !first_report
        .hits
        .first()
        .is_some_and(|hit| hit.distance.abs() <= 1.0e-6)
    {
        return Err(borsuk::BorsukError::InvalidStorage(
            "WAL A/B first exact query did not return a zero-distance nearest record".to_string(),
        ));
    }

    let mut warm_query_ms = Vec::with_capacity(queries);
    let mut warm_query_gets = Vec::with_capacity(queries);
    let mut warm_query_backing_bytes = Vec::with_capacity(queries);
    for query in 0..queries {
        let source = query.wrapping_mul(7_919) % rows;
        let started = Instant::now();
        let report =
            reopened.search_with_report(&records[source].vector, SearchOptions::exact(10))?;
        warm_query_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
        warm_query_gets.push(report.requests.gets as f64);
        warm_query_backing_bytes.push(report.backing_bytes_read as f64);
        if !report
            .hits
            .first()
            .is_some_and(|hit| hit.distance.abs() <= 1.0e-6)
        {
            return Err(borsuk::BorsukError::InvalidStorage(
                "WAL A/B warm exact query did not return a zero-distance nearest record"
                    .to_string(),
            ));
        }
    }
    let stats = reopened.stats();
    let footprint = WalFootprint {
        objects: stats.wal_record_runs,
        bytes: stats.wal_record_bytes,
        parquet_objects: stats.wal_parquet_record_runs,
        parquet_bytes: stats.wal_parquet_record_bytes,
        vortex_objects: stats.wal_vortex_record_runs,
        vortex_bytes: stats.wal_vortex_record_bytes,
    };

    let flush_started = Instant::now();
    reopened.flush()?;
    let flush_ms = flush_started.elapsed().as_secs_f64() * 1_000.0;
    drop(reopened);
    let final_index = BorsukIndex::open(&uri)?;
    if final_index.stats().records != rows {
        return Err(borsuk::BorsukError::InvalidStorage(
            "WAL A/B live-record count mismatch after flush and reopen".to_string(),
        ));
    }

    let header = "repetition,policy,element_type,metric,rows,dimensions,batch_rows,batches,wal_objects,wal_bytes,parquet_objects,parquet_bytes,vortex_objects,vortex_bytes,ingest_ms,batch_p95_ms,ingest_bytes_written,ingest_gets,ingest_puts,ingest_heads,ingest_lists,open_ms,first_query_ms,first_query_gets,first_query_backing_bytes,warm_query_p95_ms,warm_query_p99_ms,warm_query_gets_p95,warm_query_backing_bytes_p95,flush_ms,status";
    let row = format!(
        "{repetition},{policy_name},{element_type},{metric},{rows},{dimensions},{batch_rows},{},{},{},{},{},{},{},{ingest_ms:.6},{:.6},{ingest_bytes_written},{ingest_gets},{ingest_puts},{ingest_heads},{ingest_lists},{open_ms:.6},{first_query_ms:.6},{},{},{:.6},{:.6},{:.6},{:.6},{flush_ms:.6},complete",
        batch_ms.len(),
        footprint.objects,
        footprint.bytes,
        footprint.parquet_objects,
        footprint.parquet_bytes,
        footprint.vortex_objects,
        footprint.vortex_bytes,
        percentile(&batch_ms, 0.95),
        first_report.requests.gets,
        first_report.backing_bytes_read,
        percentile(&warm_query_ms, 0.95),
        percentile(&warm_query_ms, 0.99),
        percentile(&warm_query_gets, 0.95),
        percentile(&warm_query_backing_bytes, 0.95),
    );
    let csv = format!("{header}\n{row}\n");
    print!("{csv}");
    if let Some(path) = env::var_os("BORSUK_WAL_AB_OUTPUT").map(PathBuf::from) {
        if path.exists() {
            return Err(borsuk::BorsukError::InvalidStorage(format!(
                "refusing to overwrite WAL A/B output `{}`",
                path.display()
            )));
        }
        fs::write(&path, csv).map_err(|source| borsuk::BorsukError::Io { path, source })?;
    }
    Ok(())
}

fn vector(row: usize, dimensions: usize, element_type: VectorElementType) -> Vec<f32> {
    (0..dimensions)
        .map(|dimension| {
            if element_type == VectorElementType::Binary {
                return ((row >> (dimension % usize::BITS as usize)) & 1) as f32;
            }
            if element_type == VectorElementType::Int8 {
                let shift = (dimension % std::mem::size_of::<usize>()).saturating_mul(8);
                return ((row >> shift) as u8 as i8) as f32;
            }
            if element_type != VectorElementType::Float32 && dimension < usize::BITS as usize {
                return ((row >> dimension) & 1) as f32;
            }
            if element_type == VectorElementType::Float32 && dimension == 0 {
                return row as f32;
            }
            let value = row
                .wrapping_mul(31)
                .wrapping_add(dimension.wrapping_mul(17))
                % 1_009;
            value as f32 / 1_009.0
        })
        .collect()
}

fn load_dataset_records(
    dataset: PathBuf,
    rows: usize,
    dimensions: usize,
    element_type: VectorElementType,
) -> borsuk::Result<Vec<VectorRecord>> {
    if element_type != VectorElementType::Float32 {
        return Err(borsuk::BorsukError::InvalidStorage(
            "real WAL A/B datasets currently require float32".to_string(),
        ));
    }
    let path = dataset.join("train.f32");
    let bytes = fs::read(&path).map_err(|source| borsuk::BorsukError::Io {
        path: path.clone(),
        source,
    })?;
    let required = rows
        .checked_mul(dimensions)
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| {
            borsuk::BorsukError::InvalidStorage(
                "WAL A/B dataset byte requirement overflows usize".to_string(),
            )
        })?;
    if bytes.len() < required {
        return Err(borsuk::BorsukError::InvalidStorage(format!(
            "WAL A/B dataset `{}` has {} train bytes; need at least {required}",
            dataset.display(),
            bytes.len()
        )));
    }
    bytes[..required]
        .chunks_exact(dimensions * std::mem::size_of::<f32>())
        .enumerate()
        .map(|(row, encoded)| {
            let vector = encoded
                .chunks_exact(std::mem::size_of::<f32>())
                .map(|value| f32::from_le_bytes(value.try_into().expect("four-byte chunk")))
                .collect::<Vec<_>>();
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(borsuk::BorsukError::InvalidStorage(format!(
                    "WAL A/B dataset row {row} contains a non-finite value"
                )));
            }
            Ok(VectorRecord::new(format!("r{row:08}"), vector))
        })
        .collect()
}

#[derive(Default)]
struct WalFootprint {
    objects: usize,
    bytes: u64,
    parquet_objects: usize,
    parquet_bytes: u64,
    vortex_objects: usize,
    vortex_bytes: u64,
}

fn env_usize(name: &str, default: usize) -> borsuk::Result<usize> {
    env::var(name).map_or(Ok(default), |value| {
        value.parse::<usize>().map_err(|_| {
            borsuk::BorsukError::InvalidStorage(format!("{name} must be an unsigned integer"))
        })
    })
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let position = (ordered.len() - 1) as f64 * quantile;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        ordered[lower]
    } else {
        let weight = position - lower as f64;
        ordered[lower] * (1.0 - weight) + ordered[upper] * weight
    }
}

#[cfg(test)]
mod tests {
    use super::vector;
    use borsuk::VectorElementType;

    #[test]
    fn benchmark_vectors_do_not_repeat_at_the_old_modulus_boundary() {
        assert_ne!(
            vector(0, 96, VectorElementType::Float32),
            vector(1_009, 96, VectorElementType::Float32)
        );
        assert_ne!(
            vector(7_919, 96, VectorElementType::Float32),
            vector(7_919 + 1_009, 96, VectorElementType::Float32)
        );
    }

    #[test]
    fn benchmark_vectors_are_valid_for_every_primary_element_type() {
        for element_type in [
            VectorElementType::Float32,
            VectorElementType::Float16,
            VectorElementType::BFloat16,
            VectorElementType::Float8E4M3Fn,
            VectorElementType::Float8E5M2,
            VectorElementType::Int8,
            VectorElementType::Binary,
        ] {
            let values = vector(37, 96, element_type);
            element_type.canonicalize(&values).unwrap();
            assert_eq!(values.len(), 96);
        }
    }
}
