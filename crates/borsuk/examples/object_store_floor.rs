//! Object-store acknowledgement-floor qualification.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use object_store::{
    ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, parse_url_opts,
    path::Path as ObjectPath,
};
use url::Url;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const DEFAULT_SAMPLES: usize = 1_000;
const DEFAULT_WARMUPS: usize = 32;
const DEFAULT_PAYLOAD_BYTES: usize = 3_072;
const DEFAULT_HEAD_BYTES: usize = 4_096;

struct FloorProtocol {
    samples: usize,
    warmups: usize,
    payload_bytes: usize,
    head_bytes: usize,
    uri: String,
    region: String,
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * quantile).round() as usize]
}

fn validate_protocol(
    samples: usize,
    payload_bytes: usize,
    head_bytes: usize,
) -> Result<(), String> {
    if samples < 100 {
        return Err("object-store floor requires at least 100 samples".to_string());
    }
    if payload_bytes == 0 || head_bytes == 0 {
        return Err("object-store floor payload and head sizes must be positive".to_string());
    }
    Ok(())
}

fn required(name: &str) -> BenchResult<String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn configured_usize(name: &str, default: usize) -> BenchResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| format!("invalid {name}={value:?}: {error}").into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn object_path(prefix: &ObjectPath, relative: &str) -> BenchResult<ObjectPath> {
    let path = if prefix.as_ref().is_empty() {
        relative.to_string()
    } else {
        format!("{prefix}/{relative}")
    };
    Ok(ObjectPath::parse(path)?)
}

fn payload(bytes: &[u8]) -> PutPayload {
    PutPayload::from(Bytes::copy_from_slice(bytes))
}

fn put_mode(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        ..PutOptions::default()
    }
}

async fn put_once(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    bytes: &[u8],
    mode: PutMode,
) -> BenchResult<UpdateVersion> {
    Ok(store
        .put_opts(path, payload(bytes), put_mode(mode))
        .await?
        .into())
}

async fn warm_up(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    warmups: usize,
    payload_bytes: &[u8],
    head_bytes: &[u8],
) -> BenchResult<()> {
    let update_path = object_path(prefix, "warm/update-head")?;
    let chain_path = object_path(prefix, "warm/chain-head")?;
    let mut update_version = put_once(store, &update_path, head_bytes, PutMode::Create).await?;
    let mut chain_version = put_once(store, &chain_path, head_bytes, PutMode::Create).await?;
    for sample in 0..warmups {
        put_once(
            store,
            &object_path(prefix, &format!("warm/put/{sample:06}.bin"))?,
            payload_bytes,
            PutMode::Overwrite,
        )
        .await?;
        put_once(
            store,
            &object_path(prefix, &format!("warm/create/{sample:06}.bin"))?,
            payload_bytes,
            PutMode::Create,
        )
        .await?;
        update_version = put_once(
            store,
            &update_path,
            head_bytes,
            PutMode::Update(update_version),
        )
        .await?;
        put_once(
            store,
            &object_path(prefix, &format!("warm/chain/{sample:06}.bin"))?,
            payload_bytes,
            PutMode::Create,
        )
        .await?;
        chain_version = put_once(
            store,
            &chain_path,
            head_bytes,
            PutMode::Update(chain_version),
        )
        .await?;
    }
    Ok(())
}

async fn verify_conditional_semantics(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    head_bytes: &[u8],
) -> BenchResult<()> {
    let create_path = object_path(prefix, "controls/create-once")?;
    put_once(store, &create_path, head_bytes, PutMode::Create).await?;
    let duplicate = put_once(store, &create_path, head_bytes, PutMode::Create).await;
    if !matches!(duplicate, Err(ref error) if error.downcast_ref::<object_store::Error>().is_some_and(|error| matches!(error, object_store::Error::AlreadyExists { .. })))
    {
        return Err("backend did not reject a duplicate conditional create".into());
    }

    let update_path = object_path(prefix, "controls/stale-update")?;
    let stale = put_once(store, &update_path, head_bytes, PutMode::Create).await?;
    let _current = put_once(
        store,
        &update_path,
        head_bytes,
        PutMode::Update(stale.clone()),
    )
    .await?;
    let rejected = put_once(store, &update_path, head_bytes, PutMode::Update(stale)).await;
    if !matches!(rejected, Err(ref error) if error.downcast_ref::<object_store::Error>().is_some_and(|error| matches!(error, object_store::Error::Precondition { .. })))
    {
        return Err("backend did not reject a stale conditional update".into());
    }
    Ok(())
}

async fn measure(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    samples: usize,
    payload_bytes: &[u8],
    head_bytes: &[u8],
) -> BenchResult<Vec<(&'static str, usize, f64)>> {
    let update_path = object_path(prefix, "measure/update-head")?;
    let chain_path = object_path(prefix, "measure/chain-head")?;
    let mut update_version = put_once(store, &update_path, head_bytes, PutMode::Create).await?;
    let mut chain_version = put_once(store, &chain_path, head_bytes, PutMode::Create).await?;
    let mut rows = Vec::with_capacity(samples.saturating_mul(4));

    for sample in 0..samples {
        let put_path = object_path(prefix, &format!("measure/put/{sample:06}.bin"))?;
        let started = Instant::now();
        put_once(store, &put_path, payload_bytes, PutMode::Overwrite).await?;
        rows.push(("put", sample, started.elapsed().as_secs_f64() * 1_000.0));

        let create_path = object_path(prefix, &format!("measure/create/{sample:06}.bin"))?;
        let started = Instant::now();
        put_once(store, &create_path, payload_bytes, PutMode::Create).await?;
        rows.push((
            "conditional_create",
            sample,
            started.elapsed().as_secs_f64() * 1_000.0,
        ));

        let started = Instant::now();
        update_version = put_once(
            store,
            &update_path,
            head_bytes,
            PutMode::Update(update_version),
        )
        .await?;
        rows.push((
            "conditional_update",
            sample,
            started.elapsed().as_secs_f64() * 1_000.0,
        ));

        let block_path = object_path(prefix, &format!("measure/chain/{sample:06}.bin"))?;
        let started = Instant::now();
        put_once(store, &block_path, payload_bytes, PutMode::Overwrite).await?;
        chain_version = put_once(
            store,
            &chain_path,
            head_bytes,
            PutMode::Update(chain_version),
        )
        .await?;
        rows.push((
            "put_then_conditional_update",
            sample,
            started.elapsed().as_secs_f64() * 1_000.0,
        ));
    }
    Ok(rows)
}

fn write_results(
    output: &PathBuf,
    rows: &[(&'static str, usize, f64)],
    protocol: &FloorProtocol,
) -> BenchResult<()> {
    if output.exists() {
        return Err(format!("refusing to replace output {}", output.display()).into());
    }
    fs::create_dir_all(output)?;
    let mut raw = BufWriter::new(File::create(output.join("samples.csv"))?);
    writeln!(raw, "operation,sample,latency_ms")?;
    for (operation, sample, latency_ms) in rows {
        writeln!(raw, "{operation},{sample},{latency_ms:.3}")?;
    }
    raw.flush()?;

    let mut summary = BufWriter::new(File::create(output.join("summary.csv"))?);
    writeln!(summary, "operation,samples,p50_ms,p95_ms,p99_ms,max_ms")?;
    for operation in [
        "put",
        "conditional_create",
        "conditional_update",
        "put_then_conditional_update",
    ] {
        let latencies = rows
            .iter()
            .filter_map(|row| (row.0 == operation).then_some(row.2))
            .collect::<Vec<_>>();
        writeln!(
            summary,
            "{operation},{},{:.3},{:.3},{:.3},{:.3}",
            latencies.len(),
            percentile(&latencies, 0.50),
            percentile(&latencies, 0.95),
            percentile(&latencies, 0.99),
            latencies.iter().copied().fold(0.0_f64, f64::max),
        )?;
    }
    summary.flush()?;
    fs::write(
        output.join("protocol.txt"),
        format!(
            "samples={}\nwarmups={}\npayload_bytes={}\nhead_bytes={}\nuri={}\nregion={}\nconditional_create_rejected=true\nstale_update_rejected=true\n",
            protocol.samples,
            protocol.warmups,
            protocol.payload_bytes,
            protocol.head_bytes,
            protocol.uri,
            protocol.region,
        ),
    )?;
    fs::write(output.join("OBJECT_STORE_FLOOR_COMPLETE"), b"complete\n")?;
    Ok(())
}

fn run() -> BenchResult<()> {
    let uri = required("BORSUK_OBJECT_STORE_FLOOR_URI")?;
    let output = PathBuf::from(required("BORSUK_OBJECT_STORE_FLOOR_OUTPUT")?);
    if output.exists() {
        return Err(format!("refusing to replace output {}", output.display()).into());
    }
    let region = required("BORSUK_OBJECT_STORE_FLOOR_REGION")?;
    let samples = configured_usize("BORSUK_OBJECT_STORE_FLOOR_SAMPLES", DEFAULT_SAMPLES)?;
    let warmups = configured_usize("BORSUK_OBJECT_STORE_FLOOR_WARMUPS", DEFAULT_WARMUPS)?;
    let payload_len = configured_usize(
        "BORSUK_OBJECT_STORE_FLOOR_PAYLOAD_BYTES",
        DEFAULT_PAYLOAD_BYTES,
    )?;
    let head_len = configured_usize("BORSUK_OBJECT_STORE_FLOOR_HEAD_BYTES", DEFAULT_HEAD_BYTES)?;
    validate_protocol(samples, payload_len, head_len)?;
    let protocol = FloorProtocol {
        samples,
        warmups,
        payload_bytes: payload_len,
        head_bytes: head_len,
        uri: uri.clone(),
        region,
    };
    let url = Url::parse(&uri)?;
    let store_options = env::vars().filter(|(key, _)| {
        matches!(
            key.as_str(),
            "AWS_ACCESS_KEY_ID" | "AWS_SECRET_ACCESS_KEY" | "AWS_SESSION_TOKEN" | "AWS_REGION"
        )
    });
    let (store, prefix) = parse_url_opts(&url, store_options)?;
    let store: Arc<dyn ObjectStore> = store.into();
    let payload_bytes = vec![0x5a; payload_len];
    let head_bytes = vec![0xa5; head_len];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rows = runtime.block_on(async {
        verify_conditional_semantics(&store, &prefix, &head_bytes).await?;
        warm_up(&store, &prefix, warmups, &payload_bytes, &head_bytes).await?;
        measure(&store, &prefix, samples, &payload_bytes, &head_bytes).await
    })?;
    write_results(&output, &rows, &protocol)
}

fn main() {
    if let Err(error) = run() {
        eprintln!("object_store_floor: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn percentile_uses_the_frozen_nearest_rank_rule() {
        assert_eq!(super::percentile(&[4.0, 1.0, 3.0, 2.0], 0.50), 3.0);
        assert_eq!(super::percentile(&[4.0, 1.0, 3.0, 2.0], 0.95), 4.0);
    }

    #[test]
    fn floor_protocol_rejects_too_few_samples() {
        let error = super::validate_protocol(99, 16, 4).unwrap_err();
        assert!(error.contains("at least 100"), "unexpected error: {error}");
    }

    #[test]
    fn floor_protocol_bounds_payload_and_head_sizes() {
        assert!(super::validate_protocol(100, 3_072, 4_096).is_ok());
        assert!(super::validate_protocol(100, 0, 4_096).is_err());
        assert!(super::validate_protocol(100, 3_072, 0).is_err());
    }
}
