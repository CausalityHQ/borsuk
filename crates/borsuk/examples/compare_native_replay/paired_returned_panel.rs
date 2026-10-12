//! Experiment-private consumed-panel comparison; authority comes from frozen approvals.
use super::*;

const CONFIG_SCHEMA: &str = "borsuk-paired-returned-panel-config-v1";
const REPORT_SCHEMA: &str = "borsuk-paired-returned-panel-v1";
const OWNED_CAP: u64 = 256 * 1024 * 1024;
const NATIVE_HEADER_CAP: u64 = 32 * 1024;
const PANEL_REPORT_CAP: u64 = 256 * 1024;
const REFUSAL_RESERVE: u64 =
    256 * CONFIG_CAP + 128 * PANEL_REPORT_CAP + 6 * SELECTION_CAP + 8 * 1024 * 1024;
const PANEL: usize = 32;
const PANEL_ORDINALS: [usize; PANEL] = [
    69, 83, 114, 149, 151, 159, 188, 189, 211, 237, 257, 296, 321, 351, 388, 408, 413, 513, 537,
    587, 626, 667, 669, 731, 742, 821, 823, 849, 851, 890, 956, 991,
];
const PROTOCOL_SCHEMA: &str = "borsuk-paired-source-screen-protocol-v1";
const PROTOCOL_SHA256: &str = "f7184f949618791968c174a0f86f56174aedaff5a4ee9f7595747731e4fad1ea";
const TRUTH_SHA256: &str = "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f";
const REQUESTS_SHA256: &str = "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e";

#[derive(Clone, Copy)]
struct ExpectedPins<'a> {
    protocol: &'a str,
    truth: &'a str,
    requests: &'a str,
}

const FROZEN_PINS: ExpectedPins<'static> = ExpectedPins {
    protocol: PROTOCOL_SHA256,
    truth: TRUTH_SHA256,
    requests: REQUESTS_SHA256,
};

#[derive(Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Provenance {
    source_commit: String,
    source_archive_sha256: String,
    binary_sha256: String,
    original_exit_code: i32,
    original_status: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Generation {
    generation_prefix: String,
    generation_root_sha256: String,
    approval_sha256: String,
    partition: String,
    discovery_root: Artifact,
    discovery_membership: Artifact,
    discovery_leaves: Artifact,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Arm {
    completed: CompletedConfig,
    provenance: Provenance,
    generation: Generation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    max_owned_bytes: u64,
    ordinals: [usize; PANEL],
    source_protocol: CompletedInput,
    original_control: Arm,
    radial: Arm,
    spatial: Arm,
    truth: CompletedInput,
}

// Reserve worst-case shared-reader capacities, including a raced 256 KiB row.
fn allocation(cap: u64) -> Result<Value> {
    require(cap > 0 && cap <= OWNED_CAP, "paired returned owned cap")?;
    let parts = [
        ("config_parse_and_values", 256 * CONFIG_CAP),
        ("native_parser_and_raced_rows", 512 * LINE_CAP as u64),
        ("retained_native_headers", 3 * 2 * 128 * NATIVE_HEADER_CAP),
        ("source_protocol_parse", 256 * HEADER_JSON_CAP),
        ("truth_body_and_scratch", 2 * 80_000),
        ("samples_ordinals_and_descriptor_maps", 2 * 1024 * 1024),
        ("report_value", 128 * PANEL_REPORT_CAP),
        ("publisher_serialization_and_readback", 6 * SELECTION_CAP),
        ("allocator_and_io_reserve", 8 * 1024 * 1024),
    ];
    let total = parts.iter().try_fold(0, |sum, (_, n)| plus(sum, *n))?;
    require(total <= cap, "paired returned cumulative allocation cap")?;
    Ok(json!({"cap_bytes":cap,"cumulative_charged_bytes":total,
        "capacities":parts,"charged_before_allocation":true}))
}

fn same_fields(a: &Value, b: &Value, exceptions: &[&str], error: &str) -> Result<()> {
    let a = a.as_object().ok_or("paired returned row object")?;
    let b = b.as_object().ok_or("paired returned row object")?;
    require(
        a.len() == b.len()
            && a.iter().all(|(key, value)| {
                b.get(key)
                    .is_some_and(|other| exceptions.contains(&key.as_str()) || value == other)
            }),
        error,
    )
}

fn admit(c: &Config, config_path: &Path, output: &Path, pins: ExpectedPins<'_>) -> Result<()> {
    require(
        c.schema == CONFIG_SCHEMA
            && c.ordinals == PANEL_ORDINALS
            && c.ordinals.windows(2).all(|p| p[0] < p[1])
            && c.ordinals[PANEL - 1] < COUNT
            && c.source_protocol.sha256 == pins.protocol,
        "paired returned schema/explicit32 SOURCE ordinals",
    )?;
    header_pin(&c.source_protocol, HEADER_JSON_CAP)?;
    header_pin(&c.truth, 80_000)?;
    require(
        c.truth.bytes == 80_000 && c.truth.sha256 == pins.truth,
        "paired returned full truth geometry",
    )?;
    let execution = json!({"mode":"diagnostic_panel","ordinals":c.ordinals,"trace":false});
    for (arm, partition) in [
        (&c.original_control, "historical_radial"),
        (&c.radial, "radial_v1"),
        (&c.spatial, "spatial_block_v1"),
    ] {
        let completed = &arm.completed;
        let p = &arm.provenance;
        let g = &arm.generation;
        require(
            completed.schema == SCALE_CONFIG_SCHEMA
                && p.source_commit.len() == 40
                && p.source_commit
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                && valid_sha(&p.source_archive_sha256)
                && valid_sha(&p.binary_sha256)
                && p.original_exit_code == 0
                && p.original_status == "VALID"
                && completed.expected_identity["binary_sha256"] == p.binary_sha256,
            "paired returned independently approved original provenance/exit",
        )?;
        header_pin(&completed.input, 4_096_000)?;
        require(
            g.partition == partition
                && valid_sha(&g.approval_sha256)
                && valid_sha(&g.generation_root_sha256)
                && !g.generation_prefix.is_empty()
                && g.generation_prefix.len() <= 512
                && completed.expected_bound_inputs["generation_prefix"] == g.generation_prefix
                && completed.expected_bound_inputs["generation_root_sha256"]
                    == g.generation_root_sha256,
            "paired returned explicit approved generation descriptor",
        )?;
        for artifact in [
            &g.discovery_root,
            &g.discovery_membership,
            &g.discovery_leaves,
        ] {
            require(
                artifact.bytes > 0 && artifact.bytes <= FILE_CAP && valid_sha(&artifact.sha256),
                "paired returned approved discovery artifact",
            )?;
        }
        let i: Inputs = serde_json::from_value(completed.expected_bound_inputs.clone())?;
        validate_scale_inputs(&i, &completed.expected_bound_inputs)?;
        require(
            i.rows == 1_000_000
                && i.dimensions == 1024
                && i.count == COUNT
                && i.k == K
                && i.profile == "scale1m"
                && i.truth_bytes == c.truth.bytes
                && i.truth_sha256 == c.truth.sha256
                && i.requests_sha256 == pins.requests
                && completed.expected_bound_inputs["execution"] == execution
                && completed.expected_identity["execution"] == execution
                && completed.expected_bound_inputs["fetch_parallelism"] == 16
                && completed.expected_identity["fetch_parallelism"] == 16,
            "paired returned fixed1M/count1000/full-input/untraced32/fetch16 policy",
        )?;
    }
    require(
        c.radial.provenance == c.spatial.provenance,
        "paired returned same current source/binary",
    )?;
    same_fields(
        &c.radial.completed.expected_identity,
        &c.spatial.completed.expected_identity,
        &["config_sha256"],
        "paired returned exact current identity policy",
    )?;
    same_fields(
        &c.original_control.completed.expected_identity,
        &c.radial.completed.expected_identity,
        &[
            "config_sha256",
            "binary_sha256",
            "runner_source_sha256",
            "generation_source_sha256",
            "router_source_sha256",
            "codec_source_sha256",
            "source_plane_source_sha256",
            "sq8_range_source_sha256",
            "returned_source_sha256",
        ],
        "paired returned exact historical identity policy",
    )?;
    for arm in [&c.original_control, &c.spatial] {
        same_fields(
            &c.radial.completed.expected_bound_inputs,
            &arm.completed.expected_bound_inputs,
            &["generation_prefix", "generation_root_sha256"],
            "paired returned exact source/order/request/truth/scoring policy",
        )?;
    }
    let paths = [
        config_path,
        output,
        &c.source_protocol.path,
        &c.truth.path,
        &c.original_control.completed.input.path,
        &c.radial.completed.input.path,
        &c.spatial.completed.input.path,
    ];
    for (slot, path) in paths.iter().enumerate() {
        header_path(path)?;
        require(
            !paths[..slot].contains(path),
            "paired returned distinct role paths",
        )?;
    }
    Ok(())
}

fn recheck(inputs: &[HeaderSnapshot]) -> Result<()> {
    for (slot, input) in inputs.iter().enumerate() {
        input.recheck()?;
        require(
            !inputs[..slot].iter().any(|other| {
                (other.identity.dev, other.identity.ino) == (input.identity.dev, input.identity.ino)
            }),
            "paired returned distinct role descriptors",
        )?;
    }
    Ok(())
}

fn native_snapshot(pin: &CompletedInput) -> Result<HeaderSnapshot> {
    header_pin(pin, 4_096_000)?;
    let mut file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(
        identity.len == pin.bytes,
        "paired returned native exact length",
    )?;
    let mut buffer = [0_u8; 8192];
    let mut line_bytes = 0_u64;
    let mut row = 0_usize;
    let mut total = 0_u64;
    let mut digest = Sha256::new();
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        total = plus(total, n as u64)?;
        require(total <= pin.bytes, "paired returned native growth")?;
        digest.update(&buffer[..n]);
        for &byte in &buffer[..n] {
            line_bytes = plus(line_bytes, 1)?;
            // Six startup rows,32 queries,seal,32 recalls,terminal; the reader validates phases.
            let cap = if (6..6 + PANEL).contains(&row) {
                LINE_CAP as u64
            } else {
                NATIVE_HEADER_CAP
            };
            require(line_bytes <= cap, "paired returned native line cap")?;
            if byte == b'\n' {
                line_bytes = 0;
                row += 1;
            }
        }
    }
    require(
        line_bytes == 0
            && row == 6 + 2 * PANEL + 2
            && total == pin.bytes
            && format!("{:x}", digest.finalize()) == pin.sha256,
        "paired returned native full SHA/length/EOF newline",
    )?;
    let snapshot = HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    };
    snapshot.recheck()?;
    Ok(snapshot)
}

struct Reduction {
    report: Value,
    inputs: Vec<HeaderSnapshot>,
}

#[derive(Debug)]
struct Refusal {
    message: String,
    config_sha256: String,
    original_executions: Value,
    offending_arm: Option<&'static str>,
    config: HeaderSnapshot,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for Refusal {}

fn reduce_with(
    config_path: &Path,
    sha: &str,
    output: &Path,
    pins: ExpectedPins<'_>,
    mut open_truth: impl FnMut(&CompletedInput, u64) -> Result<(Vec<u8>, HeaderSnapshot)>,
) -> Result<Reduction> {
    // This hard-cap subset also covers refusals of malformed or undersized config budgets.
    require(
        REFUSAL_RESERVE <= OWNED_CAP,
        "paired returned bootstrap allocation cap",
    )?;
    let file = header_file(config_path)?;
    let identity = file_identity(&file)?;
    let (c, config_bytes): (Config, _) = read_config_file(&file, config_path, sha, CONFIG_CAP)?;
    let refusal_identity = file_identity(&file)?;
    require(
        refusal_identity == identity,
        "paired returned config lifetime before refusal context",
    )?;
    let refusal_config = HeaderSnapshot {
        file: file.try_clone()?,
        path: config_path.to_owned(),
        identity: refusal_identity,
    };
    let result = (|| -> Result<Reduction> {
        let model = allocation(c.max_owned_bytes)?;
        let mut inputs = Vec::with_capacity(6);
        inputs.push(HeaderSnapshot {
            file,
            path: config_path.to_owned(),
            identity,
        });
        admit(&c, config_path, output, pins)?;
        let (protocol, protocol_snapshot) = header_json(&c.source_protocol)?;
        require(
            protocol["schema"] == PROTOCOL_SCHEMA && protocol["ordinals"] == json!(c.ordinals),
            "paired returned ordinals match authenticated SOURCE protocol",
        )?;
        drop(protocol);
        inputs.push(protocol_snapshot);
        let mut runs = Vec::with_capacity(3);
        for arm in [&c.original_control, &c.radial, &c.spatial] {
            let completed = &arm.completed;
            let snapshot = native_snapshot(&completed.input)?;
            snapshot.recheck()?;
            let run = read_run_observed_with_identity(
                &completed.input.path,
                &completed.input.sha256,
                Some(completed),
                Some((snapshot.identity.dev, snapshot.identity.ino)),
                |_, q, _| {
                    require(
                        !q.underfill && q.returned_count == K,
                        "paired returned full top10 required",
                    )
                },
            )?;
            require(
                run.file_identity == snapshot.identity,
                "paired returned native lifetime identity",
            )?;
            snapshot.recheck()?;
            inputs.push(snapshot);
            runs.push(run);
        }
        require(
            runs[0]
                .samples
                .iter()
                .zip(&runs[1].samples)
                .all(|(a, b)| a.returned == b.returned),
            "paired returned radial/control ordered ID and score-bit parity",
        )?;
        recheck(&inputs)?;
        // All three original complete seals, terminals and control parity precede truth.
        let (truth, truth_snapshot) = open_truth(&c.truth, 80_000)?;
        require(
            truth.len() == 80_000,
            "paired returned authenticated full truth length",
        )?;
        inputs.push(truth_snapshot);
        recheck(&inputs)?;
        for row in truth.chunks_exact(K * 8) {
            let ids = truth_ids(row)?;
            require(
                ids.iter()
                    .enumerate()
                    .all(|(slot, id)| *id < 1_000_000 && !ids[..slot].contains(id)),
                "paired returned full truth ID domain/duplicates",
            )?;
        }
        let mut totals = [0_u64; 3];
        let (mut gained, mut lost) = (0_u64, 0_u64);
        let mut queries = Vec::with_capacity(PANEL);
        for (slot, &ordinal) in c.ordinals.iter().enumerate() {
            let ids = truth_ids(&truth[ordinal * K * 8..(ordinal + 1) * K * 8])?;
            let mut hits = [0_u64; 3];
            let mut entries = Vec::with_capacity(K);
            let (mut query_gained, mut query_lost) = (0_u64, 0_u64);
            for (truth_slot, id) in ids.into_iter().enumerate() {
                let found: [bool; 3] = std::array::from_fn(|arm| {
                    runs[arm].samples[slot]
                        .returned
                        .iter()
                        .any(|hit| hit.id == id)
                });
                for arm in 0..3 {
                    hits[arm] += u64::from(found[arm]);
                }
                let gain = !found[1] && found[2];
                let loss = found[1] && !found[2];
                query_gained += u64::from(gain);
                query_lost += u64::from(loss);
                entries.push(json!({"truth_slot":truth_slot,"truth_id":id,
                "original_control":found[0],"radial":found[1],"spatial":found[2],
                "gained":gain,"lost":loss}));
            }
            for arm in 0..3 {
                require(
                    hits[arm] == runs[arm].samples[slot].hits10,
                    "paired returned native per-query truth reconciliation",
                )?;
                totals[arm] = plus(totals[arm], hits[arm])?;
            }
            gained = plus(gained, query_gained)?;
            lost = plus(lost, query_lost)?;
            queries.push(json!({"ordinal":ordinal,"original_control_hits10":hits[0],
            "radial_hits10":hits[1],"spatial_hits10":hits[2],"gained":query_gained,
            "lost":query_lost,"truth_slots":entries}));
        }
        for arm in 0..3 {
            require(
                totals[arm] == runs[arm].terminal.total_hits10,
                "paired returned native total truth reconciliation",
            )?;
        }
        require(
            plus(totals[1], gained)? == plus(totals[2], lost)?,
            "paired returned slot conservation",
        )?;
        recheck(&inputs)?;
        let status = if totals[2] > totals[1] && lost == 0 {
            "ADVANCE"
        } else {
            "FAIL"
        };
        let report = json!({"schema":REPORT_SCHEMA,"status":status,"complete":true,
        "config_sha256":sha,"config_bytes":config_bytes,"allocation_model":model,
        "source_protocol":c.source_protocol,"ordinals":c.ordinals,"truth":c.truth,
        "query_count":PANEL,"truth_slots":PANEL*K,"original_control_hits10":totals[0],
        "radial_hits10":totals[1],"spatial_hits10":totals[2],"gained":gained,"lost":lost,
        "queries":queries,"radial_control_ordered_id_score_bits_parity":true,
        "truth_opened_after_all_complete_seals":true,"full_valid_top10":true,
        "original_control":{"provenance":c.original_control.provenance,"generation":c.original_control.generation,
            "input":c.original_control.completed.input,"native_terminal":runs[0].completed.as_ref().ok_or("control evidence")?.terminal},
        "radial":{"provenance":c.radial.provenance,"generation":c.radial.generation,
            "input":c.radial.completed.input,"native_terminal":runs[1].completed.as_ref().ok_or("radial evidence")?.terminal},
        "spatial":{"provenance":c.spatial.provenance,"generation":c.spatial.generation,
            "input":c.spatial.completed.input,"native_terminal":runs[2].completed.as_ref().ok_or("spatial evidence")?.terminal},
        "consumed_panel":true,"population_percentiles_valid":false,"full_cohort_qualification":false,
        "cold_latency_or_qps_claim":false,"default_promotion":false,"vendor_or_product_win_claim":false,
        "qualified":false,"native_ann_called_by_reducer":false,"reduction_reads_local_files_only":true,
        "independent_root_original_receipts_and_generation_approval_required":true});
        report_cap(&report)?;
        Ok(Reduction { report, inputs })
    })();
    result.map_err(|error| {
        let arms = [
            ("original_control", &c.original_control),
            ("radial", &c.radial),
            ("spatial", &c.spatial),
        ];
        let offending_arm = arms
            .iter()
            .find(|(_, arm)| {
                arm.provenance.original_exit_code != 0 || arm.provenance.original_status != "VALID"
            })
            .map(|(name, _)| *name);
        let original_executions = json!(
            arms.iter()
                .map(|(name, arm)| json!({"arm":name,
            "original_exit_code":arm.provenance.original_exit_code,
            "original_status":arm.provenance.original_status}))
                .collect::<Vec<_>>()
        );
        Box::new(Refusal {
            message: error.to_string(),
            config_sha256: sha.to_owned(),
            original_executions,
            offending_arm,
            config: refusal_config,
        }) as Box<dyn Error + Send + Sync>
    })
}

fn truth_ids(row: &[u8]) -> Result<[u64; K]> {
    require(row.len() == K * 8, "paired returned truth row geometry")?;
    let mut ids = [0; K];
    for (slot, bytes) in row.chunks_exact(8).enumerate() {
        ids[slot] = u64::from_le_bytes(bytes.try_into()?);
    }
    Ok(ids)
}

fn report_cap(report: &Value) -> Result<()> {
    struct Counter(u64);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() as u64 > PANEL_REPORT_CAP.saturating_sub(self.0) {
                return Err(io::Error::other("paired returned report cap"));
            }
            self.0 += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(&mut Counter(1), report)?;
    Ok(())
}

pub(super) fn execute(config: &Path, sha: &str, output: &Path) -> Result<bool> {
    execute_with(config, sha, output, FROZEN_PINS, |file, _| file.sync_all())
}

fn execute_with(
    config: &Path,
    sha: &str,
    output: &Path,
    pins: ExpectedPins<'_>,
    sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    let reduced = match reduce_with(config, sha, output, pins, loss_binary) {
        Ok(reduced) => reduced,
        Err(error) => {
            let mut report = json!({"schema":REPORT_SCHEMA,"status":"INVALID","complete":false,
                "refusal_capacity_bytes":REFUSAL_RESERVE,"hard_owned_cap_bytes":OWNED_CAP,
                "error":error.to_string().chars().take(512).collect::<String>(),
                "config_authenticated":false,"config_sha256":null,"original_executions":null,"offending_arm":null,
                "consumed_panel":true,"population_percentiles_valid":false,"full_cohort_qualification":false,
                "cold_latency_or_qps_claim":false,"default_promotion":false,"vendor_or_product_win_claim":false,
                "qualified":false,"native_ann_called_by_reducer":false,"reduction_reads_local_files_only":true,
                "independent_root_original_receipts_and_generation_approval_required":true});
            let mut inputs = Vec::with_capacity(1);
            if let Ok(refusal) = error.downcast::<Refusal>() {
                report["config_authenticated"] = json!(true);
                report["config_sha256"] = json!(refusal.config_sha256);
                report["original_executions"] = refusal.original_executions;
                report["offending_arm"] = json!(refusal.offending_arm);
                inputs.push(refusal.config);
            }
            report_cap(&report)?;
            Reduction { report, inputs }
        }
    };
    let inputs = reduced.inputs.iter().collect::<Vec<_>>();
    publish_selection_seal_with_sync(output, &reduced.report, &inputs, sync)?;
    Ok(reduced.report["status"] == "ADVANCE")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn digest(body: &[u8]) -> String {
        format!("{:x}", Sha256::digest(body))
    }

    fn pin(path: &Path, body: &[u8]) -> Value {
        std::fs::write(path, body).unwrap();
        json!({"path":path,"bytes":body.len(),"sha256":digest(body)})
    }

    fn charge(n: u64) -> Value {
        json!({"submitted_gets":n,"verified_bytes":n*100,"failed_gets":0})
    }

    fn charges(n: u64) -> Value {
        json!({"router":charge(n),"source":charge(n),"sq8":charge(n)})
    }

    fn transport(stage: &str, ordinal: Option<usize>) -> Value {
        json!({"stage":stage,"ordinal":ordinal,"before":null,"after":null})
    }

    fn encode(rows: &[Value]) -> Vec<u8> {
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row).unwrap();
            body.push(b'\n');
        }
        body
    }

    fn seal(rows: &mut [Value]) {
        let slot = rows
            .iter()
            .position(|row| row["phase"] == "all_queries_sealed")
            .unwrap();
        let prefix = encode(&rows[..slot]);
        rows[slot]["prefix_bytes"] = json!(prefix.len());
        rows[slot]["prefix_sha256"] = json!(digest(&prefix));
        let sealed = encode(&rows[..=slot]);
        let summary = &mut rows.last_mut().unwrap()["summary"];
        summary["prefix_bytes"] = json!(prefix.len());
        summary["prefix_sha256"] = json!(digest(&prefix));
        summary["sealed_bytes"] = json!(sealed.len());
        summary["sealed_sha256"] = json!(digest(&sealed));
    }

    struct Fixture {
        dir: tempfile::TempDir,
        config: Value,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            // Non-prefix ordinals prove original ordinals are never replaced by slots.
            let ordinals = PANEL_ORDINALS;
            let truth_body: Vec<_> = (0..COUNT)
                .flat_map(|ordinal| {
                    (ordinal as u64 * 32..ordinal as u64 * 32 + 10).flat_map(u64::to_le_bytes)
                })
                .collect();
            let truth = pin(&dir.path().join("truth.u64"), &truth_body);
            let protocol = pin(
                &dir.path().join("source-protocol.json"),
                &serde_json::to_vec(&json!({"schema":PROTOCOL_SCHEMA,"ordinals":ordinals}))
                    .unwrap(),
            );
            let mut config = json!({"schema":CONFIG_SCHEMA,"max_owned_bytes":OWNED_CAP,
                "ordinals":ordinals,"source_protocol":protocol,"truth":truth});
            for (arm, name, partition) in [
                (0, "original_control", "historical_radial"),
                (1, "radial", "radial_v1"),
                (2, "spatial", "spatial_block_v1"),
            ] {
                let generation = ["a", "b", "c"][arm].repeat(64);
                let provenance = json!({"source_commit":if arm==0 {"a"} else {"b"}.repeat(40),
                    "source_archive_sha256":if arm==0 {"3"} else {"4"}.repeat(64),
                    "binary_sha256":if arm==0 {"5"} else {"9"}.repeat(64),
                    "original_exit_code":0,"original_status":"VALID"});
                let execution =
                    json!({"mode":"diagnostic_panel","ordinals":ordinals,"trace":false});
                let component = if arm == 0 { "1" } else { "2" }.repeat(64);
                let identity = json!({"phase":"identity","schema":"borsuk-cohere-native-baseline-result-v7",
                    "config_sha256":(["6","7","8"][arm].repeat(64)),"binary_sha256":provenance["binary_sha256"],
                    "runner_source_sha256":component,"generation_source_sha256":component,
                    "router_source_sha256":component,"codec_source_sha256":component,
                    "source_plane_source_sha256":component,"sq8_range_source_sha256":component,"returned_source_sha256":component,
                    "scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","physical_s3_measured":false,
                    "io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
                    "s3_credential_source":"imds_instance_role_only","native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
                    "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
                    "external_gate_required":true,"truth_opened":false,"serving":{"mode":"baseline"},
                    "execution":execution,"fetch_parallelism":16});
                let bound = json!({"phase":"bound_inputs","dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
                    "revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","metric":"cosine","tie_rule":"corpus_ordinal_ascending",
                    "rows":1_000_000,"dimensions":1024,"count":1000,"k":10,"corpus_source_first":0,"query_source_first":100_000,
                    "profile":"scale1m","backend":{"kind":"local","store_root":"/synthetic/store"},"credential_source":null,
                    "generation_prefix":name,"generation_root_sha256":generation,
                    "requests_bytes":4_096_000,"requests_sha256":"c".repeat(64),"truth_bytes":80_000,"truth_sha256":truth["sha256"],
                    "native_source_sha256":"e".repeat(64),"native_sq8_sha256":"f".repeat(64),"native_order_sha256":"b".repeat(64),
                    "corpus_intervals":[{"start":0,"end":100_000},{"start":101_000,"end":1_001_000}],
                    "reserved_query_interval":{"start":100_000,"end":101_000},"reserved_queries_sha256":"c".repeat(64),
                    "serving":{"mode":"baseline"},"source_cache":"off","fetch_parallelism":16,
                    "execution":execution,"selected_count":32,"max_memory_bytes":536_870_912,
                    "cohort_receipt_sha256":"1".repeat(64),"derivation_receipt_sha256":"2".repeat(64),
                    "producer_authority":{"source_commit":"4".repeat(40),"executable_sha256":"5".repeat(64),
                        "producer_source_sha256":"6".repeat(64),"sq8_source_sha256":"7".repeat(64),"source_order_source_sha256":"8".repeat(64)},
                    "truth_opened":false});
                let mut rows = vec![
                    identity.clone(),
                    bound.clone(),
                    json!({"phase":"diagnostic_admission","execution":execution,"selected_count":32,"population_count":1000,
                        "trace":false,"diagnostic_bytes_per_active_query":0,"diagnostic_cap_bytes":536_870_912,"max_active_queries":1,
                        "charged_in_caller_pinned_bytes":true,"caller_pinned_bytes":0,"panel_line_cap_bytes":32768,"host_read_cap_bytes":32768,
                        "trace_ranges":0,"population_percentiles_valid":false,"full_cohort_qualification":false,
                        "semantics":"synthetic consumed-panel fixture","truth_opened":false,"trace_retained_bytes":0,"trace_peak_bytes":0,
                        "diagnostic_pinned_bytes":0,"trace_peak_charged_by_library_at_traced_admission":0,
                        "range_state_pinned_by_runner_bytes":0,"range_state_both_paths_bytes":0}),
                    json!({"phase":"source_binding","transport":transport("native_source",None),"charges":charge(0),"success":true,"truth_opened":false}),
                    json!({"phase":"generation_open","transport":transport("generation_open",None),"success":true,"truth_opened":false}),
                    json!({"phase":"startup","metadata":{"metadata":[],"staging_wall_ns":0,"decode_wall_ns":0,
                        "source_head_requests":0,"source_head_wall_ns":0,"router_head_requests":0,"router_head_wall_ns":0},
                        "library_cap_bytes":536_870_912,"caller_pinned_bytes":0,"codec_scratch_bytes":0,"trace_scratch_bytes":0,
                        "query_scratch_bytes":0,"truth_opened":false,"serving":{"mode":"baseline"}}),
                ];
                for (slot, &ordinal) in ordinals.iter().enumerate() {
                    let base = ordinal as u64 * 32;
                    let ids = if arm == 2 {
                        (base..base + 10).collect::<Vec<_>>()
                    } else {
                        (base..base + 9).chain([base + 10]).collect()
                    };
                    rows.push(json!({"phase":"query","ordinal":ordinal,"selected_slot":slot,"truth_opened":false,
                        "returned":ids.iter().map(|id|json!({"id":id,"score_bits":1.0_f32.to_bits()})).collect::<Vec<_>>(),
                        "returned_count":10,"underfill":false,"charges":charges(1),"sum":charge(3),
                        "stages":{"discovery":{"start_ns":1,"end_ns":2},"source":{"start_ns":2,"end_ns":3},
                            "planning":{"start_ns":3,"end_ns":4},"sq8":{"start_ns":4,"end_ns":5},"leaf_peak_inflight":1},
                        "query_wall_ns":10,"query_process_cpu_ns":1,"transport":transport("query",Some(ordinal)),
                        "trace":{"ranked_candidate_pages":[],"nomination_evaluated_units":[],"primary_page":0,"discoveries":[],
                            "semantic_leaves":[],"semantic_units":[],"semantic_seed_additions":[]},"serving":{"mode":"baseline"},
                        "source_nomination_skipped":false,"planning_scope":"source_nomination_and_cover",
                        "plan":{"selected_pages":[0],"ranges":[[0,100]],"planned_bytes":100,"target_pages":1,
                            "target_shortfall":0,"primary_pages_retained":1,"covered_pages":1,"bridge_pages":0}}));
                }
                rows.push(json!({"phase":"all_queries_sealed","count":32,"truth_opened":false,
                    "prefix_bytes":0,"prefix_sha256":"0".repeat(64),"requests_sha256":bound["requests_sha256"],
                    "generation_root_sha256":generation,"requires_successful_sync":true,"requires_successful_directory_sync":true,
                    "selected_count":32,"population_count":1000,"reserved_query_count":1000}));
                let hits = if arm == 2 { 10 } else { 9 };
                for &ordinal in &ordinals {
                    rows.push(json!({"phase":"recall","ordinal":ordinal,"hits10":hits,"recall10":hits as f64/10.0,
                        "returned_count":10,"underfill":false}));
                }
                let mut boundary = transport("query", Some(*ordinals.last().unwrap()));
                boundary["scope"] = json!("cumulative_process_native_transport");
                boundary["status_counts_omitted_from_terminal"] = json!(true);
                for key in [
                    "wire_bytes",
                    "unread_bytes",
                    "billed_bytes",
                    "billed_requests",
                ] {
                    boundary[key] = Value::Null;
                }
                rows.push(json!({"phase":"terminal","summary":{"status":"MEASURED","complete":true,
                    "queries":32,"k":10,"total_hits10":32*hits,"recall_numerator":32*hits,"recall_denominator":320,
                    "mean_recall10":hits as f64/10.0,"underfilled_queries":0,"all_queries_sealed":true,
                    "prefix_bytes":0,"prefix_sha256":"0".repeat(64),"sealed_bytes":0,"sealed_sha256":"0".repeat(64),
                    "requests_sha256":bound["requests_sha256"],"truth_sha256":truth["sha256"],"generation_root_sha256":generation,
                    "charges":charges(32),"sum":charge(96),"query_wall_ns":320,"query_process_cpu_ns":32,
                    "physical_s3_measured":false,"external_gate_required":true,"process_wall_ns":420,"process_cpu_ns":132,
                    "observed_process_peak_bytes":null,"binding_charge":charge(0),"transport_last_boundary":boundary,
                    "serving":{"mode":"baseline"},"direct_memory":null,"execution":execution,"selected_count":32,"executed_count":32,
                    "population_count":1000,"diagnostic_panel":true,"reserved_query_count":1000,"diagnostic_prefix":false,
                    "population_percentiles_valid":false,"full_cohort_qualification":false}}));
                seal(&mut rows);
                let input = pin(&dir.path().join(format!("{name}.jsonl")), &encode(&rows));
                let artifact = json!({"bytes":100,"sha256":generation});
                config[name] = json!({"completed":{"schema":SCALE_CONFIG_SCHEMA,"input":input,
                    "expected_identity":identity,"expected_bound_inputs":bound},"provenance":provenance,
                    "generation":{"generation_prefix":name,"generation_root_sha256":generation,
                        "approval_sha256":"d".repeat(64),"partition":partition,
                        "discovery_root":artifact,"discovery_membership":artifact,"discovery_leaves":artifact}});
            }
            Self { dir, config }
        }

        fn paths(&self) -> (PathBuf, String, PathBuf) {
            let path = self.dir.path().join("reducer.json");
            let body = serde_json::to_vec(&self.config).unwrap();
            std::fs::write(&path, &body).unwrap();
            (path, digest(&body), self.dir.path().join("report.json"))
        }

        fn patch(&mut self, arm: &str, reseal: bool, mutate: impl FnOnce(&mut Vec<Value>)) {
            let path = PathBuf::from(
                self.config[arm]["completed"]["input"]["path"]
                    .as_str()
                    .unwrap(),
            );
            let body = std::fs::read(&path).unwrap();
            let mut rows: Vec<Value> = body
                .split(|b| *b == b'\n')
                .filter(|r| !r.is_empty())
                .map(|r| serde_json::from_slice(r).unwrap())
                .collect();
            mutate(&mut rows);
            if reseal {
                seal(&mut rows);
            }
            self.config[arm]["completed"]["input"] = pin(&path, &encode(&rows));
        }

        fn before_truth_error(&self) -> String {
            let (path, sha, output) = self.paths();
            let opened = Cell::new(false);
            let error = reduce_with(&path, &sha, &output, self.pins(), |_, _| {
                opened.set(true);
                Err("inaccessible truth must not be opened".into())
            })
            .err()
            .expect("invalid producer must refuse");
            assert!(!opened.get(), "truth opened before refusal: {error}");
            error.to_string()
        }

        fn reduce(&self) -> Reduction {
            let (path, sha, output) = self.paths();
            reduce_with(&path, &sha, &output, self.pins(), loss_binary).unwrap()
        }

        fn pins(&self) -> ExpectedPins<'_> {
            ExpectedPins {
                protocol: self.config["source_protocol"]["sha256"].as_str().unwrap(),
                truth: self.config["truth"]["sha256"].as_str().unwrap(),
                requests:
                    self.config["radial"]["completed"]["expected_bound_inputs"]["requests_sha256"]
                        .as_str()
                        .unwrap(),
            }
        }

        fn execute(&self) -> Result<bool> {
            let (path, sha, output) = self.paths();
            execute_with(&path, &sha, &output, self.pins(), |file, _| file.sync_all())
        }
    }

    #[test]
    fn paired_returned_panel_changed_generations_and_control_provenance() {
        let fixture = Fixture::new();
        let reduced = fixture.reduce();
        let r = &reduced.report;
        assert_eq!(r["status"], "ADVANCE");
        assert_eq!(r["radial_hits10"], 288);
        assert_eq!(r["spatial_hits10"], 320);
        assert_eq!(r["gained"], 32);
        assert_eq!(r["lost"], 0);
        assert_eq!(r["queries"][0]["ordinal"], 69);
        assert_eq!(r["queries"][0]["truth_slots"][0]["truth_id"], 69 * 32);
        assert_eq!(r["queries"][0]["truth_slots"][9]["truth_id"], 69 * 32 + 9);
        assert_eq!(r["queries"][31]["ordinal"], 991);
        assert_eq!(r["queries"][31]["truth_slots"][0]["truth_id"], 991 * 32);
        assert_eq!(r["queries"][31]["truth_slots"][9]["truth_id"], 991 * 32 + 9);
        assert_eq!(r["queries"][0]["truth_slots"][9]["gained"], true);
        assert_ne!(
            r["original_control"]["provenance"]["binary_sha256"],
            r["radial"]["provenance"]["binary_sha256"]
        );
        assert_ne!(
            r["radial"]["generation"]["generation_root_sha256"],
            r["spatial"]["generation"]["generation_root_sha256"]
        );
        assert_eq!(r["qualified"], false);
        assert_eq!(r["population_percentiles_valid"], false);
        let (_, _, output) = fixture.paths();
        assert!(fixture.execute().unwrap());
        let published: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(published, *r);
        for name in ["original_control", "radial", "spatial"] {
            let bytes = std::fs::read(
                fixture.config[name]["completed"]["input"]["path"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                digest(&bytes),
                fixture.config[name]["completed"]["input"]["sha256"]
            );
        }
        let (path, sha, output) = fixture.paths();
        for (slot, ordinal) in [(0, 69), (31, 991)] {
            let wrong_slot = reduce_with(&path, &sha, &output, fixture.pins(), |pin, bytes| {
                let (mut truth, snapshot) = loss_binary(pin, bytes)?;
                // Emulate slot indexing through the test opener; production always uses loss_binary.
                let wrong = truth[slot * K * 8..(slot + 1) * K * 8].to_vec();
                truth[ordinal * K * 8..(ordinal + 1) * K * 8].copy_from_slice(&wrong);
                Ok((truth, snapshot))
            })
            .err()
            .unwrap();
            assert!(
                wrong_slot
                    .to_string()
                    .contains("native per-query truth reconciliation"),
                "slot{slot} substituted for ordinal{ordinal}: {wrong_slot}"
            );
        }
    }

    #[test]
    fn paired_returned_panel_gain_with_hidden_loss_fails() {
        let mut fixture = Fixture::new();
        fixture.patch("spatial", true, |rows| {
            let q = rows.iter_mut().find(|r| r["phase"] == "query").unwrap();
            let base = q["ordinal"].as_u64().unwrap() * 32;
            q["returned"][8]["id"] = json!(base + 9);
            q["returned"][9]["id"] = json!(base + 10);
            let recall = rows.iter_mut().find(|r| r["phase"] == "recall").unwrap();
            recall["hits10"] = json!(9);
            recall["recall10"] = json!(0.9);
            let t = &mut rows.last_mut().unwrap()["summary"];
            t["total_hits10"] = json!(319);
            t["recall_numerator"] = json!(319);
            t["mean_recall10"] = json!(319.0 / 320.0);
        });
        let r = fixture.reduce().report;
        assert_eq!(r["status"], "FAIL");
        assert_eq!(r["complete"], true);
        assert_eq!(r["gained"], 32);
        assert_eq!(r["lost"], 1);
        assert_eq!(r["queries"][0]["truth_slots"][8]["lost"], true);
        assert_eq!(r["radial_hits10"], 288);
        assert_eq!(r["spatial_hits10"], 319);
        let (_, _, output) = fixture.paths();
        assert!(!fixture.execute().unwrap());
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(output).unwrap()).unwrap()["status"],
            "FAIL"
        );
    }

    #[test]
    fn paired_returned_panel_control_ids_and_score_bits_are_invalid() {
        for score in [false, true] {
            let mut fixture = Fixture::new();
            fixture.patch("original_control", true, |rows| {
                let q = rows.iter_mut().find(|r| r["phase"] == "query").unwrap();
                if score {
                    q["returned"][9]["score_bits"] = json!(2.0_f32.to_bits());
                } else {
                    q["returned"][9]["id"] = json!(q["ordinal"].as_u64().unwrap() * 32 + 11);
                }
            });
            assert!(
                fixture
                    .before_truth_error()
                    .contains("radial/control ordered ID and score-bit parity")
            );
            let (_, _, output) = fixture.paths();
            assert!(!fixture.execute().unwrap());
            let r: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(r["status"], "INVALID");
            assert_eq!(r["complete"], false);
        }
        for (slot, arm) in ["original_control", "radial", "spatial"]
            .into_iter()
            .enumerate()
        {
            for exit in [137, 0] {
                let mut fixture = Fixture::new();
                fixture.config[arm]["provenance"]["original_exit_code"] = json!(exit);
                fixture.config[arm]["provenance"]["original_status"] = json!("INVALID");
                assert!(
                    fixture
                        .before_truth_error()
                        .contains("original provenance/exit")
                );
                let (_, sha, output) = fixture.paths();
                assert!(!fixture.execute().unwrap());
                let r: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
                assert_eq!(r["status"], "INVALID");
                assert_eq!(r["complete"], false);
                assert_eq!(r["config_authenticated"], true);
                assert_eq!(r["config_sha256"], sha);
                assert_eq!(r["offending_arm"], arm);
                assert_eq!(r["original_executions"][slot]["arm"], arm);
                assert_eq!(r["original_executions"][slot]["original_exit_code"], exit);
                assert_eq!(r["original_executions"][slot]["original_status"], "INVALID");
            }
        }
    }

    #[test]
    fn paired_returned_panel_seals_inputs_and_ordinals_precede_truth() {
        for arm in ["original_control", "radial", "spatial"] {
            for case in ["seal", "ordinal", "input", "terminal"] {
                let mut fixture = Fixture::new();
                fixture.patch(arm, case != "seal", |rows| match case {
                    "seal" => {
                        rows.iter_mut()
                            .find(|r| r["phase"] == "all_queries_sealed")
                            .unwrap()["prefix_sha256"] = json!("0".repeat(64))
                    }
                    "ordinal" => {
                        rows.iter_mut().find(|r| r["phase"] == "query").unwrap()["ordinal"] =
                            json!(0)
                    }
                    "input" => rows[1]["native_source_sha256"] = json!("0".repeat(64)),
                    _ => rows.last_mut().unwrap()["summary"]["complete"] = json!(false),
                });
                let error = fixture.before_truth_error();
                assert!(
                    error.contains(match case {
                        "seal" => "authenticated query seal",
                        "ordinal" => "query order/truth/count/underfill",
                        "input" => "expected v2 input/root/backend pins",
                        _ => "complete measured terminal",
                    }),
                    "{arm}/{case}: {error}"
                );
            }
        }
        for case in [
            "policy",
            "protocol",
            "config-ordinal",
            "current-binary",
            "current-component",
            "approval",
            "original-exit",
        ] {
            let mut fixture = Fixture::new();
            match case {
                "policy" => {
                    fixture.config["spatial"]["completed"]["expected_bound_inputs"]["native_order_sha256"] =
                        json!("0".repeat(64))
                }
                "protocol" => {
                    let path =
                        PathBuf::from(fixture.config["source_protocol"]["path"].as_str().unwrap());
                    fixture.config["source_protocol"] =
                        pin(&path, b"{\"schema\":\"wrong\",\"ordinals\":[]}");
                }
                "current-binary" => {
                    fixture.config["spatial"]["provenance"]["source_archive_sha256"] =
                        json!("0".repeat(64))
                }
                "config-ordinal" => fixture.config["ordinals"][0] = json!(68),
                "current-component" => {
                    fixture.config["spatial"]["completed"]["expected_identity"]["router_source_sha256"] =
                        json!("0".repeat(64));
                }
                "approval" => {
                    fixture.config["spatial"]["generation"]["generation_root_sha256"] =
                        json!("0".repeat(64))
                }
                _ => {
                    fixture.config["original_control"]["provenance"]["original_exit_code"] =
                        json!(1)
                }
            }
            let error = fixture.before_truth_error();
            assert!(
                error.contains(match case {
                    "policy" => "exact source/order/request/truth/scoring policy",
                    "protocol" => "ordinals match authenticated SOURCE protocol",
                    "config-ordinal" => "explicit32 SOURCE ordinals",
                    "current-binary" => "same current source/binary",
                    "current-component" => "exact current identity policy",
                    "approval" => "explicit approved generation",
                    _ => "original provenance/exit",
                }),
                "{case}: {error}"
            );
        }
        let mut fixture = Fixture::new();
        fixture.patch("spatial", false, |rows| {
            rows.iter_mut()
                .find(|r| r["phase"] == "all_queries_sealed")
                .unwrap()["prefix_sha256"] = json!("0".repeat(64));
        });
        fixture.config["truth"]["path"] = json!(fixture.dir.path().join("inaccessible-truth"));
        let (_, _, output) = fixture.paths();
        assert!(!fixture.execute().unwrap());
        let invalid: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(invalid["error"], "authenticated query seal");
    }

    #[test]
    fn paired_returned_panel_malformed_duplicate_order_nonfinite_underfill() {
        for case in ["duplicate", "order", "nonfinite", "underfill", "malformed"] {
            let mut fixture = Fixture::new();
            fixture.patch("spatial", true, |rows| {
                let q = rows.iter_mut().find(|r| r["phase"] == "query").unwrap();
                match case {
                    "duplicate" => q["returned"][1]["id"] = q["returned"][0]["id"].clone(),
                    "order" => q["returned"].as_array_mut().unwrap().swap(0, 1),
                    "nonfinite" => q["returned"][9]["score_bits"] = json!(f32::INFINITY.to_bits()),
                    "underfill" => {
                        q["returned"].as_array_mut().unwrap().pop();
                        q["returned_count"] = json!(9);
                        q["underfill"] = json!(true);
                    }
                    _ => q["returned"][0]["score_bits"] = json!("malformed"),
                }
            });
            let error = fixture.before_truth_error();
            assert!(
                error.contains(match case {
                    "duplicate" | "nonfinite" => "returned IDs/scores",
                    "order" => "returned ranking order",
                    "underfill" => "full top10 required",
                    _ => "invalid type",
                }),
                "{case}: {error}"
            );
        }
        let mut fixture = Fixture::new();
        let path = PathBuf::from(
            fixture.config["spatial"]["completed"]["input"]["path"]
                .as_str()
                .unwrap(),
        );
        let body = String::from_utf8(std::fs::read(&path).unwrap()).unwrap();
        let body = body.replacen("\"ordinal\":69", "\"ordinal\":69,\"ordinal\":69", 1);
        fixture.config["spatial"]["completed"]["input"] = pin(&path, body.as_bytes());
        assert!(fixture.before_truth_error().contains("duplicate JSON key"));
    }

    #[test]
    fn paired_returned_panel_truth_authentication_and_lifetime_mutation() {
        let fixture = Fixture::new();
        let (path, sha, output) = fixture.paths();
        let truth_path = PathBuf::from(fixture.config["truth"]["path"].as_str().unwrap());
        let mut truth = std::fs::read(&truth_path).unwrap();
        let last = truth.len() - 1;
        truth[last] ^= 1;
        std::fs::write(&truth_path, &truth).unwrap();
        assert!(
            reduce_with(&path, &sha, &output, fixture.pins(), loss_binary)
                .err()
                .unwrap()
                .to_string()
                .contains("full SHA/EOF")
        );
        for case in ["unselected-duplicate", "unselected-domain", "native-recall"] {
            let mut fixture = Fixture::new();
            if case == "native-recall" {
                fixture.patch("spatial", true, |rows| {
                    let r = rows.iter_mut().find(|r| r["phase"] == "recall").unwrap();
                    r["hits10"] = json!(9);
                    r["recall10"] = json!(0.9);
                    let t = &mut rows.last_mut().unwrap()["summary"];
                    t["total_hits10"] = json!(319);
                    t["recall_numerator"] = json!(319);
                    t["mean_recall10"] = json!(319.0 / 320.0);
                });
            } else {
                let path = PathBuf::from(fixture.config["truth"]["path"].as_str().unwrap());
                let mut truth = std::fs::read(&path).unwrap();
                let id = if case == "unselected-duplicate" {
                    999_u64 * 32
                } else {
                    1_000_000
                };
                truth[79_992..80_000].copy_from_slice(&id.to_le_bytes());
                fixture.config["truth"] = pin(&path, &truth);
                let sha = fixture.config["truth"]["sha256"].clone();
                for name in ["original_control", "radial", "spatial"] {
                    fixture.config[name]["completed"]["expected_bound_inputs"]["truth_sha256"] =
                        sha.clone();
                    fixture.patch(name, true, |rows| {
                        rows[1]["truth_sha256"] = sha.clone();
                        rows.last_mut().unwrap()["summary"]["truth_sha256"] = sha.clone();
                    });
                }
            }
            let (path, sha, output) = fixture.paths();
            let error = reduce_with(&path, &sha, &output, fixture.pins(), loss_binary)
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains(if case == "native-recall" {
                    "native per-query truth reconciliation"
                } else {
                    "full truth ID domain/duplicates"
                }),
                "{case}: {error}"
            );
        }
        for role in [
            "config",
            "protocol",
            "original_control",
            "radial",
            "spatial",
            "truth",
        ] {
            let fixture = Fixture::new();
            let (path, sha, output) = fixture.paths();
            let target = match role {
                "config" => path.clone(),
                "protocol" => {
                    PathBuf::from(fixture.config["source_protocol"]["path"].as_str().unwrap())
                }
                "truth" => PathBuf::from(fixture.config["truth"]["path"].as_str().unwrap()),
                _ => PathBuf::from(
                    fixture.config[role]["completed"]["input"]["path"]
                        .as_str()
                        .unwrap(),
                ),
            };
            let error = execute_with(&path, &sha, &output, fixture.pins(), |file, directory| {
                if !directory {
                    let replacement = target.with_extension("replacement");
                    std::fs::copy(&target, &replacement)?;
                    std::fs::rename(replacement, &target)?;
                }
                file.sync_all()
            })
            .err()
            .unwrap();
            assert!(
                error.to_string().contains("mutation/path identity"),
                "{role}: {error}"
            );
            let r: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
            assert_eq!(r["status"], "INVALID");
            assert_eq!(r["publication_failed"], true);
        }
    }

    #[test]
    fn paired_returned_panel_invalid_publication_uses_strong_sync_and_readback() {
        for case in ["file-sync", "directory-sync", "readback", "config-lifetime"] {
            let mut fixture = Fixture::new();
            fixture.config["schema"] = json!("invalid-config");
            let (path, sha, output) = fixture.paths();
            let file_called = Cell::new(false);
            let directory_called = Cell::new(false);
            let error = execute_with(&path, &sha, &output, fixture.pins(), |file, directory| {
                if directory {
                    directory_called.set(true);
                } else {
                    file_called.set(true);
                }
                if (case == "file-sync" && !directory) || (case == "directory-sync" && directory) {
                    return Err(io::Error::other("injected invalid-report sync failure"));
                }
                if !directory && case == "readback" {
                    let mut writer = file.try_clone()?;
                    writer.seek(io::SeekFrom::Start(0))?;
                    writer.write_all(b"!")?;
                }
                if !directory && case == "config-lifetime" {
                    let replacement = path.with_extension("replacement");
                    std::fs::copy(&path, &replacement)?;
                    std::fs::rename(replacement, &path)?;
                }
                file.sync_all()
            })
            .err()
            .unwrap();
            assert!(file_called.get(), "INVALID skipped file sync: {case}");
            assert_eq!(directory_called.get(), case == "directory-sync");
            assert!(
                error.to_string().contains(match case {
                    "readback" => "selection exact readback",
                    "config-lifetime" => "mutation/path identity",
                    _ => "injected invalid-report sync failure",
                }),
                "{case}: {error}"
            );
            let invalid: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(invalid["status"], "INVALID");
            assert_eq!(invalid["complete"], false);
            assert_eq!(invalid["publication_failed"], true);
        }
        for case in ["bad-sha", "malformed-json", "malformed-exit"] {
            let mut fixture = Fixture::new();
            if case == "malformed-exit" {
                fixture.config["original_control"]["provenance"]["original_exit_code"] =
                    json!("137");
            }
            let (path, mut sha, output) = fixture.paths();
            if case == "bad-sha" {
                sha = "0".repeat(64);
            }
            if case == "malformed-json" {
                let body = b"{\"original_control\":{\"provenance\":{\"original_exit_code\":137";
                std::fs::write(&path, body).unwrap();
                sha = digest(body);
            }
            let syncs = Cell::new(0);
            assert!(
                !execute_with(&path, &sha, &output, fixture.pins(), |file, _| {
                    syncs.set(syncs.get() + 1);
                    file.sync_all()
                })
                .unwrap()
            );
            assert_eq!(syncs.get(), 2);
            let invalid: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(invalid["status"], "INVALID");
            assert_eq!(invalid["complete"], false);
            assert_eq!(invalid["config_authenticated"], false);
            assert_eq!(invalid["config_sha256"], Value::Null);
            assert_eq!(invalid["original_executions"], Value::Null);
            assert_eq!(invalid["offending_arm"], Value::Null);
        }
    }

    #[test]
    fn paired_returned_panel_production_pins_refuse_before_truth() {
        for role in ["protocol", "truth", "requests"] {
            let mut fixture = Fixture::new();
            fixture.config["source_protocol"]["sha256"] = json!(PROTOCOL_SHA256);
            fixture.config["truth"]["sha256"] = json!(TRUTH_SHA256);
            for name in ["original_control", "radial", "spatial"] {
                let inputs = &mut fixture.config[name]["completed"]["expected_bound_inputs"];
                inputs["truth_sha256"] = json!(TRUTH_SHA256);
                inputs["requests_sha256"] = json!(REQUESTS_SHA256);
                inputs["reserved_queries_sha256"] = json!(REQUESTS_SHA256);
            }
            let (path, _, output) = fixture.paths();
            let admitted: Config = serde_json::from_value(fixture.config.clone()).unwrap();
            admit(&admitted, &path, &output, FROZEN_PINS).unwrap();
            match role {
                "protocol" => fixture.config["source_protocol"]["sha256"] = json!("0".repeat(64)),
                "truth" => fixture.config["truth"]["sha256"] = json!("0".repeat(64)),
                _ => {
                    for name in ["original_control", "radial", "spatial"] {
                        let inputs =
                            &mut fixture.config[name]["completed"]["expected_bound_inputs"];
                        inputs["requests_sha256"] = json!("0".repeat(64));
                        inputs["reserved_queries_sha256"] = json!("0".repeat(64));
                    }
                }
            }
            let (path, sha, output) = fixture.paths();
            let error = reduce_with(&path, &sha, &output, FROZEN_PINS, |_, _| {
                panic!("wrong production {role} pin reached truth")
            })
            .err()
            .unwrap()
            .to_string();
            assert!(
                error.contains(match role {
                    "protocol" => "explicit32 SOURCE ordinals",
                    "truth" => "full truth geometry",
                    _ => "fixed1M/count1000/full-input/untraced32/fetch16 policy",
                }),
                "{role}: {error}"
            );
            assert!(!execute(&path, &sha, &output).unwrap());
            let invalid: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(invalid["status"], "INVALID");
            assert_eq!(invalid["complete"], false);
        }
    }

    #[test]
    fn paired_returned_panel_caps_and_create_only_publication() {
        let mut fixture = Fixture::new();
        fixture.config["max_owned_bytes"] = json!(1);
        assert!(
            fixture
                .before_truth_error()
                .contains("cumulative allocation cap")
        );
        let mut fixture = Fixture::new();
        let path = PathBuf::from(
            fixture.config["spatial"]["completed"]["input"]["path"]
                .as_str()
                .unwrap(),
        );
        fixture.config["spatial"]["completed"]["input"] =
            pin(&path, &vec![b' '; NATIVE_HEADER_CAP as usize + 1]);
        assert!(fixture.before_truth_error().contains("native line cap"));
        let mut fixture = Fixture::new();
        fixture.patch("spatial", true, |rows| {
            rows.iter_mut().find(|r| r["phase"] == "query").unwrap()["trace"]["semantic_units"] =
                json!(vec![0; 20_000]);
        });
        assert_eq!(fixture.reduce().report["status"], "ADVANCE");
        let fixture = Fixture::new();
        let (_, _, output) = fixture.paths();
        std::fs::write(&output, b"occupied").unwrap();
        assert!(fixture.execute().is_err());
        assert_eq!(std::fs::read(&output).unwrap(), b"occupied");
        assert!(report_cap(&json!({"too_large":"x".repeat(PANEL_REPORT_CAP as usize)})).is_err());
        let fixture = Fixture::new();
        let (path, _, output) = fixture.paths();
        let body = vec![b' '; CONFIG_CAP as usize + 1];
        std::fs::write(&path, &body).unwrap();
        assert!(
            reduce_with(
                &path,
                &digest(&body),
                &output,
                fixture.pins(),
                |_, _| panic!("oversized config reached truth")
            )
            .err()
            .unwrap()
            .to_string()
            .contains("small reduction config")
        );
    }
}
