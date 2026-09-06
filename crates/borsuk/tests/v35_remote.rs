//! V35 selective remote-read capability and planning contracts.

use borsuk::{
    V35ArtifactIdentity, V35CandidateAccumulator, V35Dimensions, V35GroupStorage,
    V35LeafPatchBuildRequest, V35RemoteChunk, V35RemoteDirectoryBinding, V35RemoteDirectoryBlock,
    V35RemoteDispatch, V35RemoteFailureKind, V35RemoteRange, V35RemoteRangeResponse,
    V35RouteBudget, V35RoutePrefix, V35ScannedCandidate, V35SnapshotEntry, V35SnapshotVisibility,
    V35TransportFailure, V35VersionedRangeTransport, build_v35_leaf_patch_arm,
    build_v35_residual_sq_descriptor, build_v35_routing_generation, build_v35_srht,
    decode_v35_remote_directory_arrow, encode_v35_remote_directory_arrow, execute_v35_remote_plan,
    exhaustive_v35_route, plan_v35_remote_reads, project_v35_query_scalar,
    reduce_v35_scanned_candidates, select_v35_exact_pages,
};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

const MIB: u64 = 1_048_576;

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn binding(byte: u8) -> V35RemoteDirectoryBinding {
    V35RemoteDirectoryBinding::new([byte; 32], [byte + 1; 32], [byte + 2; 32]).unwrap()
}

fn selected_route(blocks: &[V35ArtifactIdentity]) -> V35RoutePrefix {
    let dimensions = V35Dimensions {
        routing: 64,
        source: 384,
    };
    let projection = build_v35_srht(dimensions, 451).unwrap();
    let mut logical_start = 0_u64;
    let arms = [0.0_f32, 1.0, 2.0]
        .into_iter()
        .enumerate()
        .map(|(ordinal, mean)| {
            let request = V35LeafPatchBuildRequest {
                assignment_max: logical_start + 1,
                assignment_min: logical_start,
                dimensions,
                group_ordinal: u32::try_from(ordinal).unwrap(),
                leaf_ordinal: u32::try_from(ordinal).unwrap(),
                logical_start,
                omitted_energies: vec![0.0; 2],
                projected_rows: vec![vec![mean; 64]; 2],
            };
            logical_start += 2;
            build_v35_leaf_patch_arm(&request, 1).unwrap()
        })
        .collect();
    let generation =
        build_v35_routing_generation(&projection, "deep-image", &digest(0x11), arms).unwrap();
    let groups = [
        V35GroupStorage::new_bound(0, 2, 200, binding(0x51), &blocks[0]).unwrap(),
        V35GroupStorage::new_bound(1, 2, 100, binding(0x51), &blocks[1]).unwrap(),
        V35GroupStorage::new_bound(2, 2, 100, binding(0x51), &blocks[2]).unwrap(),
    ];
    let query = project_v35_query_scalar(&projection, &[0.0; 384]).unwrap();
    exhaustive_v35_route(
        &generation,
        &query,
        &groups,
        V35RouteBudget::new(2, 4, 300).unwrap(),
    )
    .unwrap()
}

fn object(role: &str, uri: &str, length: u64, byte: u8) -> V35ArtifactIdentity {
    V35ArtifactIdentity {
        digest: digest(byte),
        digest_algorithm: "sha256".to_owned(),
        length,
        role: role.to_owned(),
        uri: uri.to_owned(),
    }
}

fn authenticated_directory_block(
    binding: V35RemoteDirectoryBinding,
    chunks: Vec<V35RemoteChunk>,
    uri: &str,
) -> V35RemoteDirectoryBlock {
    let (bytes, identity) = encode_v35_remote_directory_arrow(binding, &chunks, uri).unwrap();
    decode_v35_remote_directory_arrow(&bytes, &identity, binding).unwrap()
}

#[test]
fn v35_remote_directory_arrow_authenticates_binding_and_chunks() {
    // Break caught: callers can label arbitrary trusted Rust chunks as one
    // directory root without authenticating a cross-language directory block.
    let directory_binding = binding(0x51);
    let payload = object(
        "remote-code-object",
        "s3://borsuk-index/generations/g01/codes/code-0000.arrow",
        256,
        0x21,
    );
    let chunks = vec![
        V35RemoteChunk::new(
            0,
            0,
            2,
            payload,
            "version-01",
            16,
            100,
            192,
            sha256(&[0x31; 100]),
        )
        .unwrap(),
    ];
    let uri = "s3://borsuk-index/generations/g01/directory/group-0000.arrow";
    let (bytes, identity) =
        encode_v35_remote_directory_arrow(directory_binding, &chunks, uri).unwrap();
    let (again, again_identity) =
        encode_v35_remote_directory_arrow(directory_binding, &chunks, uri).unwrap();
    assert_eq!(again, bytes);
    assert_eq!(again_identity, identity);
    let decoded = decode_v35_remote_directory_arrow(&bytes, &identity, directory_binding).unwrap();
    assert_eq!(decoded.identity(), &identity);
    assert_eq!(decoded.chunks(), chunks);

    let mut corrupt = bytes.clone();
    let position = corrupt.len() / 2;
    corrupt[position] ^= 1;
    assert!(decode_v35_remote_directory_arrow(&corrupt, &identity, directory_binding).is_err());
    assert!(decode_v35_remote_directory_arrow(&bytes, &identity, binding(0x61)).is_err());
}

fn directory_blocks() -> Vec<V35RemoteDirectoryBlock> {
    let first = object(
        "remote-code-object",
        "s3://borsuk-index/generations/g01/codes/code-0000.bin",
        400,
        0x21,
    );
    let second = object(
        "remote-code-object",
        "s3://borsuk-index/generations/g01/codes/code-0001.bin",
        200,
        0x22,
    );
    let unselected = object(
        "remote-code-object",
        "s3://borsuk-index/generations/g01/codes/code-0002.bin",
        200,
        0x23,
    );
    let chunks = [
        V35RemoteChunk::new(
            0,
            0,
            1,
            first.clone(),
            "v01",
            64,
            100,
            192,
            sha256(&[0x31; 100]),
        )
        .unwrap(),
        V35RemoteChunk::new(0, 1, 1, first, "v01", 164, 100, 192, sha256(&[0x32; 100])).unwrap(),
        V35RemoteChunk::new(1, 2, 2, second, "v01", 32, 100, 384, sha256(&[0x33; 100])).unwrap(),
        V35RemoteChunk::new(
            2,
            4,
            2,
            unselected,
            "v01",
            32,
            100,
            384,
            sha256(&[0x34; 100]),
        )
        .unwrap(),
    ];
    (0..3)
        .map(|group| {
            authenticated_directory_block(
                binding(0x51),
                chunks
                    .iter()
                    .filter(|chunk| chunk.group_ordinal() == group)
                    .cloned()
                    .collect(),
                &format!("s3://borsuk-index/generations/g01/directory/group-{group:04}.arrow"),
            )
        })
        .collect()
}

enum ReadStep {
    Response(V35RemoteRangeResponse, Vec<u8>),
    Failure(V35TransportFailure),
}

struct ScriptedRangeReader {
    steps: VecDeque<ReadStep>,
    next_ticket: u64,
}

struct PipelinedTransport {
    events: Vec<String>,
    next_ticket: u64,
}

impl V35VersionedRangeTransport for PipelinedTransport {
    fn dispatch(
        &mut self,
        range: &V35RemoteRange,
    ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure> {
        self.events.push(format!("dispatch:{}", range.start()));
        let ticket = V35RemoteDispatch::new(self.next_ticket).unwrap();
        self.next_ticket += 1;
        Ok(ticket)
    }

    fn complete(
        &mut self,
        _dispatch: V35RemoteDispatch,
        range: &V35RemoteRange,
        destination: &mut [u8],
    ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
        self.events.push(format!("complete:{}", range.start()));
        let fill = if range.start() == 64 { 0x31 } else { 0x33 };
        destination.fill(fill);
        if range.start() == 64 {
            destination[100..].fill(0x32);
        }
        Ok(response(range, destination))
    }

    fn cancel(&mut self, _dispatch: V35RemoteDispatch) -> u64 {
        0
    }
}

impl V35VersionedRangeTransport for ScriptedRangeReader {
    fn dispatch(
        &mut self,
        _range: &V35RemoteRange,
    ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure> {
        let dispatch = V35RemoteDispatch::new(self.next_ticket).unwrap();
        self.next_ticket += 1;
        Ok(dispatch)
    }

    fn complete(
        &mut self,
        _dispatch: V35RemoteDispatch,
        _range: &V35RemoteRange,
        destination: &mut [u8],
    ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
        match self.steps.pop_front().expect("one scripted read step") {
            ReadStep::Response(response, body) => {
                let written = destination.len().min(body.len());
                destination[..written].copy_from_slice(&body[..written]);
                Ok(response)
            }
            ReadStep::Failure(failure) => Err(failure),
        }
    }

    fn cancel(&mut self, _dispatch: V35RemoteDispatch) -> u64 {
        0
    }
}

struct CancellationTransport {
    next_ticket: u64,
}

impl V35VersionedRangeTransport for CancellationTransport {
    fn dispatch(
        &mut self,
        _range: &V35RemoteRange,
    ) -> std::result::Result<V35RemoteDispatch, V35TransportFailure> {
        let dispatch = V35RemoteDispatch::new(self.next_ticket).unwrap();
        self.next_ticket += 1;
        Ok(dispatch)
    }

    fn complete(
        &mut self,
        _dispatch: V35RemoteDispatch,
        range: &V35RemoteRange,
        destination: &mut [u8],
    ) -> std::result::Result<V35RemoteRangeResponse, V35TransportFailure> {
        destination.fill(0x30);
        Ok(response(range, destination))
    }

    fn cancel(&mut self, _dispatch: V35RemoteDispatch) -> u64 {
        17
    }
}

fn planned_execution() -> borsuk::V35RemotePlan {
    let blocks = directory_blocks();
    let identities = blocks
        .iter()
        .map(|block| block.identity().clone())
        .collect::<Vec<_>>();
    plan_v35_remote_reads(&selected_route(&identities), &blocks).unwrap()
}

fn response(range: &V35RemoteRange, body: &[u8]) -> V35RemoteRangeResponse {
    V35RemoteRangeResponse::new(
        range.uri(),
        range.version_id(),
        range.start(),
        range.end(),
        u64::try_from(body.len()).unwrap(),
        true,
    )
    .unwrap()
}

#[test]
fn v35_remote_execution_authenticates_plan_order_and_accounts_retries() {
    // Break caught: transport can substitute a capability/body, retries are
    // invisible in the receipt, or coalescing loses logical chunk order.
    let plan = planned_execution();
    let first_body = [vec![0x31; 100], vec![0x32; 100]].concat();
    let first = response(&plan.ranges()[0], &first_body);
    let second_body = vec![0x33; 100];
    let second = response(&plan.ranges()[1], &second_body);
    let mut reader = ScriptedRangeReader {
        steps: VecDeque::from([
            ReadStep::Failure(V35TransportFailure::retryable(17)),
            ReadStep::Response(first, first_body),
            ReadStep::Response(second, second_body),
        ]),
        next_ticket: 1,
    };
    let mut delivered = Vec::new();
    let receipt = execute_v35_remote_plan(&plan, &mut reader, |chunk, bytes| {
        delivered.push((chunk.group_ordinal(), chunk.logical_start(), bytes[0]));
        Ok(chunk.decoded_length())
    })
    .unwrap();

    assert_eq!(delivered, vec![(0, 0, 0x31), (0, 1, 0x32), (1, 2, 0x33)]);
    assert_eq!(receipt.physical_get_attempts(), 3);
    assert_eq!(receipt.requested_bytes(), 500);
    assert_eq!(receipt.returned_bytes(), 317);
    assert_eq!(receipt.unique_logical_bytes(), 300);
    assert_eq!(receipt.authenticated_bytes(), 300);
    assert_eq!(receipt.decoded_bytes(), 768);
    assert_eq!(receipt.retry_attempts(), 1);
    assert_eq!(receipt.retry_requested_bytes(), 200);
    assert_eq!(receipt.retry_returned_bytes(), 200);
    assert!(reader.steps.is_empty());
}

#[test]
fn v35_remote_execution_pipelines_dispatch_but_completes_in_plan_order() {
    // Break caught: the executor serializes S3 request latency, or retains
    // concurrent bodies and delivers whichever request happens to finish first.
    let plan = planned_execution();
    let mut transport = PipelinedTransport {
        events: Vec::new(),
        next_ticket: 1,
    };
    let mut delivered = Vec::new();
    let receipt = execute_v35_remote_plan(&plan, &mut transport, |chunk, _| {
        delivered.push(chunk.logical_start());
        Ok(chunk.decoded_length())
    })
    .unwrap();
    assert_eq!(
        transport.events,
        ["dispatch:64", "dispatch:32", "complete:64", "complete:32"]
    );
    assert_eq!(delivered, [0, 1, 2]);
    assert_eq!(receipt.physical_get_attempts(), 2);
}

#[test]
fn v35_remote_execution_accounts_bytes_drained_from_cancelled_dispatches() {
    // Break caught: a failed early range cancels already-dispatched S3 work,
    // but bytes received while draining disappear from the terminal receipt.
    let plan = planned_execution();
    let mut transport = CancellationTransport { next_ticket: 1 };
    let failure = execute_v35_remote_plan(&plan, &mut transport, |_, _| Ok(0)).unwrap_err();
    assert_eq!(failure.kind(), V35RemoteFailureKind::Integrity);
    assert_eq!(failure.receipt().physical_get_attempts(), 2);
    assert_eq!(failure.receipt().returned_bytes(), 217);
    assert_eq!(failure.receipt().authenticated_bytes(), 0);
}

#[test]
fn v35_remote_execution_fails_closed_with_receipt_before_decode() {
    // Break caught: a short/corrupt body is retried or decoded, or retry
    // exhaustion discards the bytes and attempts already spent.
    let cases = [
        (
            vec![{
                let body = [vec![0x31; 100], vec![0x30; 100]].concat();
                ReadStep::Response(response(&planned_execution().ranges()[0], &body), body)
            }],
            V35RemoteFailureKind::Integrity,
            2,
            200,
        ),
        (
            vec![{
                let body = vec![0x31; 199];
                ReadStep::Response(response(&planned_execution().ranges()[0], &body), body)
            }],
            V35RemoteFailureKind::Length,
            2,
            199,
        ),
        (
            vec![
                ReadStep::Failure(V35TransportFailure::retryable(3)),
                ReadStep::Failure(V35TransportFailure::retryable(5)),
                ReadStep::Failure(V35TransportFailure::retryable(7)),
            ],
            V35RemoteFailureKind::Transport,
            4,
            15,
        ),
    ];
    for (steps, expected_kind, attempts, returned) in cases {
        let plan = planned_execution();
        let mut reader = ScriptedRangeReader {
            steps: VecDeque::from(steps),
            next_ticket: 1,
        };
        let mut decoded = false;
        let failure = execute_v35_remote_plan(&plan, &mut reader, |_, _| {
            decoded = true;
            Ok(0)
        })
        .unwrap_err();
        assert_eq!(failure.kind(), expected_kind);
        assert_eq!(failure.receipt().physical_get_attempts(), attempts);
        assert_eq!(failure.receipt().returned_bytes(), returned);
        assert_eq!(failure.receipt().authenticated_bytes(), 0);
        assert!(!decoded);
    }
}

#[test]
fn v35_remote_plan_reads_only_selected_authenticated_ranges() {
    // Break caught: planning downloads a whole code object, includes an
    // unselected group, or fails to preserve independently authenticated
    // chunk boundaries while coalescing adjacent ranges.
    let blocks = directory_blocks();
    let identities = blocks
        .iter()
        .map(|block| block.identity().clone())
        .collect::<Vec<_>>();
    let plan = plan_v35_remote_reads(&selected_route(&identities), &blocks).unwrap();
    assert_eq!(plan.ranges().len(), 2);
    assert_eq!(
        plan.ranges()
            .iter()
            .map(|range| (
                range.uri(),
                range.version_id(),
                range.start(),
                range.end(),
                range.chunk_count(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "s3://borsuk-index/generations/g01/codes/code-0000.bin",
                "v01",
                64,
                264,
                2,
            ),
            (
                "s3://borsuk-index/generations/g01/codes/code-0001.bin",
                "v01",
                32,
                132,
                1,
            ),
        ]
    );
    assert_eq!(plan.selected_groups(), 2);
    assert_eq!(plan.selected_rows(), 4);
    assert_eq!(plan.requested_code_bytes(), 300);
    assert_eq!(plan.maximum_encoded_chunk_bytes(), MIB);
    assert_eq!(plan.maximum_decoded_chunk_bytes(), 2 * MIB);
    assert_eq!(plan.maximum_query_workspace_bytes(), 32 * MIB);
    assert_eq!(plan.maximum_retries(), 2);
    assert_eq!(plan.directory_binding(), binding(0x51));
    assert_ne!(plan.query_digest(), [0; 32]);
    assert!(
        plan.ranges()
            .iter()
            .all(|range| !range.uri().contains("corpus"))
    );
}

#[test]
fn v35_remote_plan_rejects_capability_and_memory_escape_hatches() {
    // Break caught: an object outside the selected prefix, a whole-object
    // request, an endpoint/corpus path, or an oversized buffer reaches the
    // range reader and turns selective S3 search into corpus download/RAM use.
    let mut baseline = directory_blocks();
    let identities = baseline
        .iter()
        .map(|block| block.identity().clone())
        .collect::<Vec<_>>();
    let route = selected_route(&identities);

    let mut unselected_only = baseline.clone();
    unselected_only.remove(1);
    assert!(plan_v35_remote_reads(&route, &unselected_only).is_err());

    let invalid = [
        V35RemoteChunk::new(
            0,
            0,
            2,
            object(
                "remote-code-object",
                "s3://borsuk-index/generations/g01/codes/whole.bin",
                200,
                0x41,
            ),
            "v01",
            0,
            200,
            384,
            digest(0x51),
        ),
        V35RemoteChunk::new(
            0,
            0,
            2,
            object(
                "remote-code-object",
                "s3://borsuk-index/corpus/deep-image.parquet",
                300,
                0x42,
            ),
            "v01",
            10,
            200,
            384,
            digest(0x52),
        ),
        V35RemoteChunk::new(
            0,
            0,
            2,
            object(
                "remote-code-object",
                "https://s3.example.invalid/code.bin",
                300,
                0x43,
            ),
            "v01",
            10,
            200,
            384,
            digest(0x53),
        ),
        V35RemoteChunk::new(
            0,
            0,
            2,
            object(
                "remote-code-object",
                "s3://borsuk-index/generations/g01/codes/large.bin",
                2 * MIB,
                0x44,
            ),
            "v01",
            1,
            MIB + 1,
            384,
            digest(0x54),
        ),
        V35RemoteChunk::new(
            0,
            0,
            2,
            object(
                "remote-code-object",
                "s3://borsuk-index/generations/g01/codes/inflates.bin",
                300,
                0x45,
            ),
            "v01",
            1,
            200,
            2 * MIB + 1,
            digest(0x55),
        ),
    ];
    for chunk in invalid {
        assert!(chunk.is_err());
    }

    baseline.push(baseline[0].clone());
    let duplicate = baseline;
    assert!(plan_v35_remote_reads(&route, &duplicate).is_err());
}

#[test]
fn v35_remote_plan_rejects_generation_snapshot_and_directory_confusion() {
    // Break caught: an authentic route prefix is reused with another
    // generation's directory block or snapshot before any selected byte is
    // authorized.
    let blocks = directory_blocks();
    let identities = blocks
        .iter()
        .map(|block| block.identity().clone())
        .collect::<Vec<_>>();
    let route = selected_route(&identities);
    for position in 0..3 {
        let mut foreign = blocks.clone();
        let uri = foreign[position].identity().uri.clone();
        foreign[position] =
            authenticated_directory_block(binding(0x71), foreign[position].chunks().to_vec(), &uri);
        assert!(plan_v35_remote_reads(&route, &foreign).is_err());
    }
}

#[test]
fn v35_remote_sq4_and_sq8_use_f16_centers_group_sigma_and_source_order() {
    // Break caught: construction averages in f32, measures sigma from an
    // unrounded center, changes the four-sigma range, swaps SQ4 nibbles, or
    // gives a constant dimension a nonzero code.
    let rows = vec![vec![0.0, 2.0, 4.0, 7.0], vec![2.0, 4.0, 6.0, 7.0]];
    let sq4 = build_v35_residual_sq_descriptor(&rows, 4).unwrap();
    assert_eq!(sq4.rows(), 2);
    assert_eq!(sq4.dimensions(), 4);
    assert_eq!(sq4.bytes_per_row(), 2);
    assert_eq!(
        sq4.center_f16_bits(),
        &[
            half::f16::from_f32(1.0).to_bits(),
            half::f16::from_f32(3.0).to_bits(),
            half::f16::from_f32(5.0).to_bits(),
            half::f16::from_f32(7.0).to_bits(),
        ]
    );
    assert_eq!(sq4.scales(), &[8.0 / 15.0, 8.0 / 15.0, 8.0 / 15.0, 0.0]);
    assert_eq!(sq4.codes(), &[0x66, 0x60, 0x99, 0x90]);

    let sq8 = build_v35_residual_sq_descriptor(&rows, 8).unwrap();
    assert_eq!(sq8.bytes_per_row(), 4);
    assert_eq!(sq8.codes(), &[96, 96, 96, 0, 159, 159, 159, 0]);
}

#[test]
fn v35_remote_code_payload_and_scan_rows_scale_with_source_dimension_not_ram() {
    // Break caught: a source-dimensional code plane is admitted to resident
    // RAM, odd SQ4 dimensions lose their pad byte, or the eight-MiB remote
    // budget silently admits too many rows.
    let cases = [
        (384_usize, 4_u8, 192_u64, 19_200_000_000_u64, 43_690_u64),
        (768, 4, 384, 38_400_000_000, 21_845),
        (1_536, 4, 768, 76_800_000_000, 10_922),
        (3_072, 4, 1_536, 153_600_000_000, 5_461),
        (384, 8, 384, 38_400_000_000, 21_845),
        (768, 8, 768, 76_800_000_000, 10_922),
        (1_536, 8, 1_536, 153_600_000_000, 5_461),
        (3_072, 8, 3_072, 307_200_000_000, 2_730),
    ];
    for (dimensions, bits, bytes_per_row, payload, maximum_rows) in cases {
        let descriptor = build_v35_residual_sq_descriptor(&[vec![0.0; dimensions]], bits).unwrap();
        assert_eq!(u64::from(descriptor.bytes_per_row()), bytes_per_row);
        assert_eq!(bytes_per_row * 100_000_000, payload);
        assert_eq!((8 * MIB) / bytes_per_row, maximum_rows);
    }
    let odd = build_v35_residual_sq_descriptor(&[vec![0.0; 385]], 4).unwrap();
    assert_eq!(odd.bytes_per_row(), 193);
    assert_eq!(odd.codes().last(), Some(&0));
    assert!(build_v35_residual_sq_descriptor(&[vec![0.0; 384]], 3).is_err());
    assert!(build_v35_residual_sq_descriptor(&[vec![70_000.0]], 4).is_err());
}

#[test]
fn v35_remote_candidate_heap_filters_visibility_before_bounded_admission() {
    // Break caught: a stale/tombstoned row occupies the 12,288-slot heap,
    // primary/replica copies duplicate an ID, or ties depend on input order.
    let mut entries = (0..12_290_u64)
        .map(|id| V35SnapshotEntry::new(id, 1, true).unwrap())
        .collect::<Vec<_>>();
    entries[0] = V35SnapshotEntry::new(0, 2, true).unwrap();
    entries[1] = V35SnapshotEntry::new(1, 1, false).unwrap();
    let visibility = V35SnapshotVisibility::new([0x81; 32], entries).unwrap();
    let mut scanned = (0..12_290_u64)
        .map(|row| {
            V35ScannedCandidate::new(
                row as f64,
                row,
                row,
                1,
                u32::try_from(row % 32).unwrap(),
                Some(u32::try_from((row + 1) % 32).unwrap()),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    scanned.push(V35ScannedCandidate::new(-2.0, 20_000, 0, 1, 0, Some(1)).unwrap());
    scanned.push(V35ScannedCandidate::new(-1.0, 20_001, 1, 1, 0, Some(1)).unwrap());
    scanned.push(V35ScannedCandidate::new(0.5, 20_002, 0, 2, 0, Some(1)).unwrap());
    scanned.reverse();

    let reduced = reduce_v35_scanned_candidates(&scanned, &visibility).unwrap();
    assert_eq!(reduced.len(), 12_288);
    assert_eq!(reduced[0].id(), 0);
    assert_eq!(reduced[0].sequence(), 2);
    assert_eq!(reduced[0].distance().to_bits(), 0.5_f64.to_bits());
    assert!(reduced.iter().all(|candidate| candidate.id() != 1));
    assert!(
        reduced
            .windows(2)
            .all(|pair| (pair[0].distance(), pair[0].row_ordinal())
                <= (pair[1].distance(), pair[1].row_ordinal()))
    );
}

#[test]
fn v35_remote_visibility_defaults_untouched_base_rows_to_live() {
    // Break caught: the bounded delta mutation directory is treated as a
    // 100M-row allowlist, consuming gigabytes or dropping every untouched row.
    let visibility = V35SnapshotVisibility::new(
        [0xa1; 32],
        vec![
            V35SnapshotEntry::new(7, 2, true).unwrap(),
            V35SnapshotEntry::new(8, 1, false).unwrap(),
        ],
    )
    .unwrap();
    let scanned = [
        V35ScannedCandidate::new(0.0, 0, 7, 1, 0, None).unwrap(),
        V35ScannedCandidate::new(1.0, 1, 7, 2, 0, None).unwrap(),
        V35ScannedCandidate::new(2.0, 2, 8, 1, 0, None).unwrap(),
        V35ScannedCandidate::new(3.0, 3, 9, 1, 0, None).unwrap(),
    ];
    let reduced = reduce_v35_scanned_candidates(&scanned, &visibility).unwrap();
    assert_eq!(
        reduced
            .iter()
            .map(|candidate| (candidate.id(), candidate.sequence()))
            .collect::<Vec<_>>(),
        vec![(7, 2), (9, 1)]
    );
}

#[test]
fn v35_remote_candidate_accumulator_streams_only_the_plans_snapshot() {
    // Break caught: scan candidates are materialized before reduction or are
    // reduced against a stale visibility snapshot from another generation.
    let plan = planned_execution();
    let visibility =
        V35SnapshotVisibility::new([0x52; 32], vec![V35SnapshotEntry::new(7, 2, true).unwrap()])
            .unwrap();
    let mut accumulator = V35CandidateAccumulator::new(&plan, &visibility).unwrap();
    for candidate in [
        V35ScannedCandidate::new(2.0, 2, 9, 1, 4, None).unwrap(),
        V35ScannedCandidate::new(0.0, 0, 7, 1, 4, None).unwrap(),
        V35ScannedCandidate::new(1.0, 1, 7, 2, 4, None).unwrap(),
    ] {
        accumulator.admit(candidate);
    }
    let candidates = accumulator.finish();
    assert_eq!(candidates.generation_digest(), plan.generation_digest());
    assert_eq!(candidates.query_digest(), plan.query_digest());
    assert_eq!(candidates.snapshot_digest(), [0x52; 32]);
    assert_eq!(
        candidates
            .candidates()
            .iter()
            .map(|candidate| (candidate.id(), candidate.sequence()))
            .collect::<Vec<_>>(),
        vec![(7, 2), (9, 1)]
    );

    let stale = V35SnapshotVisibility::new([0x53; 32], vec![]).unwrap();
    assert!(V35CandidateAccumulator::new(&plan, &stale).is_err());
}

#[test]
fn v35_remote_page_reducer_is_coverage_greedy_and_exactly_eight_bounded() {
    // Break caught: primary and replica copies of one candidate consume two
    // page slots, input order changes page choice, or more than eight exact
    // pages are authorized.
    let visibility = V35SnapshotVisibility::new(
        [0x91; 32],
        (0..12_u64)
            .map(|id| V35SnapshotEntry::new(id, 1, true).unwrap())
            .collect(),
    )
    .unwrap();
    let candidates = [
        (0, 4, Some(7)),
        (1, 7, Some(4)),
        (2, 8, Some(9)),
        (3, 9, Some(8)),
        (4, 10, Some(11)),
        (5, 12, Some(13)),
        (6, 14, Some(15)),
        (7, 16, Some(17)),
        (8, 18, Some(19)),
        (9, 20, Some(21)),
    ]
    .into_iter()
    .map(|(row, primary, replica)| {
        V35ScannedCandidate::new(row as f64, row, row, 1, primary, replica).unwrap()
    })
    .collect::<Vec<_>>();
    let reduced = reduce_v35_scanned_candidates(&candidates, &visibility).unwrap();
    assert_eq!(
        select_v35_exact_pages(&reduced),
        vec![4, 8, 10, 12, 14, 16, 18, 20]
    );
}
