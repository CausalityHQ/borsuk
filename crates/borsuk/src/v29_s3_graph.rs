use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{Array, RecordBatch, UInt8Array, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const GRAPH_AUTHORITY_KEY: &str = "borsuk.v29.authority";
const GRAPH_DIGEST_KEY: &str = "borsuk.v29.graph_sha256";
const GRAPH_DEGREE: u8 = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V29GraphAuthority {
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) hierarchy_sha256: String,
    pub(crate) layout_sha256: String,
    pub(crate) code_sha256: String,
    pub(crate) page_roster_sha256: String,
    pub(crate) page_count: u32,
    pub(crate) degree: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V29BoundaryRow {
    pub(crate) source_ordinal: u64,
    pub(crate) physical_page: u32,
    pub(crate) alternate_leaf: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V29PageGraph {
    authority: V29GraphAuthority,
    neighbors: Vec<Vec<(u32, u32)>>,
}

impl V29PageGraph {
    pub(crate) fn neighbors(&self, page: u32) -> &[(u32, u32)] {
        usize::try_from(page)
            .ok()
            .and_then(|page| self.neighbors.get(page))
            .map_or(&[], Vec::as_slice)
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn exact_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_authority(authority: &V29GraphAuthority) -> Result<()> {
    let digests = [
        authority.source_archive_sha256.as_str(),
        authority.hierarchy_sha256.as_str(),
        authority.layout_sha256.as_str(),
        authority.code_sha256.as_str(),
        authority.page_roster_sha256.as_str(),
    ];
    if !exact_hex(&authority.source_commit, 40)
        || digests.iter().any(|digest| !exact_hex(digest, 64))
        || digests.into_iter().collect::<BTreeSet<_>>().len() != digests.len()
        || authority.page_count == 0
        || authority.degree != GRAPH_DEGREE
    {
        return Err(invalid("V29 graph authority differs"));
    }
    Ok(())
}

fn validate_graph(graph: &V29PageGraph) -> Result<()> {
    validate_authority(&graph.authority)?;
    if graph.neighbors.len() != graph.authority.page_count as usize {
        return Err(invalid("V29 graph page count differs"));
    }
    for (page, neighbors) in graph.neighbors.iter().enumerate() {
        if neighbors.len() > usize::from(graph.authority.degree) {
            return Err(invalid("V29 graph degree differs"));
        }
        let mut previous = None;
        for &(neighbor, vote) in neighbors {
            if neighbor >= graph.authority.page_count || neighbor as usize == page || vote == 0 {
                return Err(invalid("V29 graph edge differs"));
            }
            let key = (std::cmp::Reverse(vote), neighbor);
            if previous.is_some_and(|previous| previous >= key) {
                return Err(invalid("V29 graph edge order differs"));
            }
            previous = Some(key);
            if !graph.neighbors[neighbor as usize]
                .iter()
                .any(|&(other, other_vote)| other as usize == page && other_vote == vote)
            {
                return Err(invalid("V29 graph symmetry differs"));
            }
        }
    }
    Ok(())
}

pub(crate) fn build_v29_page_graph(
    authority: V29GraphAuthority,
    leaf_pages: &[Vec<u32>],
    rows: &[V29BoundaryRow],
) -> Result<V29PageGraph> {
    validate_authority(&authority)?;
    let mut page_owner = vec![None; authority.page_count as usize];
    for (leaf, pages) in leaf_pages.iter().enumerate() {
        for &page in pages {
            let owner = page_owner
                .get_mut(page as usize)
                .ok_or_else(|| invalid("V29 graph primary page differs"))?;
            if owner.replace(leaf as u32).is_some() {
                return Err(invalid("V29 graph primary page duplicates"));
            }
        }
    }
    if page_owner.iter().any(Option::is_none) {
        return Err(invalid("V29 graph primary page union differs"));
    }
    let mut source_ordinals = BTreeSet::new();
    let mut votes = BTreeMap::<(u32, u32), u32>::new();
    for row in rows {
        if !source_ordinals.insert(row.source_ordinal)
            || row.physical_page >= authority.page_count
            || row.alternate_leaf as usize >= leaf_pages.len()
        {
            return Err(invalid("V29 graph boundary row differs"));
        }
        for &other in &leaf_pages[row.alternate_leaf as usize] {
            if other == row.physical_page {
                continue;
            }
            let edge = if row.physical_page < other {
                (row.physical_page, other)
            } else {
                (other, row.physical_page)
            };
            let vote = votes.entry(edge).or_default();
            *vote = vote
                .checked_add(1)
                .ok_or_else(|| invalid("V29 graph vote overflows"))?;
        }
    }
    let mut edges = votes
        .into_iter()
        .map(|((left, right), vote)| (vote, left, right))
        .collect::<Vec<_>>();
    edges.sort_unstable_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut neighbors = vec![Vec::new(); authority.page_count as usize];
    for (vote, left, right) in edges {
        if neighbors[left as usize].len() == usize::from(authority.degree)
            || neighbors[right as usize].len() == usize::from(authority.degree)
        {
            continue;
        }
        neighbors[left as usize].push((right, vote));
        neighbors[right as usize].push((left, vote));
    }
    for page_neighbors in &mut neighbors {
        page_neighbors
            .sort_unstable_by_key(|&(neighbor, vote)| (std::cmp::Reverse(vote), neighbor));
    }
    let graph = V29PageGraph {
        authority,
        neighbors,
    };
    validate_graph(&graph)?;
    Ok(graph)
}

fn graph_digest(graph: &V29PageGraph) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(
        serde_json::to_vec(&graph.authority)
            .map_err(|_| invalid("V29 graph authority serialization differs"))?,
    );
    for (page, neighbors) in graph.neighbors.iter().enumerate() {
        for &(neighbor, vote) in neighbors {
            digest.update((page as u32).to_le_bytes());
            digest.update(neighbor.to_le_bytes());
            digest.update(vote.to_le_bytes());
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn graph_schema(authority: &V29GraphAuthority, digest: String) -> Result<Schema> {
    let metadata = BTreeMap::from([
        (
            GRAPH_AUTHORITY_KEY.to_owned(),
            serde_json::to_string(authority)
                .map_err(|_| invalid("V29 graph authority serialization differs"))?,
        ),
        (GRAPH_DIGEST_KEY.to_owned(), digest),
    ]);
    Ok(Schema::new_with_metadata(
        vec![
            Field::new("page_ordinal", DataType::UInt32, false),
            Field::new("neighbor_page_ordinal", DataType::UInt32, false),
            Field::new("vote", DataType::UInt32, false),
            Field::new("neighbor_rank", DataType::UInt8, false),
        ],
        metadata.into_iter().collect(),
    ))
}

pub(crate) fn encode_v29_page_graph(graph: &V29PageGraph) -> Result<Vec<u8>> {
    validate_graph(graph)?;
    let schema = Arc::new(graph_schema(&graph.authority, graph_digest(graph)?)?);
    let mut pages = Vec::new();
    let mut neighbors = Vec::new();
    let mut votes = Vec::new();
    let mut ranks = Vec::new();
    for (page, entries) in graph.neighbors.iter().enumerate() {
        for (rank, &(neighbor, vote)) in entries.iter().enumerate() {
            pages.push(page as u32);
            neighbors.push(neighbor);
            votes.push(vote);
            ranks.push(rank as u8);
        }
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(pages)),
            Arc::new(UInt32Array::from(neighbors)),
            Arc::new(UInt32Array::from(votes)),
            Arc::new(UInt8Array::from(ranks)),
        ],
    )
    .map_err(|_| invalid("V29 graph batch differs"))?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None)
        .map_err(|_| invalid("V29 graph Parquet writer differs"))?;
    writer
        .write(&batch)
        .map_err(|_| invalid("V29 graph Parquet write differs"))?;
    writer
        .close()
        .map_err(|_| invalid("V29 graph Parquet close differs"))?;
    Ok(bytes)
}

pub(crate) fn decode_v29_page_graph(
    bytes: &[u8],
    expected: &V29GraphAuthority,
) -> Result<V29PageGraph> {
    validate_authority(expected)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .map_err(|_| invalid("V29 graph Parquet differs"))?;
    let expected_authority = serde_json::to_string(expected)
        .map_err(|_| invalid("V29 graph authority serialization differs"))?;
    let metadata = builder.schema().metadata().clone();
    if metadata.get(GRAPH_AUTHORITY_KEY) != Some(&expected_authority)
        || metadata.get(GRAPH_DIGEST_KEY).is_none()
        || builder.schema().fields()
            != graph_schema(expected, metadata[GRAPH_DIGEST_KEY].clone())?.fields()
    {
        return Err(invalid("V29 graph schema differs"));
    }
    let mut graph = V29PageGraph {
        authority: expected.clone(),
        neighbors: vec![Vec::new(); expected.page_count as usize],
    };
    let reader = builder
        .build()
        .map_err(|_| invalid("V29 graph Parquet reader differs"))?;
    for batch in reader {
        let batch = batch.map_err(|_| invalid("V29 graph Parquet batch differs"))?;
        let page = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V29 graph page column differs"))?;
        let neighbor = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V29 graph neighbor column differs"))?;
        let vote = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V29 graph vote column differs"))?;
        let rank = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("V29 graph rank column differs"))?;
        if page.null_count() != 0
            || neighbor.null_count() != 0
            || vote.null_count() != 0
            || rank.null_count() != 0
        {
            return Err(invalid("V29 graph nullability differs"));
        }
        for row in 0..batch.num_rows() {
            let entries = graph
                .neighbors
                .get_mut(page.value(row) as usize)
                .ok_or_else(|| invalid("V29 graph page differs"))?;
            if usize::from(rank.value(row)) != entries.len() {
                return Err(invalid("V29 graph rank differs"));
            }
            entries.push((neighbor.value(row), vote.value(row)));
        }
    }
    validate_graph(&graph)?;
    if metadata[GRAPH_DIGEST_KEY] != graph_digest(&graph)? {
        return Err(invalid("V29 graph digest differs"));
    }
    Ok(graph)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V29GraphPageSelection {
    pub(crate) seed_pages: Vec<u32>,
    pub(crate) frontier_pages: Vec<u32>,
    pub(crate) pages: Vec<u32>,
    pub(crate) evidence_pages: usize,
    pub(crate) edge_visits: usize,
}

pub(crate) fn select_v29_graph_pages(
    graph: &V29PageGraph,
    evidence_pages: &[u32],
) -> Result<V29GraphPageSelection> {
    validate_graph(graph)?;
    if !(8..=128).contains(&evidence_pages.len())
        || evidence_pages
            .iter()
            .any(|page| *page >= graph.authority.page_count)
        || evidence_pages
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != evidence_pages.len()
    {
        return Err(invalid("V29 graph evidence differs"));
    }
    let seed_pages = evidence_pages[..8].to_vec();
    let seed_set = seed_pages.iter().copied().collect::<BTreeSet<_>>();
    let mut scores = BTreeMap::<u32, u64>::new();
    let mut edge_visits = 0_usize;
    for (rank, page) in evidence_pages.iter().copied().enumerate() {
        let reciprocal_rank = (1_u64 << 24) / (rank as u64 + 1);
        for &(neighbor, vote) in graph.neighbors(page) {
            edge_visits = edge_visits
                .checked_add(1)
                .ok_or_else(|| invalid("V29 graph edge work overflows"))?;
            if edge_visits > 2_048 {
                return Err(invalid("V29 graph edge work differs"));
            }
            if seed_set.contains(&neighbor) {
                continue;
            }
            let contribution = u64::from(vote)
                .checked_mul(reciprocal_rank)
                .ok_or_else(|| invalid("V29 graph score overflows"))?;
            let score = scores.entry(neighbor).or_default();
            *score = score
                .checked_add(contribution)
                .ok_or_else(|| invalid("V29 graph score overflows"))?;
        }
    }
    let mut ranked = scores.into_iter().collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let frontier_pages = ranked
        .into_iter()
        .take(2)
        .map(|(page, _)| page)
        .collect::<Vec<_>>();
    if frontier_pages.len() != 2 {
        return Err(invalid("V29 graph frontier cardinality differs"));
    }
    let mut pages = seed_pages.clone();
    pages.extend(frontier_pages.iter().copied());
    Ok(V29GraphPageSelection {
        seed_pages,
        frontier_pages,
        pages,
        evidence_pages: evidence_pages.len(),
        edge_visits,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V29BoundaryRow, V29GraphAuthority, build_v29_page_graph, decode_v29_page_graph,
        encode_v29_page_graph, select_v29_graph_pages,
    };

    fn authority() -> V29GraphAuthority {
        V29GraphAuthority {
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            hierarchy_sha256: "3".repeat(64),
            layout_sha256: "4".repeat(64),
            code_sha256: "5".repeat(64),
            page_roster_sha256: "6".repeat(64),
            page_count: 4,
            degree: 16,
        }
    }

    #[test]
    fn v29_s3_graph_builds_symmetric_canonical_boundary_votes() {
        let rows = vec![
            V29BoundaryRow {
                source_ordinal: 0,
                physical_page: 0,
                alternate_leaf: 1,
            },
            V29BoundaryRow {
                source_ordinal: 1,
                physical_page: 0,
                alternate_leaf: 1,
            },
            V29BoundaryRow {
                source_ordinal: 2,
                physical_page: 1,
                alternate_leaf: 1,
            },
            V29BoundaryRow {
                source_ordinal: 3,
                physical_page: 2,
                alternate_leaf: 0,
            },
        ];
        let graph = build_v29_page_graph(authority(), &[vec![0, 1], vec![2, 3]], &rows).unwrap();
        assert_eq!(graph.neighbors(0), &[(2, 3), (3, 2)]);
        assert_eq!(graph.neighbors(2), &[(0, 3), (1, 2)]);
        assert_eq!(graph.neighbors(3), &[(0, 2), (1, 1)]);

        let reversed = rows.into_iter().rev().collect::<Vec<_>>();
        assert_eq!(
            graph,
            build_v29_page_graph(authority(), &[vec![0, 1], vec![2, 3]], &reversed).unwrap()
        );
    }

    #[test]
    fn v29_s3_graph_parquet_roundtrip_binds_authority_and_schema() {
        let graph = build_v29_page_graph(
            authority(),
            &[vec![0, 1], vec![2, 3]],
            &[
                V29BoundaryRow {
                    source_ordinal: 0,
                    physical_page: 0,
                    alternate_leaf: 1,
                },
                V29BoundaryRow {
                    source_ordinal: 1,
                    physical_page: 2,
                    alternate_leaf: 0,
                },
            ],
        )
        .unwrap();
        let bytes = encode_v29_page_graph(&graph).unwrap();
        assert_eq!(decode_v29_page_graph(&bytes, &authority()).unwrap(), graph);

        let mut changed = authority();
        changed.code_sha256 = "7".repeat(64);
        assert!(decode_v29_page_graph(&bytes, &changed).is_err());

        let mut corrupt = bytes;
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 1;
        assert!(decode_v29_page_graph(&corrupt, &authority()).is_err());
    }

    #[test]
    fn v29_s3_graph_rejects_invalid_roles_pages_and_overflow() {
        let valid = V29BoundaryRow {
            source_ordinal: 0,
            physical_page: 0,
            alternate_leaf: 1,
        };
        let mut duplicate = valid;
        duplicate.physical_page = 1;
        assert!(
            build_v29_page_graph(authority(), &[vec![0, 1], vec![2, 3]], &[valid, duplicate])
                .is_err()
        );

        let invalid_page = V29BoundaryRow {
            physical_page: 4,
            ..valid
        };
        assert!(
            build_v29_page_graph(authority(), &[vec![0, 1], vec![2, 3]], &[invalid_page]).is_err()
        );
        let invalid_leaf = V29BoundaryRow {
            alternate_leaf: 2,
            ..valid
        };
        assert!(
            build_v29_page_graph(authority(), &[vec![0, 1], vec![2, 3]], &[invalid_leaf]).is_err()
        );

        let mut bad = authority();
        bad.degree = 0;
        assert!(build_v29_page_graph(bad, &[vec![0, 1], vec![2, 3]], &[valid]).is_err());
        let mut overlap = authority();
        overlap.layout_sha256 = overlap.code_sha256.clone();
        assert!(build_v29_page_graph(overlap, &[vec![0, 1], vec![2, 3]], &[valid]).is_err());
    }

    fn selection_graph() -> super::V29PageGraph {
        let mut graph_authority = authority();
        graph_authority.page_count = 12;
        let leaf_pages = (0..12).map(|page| vec![page]).collect::<Vec<_>>();
        let mut rows = Vec::new();
        for source_ordinal in 0..10 {
            rows.push(V29BoundaryRow {
                source_ordinal,
                physical_page: 0,
                alternate_leaf: 8,
            });
        }
        for source_ordinal in 10..19 {
            rows.push(V29BoundaryRow {
                source_ordinal,
                physical_page: 1,
                alternate_leaf: 9,
            });
        }
        build_v29_page_graph(graph_authority, &leaf_pages, &rows).unwrap()
    }

    #[test]
    fn v29_s3_select_promotes_two_boundary_pages_from_bounded_evidence() {
        let selection =
            select_v29_graph_pages(&selection_graph(), &[0, 1, 2, 3, 4, 5, 6, 7, 10, 11]).unwrap();
        assert_eq!(selection.seed_pages, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(selection.frontier_pages, vec![8, 9]);
        assert_eq!(selection.pages, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(selection.evidence_pages, 10);
        assert_eq!(selection.edge_visits, 2);
    }

    #[test]
    fn v29_s3_select_is_deterministic_and_breaks_equal_votes_by_page() {
        let mut graph_authority = authority();
        graph_authority.page_count = 12;
        let leaf_pages = (0..12).map(|page| vec![page]).collect::<Vec<_>>();
        let graph = build_v29_page_graph(
            graph_authority,
            &leaf_pages,
            &[
                V29BoundaryRow {
                    source_ordinal: 0,
                    physical_page: 0,
                    alternate_leaf: 9,
                },
                V29BoundaryRow {
                    source_ordinal: 1,
                    physical_page: 0,
                    alternate_leaf: 8,
                },
            ],
        )
        .unwrap();
        let first = select_v29_graph_pages(&graph, &[0, 1, 2, 3, 4, 5, 6, 7]).unwrap();
        let second = select_v29_graph_pages(&graph, &[0, 1, 2, 3, 4, 5, 6, 7]).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.frontier_pages, vec![8, 9]);
    }

    #[test]
    fn v29_s3_select_rejects_unbounded_duplicate_or_incomplete_evidence() {
        let graph = selection_graph();
        assert!(select_v29_graph_pages(&graph, &[0, 1, 2, 3, 4, 5, 6]).is_err());
        assert!(select_v29_graph_pages(&graph, &[0, 1, 2, 3, 4, 5, 6, 6]).is_err());
        let too_many = (0..129).map(|page| page % 12).collect::<Vec<_>>();
        assert!(select_v29_graph_pages(&graph, &too_many).is_err());
    }
}
