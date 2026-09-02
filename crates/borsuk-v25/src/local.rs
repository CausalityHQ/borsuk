use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema};

use crate::{
    Result, V25ContainmentSample, V25Control, V25QueryTruth, V25RankedRow, V25RowPages,
    exact_oracle_pages, hits, invalid, ppm, select_v25_rank_sharp_pages,
};

#[derive(Debug, Clone, PartialEq)]
pub struct V25ConstructionRow {
    pub source_ordinal: u64,
    pub vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq)]
pub struct V25LocalQuery {
    pub query_ordinal: u32,
    pub source_ordinal: u64,
    pub vector: [f32; 96],
}

fn vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float32, false)),
        96,
    )
}

pub fn validate_v25_construction_schema(schema: &Schema) -> Result<()> {
    let expected = Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 construction Parquet schema differs"));
    }
    Ok(())
}

pub fn validate_v25_page_assignment_schema(schema: &Schema) -> Result<()> {
    let expected = Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 page assignment Parquet schema differs"));
    }
    Ok(())
}

pub fn validate_v25_query_schema(schema: &Schema) -> Result<()> {
    let expected = Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 query Parquet schema differs"));
    }
    Ok(())
}

pub fn validate_v25_truth_schema(schema: &Schema) -> Result<()> {
    let page_list = |length| {
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::UInt32, false)),
            length,
        )
    };
    let expected = Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("primary_pages", page_list(10), false),
        Field::new("replica_pages", page_list(10), false),
        Field::new("oracle_pages", page_list(8), false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 truth Parquet schema differs"));
    }
    Ok(())
}

fn validate_vector(vector: &[f32; 96]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V25 vector finiteness differs"));
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>();
    if !norm.is_finite() || (norm - 1.0).abs() > 1.0e-4 {
        return Err(invalid("V25 vector normalization differs"));
    }
    Ok(())
}

pub fn evaluate_v25_exact_global(
    rows: &[V25ConstructionRow],
    assignments: &[V25RowPages],
    queries: &[V25LocalQuery],
    truths: &[V25QueryTruth],
    ranked_row_limits: &[u32],
    page_budget: u32,
) -> Result<Vec<V25ContainmentSample>> {
    if rows.is_empty()
        || rows.len() != assignments.len()
        || queries.is_empty()
        || queries.len() != truths.len()
        || page_budget != 8
        || ranked_row_limits.is_empty()
        || ranked_row_limits.windows(2).any(|pair| pair[0] >= pair[1])
        || ranked_row_limits
            .iter()
            .any(|limit| ![10, 32, 128, 512, 2_048, 4_096].contains(limit))
    {
        return Err(invalid("V25 exact-global request differs"));
    }
    let mut row_by_source = BTreeMap::new();
    for row in rows {
        validate_vector(&row.vector)?;
        if row_by_source.insert(row.source_ordinal, row).is_some() {
            return Err(invalid("V25 construction source ordinal repeats"));
        }
    }
    let mut pages_by_source = BTreeMap::new();
    for assignment in assignments {
        if assignment.replica_page == Some(assignment.primary_page)
            || pages_by_source
                .insert(assignment.source_ordinal, *assignment)
                .is_some()
        {
            return Err(invalid("V25 page assignment source ordinal repeats"));
        }
    }
    if row_by_source.keys().ne(pages_by_source.keys()) {
        return Err(invalid("V25 construction page inventory differs"));
    }

    let mut output = Vec::with_capacity(queries.len() * ranked_row_limits.len());
    for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
        validate_vector(&query.vector)?;
        if usize::try_from(query.query_ordinal).ok() != Some(query_index)
            || truth.query_ordinal != query.query_ordinal
            || !row_by_source.contains_key(&query.source_ordinal)
            || exact_oracle_pages(&truth.ground_truth_page_assignments, page_budget as usize)?
                != truth.oracle_pages
        {
            return Err(invalid("V25 exact-global query authority differs"));
        }
        let own_pages = pages_by_source
            .get(&query.source_ordinal)
            .ok_or_else(|| invalid("V25 pseudoquery page binding differs"))?;
        let mut forbidden_pages = vec![own_pages.primary_page];
        if let Some(replica) = own_pages.replica_page {
            forbidden_pages.push(replica);
        }
        forbidden_pages.sort_unstable();

        let mut ranked = Vec::with_capacity(rows.len());
        for row in rows {
            let row_pages = pages_by_source.get(&row.source_ordinal).unwrap();
            let page_is_forbidden = [Some(row_pages.primary_page), row_pages.replica_page]
                .into_iter()
                .flatten()
                .any(|page| forbidden_pages.binary_search(&page).is_ok());
            if row.source_ordinal == query.source_ordinal || page_is_forbidden {
                continue;
            }
            let dot = query
                .vector
                .iter()
                .zip(row.vector)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            let distance = 1.0 - dot;
            if !distance.is_finite() {
                return Err(invalid("V25 exact-global distance differs"));
            }
            ranked.push(V25RankedRow {
                source_ordinal: row.source_ordinal,
                distance,
                page_mass: 1,
            });
        }
        ranked.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        for limit in ranked_row_limits {
            let retained = ranked
                .iter()
                .copied()
                .take((*limit as usize).min(ranked.len()))
                .collect::<Vec<_>>();
            let retained_pages = retained
                .iter()
                .map(|row| *pages_by_source.get(&row.source_ordinal).unwrap())
                .collect::<Vec<_>>();
            let mut selected_pages =
                select_v25_rank_sharp_pages(&retained, &retained_pages, page_budget as usize)?;
            selected_pages.sort_unstable();
            let selected_hits = hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = hits(&truth.ground_truth_page_assignments, &truth.oracle_pages);
            output.push(V25ContainmentSample {
                query_ordinal: query.query_ordinal,
                control: V25Control::ExactGlobal,
                ranked_row_limit: *limit,
                candidate_rows: ranked.len() as u64,
                selected_pages,
                hits: selected_hits,
                oracle_hits,
                recall_ppm: ppm(u64::from(selected_hits), 10)?,
                oracle_attainment_ppm: ppm(u64::from(selected_hits), u64::from(oracle_hits))?,
            });
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use crate::{V25QueryTruth, V25RowPages};

    use super::{
        V25ConstructionRow, V25LocalQuery, evaluate_v25_exact_global,
        validate_v25_construction_schema, validate_v25_page_assignment_schema,
        validate_v25_query_schema, validate_v25_truth_schema,
    };

    fn vector(first: f32, second: f32) -> [f32; 96] {
        let norm = first.hypot(second);
        let mut vector = [0.0; 96];
        vector[0] = first / norm;
        vector[1] = second / norm;
        vector
    }

    #[test]
    fn v25_containment_local_schemas_are_exact_and_cross_language() {
        let vector = || {
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            )
        };
        let construction = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("vector", vector(), false),
        ]);
        let pages = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("primary_page", DataType::UInt32, false),
            Field::new("replica_page", DataType::UInt32, false),
        ]);
        let queries = Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("vector", vector(), false),
        ]);
        let page_list = || {
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10)
        };
        let truth = Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("primary_pages", page_list(), false),
            Field::new("replica_pages", page_list(), false),
            Field::new(
                "oracle_pages",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    8,
                ),
                false,
            ),
        ]);

        assert!(validate_v25_construction_schema(&construction).is_ok());
        assert!(validate_v25_page_assignment_schema(&pages).is_ok());
        assert!(validate_v25_query_schema(&queries).is_ok());
        assert!(validate_v25_truth_schema(&truth).is_ok());

        let wrong_child = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 96),
                false,
            ),
        ]);
        assert!(validate_v25_construction_schema(&wrong_child).is_err());

        let nullable_replica = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("primary_page", DataType::UInt32, false),
            Field::new("replica_page", DataType::UInt32, true),
        ]);
        assert!(validate_v25_page_assignment_schema(&nullable_replica).is_err());
    }

    #[test]
    fn v25_containment_local_exact_global_is_order_invariant_and_excludes_own_pages() {
        let query = V25LocalQuery {
            query_ordinal: 0,
            source_ordinal: 0,
            vector: vector(1.0, 0.0),
        };
        let mut rows = (0..20_u64)
            .map(|source_ordinal| V25ConstructionRow {
                source_ordinal,
                vector: vector(20.0 - source_ordinal as f32, source_ordinal as f32 + 1.0),
            })
            .collect::<Vec<_>>();
        let pages = (0..20_u64)
            .map(|source_ordinal| V25RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal).unwrap(),
                replica_page: (source_ordinal == 0).then_some(19),
            })
            .collect::<Vec<_>>();
        let truth = V25QueryTruth {
            query_ordinal: 0,
            ground_truth_page_assignments: vec![
                vec![1],
                vec![1],
                vec![2],
                vec![2],
                vec![3],
                vec![4],
                vec![5],
                vec![6],
                vec![7],
                vec![8],
            ],
            oracle_pages: (1..=8).collect(),
        };

        let expected = evaluate_v25_exact_global(
            &rows,
            &pages,
            std::slice::from_ref(&query),
            std::slice::from_ref(&truth),
            &[10, 32],
            8,
        )
        .unwrap();
        rows.reverse();
        let reversed = evaluate_v25_exact_global(
            &rows,
            &pages,
            std::slice::from_ref(&query),
            std::slice::from_ref(&truth),
            &[10, 32],
            8,
        )
        .unwrap();
        assert_eq!(reversed, expected);
        assert_eq!(expected.len(), 2);
        assert!(expected.iter().all(|sample| {
            sample.selected_pages.len() == 8
                && !sample.selected_pages.contains(&0)
                && !sample.selected_pages.contains(&19)
        }));
    }
}
