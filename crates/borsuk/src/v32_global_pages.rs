//! Query-independent compact global geometry for the bounded V32 diagnostic.

use crate::{BorsukError, Result};

pub(crate) struct GlobalPageOwners {
    pub(crate) owners: Vec<u32>,
    pub(crate) row_counts: Vec<u16>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn normalize(mut vector: [f32; 96]) -> Result<[f32; 96]> {
    let norm = vector.iter().map(|v| v * v).sum::<f32>();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V32 global geometry centroid differs"));
    }
    let inverse = norm.sqrt().recip();
    for value in &mut vector {
        *value *= inverse;
    }
    Ok(vector)
}

struct Geometry<'a> {
    vectors: &'a [[f32; 96]],
    sources: &'a [u64],
    inverse_norms: Vec<f32>,
    margins: Vec<f32>,
    output: GlobalPageOwners,
}

impl Geometry<'_> {
    fn cosine(&self, logical: u32, centroid: &[f32; 96]) -> Result<f32> {
        let logical = logical as usize;
        let similarity = self.vectors[logical]
            .iter()
            .zip(centroid)
            .map(|(value, center)| value * center)
            .sum::<f32>()
            * self.inverse_norms[logical];
        if !similarity.is_finite() {
            return Err(invalid("V32 global geometry similarity differs"));
        }
        Ok(similarity)
    }

    fn centroid(&self, indices: &[u32]) -> Result<[f32; 96]> {
        let mut sums = [0.0; 96];
        for logical in indices {
            for (sum, value) in sums.iter_mut().zip(self.vectors[*logical as usize]) {
                *sum += value;
            }
        }
        normalize(sums)
    }

    fn margin_sort(
        &mut self,
        indices: &mut [u32],
        left: &[f32; 96],
        right: &[f32; 96],
    ) -> Result<()> {
        for logical in indices.iter().copied() {
            let margin = self.cosine(logical, right)? - self.cosine(logical, left)?;
            if !margin.is_finite() {
                return Err(invalid("V32 global geometry margin differs"));
            }
            self.margins[logical as usize] = margin;
        }
        indices.sort_unstable_by(|a, b| {
            self.margins[*a as usize]
                .total_cmp(&self.margins[*b as usize])
                .then_with(|| self.sources[*a as usize].cmp(&self.sources[*b as usize]))
        });
        Ok(())
    }

    fn partition(&mut self, indices: &mut [u32], pages: usize) -> Result<()> {
        if pages == 1 {
            let page = self.output.row_counts.len() as u32;
            self.output.row_counts.push(indices.len() as u16);
            for logical in indices {
                self.output.owners[*logical as usize] = page;
            }
            return Ok(());
        }
        let left_pages = pages / 2;
        let left_size =
            left_pages * (indices.len() / pages) + (indices.len() % pages).min(left_pages);
        indices.sort_unstable_by_key(|logical| self.sources[*logical as usize]);
        let mut left = self.centroid(&indices[..1])?;
        let mut farthest = indices[0];
        let mut farthest_cosine = self.cosine(farthest, &left)?;
        for logical in indices.iter().copied().skip(1) {
            let cosine = self.cosine(logical, &left)?;
            if cosine.total_cmp(&farthest_cosine).is_lt() {
                // Source-sorted traversal preserves the smallest-source tie.
                farthest = logical;
                farthest_cosine = cosine;
            }
        }
        let mut right = self.centroid(&[farthest])?;
        for _ in 0..4 {
            self.margin_sort(indices, &left, &right)?;
            left = self.centroid(&indices[..left_size])?;
            right = self.centroid(&indices[left_size..])?;
        }
        self.margin_sort(indices, &left, &right)?;
        let (left_indices, right_indices) = indices.split_at_mut(left_size);
        self.partition(left_indices, left_pages)?;
        self.partition(right_indices, pages - left_pages)
    }
}

pub(crate) fn global_balanced_pages(
    vectors: &[[f32; 96]],
    sources: &[u64],
    capacity: usize,
) -> Result<GlobalPageOwners> {
    let count = vectors.len();
    if count == 0 || count > 1_000_000 || sources.len() != count || !(1..=480).contains(&capacity) {
        return Err(invalid("V32 global geometry shape differs"));
    }
    let mut indices = (0..count as u32).collect::<Vec<_>>();
    indices.sort_unstable_by_key(|logical| sources[*logical as usize]);
    if indices
        .windows(2)
        .any(|pair| sources[pair[0] as usize] == sources[pair[1] as usize])
    {
        return Err(invalid("V32 global geometry source identity differs"));
    }
    let mut inverse_norms = Vec::with_capacity(count);
    for vector in vectors {
        let squared = vector.iter().map(|value| value * value).sum::<f32>();
        if vector.iter().any(|value| !value.is_finite()) || !squared.is_finite() || squared <= 0.0 {
            return Err(invalid("V32 global geometry vector differs"));
        }
        inverse_norms.push(squared.sqrt().recip());
    }
    let pages = count.div_ceil(capacity);
    let mut geometry = Geometry {
        vectors,
        sources,
        inverse_norms,
        margins: vec![0.0; count],
        output: GlobalPageOwners {
            owners: vec![u32::MAX; count],
            row_counts: Vec::with_capacity(pages),
        },
    };
    geometry.partition(&mut indices, pages)?;
    Ok(geometry.output)
}

#[cfg(test)]
mod tests {
    use super::global_balanced_pages;
    use crate::v30_s3_layout::{V30LayoutRecord, partition_v30_leaf_pages};

    fn fixture() -> (Vec<[f32; 96]>, Vec<u64>) {
        let vectors = (0..33)
            .map(|i| {
                let mut vector = [0.0; 96];
                vector[0] = 1.0;
                vector[1 + i % 8] = 0.25 + (i / 8) as f32 * 0.125;
                vector
            })
            .collect();
        (vectors, (0..33).map(|i| 100 + i * 7).collect())
    }

    #[test]
    fn v32_global_pages_balances_and_reverses() {
        // Break: lost ownership, capacity rounding, or input-order seeding.
        let (mut vectors, mut sources) = fixture();
        let first = global_balanced_pages(&vectors, &sources, 4).unwrap();
        assert_eq!(first.owners.len(), 33);
        assert_eq!(first.row_counts.len(), 9);
        assert_eq!(
            first
                .row_counts
                .iter()
                .map(|n| usize::from(*n))
                .sum::<usize>(),
            33
        );
        assert!(first.row_counts.iter().all(|n| (3..=4).contains(n)));
        for (page, count) in first.row_counts.iter().enumerate() {
            assert_eq!(
                first
                    .owners
                    .iter()
                    .filter(|owner| **owner as usize == page)
                    .count(),
                usize::from(*count)
            );
        }
        vectors.reverse();
        sources.reverse();
        let mut reverse = global_balanced_pages(&vectors, &sources, 4).unwrap();
        reverse.owners.reverse();
        assert_eq!(first.owners, reverse.owners);
        assert_eq!(first.row_counts, reverse.row_counts);
    }

    #[test]
    fn v32_global_pages_rejects_invalid_authority() {
        // Break: accepting ambiguous source identity or invalid cosine geometry.
        let (vectors, sources) = fixture();
        assert!(global_balanced_pages(&[], &[], 4).is_err());
        assert!(global_balanced_pages(&vectors, &sources[..32], 4).is_err());
        for capacity in [0, 481, usize::MAX] {
            assert!(global_balanced_pages(&vectors, &sources, capacity).is_err());
        }
        let mut duplicate = sources.clone();
        duplicate[1] = duplicate[0];
        assert!(global_balanced_pages(&vectors, &duplicate, 4).is_err());
        for value in [f32::NAN, f32::INFINITY, f32::MAX] {
            let mut invalid = vectors.clone();
            invalid[0][0] = value;
            assert!(global_balanced_pages(&invalid, &sources, 4).is_err());
        }
        let mut zero = vectors.clone();
        zero[0] = [0.0; 96];
        assert!(global_balanced_pages(&zero, &sources, 4).is_err());
    }

    #[test]
    fn v32_global_pages_matches_existing_scalar_splitter() {
        // Break: changed seed normalization, margins, centroid order or recursion.
        let (vectors, sources) = fixture();
        for capacity in [1, 2, 4, 7, 33, 480] {
            let actual = global_balanced_pages(&vectors, &sources, capacity).unwrap();
            let records = vectors
                .iter()
                .zip(&sources)
                .map(|(vector, source)| V30LayoutRecord {
                    leaf_ordinal: 0,
                    source_ordinal: *source,
                    base_code: vec![0; 24],
                    high_code: None,
                    vector: *vector,
                })
                .collect();
            let reference = partition_v30_leaf_pages(records, capacity).unwrap();
            let mut expected = vec![u32::MAX; sources.len()];
            for (page, records) in reference.iter().enumerate() {
                for record in records {
                    let logical = sources
                        .iter()
                        .position(|s| *s == record.source_ordinal)
                        .unwrap();
                    expected[logical] = page as u32;
                }
            }
            assert_eq!(actual.owners, expected, "capacity={capacity}");
        }
    }
}
