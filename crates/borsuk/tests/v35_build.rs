//! V35 bounded streaming writer and immutable-delta contracts.

use borsuk::{V35MortonModel, train_v35_morton_model};

fn training_rows() -> Vec<Vec<f64>> {
    (0..256)
        .map(|row| {
            (0..18)
                .map(|dimension| {
                    let centered = f64::from(row) - 127.5;
                    let scale = if dimension < 16 {
                        f64::from(18 - dimension)
                    } else {
                        1.0
                    };
                    centered * scale
                })
                .collect()
        })
        .collect()
}

#[test]
fn v35_build_morton_model_selects_variance_quantiles_and_msb_interleave() {
    // Break caught: build order depends on queries, coordinate ties select the
    // later dimension, equality crosses a quantile, or Morton bits are LSB-first.
    let rows = training_rows();
    let model = train_v35_morton_model(&rows).unwrap();
    assert_eq!(
        model.selected_coordinates(),
        &(0_u16..16).collect::<Vec<_>>()
    );
    assert_eq!(model.boundaries(0).unwrap().len(), 255);
    assert_eq!(model.boundaries(0).unwrap()[0], -2295.0);
    assert_eq!(model.boundaries(0).unwrap()[254], 2277.0);

    let exact_boundary = rows[1].clone();
    let just_above = exact_boundary
        .iter()
        .enumerate()
        .map(|(dimension, value)| {
            if dimension < 16 {
                value + f64::EPSILON * value.abs().max(1.0)
            } else {
                *value
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(model.key(&exact_boundary).unwrap(), 0xffff);
    assert_eq!(model.key(&just_above).unwrap(), 0xffff_0000);

    let bytes = model.canonical_bytes().unwrap();
    assert_eq!(V35MortonModel::from_canonical_bytes(&bytes).unwrap(), model);
    let mut noncanonical = bytes.clone();
    noncanonical.push(b' ');
    assert!(V35MortonModel::from_canonical_bytes(&noncanonical).is_err());
}
