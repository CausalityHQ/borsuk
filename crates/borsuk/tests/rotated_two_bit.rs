use borsuk::rotated_two_bit::RotatedTwoBitCodec;
use sha2::{Digest, Sha256};

#[test]
fn matches_frozen_python_record_and_scores_with_bounded_scratch() {
    let mean = (0..768)
        .map(|i| (i % 5) as f32 * 0.25 - 0.5)
        .collect::<Vec<_>>();
    let row = mean
        .iter()
        .enumerate()
        .map(|(i, &m)| m + (i % 17) as f32 * 0.125 - 1.0)
        .collect::<Vec<_>>();
    let query = (0..768)
        .map(|i| (i % 11) as f32 * 0.0625 - 0.3125)
        .collect::<Vec<_>>();
    let codec = RotatedTwoBitCodec::new(&mean, 20260923).unwrap();
    let record = codec.encode(&row).unwrap();
    assert_eq!(record.len(), 200);
    assert_eq!(
        format!("{:x}", Sha256::digest(&record[..196])),
        "b5bba8b192b2171003a6b69504497ce436f13fe6bd1aa8a93f8b13b9787e40b3"
    );
    let prepared = codec.prepare_query(&query, 400 * 1024).unwrap();
    assert!(codec.prepare_query(&query, 1).is_err());
    assert_eq!(prepared.scratch_bytes(), 192 * 256 * 8);
    assert!((prepared.score(&record).unwrap() - 0.008058264073722092).abs() < 1e-7);
    assert!(prepared.score(&record[..199]).is_err());
    let mut corrupt = record;
    corrupt[196..].copy_from_slice(&f32::NAN.to_le_bytes());
    assert!(prepared.score(&corrupt).is_err());
}

#[test]
fn pads_tail_dimensions_and_rejects_nonfinite_or_zero_vectors() {
    let codec = RotatedTwoBitCodec::new(&[0.0; 5], 7).unwrap();
    assert_eq!(codec.dimensions(), 5);
    assert_eq!(codec.record_bytes(), 10);
    let query = [1.0, 2.0, -1.0, 0.5, 3.0];
    let matching = codec.encode(&query).unwrap();
    let opposite = codec.encode(&query.map(|x| -x)).unwrap();
    let prepared = codec.prepare_query(&query, 8192).unwrap();
    assert!(prepared.score(&matching).unwrap() > 0.95);
    assert!(prepared.score(&opposite).unwrap() < -0.95);
    assert!(codec.encode(&[0.0; 5]).is_err());
    assert!(codec.prepare_query(&[0.0; 5], 8192).is_err());
    assert!(codec.encode(&[f32::NAN; 5]).is_err());
    assert!(RotatedTwoBitCodec::new(&[], 7).is_err());
    assert!(RotatedTwoBitCodec::new(&[f32::INFINITY], 7).is_err());
}
