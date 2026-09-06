//! Query-independent, bounded construction primitives for the V35 format.

use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{Array, FixedSizeListArray, Float64Array, RecordBatch, UInt16Array};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const MORTON_COORDINATES: usize = 16;
const MORTON_BOUNDARIES: usize = 255;
const MORTON_FORMAT: &str = "borsuk-v35-morton-model-arrow-v1";
const MORTON_METADATA_KEY: &str = "borsuk.v35.morton-model.manifest";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V35MortonManifest {
    format: String,
    source_dimensions: u32,
}

fn morton_schema(source_dimensions: usize) -> Result<Arc<Schema>> {
    let manifest = V35MortonManifest {
        format: MORTON_FORMAT.to_owned(),
        source_dimensions: u32::try_from(source_dimensions)
            .map_err(|_| invalid("V35 Morton source dimensions overflow"))?,
    };
    let manifest = serde_json::to_string(&manifest)
        .map_err(|_| invalid("V35 Morton manifest cannot be serialized"))?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("coordinate", DataType::UInt16, false),
            Field::new(
                "boundaries",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float64, false)),
                    MORTON_BOUNDARIES as i32,
                ),
                false,
            ),
        ],
        HashMap::from([(MORTON_METADATA_KEY.to_owned(), manifest)]),
    )))
}

#[derive(Debug, Clone, PartialEq)]
/// Frozen sixteen-coordinate quantile model for a 128-bit Morton build key.
pub struct V35MortonModel {
    selected_coordinates: Vec<u16>,
    boundaries: Vec<Vec<f64>>,
    source_dimensions: usize,
}

impl V35MortonModel {
    /// Selected projected coordinates in decreasing population-variance order.
    pub fn selected_coordinates(&self) -> &[u16] {
        &self.selected_coordinates
    }

    /// The 255 increasing quantile boundaries for one selected-coordinate slot.
    pub fn boundaries(&self, selected_slot: usize) -> Option<&[f64]> {
        self.boundaries.get(selected_slot).map(Vec::as_slice)
    }

    /// Compute the exact most-significant-bit-first interleaved Morton key.
    pub fn key(&self, projected_row: &[f64]) -> Result<u128> {
        if projected_row.len() != self.source_dimensions
            || projected_row.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V35 Morton source row differs"));
        }
        let buckets = self
            .selected_coordinates
            .iter()
            .zip(&self.boundaries)
            .map(|(coordinate, boundaries)| {
                let value = projected_row[usize::from(*coordinate)];
                u8::try_from(boundaries.partition_point(|boundary| *boundary < value))
                    .expect("255 boundaries fit one byte")
            })
            .collect::<Vec<_>>();
        let mut key = 0_u128;
        for bit in (0..8).rev() {
            for bucket in &buckets {
                key = (key << 1) | u128::from((bucket >> bit) & 1);
            }
        }
        Ok(key)
    }

    /// Encode the model as strict cross-language Arrow IPC.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        validate_model(self)?;
        let schema = morton_schema(self.source_dimensions)?;
        let values = self
            .boundaries
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let boundaries = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float64, false)),
            MORTON_BOUNDARIES as i32,
            Arc::new(Float64Array::from(values)),
            None,
        )?;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt16Array::from(self.selected_coordinates.clone())),
                Arc::new(boundaries),
            ],
        )?;
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut bytes = Vec::new();
        let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        Ok(bytes)
    }

    /// Authenticate and decode the one strict Arrow IPC representation.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
        let schema = reader.schema();
        let manifest_json = schema
            .metadata()
            .get(MORTON_METADATA_KEY)
            .ok_or_else(|| invalid("V35 Morton manifest is missing"))?;
        let manifest: V35MortonManifest = serde_json::from_str(manifest_json)
            .map_err(|_| invalid("V35 Morton manifest differs"))?;
        if schema.metadata().len() != 1
            || serde_json::to_string(&manifest)
                .map_err(|_| invalid("V35 Morton manifest cannot be serialized"))?
                != *manifest_json
            || manifest.format != MORTON_FORMAT
            || manifest.source_dimensions < MORTON_COORDINATES as u32
            || reader.num_batches() != 1
            || schema.as_ref() != morton_schema(manifest.source_dimensions as usize)?.as_ref()
        {
            return Err(invalid("V35 Morton model Arrow authority differs"));
        }
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| invalid("V35 Morton model Arrow batch is missing"))?;
        if reader.next().is_some() || batch.num_rows() != MORTON_COORDINATES {
            return Err(invalid("V35 Morton model Arrow rows differ"));
        }
        let coordinates = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("V35 Morton coordinate column differs"))?;
        let boundary_rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V35 Morton boundary column differs"))?;
        let values = boundary_rows
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("V35 Morton boundary values differ"))?;
        if coordinates.null_count() != 0
            || boundary_rows.null_count() != 0
            || values.null_count() != 0
        {
            return Err(invalid("V35 Morton model nullability differs"));
        }
        let selected_coordinates = coordinates.values().to_vec();
        let boundaries = values
            .values()
            .as_chunks::<MORTON_BOUNDARIES>()
            .0
            .iter()
            .map(|row| row.to_vec())
            .collect::<Vec<_>>();
        let source_dimensions = manifest.source_dimensions as usize;
        let model = Self {
            selected_coordinates,
            boundaries,
            source_dimensions,
        };
        validate_model(&model)?;
        if model.canonical_bytes()? != bytes {
            return Err(invalid("V35 Morton model bytes are noncanonical"));
        }
        Ok(model)
    }
}

fn validate_model(model: &V35MortonModel) -> Result<()> {
    let mut unique = model.selected_coordinates.clone();
    unique.sort_unstable();
    unique.dedup();
    if model.selected_coordinates.len() != MORTON_COORDINATES
        || unique.len() != MORTON_COORDINATES
        || model.boundaries.len() != MORTON_COORDINATES
        || model.source_dimensions < MORTON_COORDINATES
        || model
            .selected_coordinates
            .iter()
            .any(|coordinate| usize::from(*coordinate) >= model.source_dimensions)
        || model.boundaries.iter().any(|boundaries| {
            boundaries.len() != MORTON_BOUNDARIES
                || boundaries.iter().any(|value| !value.is_finite())
                || boundaries.windows(2).any(|pair| pair[0] > pair[1])
        })
    {
        return Err(invalid("V35 Morton model authority differs"));
    }
    Ok(())
}

/// Train a deterministic query-independent Morton model from projected sample rows.
pub fn train_v35_morton_model(projected_rows: &[Vec<f64>]) -> Result<V35MortonModel> {
    let dimensions = projected_rows.first().map_or(0, Vec::len);
    if projected_rows.len() < 256
        || dimensions < MORTON_COORDINATES
        || dimensions > usize::from(u16::MAX) + 1
        || projected_rows
            .iter()
            .any(|row| row.len() != dimensions || row.iter().any(|value| !value.is_finite()))
    {
        return Err(invalid("V35 Morton training sample differs"));
    }
    let count = projected_rows.len() as f64;
    let mut ranked = (0..dimensions)
        .map(|coordinate| {
            let mean = projected_rows
                .iter()
                .fold(0.0, |sum, row| sum + row[coordinate])
                / count;
            let variance = projected_rows.iter().fold(0.0, |sum, row| {
                let delta = row[coordinate] - mean;
                delta.mul_add(delta, sum)
            }) / count;
            (variance, coordinate)
        })
        .collect::<Vec<_>>();
    if ranked.iter().any(|(variance, _)| !variance.is_finite()) {
        return Err(invalid("V35 Morton training variance is nonfinite"));
    }
    ranked.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
    let selected_coordinates = ranked
        .iter()
        .take(MORTON_COORDINATES)
        .map(|(_, coordinate)| *coordinate as u16)
        .collect::<Vec<_>>();
    let boundaries = selected_coordinates
        .iter()
        .map(|coordinate| {
            let mut values = projected_rows
                .iter()
                .map(|row| row[usize::from(*coordinate)])
                .collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            (1..=MORTON_BOUNDARIES)
                .map(|quantile| {
                    let rank = quantile
                        .checked_mul(values.len())
                        .expect("bounded sample rank")
                        .div_ceil(256)
                        - 1;
                    values[rank]
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let model = V35MortonModel {
        selected_coordinates,
        boundaries,
        source_dimensions: dimensions,
    };
    validate_model(&model)?;
    Ok(model)
}
