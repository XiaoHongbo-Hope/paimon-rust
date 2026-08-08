// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Helpers shared by fixed-bucket postpone batch planning.

use crate::spec::{BinaryRow, DataField, DataType, IntType, VALUE_KIND_FIELD_NAME};
use arrow_array::{
    Array, ArrayRef, BinaryArray, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Return the Java-compatible BinaryRow size, excluding `_VALUE_KIND`.
pub(crate) fn binary_row_batch_size(
    batch: &RecordBatch,
    fields: &[DataField],
) -> crate::Result<i64> {
    let field_count = fields.len();
    let has_value_kind = batch.num_columns() == field_count + 1
        && batch.schema().field(field_count).name() == VALUE_KIND_FIELD_NAME
        && batch.schema().field(field_count).data_type() == &arrow_schema::DataType::Int8;
    if batch.num_columns() != field_count && !has_value_kind {
        return Err(data_invalid(format!(
                "BinaryRow size planning expected {field_count} user columns with an optional trailing {VALUE_KIND_FIELD_NAME}, got {}",
                batch.num_columns()
            )));
    }

    let mut total = 0_u128;
    for row in 0..batch.num_rows() {
        total = total.saturating_add(row_size(&batch.columns()[..field_count], row, fields)?);
    }
    Ok(total.min(i64::MAX as u128) as i64)
}

fn row_size(arrays: &[ArrayRef], row: usize, fields: &[DataField]) -> crate::Result<u128> {
    let mut size = BinaryRow::cal_fix_part_size_in_bytes(fields.len() as i32) as u128;
    for (array, field) in arrays.iter().zip(fields) {
        size = size.saturating_add(variable_size(array, row, field.data_type())?);
    }
    Ok(size)
}

fn variable_size(array: &ArrayRef, row: usize, data_type: &DataType) -> crate::Result<u128> {
    // Java's BinaryRowWriter reserves these fixed variable regions even for a
    // null top-level field.
    match data_type {
        DataType::Decimal(decimal) if decimal.precision() > 18 => return Ok(16),
        DataType::Timestamp(timestamp) if timestamp.precision() > 3 => return Ok(8),
        DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() > 3 => return Ok(8),
        _ => {}
    }

    if array.is_null(row) {
        return Ok(0);
    }

    match data_type {
        DataType::Char(_) | DataType::VarChar(_) => {
            let len = string_len(array, row, data_type)?;
            Ok(binary_size(len))
        }
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => {
            let len = binary_len(array, row, data_type)?;
            Ok(binary_size(len))
        }
        DataType::Variant(_) => variant_size(array, row, data_type),
        DataType::Array(array_type) => {
            let array = downcast::<ListArray>(array, "ListArray", data_type)?;
            let offsets = array.value_offsets();
            Ok(round_to_word(binary_array_size(
                array.values(),
                offsets[row] as usize,
                offsets[row + 1] as usize,
                array_type.element_type(),
            )?))
        }
        DataType::Map(map_type) => {
            let map = downcast::<MapArray>(array, "MapArray", data_type)?;
            let offsets = map.value_offsets();
            let entries = map.entries();
            map_size(
                entries,
                offsets[row] as usize,
                offsets[row + 1] as usize,
                map_type.key_type(),
                map_type.value_type(),
            )
        }
        DataType::Multiset(multiset_type) => {
            let map = downcast::<MapArray>(array, "MapArray", data_type)?;
            let offsets = map.value_offsets();
            let entries = map.entries();
            map_size(
                entries,
                offsets[row] as usize,
                offsets[row + 1] as usize,
                multiset_type.element_type(),
                &DataType::Int(IntType::new()),
            )
        }
        DataType::Row(row_type) => {
            let struct_array = downcast::<StructArray>(array, "StructArray", data_type)?;
            Ok(round_to_word(row_size(
                struct_array.columns(),
                row,
                row_type.fields(),
            )?))
        }
        DataType::Vector(vector_type) => {
            let element_bytes = u128::from(vector_type.length())
                .saturating_mul(primitive_width(vector_type.element_type())?);
            Ok(round_to_word(4_u128.saturating_add(element_bytes)))
        }
        DataType::Boolean(_)
        | DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Float(_)
        | DataType::Double(_)
        | DataType::Date(_)
        | DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Decimal(_) => Ok(0),
    }
}

fn binary_array_size(
    values: &ArrayRef,
    start: usize,
    end: usize,
    element_type: &DataType,
) -> crate::Result<u128> {
    if end < start || end > values.len() {
        return Err(data_invalid(format!(
            "Invalid nested array range [{start}, {end}) for {} values",
            values.len()
        )));
    }
    let count = end - start;
    let header = 4_u128.saturating_add((count as u128).div_ceil(32).saturating_mul(4));
    let fixed = (count as u128).saturating_mul(fixed_width(element_type));
    let mut size = round_to_word(header.saturating_add(fixed));
    for row in start..end {
        if !values.is_null(row) {
            size = size.saturating_add(variable_size(values, row, element_type)?);
        }
    }
    Ok(size)
}

fn map_size(
    entries: &StructArray,
    start: usize,
    end: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> crate::Result<u128> {
    if entries.num_columns() != 2 {
        return Err(data_invalid(format!(
            "BinaryMap size planning expected 2 entry columns, got {}",
            entries.num_columns()
        )));
    }
    let keys = binary_array_size(entries.column(0), start, end, key_type)?;
    let values = binary_array_size(entries.column(1), start, end, value_type)?;
    Ok(round_to_word(
        4_u128.saturating_add(keys).saturating_add(values),
    ))
}

fn variant_size(array: &ArrayRef, row: usize, data_type: &DataType) -> crate::Result<u128> {
    let variant = downcast::<StructArray>(array, "StructArray", data_type)?;
    if variant.num_columns() != 2 {
        return Err(data_invalid(format!(
            "Variant size planning expected 2 child columns, got {}",
            variant.num_columns()
        )));
    }
    let value_len = binary_len(variant.column(0), row, data_type)?;
    let metadata_len = binary_len(variant.column(1), row, data_type)?;
    Ok(round_to_word(
        4_u128
            .saturating_add(value_len as u128)
            .saturating_add(metadata_len as u128),
    ))
}

fn string_len(array: &ArrayRef, row: usize, data_type: &DataType) -> crate::Result<usize> {
    Ok(downcast::<StringArray>(array, "StringArray", data_type)?
        .value(row)
        .len())
}

fn binary_len(array: &ArrayRef, row: usize, data_type: &DataType) -> crate::Result<usize> {
    Ok(downcast::<BinaryArray>(array, "BinaryArray", data_type)?
        .value(row)
        .len())
}

fn fixed_width(data_type: &DataType) -> u128 {
    match data_type {
        DataType::Boolean(_) | DataType::TinyInt(_) => 1,
        DataType::SmallInt(_) => 2,
        DataType::Int(_) | DataType::Float(_) | DataType::Date(_) | DataType::Time(_) => 4,
        DataType::BigInt(_)
        | DataType::Double(_)
        | DataType::Char(_)
        | DataType::VarChar(_)
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob(_)
        | DataType::Variant(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Decimal(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_)
        | DataType::Vector(_) => 8,
    }
}

fn primitive_width(data_type: &DataType) -> crate::Result<u128> {
    match data_type {
        DataType::Boolean(_) | DataType::TinyInt(_) => Ok(1),
        DataType::SmallInt(_) => Ok(2),
        DataType::Int(_) | DataType::Float(_) => Ok(4),
        DataType::BigInt(_) | DataType::Double(_) => Ok(8),
        other => Err(data_invalid(format!(
            "Unsupported vector element type for size planning: {other:?}"
        ))),
    }
}

fn binary_size(len: usize) -> u128 {
    if len <= 7 {
        0
    } else {
        round_to_word(len as u128)
    }
}

fn round_to_word(size: u128) -> u128 {
    size.saturating_add(7) / 8 * 8
}

fn downcast<'a, T: 'static>(
    array: &'a ArrayRef,
    expected: &str,
    data_type: &DataType,
) -> crate::Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| type_mismatch(expected, data_type))
}

fn type_mismatch(expected: &str, data_type: &DataType) -> crate::Error {
    data_invalid(format!(
        "BinaryRow size planning expected {expected} for {data_type:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{ArrayType, IntType, LocalZonedTimestampType, TimestampType, VarCharType};
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, Int8Array, ListArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_java_binary_row_size() {
        let row_count = 1_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("left", ArrowDataType::Utf8, false),
            Field::new("right", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..row_count)),
                Arc::new(StringArray::from(vec!["a"; row_count as usize])),
                Arc::new(StringArray::from(vec!["b"; row_count as usize])),
            ],
        )
        .unwrap();
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "left".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(
                2,
                "right".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];

        assert_eq!(binary_row_batch_size(&batch, &fields).unwrap(), 32_000);
        assert_ne!(batch.get_array_memory_size() as i64, 32_000);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("value", ArrowDataType::Int32, false),
            Field::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(Int8Array::from(vec![0])),
            ],
        )
        .unwrap();
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ];

        assert_eq!(binary_row_batch_size(&batch, &fields).unwrap(), 24);

        let array =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), Some(2)])]);
        let timestamp =
            TimestampMicrosecondArray::from(vec![Some(1_234_567_i64)]).with_timezone("UTC");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("items", array.data_type().clone(), true),
            Field::new(
                "event_time",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(array),
                Arc::new(timestamp),
            ],
        )
        .unwrap();
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "items".to_string(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            DataField::new(
                2,
                "event_time".to_string(),
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
            ),
        ];

        assert_eq!(binary_row_batch_size(&batch, &fields).unwrap(), 56);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![None]))],
        )
        .unwrap();
        let fields = vec![DataField::new(
            0,
            "event_time".to_string(),
            DataType::Timestamp(TimestampType::new(6).unwrap()),
        )];

        assert_eq!(binary_row_batch_size(&batch, &fields).unwrap(), 24);
    }
}
