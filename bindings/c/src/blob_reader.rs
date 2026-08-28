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

use std::collections::HashMap;
use std::ffi::c_void;

use paimon::BlobReader;

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{paimon_result_blob_reader, paimon_result_read_blobs};
use crate::runtime;
use crate::types::{paimon_blob_reader, paimon_byte_slice, paimon_bytes_array, paimon_option};

fn read_error(error: *mut paimon_error) -> paimon_result_read_blobs {
    paimon_result_read_blobs {
        blobs: paimon_bytes_array::empty(),
        error,
    }
}

/// Create a standalone BlobDescriptor reader.
///
/// # Safety
/// `options` must point to `options_len` valid `paimon_option` values. Keys and
/// values must be null-terminated UTF-8 strings and are borrowed for this call.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_new(
    options: *const paimon_option,
    options_len: usize,
) -> paimon_result_blob_reader {
    if options_len > 0 && options.is_null() {
        return paimon_result_blob_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null pointer passed for `options`".to_string(),
            ),
        };
    }

    let mut storage_options = HashMap::with_capacity(options_len);
    if options_len > 0 {
        for option in std::slice::from_raw_parts(options, options_len) {
            let key = match validate_cstr(option.key, "option key") {
                Ok(value) => value,
                Err(error) => {
                    return paimon_result_blob_reader {
                        reader: std::ptr::null_mut(),
                        error,
                    };
                }
            };
            let value = match validate_cstr(option.value, "option value") {
                Ok(value) => value,
                Err(error) => {
                    return paimon_result_blob_reader {
                        reader: std::ptr::null_mut(),
                        error,
                    };
                }
            };
            storage_options.insert(key, value);
        }
    }

    let reader = Box::new(BlobReader::new(storage_options));
    let wrapper = Box::new(paimon_blob_reader {
        inner: Box::into_raw(reader) as *mut c_void,
    });
    paimon_result_blob_reader {
        reader: Box::into_raw(wrapper),
        error: std::ptr::null_mut(),
    }
}

/// Read serialized BlobDescriptors in one batch, preserving input order.
///
/// Input buffers are borrowed for this call. The returned buffers remain owned
/// by the caller until released with `paimon_bytes_array_free`.
///
/// # Safety
/// `reader` must be returned by `paimon_blob_reader_new`. `descriptors` must
/// point to `descriptors_len` valid byte slices whose data remains alive for
/// this call.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_read_blobs(
    reader: *const paimon_blob_reader,
    descriptors: *const paimon_byte_slice,
    descriptors_len: usize,
) -> paimon_result_read_blobs {
    if let Err(error) = check_non_null(reader, "blob reader") {
        return read_error(error);
    }
    if descriptors_len > 0 && descriptors.is_null() {
        return read_error(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null pointer passed for `descriptors`".to_string(),
        ));
    }

    let mut owned = Vec::with_capacity(descriptors_len);
    if descriptors_len > 0 {
        for (index, descriptor) in std::slice::from_raw_parts(descriptors, descriptors_len)
            .iter()
            .enumerate()
        {
            if descriptor.len > 0 && descriptor.data.is_null() {
                return read_error(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!(
                        "null data pointer for BlobDescriptor input index {index}, URI unavailable"
                    ),
                ));
            }
            let bytes = if descriptor.len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(descriptor.data, descriptor.len)
            };
            owned.push(bytes.to_vec());
        }
    }

    let reader = &*((*reader).inner as *const BlobReader);
    match runtime().block_on(reader.read_blobs(&owned)) {
        Ok(values) => paimon_result_read_blobs {
            blobs: paimon_bytes_array::new(values),
            error: std::ptr::null_mut(),
        },
        Err(error) => read_error(paimon_error::from_paimon(error)),
    }
}

/// Free a standalone BlobDescriptor reader.
///
/// # Safety
/// Only call with a reader returned by `paimon_blob_reader_new`.
#[no_mangle]
pub unsafe extern "C" fn paimon_blob_reader_free(reader: *mut paimon_blob_reader) {
    if reader.is_null() {
        return;
    }
    let reader = Box::from_raw(reader);
    if !reader.inner.is_null() {
        drop(Box::from_raw(reader.inner as *mut BlobReader));
    }
}
