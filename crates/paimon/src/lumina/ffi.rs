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

use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::ffi::{c_char, c_float, c_int, c_void, CStr, CString};
use std::io::{Read, Seek, SeekFrom};
use std::sync::OnceLock;

const ERR_BUF_SIZE: usize = 4096;

static LIBRARY: OnceLock<Library> = OnceLock::new();

fn load_library() -> crate::Result<&'static Library> {
    if let Some(library) = LIBRARY.get() {
        return Ok(library);
    }

    let lib_path = std::env::var("LUMINA_LIB_PATH").unwrap_or_else(|_| {
        if cfg!(target_os = "macos") {
            "liblumina_py.dylib".to_string()
        } else {
            "liblumina_py.so".to_string()
        }
    });

    let library = unsafe {
        Library::new(&lib_path).map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to load lumina library from '{}': {}", lib_path, e),
            source: None,
        })?
    };

    let _ = LIBRARY.set(library);
    Ok(LIBRARY.get().expect("lumina library should be initialized"))
}

#[cfg(test)]
mod tests {
    use super::load_library;

    #[test]
    fn test_load_library_failure_does_not_cache() {
        // SAFETY: This test runs in isolation; env var mutation is acceptable here.
        unsafe {
            std::env::set_var(
                "LUMINA_LIB_PATH",
                "/path/that/does/not/exist/liblumina_missing.dylib",
            );
        }
        assert!(load_library().is_err());
        unsafe {
            std::env::set_var(
                "LUMINA_LIB_PATH",
                "/another/missing/liblumina_missing.dylib",
            );
        }
        assert!(load_library().is_err());
        unsafe {
            std::env::remove_var("LUMINA_LIB_PATH");
        }
    }
}

fn check_error(ret: c_int, err_buf: &[u8; ERR_BUF_SIZE]) -> crate::Result<()> {
    if ret != 0 {
        let c_str = unsafe { CStr::from_ptr(err_buf.as_ptr() as *const c_char) };
        let msg = c_str.to_string_lossy().to_string();
        return Err(crate::Error::DataInvalid {
            message: format!("Lumina error: {}", msg),
            source: None,
        });
    }
    Ok(())
}

fn options_to_json(options: &HashMap<String, String>) -> crate::Result<CString> {
    let json = serde_json::to_string(options).map_err(|e| crate::Error::DataInvalid {
        message: format!("Failed to serialize options: {}", e),
        source: None,
    })?;
    CString::new(json).map_err(|e| crate::Error::DataInvalid {
        message: format!("Failed to create CString: {}", e),
        source: None,
    })
}

pub struct LuminaSearcher {
    handle: *mut c_void,
    _stream_ctx: Option<Box<StreamContext>>,
}

// SAFETY: The liblumina C API handles are not tied to a specific thread.
// Each LuminaSearcher owns its handle exclusively and the C library
// uses internal locking for thread safety. The handle is only accessed
// through &self or &mut self, so no data races can occur.
unsafe impl Send for LuminaSearcher {}

impl LuminaSearcher {
    pub fn create(options: &HashMap<String, String>) -> crate::Result<Self> {
        let lib = load_library()?;
        let opts_json = options_to_json(options)?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let handle: *mut c_void = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(*const c_char, *mut c_char, c_int) -> *mut c_void,
            > = lib
                .get(b"lumina_searcher_create")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_searcher_create not found: {}", e),
                    source: None,
                })?;
            func(
                opts_json.as_ptr(),
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        if handle.is_null() {
            let c_str = unsafe { CStr::from_ptr(err_buf.as_ptr() as *const c_char) };
            let msg = c_str.to_string_lossy().to_string();
            return Err(crate::Error::DataInvalid {
                message: format!("Failed to create Lumina searcher: {}", msg),
                source: None,
            });
        }

        Ok(Self {
            handle,
            _stream_ctx: None,
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn open_stream<S: Read + Seek + Send + 'static>(&mut self, stream: S) -> crate::Result<()> {
        let lib = load_library()?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let ctx = Box::new(StreamContext::new(stream));
        let ctx_ptr = &*ctx as *const StreamContext as *mut c_void;

        let ret: c_int = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    *mut c_void,
                    unsafe extern "C" fn(*mut c_void, *mut c_char, u64) -> c_int,
                    unsafe extern "C" fn(*mut c_void, u64) -> c_int,
                    unsafe extern "C" fn(*mut c_void) -> u64,
                    unsafe extern "C" fn(*mut c_void) -> u64,
                    *mut c_char,
                    c_int,
                ) -> c_int,
            > = lib
                .get(b"lumina_searcher_open_stream")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_searcher_open_stream not found: {}", e),
                    source: None,
                })?;
            func(
                self.handle,
                ctx_ptr,
                stream_read_cb,
                stream_seek_cb,
                stream_tell_cb,
                stream_length_cb,
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        check_error(ret, &err_buf)?;
        self._stream_ctx = Some(ctx);
        Ok(())
    }

    pub fn search(
        &self,
        query: &[f32],
        n: i32,
        k: i32,
        distances: &mut [f32],
        labels: &mut [u64],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let lib = load_library()?;
        let opts_json = options_to_json(options)?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let ret: c_int = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    *const c_float,
                    c_int,
                    c_int,
                    *mut c_float,
                    *mut u64,
                    *const c_char,
                    *mut c_char,
                    c_int,
                ) -> c_int,
            > = lib
                .get(b"lumina_searcher_search")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_searcher_search not found: {}", e),
                    source: None,
                })?;
            func(
                self.handle,
                query.as_ptr(),
                n,
                k,
                distances.as_mut_ptr(),
                labels.as_mut_ptr(),
                opts_json.as_ptr(),
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        check_error(ret, &err_buf)
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub fn search_with_filter(
        &self,
        query: &[f32],
        n: i32,
        k: i32,
        distances: &mut [f32],
        labels: &mut [u64],
        filter_ids: &[u64],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let lib = load_library()?;
        let opts_json = options_to_json(options)?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let ret: c_int = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    *const c_float,
                    c_int,
                    c_int,
                    *mut c_float,
                    *mut u64,
                    *const u64,
                    u64,
                    *const c_char,
                    *mut c_char,
                    c_int,
                ) -> c_int,
            > = lib
                .get(b"lumina_searcher_search_with_filter")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_searcher_search_with_filter not found: {}", e),
                    source: None,
                })?;
            func(
                self.handle,
                query.as_ptr(),
                n,
                k,
                distances.as_mut_ptr(),
                labels.as_mut_ptr(),
                filter_ids.as_ptr(),
                filter_ids.len() as u64,
                opts_json.as_ptr(),
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        check_error(ret, &err_buf)
    }

    pub fn get_count(&self) -> crate::Result<u64> {
        let lib = load_library()?;
        unsafe {
            let func: Symbol<unsafe extern "C" fn(*mut c_void) -> u64> = lib
                .get(b"lumina_searcher_get_count")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_searcher_get_count not found: {}", e),
                    source: None,
                })?;
            Ok(func(self.handle))
        }
    }

    pub fn get_dimension(&self) -> crate::Result<u32> {
        let lib = load_library()?;
        unsafe {
            let func: Symbol<unsafe extern "C" fn(*mut c_void) -> u32> = lib
                .get(b"lumina_searcher_get_dimension")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_searcher_get_dimension not found: {}", e),
                    source: None,
                })?;
            Ok(func(self.handle))
        }
    }
}

impl Drop for LuminaSearcher {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            if let Ok(lib) = load_library() {
                unsafe {
                    if let Ok(func) =
                        lib.get::<unsafe extern "C" fn(*mut c_void)>(b"lumina_searcher_destroy")
                    {
                        func(self.handle);
                    }
                }
            }
            self.handle = std::ptr::null_mut();
        }
    }
}

pub struct LuminaBuilder {
    handle: *mut c_void,
}

// SAFETY: Same as LuminaSearcher — the builder handle is exclusively owned
// and the C library is thread-safe for independent handles.
unsafe impl Send for LuminaBuilder {}

impl LuminaBuilder {
    pub fn create(options: &HashMap<String, String>) -> crate::Result<Self> {
        let lib = load_library()?;
        let opts_json = options_to_json(options)?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let handle: *mut c_void = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(*const c_char, *mut c_char, c_int) -> *mut c_void,
            > = lib
                .get(b"lumina_builder_create")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_builder_create not found: {}", e),
                    source: None,
                })?;
            func(
                opts_json.as_ptr(),
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        if handle.is_null() {
            let c_str = unsafe { CStr::from_ptr(err_buf.as_ptr() as *const c_char) };
            let msg = c_str.to_string_lossy().to_string();
            return Err(crate::Error::DataInvalid {
                message: format!("Failed to create Lumina builder: {}", msg),
                source: None,
            });
        }

        Ok(Self { handle })
    }

    pub fn pretrain(&self, vectors: &[f32], n: i32, dim: i32) -> crate::Result<()> {
        let lib = load_library()?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let ret: c_int = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    *const c_float,
                    c_int,
                    c_int,
                    *mut c_char,
                    c_int,
                ) -> c_int,
            > = lib
                .get(b"lumina_builder_pretrain")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_builder_pretrain not found: {}", e),
                    source: None,
                })?;
            func(
                self.handle,
                vectors.as_ptr(),
                n,
                dim,
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        check_error(ret, &err_buf)
    }

    pub fn insert(&self, vectors: &[f32], ids: &[u64], n: i32, dim: i32) -> crate::Result<()> {
        let lib = load_library()?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let ret: c_int = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    *const c_float,
                    *const u64,
                    c_int,
                    c_int,
                    *mut c_char,
                    c_int,
                ) -> c_int,
            > = lib
                .get(b"lumina_builder_insert")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_builder_insert not found: {}", e),
                    source: None,
                })?;
            func(
                self.handle,
                vectors.as_ptr(),
                ids.as_ptr(),
                n,
                dim,
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        check_error(ret, &err_buf)
    }

    pub fn dump(&self, path: &str) -> crate::Result<()> {
        let lib = load_library()?;
        let c_path = CString::new(path).map_err(|e| crate::Error::DataInvalid {
            message: format!("Invalid path: {}", e),
            source: None,
        })?;
        let mut err_buf = [0u8; ERR_BUF_SIZE];

        let ret: c_int = unsafe {
            let func: Symbol<
                unsafe extern "C" fn(*mut c_void, *const c_char, *mut c_char, c_int) -> c_int,
            > = lib
                .get(b"lumina_builder_dump")
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Symbol lumina_builder_dump not found: {}", e),
                    source: None,
                })?;
            func(
                self.handle,
                c_path.as_ptr(),
                err_buf.as_mut_ptr() as *mut c_char,
                ERR_BUF_SIZE as c_int,
            )
        };

        check_error(ret, &err_buf)
    }
}

impl Drop for LuminaBuilder {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            if let Ok(lib) = load_library() {
                unsafe {
                    if let Ok(func) =
                        lib.get::<unsafe extern "C" fn(*mut c_void)>(b"lumina_builder_destroy")
                    {
                        func(self.handle);
                    }
                }
            }
            self.handle = std::ptr::null_mut();
        }
    }
}

struct StreamContext {
    inner: std::sync::Mutex<Box<dyn ReadSeekLen + Send>>,
}

trait ReadSeekLen: Read + Seek {
    fn length(&self) -> u64;
}

struct ReadSeekLenImpl<S: Read + Seek + Send> {
    stream: S,
    len: u64,
}

impl<S: Read + Seek + Send> ReadSeekLenImpl<S> {
    fn new(mut stream: S) -> Self {
        let len = stream.seek(SeekFrom::End(0)).unwrap_or(0);
        let _ = stream.seek(SeekFrom::Start(0));
        Self { stream, len }
    }
}

impl<S: Read + Seek + Send> Read for ReadSeekLenImpl<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<S: Read + Seek + Send> Seek for ReadSeekLenImpl<S> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.stream.seek(pos)
    }
}

impl<S: Read + Seek + Send> ReadSeekLen for ReadSeekLenImpl<S> {
    fn length(&self) -> u64 {
        self.len
    }
}

impl StreamContext {
    fn new<S: Read + Seek + Send + 'static>(stream: S) -> Self {
        Self {
            inner: std::sync::Mutex::new(Box::new(ReadSeekLenImpl::new(stream))),
        }
    }
}

unsafe extern "C" fn stream_read_cb(ctx: *mut c_void, buf: *mut c_char, size: u64) -> c_int {
    let ctx = &*(ctx as *const StreamContext);
    let mut guard = match ctx.inner.lock() {
        Ok(g) => g,
        Err(_) => return -1,
    };
    let slice = std::slice::from_raw_parts_mut(buf as *mut u8, size as usize);
    let mut total_read = 0usize;
    while total_read < size as usize {
        match guard.read(&mut slice[total_read..]) {
            Ok(0) => break,
            Ok(n) => total_read += n,
            Err(_) => return -1,
        }
    }
    std::cmp::min(total_read, c_int::MAX as usize) as c_int
}

unsafe extern "C" fn stream_seek_cb(ctx: *mut c_void, position: u64) -> c_int {
    let ctx = &*(ctx as *const StreamContext);
    let mut guard = match ctx.inner.lock() {
        Ok(g) => g,
        Err(_) => return -1,
    };
    match guard.seek(SeekFrom::Start(position)) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

unsafe extern "C" fn stream_tell_cb(ctx: *mut c_void) -> u64 {
    let ctx = &*(ctx as *const StreamContext);
    let mut guard = match ctx.inner.lock() {
        Ok(g) => g,
        Err(_) => return 0,
    };
    guard.seek(SeekFrom::Current(0)).unwrap_or(0)
}

unsafe extern "C" fn stream_length_cb(ctx: *mut c_void) -> u64 {
    let ctx = &*(ctx as *const StreamContext);
    let guard = match ctx.inner.lock() {
        Ok(g) => g,
        Err(_) => return 0,
    };
    guard.length()
}
