// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! C ABI ownership: each successful open/string/stream has exactly one release.
//! Callers must pass valid pointers, exclusively own mutable outputs, and never
//! reuse released handles. Panics at exported function boundaries become errors.
use crate::{Dataset, Error, ErrorKind, Result};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use std::{
    ffi::{c_char, CStr, CString},
    panic::{catch_unwind, AssertUnwindSafe},
    ptr,
};

#[repr(C)]
pub struct RcError {
    pub code: i32,
    pub message: *mut c_char,
}
fn run(error: *mut RcError, f: impl FnOnce() -> Result<()>) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(Error::new(ErrorKind::Internal, "native reader panicked")));
    match result {
        Ok(()) => 0,
        Err(e) => {
            if !error.is_null() {
                unsafe {
                    (*error).code = e.kind as i32;
                    (*error).message = CString::new(e.message.replace('\0', "\\0"))
                        .unwrap()
                        .into_raw();
                }
            }
            e.kind as i32
        }
    }
}
unsafe fn text<'a>(p: *const c_char) -> Result<&'a str> {
    if p.is_null() {
        return Err(Error::new(ErrorKind::InvalidArgument, "null string"));
    }
    CStr::from_ptr(p)
        .to_str()
        .map_err(|e| Error::new(ErrorKind::InvalidArgument, e))
}
unsafe fn ds<'a>(p: *const Dataset) -> Result<&'a Dataset> {
    p.as_ref()
        .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "null dataset"))
}
#[no_mangle]
pub extern "C" fn raincloud_abi_version() -> u32 {
    1
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_open(
    options: *const c_char,
    slug: *const c_char,
    format: *const c_char,
    out: *mut *mut Dataset,
    error: *mut RcError,
) -> i32 {
    run(error, || {
        if out.is_null() {
            return Err(Error::new(ErrorKind::InvalidArgument, "null output"));
        }
        *out = ptr::null_mut();
        let options: serde_json::Value = serde_json::from_str(if options.is_null() {
            "{}"
        } else {
            text(options)?
        })?;
        let value = Dataset::load(
            &options,
            text(slug)?,
            if format.is_null() {
                "auto"
            } else {
                text(format)?
            },
        )?;
        *out = Box::into_raw(Box::new(value));
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_metadata(
    handle: *const Dataset,
    out: *mut *mut c_char,
    error: *mut RcError,
) -> i32 {
    run(error, || {
        if out.is_null() {
            return Err(Error::new(ErrorKind::InvalidArgument, "null output"));
        }
        *out = ptr::null_mut();
        let value = serde_json::to_string(ds(handle)?.metadata())?;
        *out = CString::new(value).unwrap().into_raw();
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_path(
    handle: *const Dataset,
    out: *mut *mut c_char,
    error: *mut RcError,
) -> i32 {
    run(error, || {
        if out.is_null() {
            return Err(Error::new(ErrorKind::InvalidArgument, "null output"));
        }
        *out = ptr::null_mut();
        let path = ds(handle)?.path()?;
        *out = CString::new(
            path.to_str()
                .ok_or_else(|| Error::new(ErrorKind::InvalidArgument, "path is not UTF-8"))?,
        )
        .map_err(|e| Error::new(ErrorKind::InvalidArgument, e))?
        .into_raw();
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_batches(
    handle: *const Dataset,
    batch_size: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut RcError,
) -> i32 {
    run(error, || {
        if out.is_null() {
            return Err(Error::new(ErrorKind::InvalidArgument, "null stream output"));
        }
        // out must be uninitialized/released storage, never an existing live stream.
        ptr::write(out, FFI_ArrowArrayStream::empty());
        let reader = ds(handle)?.batches(batch_size)?;
        ptr::write(out, FFI_ArrowArrayStream::new(Box::new(reader)));
        Ok(())
    })
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_close(handle: *mut Dataset) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_string_free(value: *mut c_char) {
    if !value.is_null() {
        drop(CString::from_raw(value));
    }
}
#[no_mangle]
pub unsafe extern "C" fn raincloud_error_free(error: *mut RcError) {
    if let Some(e) = error.as_mut() {
        raincloud_string_free(e.message);
        e.message = ptr::null_mut();
        e.code = 0;
    }
}
