// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// The nimble@cpp lane: callbacks over upstream Nimble (nimble_lane.cpp) and the sidecar
// contract that runs them (raincloud_nimble_ffi, sidecars/rust/nimble-ffi/src/lib.rs).
#pragma once

#include <cstddef>

#include "velox/vector/arrow/Abi.h"

extern "C" {
int nimble_lane_write(ArrowArrayStream* input, const char* output, char* error, size_t error_len);
int nimble_lane_read(const char* input, ArrowArrayStream* out, char* error, size_t error_len);

typedef int (*nimble_lane_write_fn)(ArrowArrayStream*, const char*, char*, size_t);
typedef int (*nimble_lane_read_fn)(const char*, ArrowArrayStream*, char*, size_t);
int raincloud_nimble_write_main(int argc, const char* const* argv, nimble_lane_write_fn write,
                                nimble_lane_read_fn read);
int raincloud_nimble_read_main(int argc, const char* const* argv, nimble_lane_read_fn read);
}
