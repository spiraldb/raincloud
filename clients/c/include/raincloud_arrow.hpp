// SPDX-License-Identifier: Apache-2.0
#ifndef RAINCLOUD_ARROW_HPP
#define RAINCLOUD_ARROW_HPP
#include "raincloud.hpp"
#include <arrow/c/bridge.h>
namespace raincloud {
/* Requires Arrow C++ only in the consuming application; Raincloud's shared
 * library exposes a C ABI and does not link against that Arrow C++ version.
 * Resolving the artifact (an offline miss, a checksum mismatch, ...) throws
 * raincloud::Error with its code, as the handle operations do; the returned
 * Result and reader->Next() carry Arrow's own status for the stream. */
inline arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> batches(const Dataset& ds, size_t batch_size=65536) {
  ArrowArrayStream stream{}; raincloud_error error{};
  check(raincloud_batches(ds.get(),batch_size,&stream,&error),error);
  auto reader=arrow::ImportRecordBatchReader(&stream);
  if(stream.release) stream.release(&stream);
  return reader;
}
}
#endif
