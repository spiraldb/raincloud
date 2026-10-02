// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// raincloud-nimble: Arrow <-> Nimble through upstream Nimble's own Velox writer and reader.
//
//   raincloud-nimble write <out.nimble>   an Arrow IPC stream on stdin  -> a Nimble file
//   raincloud-nimble read <in.nimble>     a Nimble file -> an Arrow IPC stream on stdout
//
// A pure codec: the `nimble@cpp` lane's sidecar binaries (`sidecars/rust`, `nimble-write` and
// `nimble-read`) read the canonical, stream it in, read the file back out and compare, and own
// the report. Arrow crosses into Velox through the C data interface (nanoarrow for the IPC
// stream, Velox's Arrow bridge for the vectors).
//
// Writing uses `nimble::VeloxWriter` with default `VeloxWriterOptions`; reading uses
// `nimble::VeloxReader` with no selector, so the file is read as the type it records. Neither
// side converts a column: a type Velox's bridge or Nimble does not take is an error, printed to
// stderr with exit status 1.

#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/writer/VeloxWriter.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace velox = facebook::velox;
namespace nimble = facebook::nimble;

namespace {

// Rows per batch the reader returns. Batch boundaries are not compared.
constexpr uint64_t kReadBatchRows = 64 * 1024;

void check(ArrowErrorCode code, const ArrowError& error, const char* what) {
  if (code != NANOARROW_OK) {
    throw std::runtime_error(std::string(what) + ": " + error.message);
  }
}

// Releases a C data interface struct when it goes out of scope, unless something took it.
template <typename T>
struct Released {
  T value{};
  ~Released() {
    if (value.release != nullptr) value.release(&value);
  }
};

struct Pools {
  std::shared_ptr<velox::memory::MemoryPool> root;
  std::shared_ptr<velox::memory::MemoryPool> leaf;
};

int write(const std::string& output, const Pools& pools) {
  ArrowError error{};
  ArrowIpcInputStream input{};
  check(ArrowIpcInputStreamInitFile(&input, stdin, /*close_on_release=*/0), error, "stdin");
  Released<ArrowArrayStream> stream;
  check(ArrowIpcArrayStreamReaderInit(&stream.value, &input, nullptr), error, "Arrow IPC stream");

  Released<ArrowSchema> schema;
  if (stream.value.get_schema(&stream.value, &schema.value) != 0) {
    throw std::runtime_error(std::string("Arrow IPC schema: ") +
                             stream.value.get_last_error(&stream.value));
  }
  const auto type = velox::importFromArrow(schema.value);
  if (!type->isRow()) throw std::runtime_error("the stream's schema is not a struct: " + type->toString());

  nimble::VeloxWriter writer(
      type, std::make_unique<velox::LocalWriteFile>(output, false, /*shouldThrowOnFileAlreadyExists=*/false),
      *pools.root, nimble::VeloxWriterOptions{});
  while (true) {
    Released<ArrowArray> array;
    if (stream.value.get_next(&stream.value, &array.value) != 0) {
      throw std::runtime_error(std::string("Arrow IPC batch: ") +
                               stream.value.get_last_error(&stream.value));
    }
    if (array.value.release == nullptr) break;  // end of stream
    // The import takes the schema as well as the array: each batch gets its own copy.
    Released<ArrowSchema> owned;
    check(ArrowSchemaDeepCopy(&schema.value, &owned.value), error, "copy the schema");
    writer.write(velox::importFromArrowAsOwner(owned.value, array.value, pools.leaf.get()));
  }
  writer.close();
  return 0;
}

int read(const std::string& input, const Pools& pools) {
  velox::LocalReadFile file(input);
  nimble::VeloxReader reader(&file, *pools.leaf);

  ArrowError error{};
  ArrowIpcOutputStream output{};
  check(ArrowIpcOutputStreamInitFile(&output, stdout, /*close_on_release=*/0), error, "stdout");
  struct Writer {
    ArrowIpcWriter value{};
    ~Writer() { ArrowIpcWriterReset(&value); }
  } writer;
  check(ArrowIpcWriterInit(&writer.value, &output), error, "Arrow IPC writer");

  // The schema the reader reads the file as, from an empty vector of its type.
  Released<ArrowSchema> schema;
  velox::exportToArrow(velox::BaseVector::create(reader.type(), 0, pools.leaf.get()), schema.value);
  check(ArrowIpcWriterWriteSchema(&writer.value, &schema.value, &error), error, "write the schema");

  velox::VectorPtr batch;
  while (reader.next(kReadBatchRows, batch)) {
    Released<ArrowArray> array;
    velox::exportToArrow(batch, array.value, pools.leaf.get());
    struct View {
      ArrowArrayView value{};
      ~View() { ArrowArrayViewReset(&value); }
    } view;
    check(ArrowArrayViewInitFromSchema(&view.value, &schema.value, &error), error, "array view");
    check(ArrowArrayViewSetArray(&view.value, &array.value, &error), error, "array view");
    check(ArrowIpcWriterWriteArrayView(&writer.value, &view.value, &error), error, "write a batch");
  }
  check(ArrowIpcWriterWriteArrayView(&writer.value, nullptr, &error), error, "end the stream");
  std::fflush(stdout);
  return 0;
}

int usage() {
  std::cerr << "usage: raincloud-nimble write <out.nimble>   (an Arrow IPC stream on stdin)\n"
               "       raincloud-nimble read <in.nimble>     (an Arrow IPC stream on stdout)\n";
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) return usage();
  const std::string command = argv[1];
  if (command != "write" && command != "read") return usage();
  try {
    velox::memory::initializeMemoryManager(velox::memory::MemoryManager::Options{});
    Pools pools;
    pools.root = velox::memory::memoryManager()->addRootPool("raincloud-nimble");
    pools.leaf = pools.root->addLeafChild("leaf");
    return command == "write" ? write(argv[2], pools) : read(argv[2], pools);
  } catch (const velox::VeloxException& e) {
    // Its reason; what() adds the source, code, context and a stack trace.
    std::cerr << "raincloud-nimble " << command << ": " << e.message() << "\n";
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "raincloud-nimble " << command << ": " << e.what() << "\n";
    return 1;
  }
}
