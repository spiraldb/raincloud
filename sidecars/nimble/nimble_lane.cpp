// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// The nimble@cpp lane's two callbacks over upstream Nimble, for the sidecar contract the
// Rust library `raincloud_nimble_ffi` (sidecars/rust/nimble-ffi) runs: write an Arrow C
// stream to a Nimble file with `nimble::VeloxWriter` at default options, and read a Nimble
// file back as an Arrow C stream with `nimble::VeloxReader`, as the type the file records.
// Arrow crosses into Velox through Velox's own Arrow bridge; nothing converts a column, and
// a type the bridge or Nimble does not take is the callback's error.

#include "nimble_lane.h"

#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/writer/VeloxWriter.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <cerrno>
#include <cstring>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

namespace velox = facebook::velox;
namespace nimble = facebook::nimble;

namespace {

// Rows per batch the reader returns. Batch boundaries are not compared.
constexpr uint64_t kReadBatchRows = 64 * 1024;

struct Pools {
  std::shared_ptr<velox::memory::MemoryPool> root;
  std::shared_ptr<velox::memory::MemoryPool> leaf;
};

const Pools& pools() {
  static const Pools pools = [] {
    velox::memory::initializeMemoryManager(velox::memory::MemoryManager::Options{});
    Pools made;
    made.root = velox::memory::memoryManager()->addRootPool("nimble-lane");
    made.leaf = made.root->addLeafChild("leaf");
    return made;
  }();
  return pools;
}

// An exception's message, for the Rust side: a VeloxException's reason only (what() adds
// the source, code, context and a stack trace).
std::string describe(const std::exception& e) {
  if (const auto* velox = dynamic_cast<const velox::VeloxException*>(&e)) return velox->message();
  return e.what();
}

int failed(char* error, size_t error_len, const std::string& message) {
  if (error_len > 0) {
    std::strncpy(error, message.c_str(), error_len - 1);
    error[error_len - 1] = '\0';
  }
  return 1;
}

template <typename T>
struct Released {
  T value{};
  ~Released() {
    if (value.release != nullptr) value.release(&value);
  }
};

std::string streamError(ArrowArrayStream* stream, const char* what) {
  const char* said = stream->get_last_error(stream);
  return std::string(what) + (said != nullptr ? std::string(": ") + said : std::string());
}

// The stream `read` hands back: VeloxReader's batches, exported batch by batch.
struct ReadStream {
  std::unique_ptr<velox::LocalReadFile> file;
  std::unique_ptr<nimble::VeloxReader> reader;
  std::string error;
};

int readSchema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* self = static_cast<ReadStream*>(stream->private_data);
  try {
    velox::exportToArrow(velox::BaseVector::create(self->reader->type(), 0, pools().leaf.get()), *out);
    return 0;
  } catch (const std::exception& e) {
    self->error = describe(e);
    return EIO;
  }
}

int readNext(ArrowArrayStream* stream, ArrowArray* out) {
  auto* self = static_cast<ReadStream*>(stream->private_data);
  try {
    velox::VectorPtr batch;
    if (!self->reader->next(kReadBatchRows, batch)) {
      out->release = nullptr;  // end of stream
      return 0;
    }
    velox::exportToArrow(batch, *out, pools().leaf.get());
    return 0;
  } catch (const std::exception& e) {
    self->error = describe(e);
    return EIO;
  }
}

const char* readError(ArrowArrayStream* stream) {
  auto* self = static_cast<ReadStream*>(stream->private_data);
  return self->error.empty() ? nullptr : self->error.c_str();
}

void readRelease(ArrowArrayStream* stream) {
  delete static_cast<ReadStream*>(stream->private_data);
  stream->release = nullptr;
}

}  // namespace

extern "C" int nimble_lane_write(ArrowArrayStream* input, const char* output, char* error,
                                 size_t error_len) {
  Released<ArrowArrayStream> stream;
  stream.value = *input;  // ours now: released here, whatever happens
  input->release = nullptr;
  try {
    Released<ArrowSchema> schema;
    if (stream.value.get_schema(&stream.value, &schema.value) != 0) {
      throw std::runtime_error(streamError(&stream.value, "the canonical's schema"));
    }
    const auto type = velox::importFromArrow(schema.value);
    nimble::VeloxWriter writer(
        type,
        std::make_unique<velox::LocalWriteFile>(output, false, /*shouldThrowOnFileAlreadyExists=*/false),
        *pools().root, nimble::VeloxWriterOptions{});
    while (true) {
      Released<ArrowArray> array;
      if (stream.value.get_next(&stream.value, &array.value) != 0) {
        throw std::runtime_error(streamError(&stream.value, "a canonical batch"));
      }
      if (array.value.release == nullptr) break;  // end of stream
      // A view over the batch's buffers, which outlive the write that encodes them.
      writer.write(velox::importFromArrowAsViewer(schema.value, array.value, pools().leaf.get()));
    }
    writer.close();
    return 0;
  } catch (const std::exception& e) {
    return failed(error, error_len, describe(e));
  }
}

extern "C" int nimble_lane_read(const char* input, ArrowArrayStream* out, char* error,
                                size_t error_len) {
  try {
    auto self = std::make_unique<ReadStream>();
    self->file = std::make_unique<velox::LocalReadFile>(input);
    self->reader = std::make_unique<nimble::VeloxReader>(self->file.get(), *pools().leaf);
    out->get_schema = readSchema;
    out->get_next = readNext;
    out->get_last_error = readError;
    out->release = readRelease;
    out->private_data = self.release();
    return 0;
  } catch (const std::exception& e) {
    return failed(error, error_len, describe(e));
  }
}
