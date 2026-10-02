# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
#
# raincloud-nimble, grafted onto the pinned Nimble checkout's own CMake project.
#
# Not a standalone project: `build.sh` injects it into Nimble's `project()` call through
# `-DCMAKE_PROJECT_Nimble_INCLUDE`, so the tool compiles with exactly the flags, dependencies
# and targets Nimble itself is built with. None of Nimble's or Velox's targets exist yet at
# that point, so the executable is declared at the end of Nimble's top-level directory.

set(RAINCLOUD_NIMBLE_DIR "${CMAKE_CURRENT_LIST_DIR}")

# nanoarrow reads and writes the Arrow IPC stream the sidecar pipes through the tool. A
# release tarball, pinned by sha256 (it vendors flatcc, so nothing else is fetched).
include(FetchContent)
set(NANOARROW_IPC ON CACHE BOOL "" FORCE)
set(NANOARROW_NAMESPACE "RaincloudNimble" CACHE STRING "" FORCE)
FetchContent_Declare(
  nanoarrow
  URL https://github.com/apache/arrow-nanoarrow/releases/download/apache-arrow-nanoarrow-0.9.0/apache-arrow-nanoarrow-0.9.0.tar.gz
  URL_HASH SHA256=801200a0e95e869d5c4bdeb5b535dba58551482bb782b7dc8bd599c8b6e8cacf
)
# Now, not in the deferred call below: CMake creates no subdirectory during deferred execution.
FetchContent_MakeAvailable(nanoarrow)

function(raincloud_nimble_targets)
  add_executable(raincloud-nimble "${RAINCLOUD_NIMBLE_DIR}/raincloud_nimble.cpp")
  target_link_libraries(
    raincloud-nimble
    PRIVATE
      nimble_velox_writer
      nimble_velox_reader
      velox_arrow_bridge
      velox_file
      velox_vector
      velox_memory
      nanoarrow::nanoarrow
      nanoarrow::nanoarrow_ipc
      Folly::folly
  )
endfunction()

cmake_language(DEFER CALL raincloud_nimble_targets)
