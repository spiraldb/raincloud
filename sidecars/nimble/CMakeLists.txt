# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
#
# The nimble@cpp lane's binaries, grafted onto the pinned Nimble checkout's own CMake project.
#
# Not a standalone project: `build.sh` injects it into Nimble's `project()` call through
# `-DCMAKE_PROJECT_Nimble_INCLUDE`, so the binaries compile with exactly the flags,
# dependencies and targets Nimble itself is built with, and passes RAINCLOUD_NIMBLE_FFI, the
# Rust static library that runs the sidecar contract (sidecars/rust/nimble-ffi). None of
# Nimble's or Velox's targets exist yet at that point, so the targets are declared at the
# end of Nimble's top-level directory.

set(RAINCLOUD_NIMBLE_DIR "${CMAKE_CURRENT_LIST_DIR}")
if(NOT RAINCLOUD_NIMBLE_FFI)
  message(FATAL_ERROR "RAINCLOUD_NIMBLE_FFI must name libraincloud_nimble_ffi.a (see build.sh)")
endif()

function(raincloud_nimble_targets)
  find_package(Threads REQUIRED)
  add_library(raincloud_nimble_lane STATIC "${RAINCLOUD_NIMBLE_DIR}/nimble_lane.cpp")
  target_link_libraries(
    raincloud_nimble_lane
    PUBLIC
      nimble_velox_writer
      nimble_velox_reader
      velox_arrow_bridge
      velox_file
      velox_vector
      velox_memory
      Folly::folly
  )
  foreach(binary export read)
    add_executable(raincloud-${binary}-nimble-cpp "${RAINCLOUD_NIMBLE_DIR}/${binary}_main.cpp")
    target_link_libraries(
      raincloud-${binary}-nimble-cpp
      PRIVATE raincloud_nimble_lane "${RAINCLOUD_NIMBLE_FFI}" Threads::Threads ${CMAKE_DL_LIBS} m
    )
  endforeach()
endfunction()

cmake_language(DEFER CALL raincloud_nimble_targets)
