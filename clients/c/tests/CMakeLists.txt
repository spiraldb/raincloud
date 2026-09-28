# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
cmake_minimum_required(VERSION 3.21)
project(raincloud_installed_consumer LANGUAGES C CXX)
# The runner passes the version it is testing (from raincloud/__init__.py).
if(NOT RAINCLOUD_EXPECTED_VERSION)
  message(FATAL_ERROR "pass -DRAINCLOUD_EXPECTED_VERSION=X.Y.Z")
endif()
find_package(raincloud ${RAINCLOUD_EXPECTED_VERSION} CONFIG REQUIRED)
add_executable(read-c read.c)
target_link_libraries(read-c PRIVATE raincloud::reader)
# The Arrow C++ bridge uses the consumer's Arrow installation.
if(ARROW_INCLUDE AND ARROW_LIBRARY)
  add_library(consumer_arrow SHARED IMPORTED)
  set_target_properties(consumer_arrow PROPERTIES
    IMPORTED_LOCATION "${ARROW_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE}")
  add_executable(read-cpp read.cpp)
  target_compile_features(read-cpp PRIVATE cxx_std_17)
  target_link_libraries(read-cpp PRIVATE raincloud::reader consumer_arrow)
endif()
