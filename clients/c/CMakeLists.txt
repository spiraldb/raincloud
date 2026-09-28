# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
cmake_minimum_required(VERSION 3.21)
# The release version has one authority; a copy here would let the package
# version drift from the wheel and crate built from the same tree. CMake's
# project(VERSION) takes numeric components only, so a pre-release such as
# 0.3.1rc1 gives the package version 0.3.1.
file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/../../raincloud/__init__.py" RAINCLOUD_VERSION_LINE
  REGEX "^__version__ = \"[0-9]+\\.[0-9]+\\.[0-9]+")
if(NOT RAINCLOUD_VERSION_LINE)
  message(FATAL_ERROR "no __version__ = \"X.Y.Z...\" in raincloud/__init__.py")
endif()
string(REGEX REPLACE "^__version__ = \"([0-9]+\\.[0-9]+\\.[0-9]+).*$" "\\1" RAINCLOUD_VERSION "${RAINCLOUD_VERSION_LINE}")
project(raincloud VERSION ${RAINCLOUD_VERSION} LANGUAGES C CXX)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_program(CARGO cargo REQUIRED)
set(RAINCLOUD_CARGO_TARGET "${CMAKE_CURRENT_BINARY_DIR}/cargo" CACHE PATH "Cargo target directory")
option(RAINCLOUD_VORTEX "Include the optional native Vortex reader" ON)
set(RAINCLOUD_FEATURES)
if(NOT RAINCLOUD_VORTEX)
  list(APPEND RAINCLOUD_FEATURES --no-default-features)
endif()
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(RAINCLOUD_PROFILE dev)
  set(RAINCLOUD_PROFILE_DIR debug)
else()
  set(RAINCLOUD_PROFILE release)
  set(RAINCLOUD_PROFILE_DIR release)
endif()
set(RAINCLOUD_LIBRARY_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}raincloud_reader${CMAKE_SHARED_LIBRARY_SUFFIX}")
set(RAINCLOUD_LIBRARY "${RAINCLOUD_CARGO_TARGET}/${RAINCLOUD_PROFILE_DIR}/${RAINCLOUD_LIBRARY_NAME}")
set(RAINCLOUD_RUNTIME_DIR "${CMAKE_INSTALL_LIBDIR}")
if(WIN32)
  set(RAINCLOUD_LIBRARY_NAME "raincloud_reader.dll")
  set(RAINCLOUD_LIBRARY "${RAINCLOUD_CARGO_TARGET}/${RAINCLOUD_PROFILE_DIR}/${RAINCLOUD_LIBRARY_NAME}")
  set(RAINCLOUD_RUNTIME_DIR "${CMAKE_INSTALL_BINDIR}")
  if(MSVC)
    set(RAINCLOUD_IMPORT_NAME "raincloud_reader.dll.lib")
  else()
    set(RAINCLOUD_IMPORT_NAME "libraincloud_reader.dll.a")
  endif()
  install(FILES "${RAINCLOUD_CARGO_TARGET}/${RAINCLOUD_PROFILE_DIR}/${RAINCLOUD_IMPORT_NAME}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
add_custom_target(raincloud_native ALL
  COMMAND ${CARGO} build --locked --profile ${RAINCLOUD_PROFILE} ${RAINCLOUD_FEATURES} --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/../rust/Cargo.toml --target-dir ${RAINCLOUD_CARGO_TARGET}
  BYPRODUCTS "${RAINCLOUD_LIBRARY}" VERBATIM)
install(FILES "${RAINCLOUD_LIBRARY}" DESTINATION ${RAINCLOUD_RUNTIME_DIR})
install(FILES include/raincloud.h include/raincloud_arrow_abi.h include/raincloud.hpp include/raincloud_arrow.hpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
configure_package_config_file(cmake/raincloudConfig.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/raincloudConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/raincloud"
  PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR RAINCLOUD_RUNTIME_DIR)
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/raincloudConfigVersion.cmake"
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMinorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/raincloudConfig.cmake" "${CMAKE_CURRENT_BINARY_DIR}/raincloudConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/raincloud")
