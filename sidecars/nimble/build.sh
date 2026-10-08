#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
#
# Build the `nimble@cpp` lane's binaries, raincloud-export-nimble-cpp and
# raincloud-read-nimble-cpp, inside a pinned Nimble checkout. Nimble has no releases and no
# packages, so it is built from source: Nimble at the commit below (upstream plus
# host-compatibility build fixes), its Velox and OpenZL submodules, the Folly and other
# dependencies Velox fetches, and four small libraries bootstrapped here. The binaries link
# raincloud_nimble_ffi (sidecars/rust/nimble-ffi), the Rust library that runs the sidecar
# contract, built here too.
#
#   RAINCLOUD_NIMBLE_SRC=<nimble checkout> sidecars/nimble/build.sh <ROOT>
#
# RAINCLOUD_NIMBLE_SRC is a clone of the Nimble fork (https://github.com/mprammer/nimble, branch
# `raincloud`) at `nimble_pin`, with its submodules
# initialized; it is never edited. ROOT holds everything the build creates (about 4 GB) and
# must be on a disk, not tmpfs, and outside the repository and the checkout:
#
#   downloads/  the pinned tarballs, sha256-verified
#   bootstrap/  their sources and build trees      deps/   their install prefix
#   nimble/     the Nimble build tree (Velox's fetched dependencies land in its _deps/)
#   cargo/      the Rust library's build tree
#   bin/        the two binaries and nimble-cpp.provenance, written last
#
# A cold build needs the network, cargo and several minutes (about 8 at 8 jobs). Other
# environment: RAINCLOUD_NIMBLE_JOBS (default: half the CPUs); RAINCLOUD_BUILD_LOCK, a file
# the heavy steps take a shared flock on (waiting up to an hour), for a machine whose users
# coordinate long jobs that way (unset: no lock). Point RAINCLOUD_SIDECAR_NIMBLE_CPP and RAINCLOUD_READER_NIMBLE_CPP at the two
# binaries in ROOT/bin to use them.
set -euo pipefail

# The Nimble commit: upstream acead744 (2026-08-07) and the build fixes on top of it. The
# submodule commits are the ones it records, checked against its gitlinks.
nimble_pin=b8ebbcbbbc5df5a6e9ea4988ec7a4090a52ecfb4
velox_pin=351b0a72f446c75e0bd0ad179063a7a02f8894fc
openzl_pin=6b48fa4868160ed1e5c78ac422639615dd0dcf28

# Libraries the host lacks and Velox does not fetch: name, version, URL, sha256. libdwarf
# because Nimble calls Folly's symbolizer, which Folly builds only with it; FlatBuffers is
# Nimble's metadata runtime.
deps=(
  "double-conversion 3.3.1 https://codeload.github.com/google/double-conversion/tar.gz/refs/tags/v3.3.1 fe54901055c71302dcdc5c3ccbe265a6c191978f3761ce1414d0895d6b0ea90e"
  "fmt 11.2.0 https://codeload.github.com/fmtlib/fmt/tar.gz/refs/tags/11.2.0 bc23066d87ab3168f27cef3e97d545fa63314f5c79df5ea444d41d56f962c6af"
  "libdwarf 0.12.0 https://github.com/davea42/libdwarf-code/releases/download/v0.12.0/libdwarf-0.12.0.tar.xz 444dc1c5176f04d3ebc50341552a8b2ea6c334f8f1868a023a740ace0e6eae9f"
  "flatbuffers 25.12.19 https://codeload.github.com/google/flatbuffers/tar.gz/refs/tags/v25.12.19 f81c3162b1046fe8b84b9a0dbdd383e24fdbcf88583b9cb6028f90d04d90696a"
)

die() { echo "build.sh: $*" >&2; exit 1; }
step() { echo; echo "==> $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

# Every option before the lock file; -o so a daemon the command starts does not keep the lock.
build_lock=${RAINCLOUD_BUILD_LOCK:-}
quiet() {
  local status=0
  if [[ -n $build_lock ]]; then
    flock -E 75 -s -o -w 3600 "$build_lock" "$@" || status=$?
    [[ $status -ne 75 ]] || die "timed out waiting for $build_lock to run: $*"
  else
    "$@" || status=$?
  fi
  [[ $status -eq 0 ]] || die "exit status $status from: $*"
}

for tool in cmake ninja cc c++ cargo pkg-config git curl sha256sum tar xz; do
  command -v "$tool" >/dev/null || die "missing prerequisite: $tool"
done
[[ $# -eq 1 && -n ${RAINCLOUD_NIMBLE_SRC:-} ]] ||
  die "usage: RAINCLOUD_NIMBLE_SRC=<nimble checkout at $nimble_pin> $0 <ROOT>"
here=$(cd "$(dirname "$0")" && pwd -P)
repo=$(cd "$here/../.." && pwd -P)
src=$(realpath -e -- "$RAINCLOUD_NIMBLE_SRC") || die "RAINCLOUD_NIMBLE_SRC=$RAINCLOUD_NIMBLE_SRC does not exist"
root=$(realpath -m -- "$1")
for tree in "$repo" "$src"; do
  [[ $root != "$tree" && $root != "$tree"/* && $tree != "$root"/* ]] || die "ROOT $root overlaps $tree"
done
existing=$root
while [[ ! -e $existing ]]; do existing=$(dirname "$existing"); done
case $(stat -f -c %T -- "$existing") in tmpfs | ramfs) die "ROOT $root is on tmpfs; use a disk" ;; esac
jobs=${RAINCLOUD_NIMBLE_JOBS:-$(($(nproc) / 2))}

step "checking the Nimble checkout $src"
[[ $(git -C "$src" rev-parse HEAD) == "$nimble_pin" ]] || die "$src is not at $nimble_pin"
gitlinks=$(git -C "$src" ls-tree HEAD velox openzl)
grep -qF "commit $velox_pin"$'\t'velox <<<"$gitlinks" || die "the pin does not record velox at $velox_pin"
grep -qF "commit $openzl_pin"$'\t'openzl <<<"$gitlinks" || die "the pin does not record openzl at $openzl_pin"
[[ $(git -C "$src/velox" rev-parse HEAD 2>/dev/null) == "$velox_pin" ]] ||
  die "velox is not checked out at $velox_pin (git -C $src submodule update --init --recursive)"
[[ $(git -C "$src/openzl" rev-parse HEAD 2>/dev/null) == "$openzl_pin" ]] || die "openzl is not checked out at $openzl_pin"
[[ -z $(git -C "$src" status --porcelain --untracked-files=no) ]] || die "$src has tracked modifications"

mkdir -p "$root/downloads" "$root/bootstrap" "$root/deps" "$root/bin"
compilers=(-DCMAKE_C_COMPILER="$(command -v cc)" -DCMAKE_CXX_COMPILER="$(command -v c++)")
for dep in "${deps[@]}"; do
  read -r name version url sha256 <<<"$dep"
  flags=(-DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
  case $name in
    double-conversion) flags+=(-DBUILD_TESTING=OFF) ;;
    fmt) flags+=(-DFMT_TEST=OFF -DFMT_DOC=OFF) ;;
    libdwarf) flags+=(-DBUILD_NON_SHARED=ON -DBUILD_SHARED=OFF -DPIC_ALWAYS=ON -DBUILD_DWARFDUMP=OFF) ;;
    flatbuffers) flags+=(-DFLATBUFFERS_BUILD_TESTS=OFF) ;;
  esac
  tarball=$root/downloads/$name-$version.archive
  stamp=$root/deps/.installed-$name
  key=$(printf '%s\n' "$dep" "${flags[@]}" "${compilers[@]}" | sha256sum | cut -d' ' -f1)
  if [[ ! -f $tarball ]]; then
    step "downloading $name $version"
    curl -fsSL --retry 3 -o "$tarball.part" "$url"
    mv "$tarball.part" "$tarball"
  fi
  echo "$sha256  $tarball" | sha256sum -c --quiet - || die "$tarball does not match its pinned sha256"
  [[ -f $stamp && $(<"$stamp") == "$key" ]] && continue
  step "building $name $version"
  rm -rf "$root/bootstrap/$name-$version" "$root/bootstrap/$name-build"
  tar -xf "$tarball" -C "$root/bootstrap"
  quiet cmake -S "$root/bootstrap/$name-$version" -B "$root/bootstrap/$name-build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release "${compilers[@]}" -DCMAKE_INSTALL_PREFIX="$root/deps" "${flags[@]}"
  quiet cmake --build "$root/bootstrap/$name-build" -j "$jobs"
  quiet cmake --install "$root/bootstrap/$name-build"
  echo "$key" >"$stamp"
done

step "building raincloud_nimble_ffi"
# Against the host's libzstd, which the C++ side links too: one zstd in the binary.
ffi=$root/cargo/release/libraincloud_nimble_ffi.a
quiet env ZSTD_SYS_USE_PKG_CONFIG=1 CARGO_TARGET_DIR="$root/cargo" \
  cargo build --release --locked --manifest-path "$repo/sidecars/rust/Cargo.toml" -p raincloud-nimble-ffi
[[ -f $ffi ]] || die "the cargo build did not produce $ffi"

step "configuring Nimble into $root/nimble"
quiet cmake -S "$src" -B "$root/nimble" -G Ninja -DCMAKE_BUILD_TYPE=Release "${compilers[@]}" \
  -DCMAKE_PREFIX_PATH="$root/deps" \
  -DVELOX_ENABLE_GEO=OFF -DVELOX_BUILD_TESTING=OFF -DBUILD_TESTING=OFF \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -Dglog_SOURCE=BUNDLED \
  -DCMAKE_FIND_PACKAGE_TARGETS_GLOBAL=TRUE -DCMAKE_DISABLE_FIND_PACKAGE_LibUring=TRUE \
  -DVELOX_MONO_LIBRARY=OFF -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_PROJECT_Nimble_INCLUDE="$here/CMakeLists.txt" -DRAINCLOUD_NIMBLE_FFI="$ffi"

binaries=(raincloud-export-nimble-cpp raincloud-read-nimble-cpp)
step "building ${binaries[*]}"
rm -f "$root/bin/nimble-cpp.provenance"
for binary in "${binaries[@]}"; do rm -f "$root/bin/$binary"; done
quiet cmake --build "$root/nimble" --target "${binaries[@]}" -j "$jobs"
for binary in "${binaries[@]}"; do
  [[ -x $root/nimble/$binary ]] || die "the build did not produce $root/nimble/$binary"
  cp "$root/nimble/$binary" "$root/bin/$binary.part"
  mv "$root/bin/$binary.part" "$root/bin/$binary"
done
{
  echo "nimble $nimble_pin"
  echo "velox $velox_pin"
  echo "openzl $openzl_pin"
  for dep in "${deps[@]}"; do read -r name version _ _ <<<"$dep"; echo "$name $version"; done
  echo "compiler $(c++ --version | head -n 1)"
  echo "rust $(rustc --version)"
  echo "sources $(cd "$here" && sha256sum ./*.cpp ./*.h CMakeLists.txt build.sh | sha256sum | cut -c1-16)"
  for binary in "${binaries[@]}"; do echo "sha256 $binary $(sha256sum "$root/bin/$binary" | cut -d' ' -f1)"; done
} >"$root/bin/nimble-cpp.provenance"
echo "built ${binaries[*]} in $root/bin"
