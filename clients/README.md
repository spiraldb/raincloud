# Prepared-data readers

Raincloud 0.3 provides lazy dataset handles in Python, Rust, C, C++, and Java.
Metadata access never fetches artifact bytes. Resolving a path, reading a schema,
or opening batches can download the selected artifact. Reads do not build data.
Use `raincloud build SLUG`, or explicitly opt in with Python `load(..., build=True)`.
These sources are not yet published as native binary packages.

There is one implementation of everything before the bytes: catalog selection,
settings, resolution, downloads and checksums all live in the Python package,
behind the `raincloud` command line tool. The Rust, C, C++ and
Java readers run `raincloud describe` and `raincloud load` (with `--json`) and
decode the file it names; they never parse a catalog themselves. Their options
are the same settings the TOML takes, passed through for the CLI to validate in
its environment (`RAINCLOUD_SETTINGS`), never on its command line, which every
account on the machine can read. They find the CLI through the `cli` option,
then `RAINCLOUD_CLI`, then `PATH`.

Opening a handle, and each path, schema or batches call, runs one `raincloud`
process and blocks until it exits. The process inherits the caller's
environment, working directory (relative settings resolve against it) and
stderr, so its warnings reach the caller's terminal or log, and it may wait on
the store's download lock with no timeout.

Settings resolve as API overrides > environment > TOML > native user
directories. `data_dir` can point at a mounted HDD; reads use valid files in
place before checking `cache_dir` and a mirror. Readers never refresh a catalog
implicitly. Use the `raincloud catalog` commands to pack/update/pin/rollback, or
select a bundle or pack directory directly. A handle stays on the catalog
generation it opened with, in every language: an installed revision or a pack
directory resolves to that same generation again, and a checkout, local
manifest or bundled catalog that has changed since the handle opened is refused
(a catalog error) rather than read. A handle's metadata names its
`catalog_source` and `catalog_revision`: select `catalog_source` (the `catalog`
setting) to reopen the same catalog, and compare `catalog_revision` to detect a
change.

## Selection and integrity

`auto` prefers Vortex, Parquet, then canonical Arrow IPC among advertised artifacts
the reader can decode: Python's installed readers, or the formats a native library
was built with (a build without Vortex never picks it). A dataset has one file per format; which writer made
it (`py`, `rs`, ...) is recorded in the catalog and shown in the dataset's metadata,
but is never something to ask for. Selection is deterministic from catalog metadata;
it does not probe remote files. An explicitly selected format never substitutes another.

Mirrors use Python's transports (file, HTTP(S), and optional fsspec ones such
as S3) for every client. A mirror artifact is downloaded atomically before batch reading; these APIs do
not perform remote range scans. Offline mode never downloads or builds.
The catalog is the authority on each artifact's sha256 and byte size. A local
file at its key with the catalog's size is the artifact; one with another size
is refused, with the command that fixes it. Mirror bytes must match the
catalog's sha256 (its size when it records none) or they are refused with a
checksum mismatch. Serving a read-only local store needs no writes.

The Python reader uses PyArrow/Vortex Python. The Rust, C, C++, and Java APIs share
the Rust reader core; Java imports native batches through Arrow's C stream JNI
bridge. The existing JVM and other conformance sidecars remain separate readers.
These clients do not run conformance tests during ordinary reads. Format support
does not promise support for every type or preservation of every extension
annotation: Vortex may infer a different physical Arrow representation, and
unsupported types fail in the selected reader. Build without Rust's default
`vortex` feature for an IPC/Parquet-only library: `auto` then never selects
Vortex, and an explicit Vortex handle resolves its path but refuses to decode it.
Column projection (`columns=`) is Python-only; the native APIs read every column.

## Python

The base Python install reads IPC and Parquet. Add `[vortex]` for Vortex;
`raincloud capabilities` reports installed reader modules.

```python
import raincloud

ds = raincloud.load("uci-seeds", format="parquet")
print(ds.catalog_revision, ds.artifacts)  # metadata only
with ds.batches(batch_size=65536, columns=["area"]) as batches:
    for batch in batches:
        consume(batch)                 # PyArrow RecordBatch
# ds.to_arrow() explicitly materializes the entire table.
```

## Rust

Build from `clients/rust` with Cargo (Rust 1.95 or newer). Reads need the
`raincloud` CLI installed. Use the library as a path dependency until it is published.

```rust,no_run
use raincloud_reader::{Dataset, RecordBatchReader};
use serde_json::json;

let ds = raincloud_reader::load("uci-seeds")?;            // defaults, automatic format
let ds = Dataset::load(&json!({"config": "/path/to/config.toml"}), "uci-seeds", "parquet")?;
let batches = ds.batches(65536)?;
println!("{:?}", batches.schema());
for batch in batches { consume(batch?); }
```

The synchronous API owns its reader/runtime and drops them with the iterator.
Call it on a blocking thread when integrating with an async application.
Returned batches own their buffers. `batch_size` bounds returned rows, not the
size of an encoded IPC/Vortex batch that a decoder must decompress.

## C and C++

```sh
cmake -S clients/c -B build/native -DCMAKE_INSTALL_PREFIX="$HOME/.local"
cmake --build build/native
cmake --install build/native
```

An installed CMake consumer can use:

```cmake
find_package(raincloud 0.3 CONFIG REQUIRED)
target_link_libraries(my_reader PRIVATE raincloud::reader)
```

Pass `-DCMAKE_PREFIX_PATH=/path/to/install` when configuring the consumer.
`-DRAINCLOUD_VORTEX=OFF` builds only
IPC/Parquet support. Native binaries are specific to their OS/architecture.

The C interface is `raincloud.h`, linked with `libraincloud_reader`. Pure C users
can include the vendored Apache Arrow ABI definitions in `raincloud_arrow_abi.h`;
no Arrow C++ dependency is needed. `raincloud_open` returns an opaque dataset,
`raincloud_metadata` returns JSON, and `raincloud_batches` fills an
`ArrowArrayStream`. Release every returned string, schema, array, stream, and
handle using its documented release function. A stream remains usable after the
dataset handle is closed. `raincloud_abi_version()` currently returns 1. ABI
changes are additive: the version rises when an entry point is added, so check
that it is at least the version that introduced the newest entry point you call
(`raincloud.h` states the rule).

The shared library's SONAME is `libraincloud_reader.so` (on macOS its install
name is `@rpath/libraincloud_reader.dylib`), so a consumer records the bare name
and finds the library at run time, not at the path it was linked from. Give an
installed consumer a run path to the install's library directory
(`-DCMAKE_INSTALL_RPATH=/path/to/install/lib` or the target's `INSTALL_RPATH`,
or `-Wl,-rpath,...` when linking by hand), or set `LD_LIBRARY_PATH`; a
consumer run from its CMake build tree already has one.

`raincloud.hpp` is a move-only RAII wrapper. Applications using Arrow C++ can
include `raincloud_arrow.hpp` to receive an Arrow `RecordBatchReader`:

```cpp
raincloud::Dataset ds("uci-seeds", "parquet");
ARROW_ASSIGN_OR_RAISE(auto reader, raincloud::batches(ds));
while (true) {
  ARROW_ASSIGN_OR_RAISE(auto batch, reader->Next());
  if (!batch) break;
  consume(batch);
}
```

Raincloud exposes a C ABI; it does not link against the application's Arrow C++
version. See `clients/c/tests/` for complete standalone C and C++ consumers.

## Java

Build the standalone Gradle library using the repository's wrapper:

```sh
bash sidecars/java/gradlew -p clients/java installDist \
  -PraincloudNativeLibrary=/path/to/lib/libraincloud_reader.so
```

`clients/java/build/install/raincloud-reader/` contains `lib/` with the Java
runtime classpath and, when supplied, `native/` with the native library. Copy that
directory as a unit, then use `-cp "/path/to/distribution/lib/*"` and
`-Djna.library.path=/path/to/distribution/native`. The standalone example in
`clients/java/examples/Read.java` can be compiled against this classpath.
`jar` and `sourcesJar` remain available for applications managing their own deps.

Alternatively, install the native library above and make its directory visible through
`-Djna.library.path=/path/to/lib`. Use JDK 17+ and
`--add-opens=java.base/java.nio=ALL-UNNAMED`. The library's JAR does not
bundle native binaries. Native library and JVM architecture must match. This integration is verified on Linux x86-64; other
platform binaries still need packaging and verification. Its runtime dependencies
are Arrow Java 19 (`arrow-vector`, `arrow-c-data`, `arrow-memory-netty`), JNA and
Jackson (`jackson-databind`); `bash sidecars/java/gradlew -p clients/java dependencies
--configuration runtimeClasspath` lists them with versions.

```java
try (var allocator = new RootAllocator();
     var ds = Raincloud.load("uci-seeds", "parquet", Map.of());
     var reader = ds.batches(allocator, 65536)) {
    while (reader.loadNextBatch()) {
        consume(reader.getVectorSchemaRoot());
    }
}
```

Java vectors are valid until the next `loadNextBatch()` or reader close; retain
or copy explicitly to keep them longer. Close readers before their allocator.
Dataset close is independent of readers already opened from it.

## Errors and verification

C returns stable error codes and an owned diagnostic string. C++ exposes
`raincloud::Error` for handle and resolution operations, including resolving the
artifact inside `raincloud::batches`, and Arrow status for stream operations.
Rust exposes `ErrorKind`; Java exposes `RaincloudException.Kind`, whose `code`
is the C value. Missing revisions, unknown slugs, unsupported formats, offline
misses, absent artifacts, checksum mismatches, unreadable mirrors (transport),
corrupt artifacts and unsupported types are separate categories; `raincloud.h`
lists the codes and the rules for adding one. Stream callbacks/iterators also
expose the underlying Arrow decode error, and a decoder panic becomes such an
error rather than unwinding into the caller. Corrupt bytes and unsupported
physical types are never silently converted to another format.

Run `python scripts/test_native_readers.py` in a dev/build Python environment
with Cargo, CMake, a C/C++ compiler, and JDKs 17 and 21 available (the Hardwood
lane builds on 21; Gradle's toolchain resolver provisions a missing JDK), after
`git submodule update --init --recursive`. It builds the native library (with and
without Vortex), generates one small fixture, and verifies Rust, Python/C ABI, the
installed CMake package with its C and C++ consumers (checking the library's
SONAME and the bare name its consumers record), and the Java library and
installed distribution against it. It also builds the Rust and Java sidecars and
runs the conformance suites that use them (`PYTEST_MODULES` in the script lists
the pytest modules). The C/C++ consumers run with an empty executable search path and the
CLI named by `RAINCLOUD_CLI`, so a reader that silently depended on `PATH` would
fail. The runner currently targets Linux; macOS/Windows native builds are not
release-verified.
