# Optional format engines

The `sidecars/` programs add independent writer and conformance-reader engines to
the Python pipeline. They use a file/JSON subprocess interface. The prepared-data
library APIs in [`clients/`](../clients/README.md) are separate and do not run
these programs during ordinary reads.

## Which writer a build uses

A dataset has one file per format; the install's `formats` setting says which
formats a build writes, never which writer. For each format a build takes the first writer that is installed,
from the first of these that names one:

1. the recipe's `export.priority` (a list for every format, or a map such as
   `{"parquet": ["rs", "py"]}`),
2. the catalog's `export_priority`,
3. `RAINCLOUD_EXPORT_PRIORITY` (e.g. `rs,py`),
4. the built-in order `py, rs, java, cpp, canonical` (`canonical` writes only the
   canonical Arrow IPC file, so it is the last resort for the `arrow` format).

A writer that is not installed falls through to the next one in that order, so a
default build always has the Python exporters. Only a cell named explicitly
(`--format parquet@rs`) is skipped, with a note, when its tool is missing.
Compliance records `pass`, `fail`, `skip`, and non-applicable pairs separately.
Nothing here installs a tool or upgrades a dependency automatically.

**Installing a Rust or Java writer changes what some builds produce.** The 32
TPC SF100 recipes put `rs` first for Parquet, so on a machine with
`raincloud-export-parquet-rs` installed they are written by arrow-rs rather than
pyarrow, a different file with a different sha256. A sidecar writer is bounded by
`RAINCLOUD_EXPORT_TIMEOUT` (default 6 h; `0` disables it) and a sidecar reader by
`RAINCLOUD_SIDECAR_TIMEOUT` (default 1800 s). A writer that runs out of time is
recorded as the format's unavailable measurement and the build carries on without
it.

## Install and reuse on Linux

Build Rust executables into a user-selected prefix:

```bash
export RAINCLOUD_TOOLS_ROOT="$HOME/.local/share/raincloud-tools"
cargo install --path sidecars/rust --locked --root "$RAINCLOUD_TOOLS_ROOT/rust"
export RAINCLOUD_SIDECAR_PARQUET_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/parquet-write"
export RAINCLOUD_READER_PARQUET_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/parquet-read"
export RAINCLOUD_SIDECAR_VORTEX_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/vortex-write"
export RAINCLOUD_READER_VORTEX_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/vortex-read"
export RAINCLOUD_SIDECAR_ORC_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/orc-write"
export RAINCLOUD_READER_ORC_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/orc-read"
export RAINCLOUD_SIDECAR_AVRO_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/avro-write"
export RAINCLOUD_READER_AVRO_RS="$RAINCLOUD_TOOLS_ROOT/rust/bin/avro-read"
```

The ORC lane (`orc@rs`) is orc-rust, pinned exactly in `sidecars/rust/Cargo.toml`.
It writes zstd with orc-rust's default stripe size, and panics on a type it does
not write (anything but signed integers, floats, strings, binary, booleans,
`date32` and timestamps); the panic is its report, and the build records ORC
unavailable for that dataset. The Python lane (`orc@py`, pyarrow's Apache ORC C++
library) needs no sidecar.

Avro has two lanes and no Python one (pyarrow reads and writes no Avro): `avro@rs`,
arrow-avro (released with arrow-rs, pinned with it), and `avro@java`, Arrow Java's own Avro
adapter over Apache Avro's Java implementation (the `avro-java` project). Both write a
zstandard object container file, one block per canonical batch (Rust) or Avro's own
block size (Java), with the same fixed sync marker, `raincloud-avro01`: each library
otherwise draws one at random, so a rebuild would change the file's sha256. Avro Java
takes the marker as an argument; arrow-avro offers no way to choose it, so the Rust lane
overwrites the marker it drew in place, after the header and after each block, leaving
every other byte arrow-avro's. Nothing converts a column for either library. Arrow
Java 19.0.0's adapter reads with its legacy mapping (the only one its public API
offers), which decodes a nullable Avro field into a sparse union of `null` and the
value type; every comparator reads such a union as the nullable column it spells.

Where equality turns on representation rather than data, the three comparators share
one rule set, declared once in [`compare_cases/`](compare_cases/): pairs of Arrow files
and the verdict each must get, which every lane's tests read (`generate.py` writes
them). A union of exactly `null` and `T` is a nullable `T`; zoned timestamps compare by
instant whatever zone labels them (naive and zoned differ); an integer and a scale-0
decimal holding the same values are equal.

Both ORC lanes widen what ORC cannot hold before writing, always rather than by the
data's range, so a dataset's ORC schema never changes with its values: uint8 → int16,
uint16 → int32, uint32 → int64, uint64 → decimal(20, 0), and view types to their plain
types. The comparators read each back as the canonical's type. orc-rust 0.9.0 writes
no decimals, so a uint64 column is still unavailable in `orc@rs`.

Build Java distributions with JDK 17 and the pinned submodule. The
parquet-hardwood project builds on Java 21, because Hardwood's jar targets it. If a
JDK a project needs is not installed, Gradle's toolchain resolver provisions one,
which may download it:

```bash
git submodule update --init --recursive
bash sidecars/java/gradlew -p sidecars/java :parquet-java:installDist \
    :parquet-hardwood:installDist :vortex-jni-reader:installDist :avro-java:installDist
```

Copy each complete directory from the corresponding project's `build/install/`
into a versioned directory of your choice. Keep `bin/` and `lib/` together. Point
these environment variables at the copied launchers:

| Setting | Directory | Launcher |
|---|---|---|
| `RAINCLOUD_SIDECAR_PARQUET_JAVA` | `raincloud-export-parquet-java` | `bin/raincloud-export-parquet-java` |
| `RAINCLOUD_READER_PARQUET_JAVA` | `raincloud-export-parquet-java` | `bin/raincloud-read-parquet-java` |
| `RAINCLOUD_SIDECAR_PARQUET_HARDWOOD` | `raincloud-export-parquet-hardwood` | `bin/raincloud-export-parquet-hardwood` |
| `RAINCLOUD_READER_PARQUET_HARDWOOD` | `raincloud-export-parquet-hardwood` | `bin/raincloud-read-parquet-hardwood` |
| `RAINCLOUD_SIDECAR_VORTEX_JNI` | `raincloud-read-vortex-jni` | `bin/raincloud-export-vortex-jni` |
| `RAINCLOUD_READER_VORTEX_JNI` | `raincloud-read-vortex-jni` | `bin/raincloud-read-vortex-jni` |
| `RAINCLOUD_SIDECAR_AVRO_JAVA` | `raincloud-export-avro-java` | `bin/raincloud-export-avro-java` |
| `RAINCLOUD_READER_AVRO_JAVA` | `raincloud-export-avro-java` | `bin/raincloud-read-avro-java` |

The `vortex-jni-reader` project holds the `vortex@jni` writer as well as its
reader, so its one distribution carries both launchers.

Java needs a JDK/JRE at runtime: 17 or newer, and 21 or newer for the Hardwood
launchers. Those keep `JAVA_HOME` when it names Java 21 or newer and otherwise
use the Java 21 toolchain they were built with, while it is still installed; a
copy on a machine without that JDK needs `JAVA_HOME` set to a JDK 21+. Installed
Rust executables need neither Cargo nor a source checkout. Binary/JVM architecture
must match the host. These engines have been verified on Linux x86-64;
other native platforms remain unverified.

Explicit `RAINCLOUD_SIDECAR_<FORMAT>_<IMPLEMENTATION>` and
`RAINCLOUD_READER_<FORMAT>_<IMPLEMENTATION>` paths take precedence over PATH
lookup. PATH launcher names follow `raincloud-export-parquet-rs` or
`raincloud-read-parquet-rs`, for example. The Rust crate's binaries keep their
shorter names (`parquet-write`, ...), so the Rust lanes are found only through
the explicit settings above.

Every sidecar writes its report even when it fails, a Rust panic included. A
writer's `roundtrip` is `true` or `false` for a measured self-verify: `false`
carries the cause, and the build never promotes that file (a Rust writer
removes its output on any error; the build discards what a Java writer leaves in
its scratch path). `null`
means the file was written but its self-verify could not be measured (a
comparator gap, or a JVM out of memory): the build promotes it and records the
round-trip as unmeasured (`Compliance(roundtrip=None)`). A reader reports `pass`,
`fail` or `skip`, with the cause. Exit 0 means a report was written, exit 2 is a
usage error (an unknown, repeated or missing option; no report), and exit 1 means
the report itself could not be written.

The row-group knobs (`RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES`,
`RAINCLOUD_ROW_GROUP_MAX_ROWS`) read the same way in every lane, Python's
included: unset means the default; after trimming ASCII whitespace, empty or `0`
means no cap; otherwise the value must be plain ASCII digits with an optional
fraction and exponent (`^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`, so no sign,
`1_000`, `10,000`, units or `inf`), and is truncated to a whole number, which
must not be 0. A value that is not UTF-8 is refused too, naming the variable.
[`knob_cases.json`](knob_cases.json) holds the cases all three lanes' tests
read. A recipe's `write.row_group_size_rows` wins over
`RAINCLOUD_ROW_GROUP_MAX_ROWS` in every lane: the sidecars never see the recipe,
so the build passes that cap to them as `RAINCLOUD_ROW_GROUP_MAX_ROWS` in their
environment.

The Parquet writers also share one set of write options
(`raincloud/pipeline/spec.py::ParquetOptions`, which documents each), which the build
passes to a sidecar in this form:

| variable | value | from |
|---|---|---|
| `RAINCLOUD_PARQUET_COMPRESSION` | `zstd`, `snappy`, `gzip`, `lz4` (LZ4_RAW), `brotli` or `none` | the recipe's `write.compression` |
| `RAINCLOUD_PARQUET_STATISTICS` | `1` or `0` | the recipe's `write.statistics` |
| `RAINCLOUD_PARQUET_COMPRESSION_LEVEL` | a whole number in the codec's range: zstd 1-22, gzip 0-9, brotli 0-11 | the install, only when set |
| `RAINCLOUD_PARQUET_STATISTICS_COLUMNS` | N: statistics only for the first N leaf columns | the install, only when set |
| `RAINCLOUD_PARQUET_PAGE_INDEX` | `1` or `0`: a ColumnIndex and OffsetIndex for every column chunk, or neither | the install, only when set |
| `RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS` | N: page statistics only for the first N leaf columns, chunk statistics for all | the install, only when set |
| `RAINCLOUD_PARQUET_PAGE_BYTES` | data page size target | the install, only when set |
| `RAINCLOUD_PARQUET_PAGE_ROWS` | data page row limit | the install, only when set |
| `RAINCLOUD_PARQUET_DICTIONARY` | `1` or `0`: dictionary encoding, or PLAIN | the install, only when set |
| `RAINCLOUD_PARQUET_DICTIONARY_PAGE_BYTES` | dictionary page size limit | the install, only when set |
| `RAINCLOUD_PARQUET_PAGE_CHECKSUMS` | `1` or `0`: a CRC in every page header, or none | the install, only when set |

An unset variable is the lane's library default (zstd and statistics on, for the first
two). Counts use the count grammar, `0` meaning no limit (every column, for the two
column counts). Switches read `1/true/yes/on` and `0/false/no/off` in any case, empty as
unset, in every lane. Settings that contradict each other or the recipe (a page index
with statistics off; `PAGE_INDEX=0` with `PAGE_INDEX_COLUMNS`; a level for snappy, lz4
or none, or out of the codec's range) are refused for every lane before any writer runs.
A lane refuses, as a failed round-trip naming the variable, a setting its library
cannot honour:

| | py (pyarrow 24) | rs (arrow-rs 59.2) | java (parquet-arrow-java 0.3.0) | hardwood (1.1.0.Beta1) |
|---|---|---|---|---|
| codec | all | all | not `brotli` | all |
| compression level | yes | yes | yes | no |
| statistics off | yes | yes | yes | no |
| statistics, first N columns | yes | yes | yes | no |
| page index on | yes | yes | yes | no |
| page index off | yes | yes | no, with statistics on | yes |
| page index, first N columns | no: all or none | yes | no | no |
| page bytes | yes | yes | yes | yes |
| page rows | yes | yes, checked every 1,024 values | yes | no |
| dictionary on / off | yes | yes | yes | yes (off is PLAIN) |
| dictionary page bytes | yes | yes | yes | no |
| page checksums on | yes | no | yes (its default) | yes (always) |
| page checksums off | yes (its default) | yes (always) | yes | no |

Each library measures a page its own way, so the same `RAINCLOUD_PARQUET_PAGE_BYTES`
does not give identical pages in every lane. The java column's page index gaps are
parquet-java's: it writes a page index for every column that has statistics.

The ORC, Avro and Vortex writers read their own settings the same way
(`spec.FORMAT_SETTINGS`): the build passes each set one, in canonical form, under
the name in `AGENTS.md`'s table, and a lane refuses what its library cannot do. With
none set, every lane writes exactly what it wrote before (the codec is zstd).

| setting | honoured by | refused by |
|---|---|---|
| `RAINCLOUD_ORC_COMPRESSION` (zstd, snappy, zlib, lz4, none) | orc@py, orc@rs | |
| `RAINCLOUD_ORC_COMPRESSION_STRATEGY` | orc@py | orc@rs (no strategy) |
| `RAINCLOUD_ORC_STRIPE_BYTES` | orc@py, orc@rs (each measures a stripe its own way) | |
| `RAINCLOUD_ORC_COMPRESSION_BLOCK_BYTES` | orc@py (multiples of 64 KiB only), orc@rs | |
| `RAINCLOUD_AVRO_COMPRESSION` (zstd, deflate, snappy, bzip2, xz, none) | avro@rs, avro@java | |
| `RAINCLOUD_AVRO_COMPRESSION_LEVEL` | avro@java | avro@rs (arrow-avro has no level) |
| `RAINCLOUD_AVRO_BLOCK_BYTES` | avro@java (Avro's sync interval) | avro@rs (one block per batch) |
| `RAINCLOUD_VORTEX_COMPACT` | vortex@py, vortex@rs (BtrBlocks compact, within the session's editions) | vortex@jni (no write strategy) |
| `RAINCLOUD_VORTEX_ROW_BLOCK_ROWS`, `_DATA_BLOCK_BYTES` | vortex@rs | vortex@py, vortex@jni (no block settings) |

## The Nimble lane

Nimble has one implementation, Meta's C++ (facebookincubator/nimble), with no releases or
packages, so `nimble@cpp` is built from source. [`sidecars/nimble/build.sh`](nimble/build.sh)
builds the lane's two binaries, `raincloud-export-nimble-cpp` and `raincloud-read-nimble-cpp`,
inside a Nimble checkout at a pinned commit: upstream plus build fixes for a
current Linux toolchain (a host `liburing.h` that Folly mistakes for its own, GCC 16's
`<cstdint>`, two Velox libraries a minimal build links but never declares), kept on the
`raincloud` branch of a Nimble fork, [mprammer/nimble](https://github.com/mprammer/nimble).
A cold build needs the network, about 4 GB on disk and several minutes, so CI does not
build it and records the lane as absent.

```bash
git clone --branch raincloud --recurse-submodules https://github.com/mprammer/nimble /path/to/nimble
RAINCLOUD_NIMBLE_SRC=/path/to/nimble sidecars/nimble/build.sh /srv/raincloud-tools/nimble
export RAINCLOUD_SIDECAR_NIMBLE_CPP=/srv/raincloud-tools/nimble/bin/raincloud-export-nimble-cpp
export RAINCLOUD_READER_NIMBLE_CPP=/srv/raincloud-tools/nimble/bin/raincloud-read-nimble-cpp
```

The binaries are C++ over upstream Nimble's `VeloxWriter` (default options) and
`VeloxReader` (the file read as the type it records), and they link `nimble-ffi`, a
member of the Rust crate, which runs the sidecar contract as every Rust lane does: it
reads the canonical with arrow-rs, hands its batches to the writer in memory through the
Arrow C stream interface, takes the read-back the same way, compares and reports. Velox's
own Arrow bridge imports and exports the batches; nothing converts a column. The build
links both halves against the host's libzstd, so the binary carries one zstd.

## How each lane judges a round trip

All three comparators check column names and nested shape first, then values. A
row-count difference is reported up front by Python and when a stream ends early
by Rust and the JVM, which compare one batch window at a time. They differ on the
representation changes they can judge:

| Representation change (canonical → artifact) | Python `_roundtrip_verdict` | Rust `logical_eq` | JVM `LogicalCompare` |
|---|---|---|---|
| string / large_string / string_view | pass | pass | pass |
| integer width or signedness | pass if the values are exact | pass if the values are exact | pass if exact (BigInteger) |
| float width (incl. half) | pass only if reversible | pass only if reversible | compares decoded values bitwise, so the same pass/fail |
| timestamp unit, same timezone | pass if the instant survives | pass if the instant survives | compares the instant: pass or fail |
| timestamp timezone relabelled (both zoned) | pass if the instants match | pass if the instants match | pass if the instants match |
| timestamp timezone dropped or added | fail | fail | fail |
| decimal precision/scale | reversible cast | reversible cast | skip (gap) |
| integer ↔ scale-0 decimal | pass if the values are exact | pass if the values are exact | pass if exact |
| date32 / date64 | reversible cast | reversible cast | skip (gap) |
| time32 / time64, duration unit | reversible cast | reversible cast | skip (gap) |
| dictionary ↔ plain, top level | pass | pass | decoded to values, then compared |
| dictionary inside a nested column | pass | pass | skip (gap) |
| struct with duplicate child names | compared | compared | skip (gap) |
| union of `null` and `T` ↔ `T` | compared as `T` | compared as `T` | compared as `T` |
| any other union ↔ non-union | fail | fail | fail |
| fixed-size ↔ variable binary/list | reversible cast | reversible cast | compared by value |

A JVM `skip` means the JVM lane is unmeasured for that cell, never that the
artifact is suspect. When a JVM writer's own self-verify hits such a gap it
reports `"roundtrip": null` (unmeasured) rather than a failure.

Every lane compares one batch from each side at a time, so memory does not grow
with the table. The JVM lanes box each cell as a heap object, several times its
Arrow size, so a very wide or very large batch can still exhaust the default JVM
heap, or Arrow's off-heap allocator. A JVM reader records that as `skip`
("comparator resource limit (out of memory)") rather than `fail`, and a JVM
writer's self-verify as `"roundtrip": null`, keeping the file it wrote; raise `-Xmx` or `-XX:MaxDirectMemorySize` through `JAVA_OPTS` to measure
it. An out-of-memory error a library wraps counts the same. "Requested array size
exceeds VM limit" does not: it is the implementation asking for an array longer than
any JVM allows, which no `-Xmx` fixes, so a reader records it as `fail` ("read error:
requested an array past the JVM's array size limit") and a writer as
`"roundtrip": false`, whether it came from the write or the self-verify.

The Rust Parquet lane (the parquet@rs reader, and the parquet@rs writer's
self-verify) sizes its read batches from the file's row-group metadata: about
256 MiB decoded per batch, at most 65,536 rows, never less than one row, and
never spanning a row group larger than that. arrow-rs decodes a binary or string
column into an array with 32-bit offsets, so a batch holding 2 GiB of one column
cannot be read; a fixed row count would put all 2.46 GB of
peoples-speech-clean-validation's audio in one batch. The plan sees each group's
average row, so only a row group over 2 GiB whose rows are very uneven can still
overflow, and the error then names the row groups and the batch size it planned.

## VARIANT in the Parquet lanes

A canonical VARIANT column is its storage struct (`metadata`, `value`) carrying
the Arrow canonical extension `arrow.parquet.variant`. Each Parquet writer hands
that to its library as it is, and reports `variant_faithful` as measured on the
file it wrote: true when the file declares Parquet's VARIANT logical type on the
column and the lane's own reader reads it back as the extension, and otherwise
false with a note naming what was not kept (`ARROW:schema` alone carries the
extension name through a plain group, so the read-back is not enough by itself).
A canonical with no VARIANT column is `true`.

- parquet@rs: arrow-rs 59.2 maps the extension to the logical type, both ways,
  with its `variant_experimental` feature, which `Cargo.toml` enables.
- parquet@java: parquet-arrow-java annotates the group with parquet-java's
  `VariantLogicalTypeAnnotation` and reads a VARIANT group back as the extension.
- parquet@hardwood: Hardwood's schema builders attach no logical type to a
  group, so the lane annotates the group through `FileSchema.toSchemaElements` /
  `fromSchemaElements`, Hardwood's other public route to a schema, and its reader
  maps a group Hardwood reports as VARIANT to the extension. Hardwood 1.1.0.Beta1's
  `ColumnBatch.struct` takes no validity for a VARIANT group, so a column with a
  null VARIANT row is `"roundtrip": false`, `"unsupported type"`, no file, with
  Hardwood's message: its limit, never written as a plain struct instead.

vortex@py, vortex@rs and vortex@jni still hand Vortex the storage struct without
the extension and report `variant_faithful: false`.

## Memory leaks

A JVM lane closes the allocator it read and wrote through; memory still held
then (a leak in the lane or in the library under it) is recorded in the
report's note ("memory leak: allocator ROOT still held N B at close (...)") and
also printed to stderr. It leaves the verdict as it is: a leak says buffers were
not released, not that the data is wrong.

## The Hardwood and Vortex JNI lanes

`parquet@hardwood` writes and reads Parquet through
[Hardwood](https://github.com/hardwood-hq/hardwood) (`dev.hardwood:hardwood-core`,
pinned as `hardwoodVersion` in `sidecars/java/gradle.properties`). Hardwood 1.0
only reads; writing arrived in 1.1.0.Beta1, the newest release, which both lanes
use. Hardwood has no Arrow API, so the lane maps Arrow to Hardwood's columnar
writer and reader itself, following the Arrow C++ and Rust type mapping, and
writes no `ARROW:schema` footer. What it cannot carry is reported, never guessed:

- The writer refuses durations, intervals, unions, run-end encoding, list
  views, a dictionary inside a nested column, duplicate sibling names, and a
  field name containing `.` above a struct, list or map (Hardwood addresses those
  by dotted path), and a null VARIANT row (see above): `"roundtrip": false`,
  `"unsupported type"`, no file. SECOND
  times and date64 are written (as MILLIS and days) but read back in another unit,
  a comparator gap, so the round trip is unmeasured. A timezone other than UTC
  is not kept (Parquet records only "adjusted to UTC"), but the instants are, and
  zoned timestamps compare by instant.
- The reader reports `INT96`, `INTERVAL`, a key-only map, a repeated field
  outside a `LIST` or `MAP`, and a layer layout it does not expect as a
  comparator gap (`skip`). It bundles zstd, snappy, lz4 and brotli (brotli4j, with
  its native library for Linux and macOS on x86-64 and aarch64).
- Hardwood 1.1.0.Beta1 fails to read a page header whose statistics are longer
  than its first 1 KiB read of the header: it raises "Malformed Parquet metadata"
  where it means to read further (fixed after the release by hardwood bdecd568,
  #1104, not yet released). pyarrow writes page statistics up to 4 KiB, so a
  parquet@py file with long strings, such as finepdfs-en-test's `text`, is a
  measured `fail` for this reader until the pin moves past the fix. On 2026-09-24
  that was 14 of the 304 store Parquet files under 100 MB.
- Hardwood 1.1.0.Beta1 sizes its read batches assuming 16 bytes for every binary
  or string value, whatever the column holds. peoples-speech-clean-validation
  (2.46 GB of audio in 18,622 rows) then becomes one batch whose value buffer is
  past the JVM's array size limit: a measured `fail` for this reader. The lane
  uses Hardwood's own batch sizing and does not work around it.

The row cap binds exactly: Hardwood cuts a row group at `RAINCLOUD_ROW_GROUP_MAX_ROWS`
rows (the recipe's `write.row_group_size_rows` when it declares one), across batch
boundaries, so its row-group plan matches parquet@rs's. The byte target is
Hardwood's `rowGroupBufferTargetBytes`, which measures what the writer holds for
the open row group (level streams, dictionary indices, value stores and
dictionaries) rather than the encoded size the Python and Rust lanes measure, so
the same `RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES` can give differently sized
groups. Hardwood has no setting that measures encoded bytes.

The `vortex@jni` writer hands the canonical's batches to vortex-jni's
`VortexWriter` over the Arrow C Data Interface, with the same vortex-jni as the
reader, and self-verifies through that reader. Like vortex@py and vortex@rs it
writes a VARIANT column as its storage struct. Top-level dictionaries are handed
over decoded, since vortex-jni exports the writer's schema without dictionaries;
Vortex picks its own encodings either way. A type Vortex refuses (fixed-size
binary, durations, intervals, unions in the 0.86 line) is `"roundtrip": false`
with Vortex's reason and no file. vortex-jni 0.86.1 never releases the schema its
writer builder exports; the lane gives that export an allocator of its own, so
the few hundred bytes a column it keeps do not mask a leak of the lane's own
(which is noted, as above).

## Update deliberately

Keep an existing working installation until the replacement passes a small
compliance run. Install a new revision into another prefix, select its launcher
paths, then run `python -m raincloud.pipeline.compliance --help` for matrix options.
Ordinary dataset reads do not run this matrix. The small Seeds dataset is suitable
for verification without a full-catalog rebuild.

Python Vortex, the Rust sidecars/native reader core, and Java Vortex JNI use the
0.86 line together (`pyproject.toml`, both `Cargo.toml` files and
`sidecars/java/gradle.properties`; `tests/test_native_protocol.py` checks they agree).
Upgrade their pins together and re-measure supported cells; a package version
change alone is not evidence of conformance. Preserve skipped and unsupported
lanes in the results. Software updates do not move stored data or refresh an
active catalog revision.
