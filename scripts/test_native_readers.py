# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Build and exercise the native reader APIs, and the sidecars' conformance lanes.

Needs Cargo, CMake, a C/C++ compiler, JDK 17 (and a Java 21 toolchain for the
parquet@hardwood lanes, which Gradle provisions when none is installed) and the
parquet-arrow-java submodule (`git submodule update --init --recursive`). Linux only
for now.

Native clients, against one shared fixture (`tests/reader_fixture.py`):
  - the Rust crate's tests, with and without the `vortex` feature;
  - the C ABI through ctypes, including a build without Vortex, and every other
    pytest module in `PYTEST_MODULES`;
  - C and C++ consumers compiled directly, and again through the installed CMake
    package (`find_package(raincloud)`), with Vortex on and off, checking that the
    installed library carries its SONAME and that consumers record the bare name;
  - the Java library's tests, and its installed distribution running
    clients/java/examples/Read.java.

Sidecars: builds the Rust and Java conformance binaries, which the modules in
`PYTEST_MODULES` then use.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Every pytest module this runner covers, with the native library, the fixture
# and the sidecars in place.
PYTEST_MODULES = (
    "tests/test_native_clients.py", "tests/test_dataset_readers.py", "tests/test_rust_client_recipe_parity.py",
    "tests/test_reader_fidelity.py", "tests/test_schema_conformance.py",
    "tests/test_publication_recovery.py", "tests/test_reader_publication.py",
    "tests/test_client_boundaries.py", "tests/test_export_conformance.py",
    "tests/test_rust_sidecars.py", "tests/test_native_protocol.py", "tests/test_jvm_sidecar_lanes.py",
    "tests/test_parquet_variant_sidecars.py", "tests/test_parquet_java_list_naming.py",
    "tests/test_variant_storage_nullability.py",
)

LIBRARY = "libraincloud_reader.so"


def dynamic_entries(path, kind):
    """The `kind` entries (SONAME, NEEDED) of an ELF file's dynamic section."""
    out = subprocess.run(["readelf", "-d", str(path)], check=True, stdout=subprocess.PIPE, text=True).stdout
    return re.findall(rf"\({kind}\)\s.*\[(.+)\]", out)


def main():
    import pyarrow as pa

    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    from tests.reader_fixture import create
    # This runner currently validates Linux binaries. No publication/installation.
    if sys.platform != "linux":
        raise SystemExit("native reader verification runner currently targets Linux")
    target = Path(os.environ.get("CARGO_TARGET_DIR", root / ".tmp/native-target")).resolve()
    env = {**os.environ, "CARGO_TARGET_DIR": str(target)}

    def run(args, **kwargs):
        subprocess.run([str(a) for a in args], cwd=kwargs.pop("cwd", root), env=kwargs.pop("env", env),
                       check=True, **kwargs)

    client = ["--locked", "--manifest-path", "clients/rust/Cargo.toml"]
    run(["cargo", "build", *client])
    # Without Vortex, in its own target directory so neither build evicts the other.
    no_vortex_target = target / "no-vortex"
    run(["cargo", "build", *client, "--no-default-features", "--target-dir", no_vortex_target])
    run(["cargo", "build", "--locked", "--manifest-path", "sidecars/rust/Cargo.toml",
         "--bin", "parquet-read", "--bin", "vortex-read", "--bin", "parquet-write", "--bin", "vortex-write",
         "--bin", "orc-read", "--bin", "orc-write", "--bin", "avro-read", "--bin", "avro-write",
         "--bin", "nimble-read", "--bin", "nimble-write"])
    lib = target / "debug"
    for binary in ("parquet-read", "vortex-read", "parquet-write", "vortex-write", "orc-read", "orc-write",
                   "avro-read", "avro-write"):
        if not os.access(lib / binary, os.X_OK):
            raise RuntimeError(f"required conformance reader is not executable: {lib / binary}")
    if not env.get("JAVA_HOME") and shutil.which("java"):
        env["JAVA_HOME"] = str(Path(shutil.which("java")).resolve().parent.parent)
    # parquet-hardwood builds on a Java 21 toolchain (Gradle provisions one if none is
    # installed); its launchers default to that JDK when JAVA_HOME names an older one.
    run(["bash", "sidecars/java/gradlew", "-p", "sidecars/java", ":parquet-java:installDist",
         ":parquet-hardwood:installDist", ":vortex-jni-reader:installDist", ":avro-java:installDist",
         "--no-daemon"])
    install = root / "sidecars/java"
    parquet_java = install / "parquet-java/build/install/raincloud-export-parquet-java/bin"
    hardwood = install / "parquet-hardwood/build/install/raincloud-export-parquet-hardwood/bin"
    vortex_jni = install / "vortex-jni-reader/build/install/raincloud-read-vortex-jni/bin"
    avro_java = install / "avro-java/build/install/raincloud-export-avro-java/bin"
    env.update(
        RAINCLOUD_SIDECAR_VORTEX_RS=str(lib / "vortex-write"),
        RAINCLOUD_SIDECAR_PARQUET_JAVA=str(parquet_java / "raincloud-export-parquet-java"),
        RAINCLOUD_READER_PARQUET_JAVA=str(parquet_java / "raincloud-read-parquet-java"),
        RAINCLOUD_SIDECAR_PARQUET_HARDWOOD=str(hardwood / "raincloud-export-parquet-hardwood"),
        RAINCLOUD_READER_PARQUET_HARDWOOD=str(hardwood / "raincloud-read-parquet-hardwood"),
        RAINCLOUD_SIDECAR_VORTEX_JNI=str(vortex_jni / "raincloud-export-vortex-jni"),
        RAINCLOUD_READER_VORTEX_JNI=str(vortex_jni / "raincloud-read-vortex-jni"),
        RAINCLOUD_SIDECAR_AVRO_JAVA=str(avro_java / "raincloud-export-avro-java"),
        RAINCLOUD_READER_AVRO_JAVA=str(avro_java / "raincloud-read-avro-java"),
    )
    (root / ".tmp").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="raincloud-reader-", dir=root / ".tmp") as temp:
        temp = Path(temp)
        _, options = create(temp)
        # The same catalog with nothing prepared: every consumer checks a typed miss.
        missing = temp / "missing.json"
        missing.write_text(json.dumps({**options, "data_dir": str(temp / "missing")}))
        # Native readers dispatch resolution to this interpreter's CLI. It is
        # named explicitly, so the C/C++ probes below still run with no PATH.
        env.update(RAINCLOUD_CLI=str(Path(sys.executable).with_name("raincloud")),
                   RAINCLOUD_TEST_FIXTURE=str(temp),
                   RAINCLOUD_NATIVE_LIBRARY=str(lib / "libraincloud_reader.so"),
                   RAINCLOUD_NATIVE_LIBRARY_NO_VORTEX=str(no_vortex_target / "debug/libraincloud_reader.so"),
                   RAINCLOUD_READER_PARQUET_RS=str(lib / "parquet-read"),
                   RAINCLOUD_SIDECAR_PARQUET_RS=str(lib / "parquet-write"),
                   RAINCLOUD_READER_VORTEX_RS=str(lib / "vortex-read"),
                   RAINCLOUD_SIDECAR_ORC_RS=str(lib / "orc-write"),
                   RAINCLOUD_READER_ORC_RS=str(lib / "orc-read"),
                   RAINCLOUD_SIDECAR_AVRO_RS=str(lib / "avro-write"),
                   RAINCLOUD_READER_AVRO_RS=str(lib / "avro-read"),
                   # nimble@cpp also needs RAINCLOUD_NIMBLE_TOOL (sidecars/nimble/build.sh),
                   # which this runner does not build: without it the lane is absent.
                   RAINCLOUD_SIDECAR_NIMBLE_CPP=str(lib / "nimble-write"),
                   RAINCLOUD_READER_NIMBLE_CPP=str(lib / "nimble-read"))
        run(["cargo", "test", *client])
        run(["cargo", "test", *client, "--no-default-features", "--target-dir", no_vortex_target])
        run([sys.executable, "-m", "pytest", *PYTEST_MODULES, "-q"])

        include = root / "clients/c/include"
        arrow_dir = Path(pa.get_library_dirs()[0])
        arrow_lib = sorted(arrow_dir.glob("libarrow.so*"), key=lambda p: len(p.name))[0]
        run(["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "clients/c/tests/read.c", f"-I{include}", f"-L{lib}", "-lraincloud_reader", f"-Wl,-rpath,{lib}", "-o", temp / "read-c"])
        run(["c++", "-std=c++17", "-Wall", "-Wextra", "-Werror", "clients/c/tests/read.cpp", f"-I{include}", "-isystem", pa.get_include(), f"-L{lib}", "-lraincloud_reader", f"-Wl,-rpath,{lib}", arrow_lib, f"-Wl,-rpath,{arrow_dir}", "-o", temp / "read-cpp"])
        for exe in ("read-c", "read-cpp"):
            run([temp / exe, temp / "options.json", missing], env={**env, "PATH": "/nonexistent"})

        # The installed CMake package, as a downstream project consumes it. A
        # Release consumer build proves the checks do not depend on assert().
        from raincloud import __version__
        version = re.match(r"\d+\.\d+\.\d+", __version__).group()
        for vortex in ("ON", "OFF"):
            build, prefix, consumer = (temp / f"cmake-{vortex}-{part}" for part in ("build", "prefix", "consumer"))
            run(["cmake", "-S", "clients/c", "-B", build, f"-DCMAKE_INSTALL_PREFIX={prefix}",
                 f"-DRAINCLOUD_VORTEX={vortex}", f"-DRAINCLOUD_CARGO_TARGET={target / f'cmake-{vortex}'}"])
            run(["cmake", "--build", build])
            run(["cmake", "--install", build])
            run(["cmake", "-S", "clients/c/tests", "-B", consumer, "-DCMAKE_BUILD_TYPE=Release",
                 f"-DCMAKE_PREFIX_PATH={prefix}", f"-DARROW_INCLUDE={pa.get_include()}",
                 f"-DARROW_LIBRARY={arrow_lib}", f"-DRAINCLOUD_EXPECTED_VERSION={version}"])
            run(["cmake", "--build", consumer])
            # The build tree's run path finds the library either way; only the
            # bare name keeps a consumer working once the install moves.
            installed = sorted(prefix.glob(f"lib*/{LIBRARY}"))
            if len(installed) != 1 or dynamic_entries(installed[0], "SONAME") != [LIBRARY]:
                raise RuntimeError(f"installed {installed} does not carry SONAME {LIBRARY}")
            for exe in ("read-c", "read-cpp"):
                if LIBRARY not in dynamic_entries(consumer / exe, "NEEDED"):
                    raise RuntimeError(f"{consumer / exe} does not record {LIBRARY} by its bare name")
            extra = [] if vortex == "ON" else ["no-vortex"]
            for exe in ("read-c", "read-cpp"):
                run([consumer / exe, temp / "options.json", missing, *extra], env={**env, "PATH": "/nonexistent"})

        run(["bash", "sidecars/java/gradlew", "-p", "clients/java", "test", "installDist", "--rerun-tasks",
             "--no-daemon", f"-Draincloud.native.path={lib}", f"-Draincloud.fixture={temp}",
             f"-PraincloudNativeLibrary={lib / 'libraincloud_reader.so'}"])
        # The installed distribution, compiled against and run as a user would.
        dist = root / "clients/java/build/install/raincloud-reader"
        classes = temp / "read-java"
        run(["javac", "-cp", f"{dist}/lib/*", "-d", classes, "clients/java/examples/Read.java"])
        run(["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", f"-Djna.library.path={dist}/native",
             "-cp", f"{classes}:{dist}/lib/*", "Read", temp / "options.json"])


if __name__ == "__main__":
    main()
