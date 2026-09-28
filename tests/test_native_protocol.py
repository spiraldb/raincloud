# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Native-client and sidecar contracts.

The native tests run when RAINCLOUD_NATIVE_LIBRARY names the built cdylib;
RAINCLOUD_NATIVE_LIBRARY_NO_VORTEX names one built with --no-default-features.
The cross-file checks below them need only the checkout.
"""
import ctypes as c
import json
import os
import re
import sys
import tomllib
from pathlib import Path

import pyarrow as pa
import pytest

from raincloud import exceptions
from tests.reader_fixture import create
from tests.test_native_clients import Native, NativeError, Stream
from tests.test_native_clients import native as native  # noqa: F401 (fixture)

ROOT = Path(__file__).resolve().parent.parent
FORMATS = ("arrow", "parquet", "vortex")


def _artifact(options, fmt):
    return next((Path(options["data_dir"]) / "v2/tiny" / fmt).iterdir())


def _read_all(native, handle, batch_size=3):
    stream = Stream()
    native.call("raincloud_batches", handle, batch_size, c.byref(stream))
    with pa.RecordBatchReader._import_from_c(c.addressof(stream)) as reader:
        return reader.read_all()


# --- settings never reach a command line ---------------------------------

def test_settings_reach_the_cli_through_its_environment_not_its_argv(native, tmp_path):
    secret = "s3cr3t-token"
    calls = tmp_path / "calls.jsonl"
    fake = tmp_path / "raincloud"
    fake.write_text(f"""#!{sys.executable}
import json, os, sys
with open({str(calls)!r}, "a") as f:
    f.write(json.dumps({{"argv": sys.argv[1:], "env": os.environ.get("RAINCLOUD_SETTINGS"),
                        "cmdline": open("/proc/self/cmdline", "rb").read().decode()}}) + "\\n")
print(json.dumps({{"slug": "tiny", "format": "parquet", "catalog_source": "checkout",
                  "catalog_revision": "{"a" * 64}", "path": "/data/tiny.parquet"}}))
""")
    fake.chmod(0o755)
    mirror = f"https://reader:{secret}@mirror.example/v2?X-Amz-Signature={secret}"
    handle = native.open({"cli": str(fake), "mirror": mirror}, "auto")
    try:
        assert native.string("raincloud_path", handle) == "/data/tiny.parquet"
    finally:
        native.close(handle)
    records = [json.loads(line) for line in calls.read_text().splitlines()]
    assert len(records) == 2
    for record in records:
        assert secret not in " ".join(record["argv"]) and secret not in record["cmdline"]
        assert "--settings-env" in record["argv"]
        assert json.loads(record["env"])["mirror"] == mirror
    assert records[0]["argv"][-2:] == ["--", "tiny"]


# --- error codes -------------------------------------------------

@pytest.mark.parametrize("fmt", FORMATS)
def test_same_size_damage_is_a_corrupt_artifact(native, tmp_path, fmt):
    # Same-size damage passes the catalog's size check and must fail to decode:
    # code 14, never UNSUPPORTED_TYPE (11), whose remedy (another format) is wrong.
    _, options = create(tmp_path)
    path = _artifact(options, fmt)
    path.write_bytes(bytes(path.stat().st_size))
    handle = native.open(options, fmt)
    try:
        with pytest.raises(NativeError) as error:
            _read_all(native, handle)
        assert error.value.code == 14, error.value
    finally:
        native.close(handle)


def test_a_failing_mirror_is_a_transport_error(native, tmp_path):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    class Failing(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_error(500, "mirror is down")

        do_HEAD = do_GET

        def log_message(self, *args):
            pass

    _, options = create(tmp_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Failing)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    options.update(mirror=f"http://127.0.0.1:{server.server_port}", data_dir=str(tmp_path / "absent"), offline=False)
    handle = native.open(options, "parquet")
    try:
        with pytest.raises(NativeError) as error:
            native.string("raincloud_path", handle)
        assert error.value.code == 10, error.value
    finally:
        native.close(handle)
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("slug,code", [("no-such-dataset", 4), ("-x", 4), ("--help", 4), ("", 1)])
def test_slugs_are_names_never_options(native, tmp_path, slug, code):
    _, options = create(tmp_path)
    with pytest.raises(NativeError) as error:
        native.open(options, "auto", slug=slug)
    assert error.value.code == code, error.value


@pytest.mark.parametrize("fmt", ["-x", "--readers", ""])
def test_formats_are_values_never_options(native, tmp_path, fmt):
    # Refused natively as INVALID_ARGUMENT, not by argparse as a usage error.
    _, options = create(tmp_path)
    with pytest.raises(NativeError) as error:
        native.open(options, fmt)
    assert error.value.code == 1, error.value


# --- format choice belongs to the native build -----------------------------

def test_auto_chooses_among_native_readers_not_pythons(native, tmp_path, monkeypatch):
    # The CLI's Python has no Vortex reader here; the native library does.
    blocker = tmp_path / "no-vortex"
    blocker.mkdir()
    (blocker / "sitecustomize.py").write_text("import sys\nsys.modules['vortex'] = None\n")
    monkeypatch.setenv("PYTHONPATH", str(blocker))
    table, options = create(tmp_path / "fixture")
    handle = native.open(options, "auto")
    try:
        assert native.metadata(handle)["format"] == "vortex"
        assert _read_all(native, handle).cast(table.schema).equals(table)
    finally:
        native.close(handle)


def test_a_build_without_vortex_never_auto_selects_it(tmp_path, monkeypatch):
    path = os.environ.get("RAINCLOUD_NATIVE_LIBRARY_NO_VORTEX")
    if not path:
        pytest.skip("set RAINCLOUD_NATIVE_LIBRARY_NO_VORTEX to a --no-default-features build")
    monkeypatch.setenv("RAINCLOUD_CLI", str(Path(sys.executable).with_name("raincloud")))
    lib = Native(path)
    table, options = create(tmp_path)
    handle = lib.open(options, "auto")
    try:
        assert lib.metadata(handle)["format"] == "parquet"
        assert _read_all(lib, handle).equals(table)
    finally:
        lib.close(handle)
    # Asked for by name, Vortex still resolves to a path; only decoding it is refused.
    handle = lib.open(options, "vortex")
    try:
        assert Path(lib.string("raincloud_path", handle)).is_file()
        with pytest.raises(NativeError) as error:
            _read_all(lib, handle)
        assert error.value.code == 5
    finally:
        lib.close(handle)


# --- a handle keeps its catalog generation ---------------------------------

def test_a_handle_refuses_a_catalog_that_changed_after_it_opened(native, tmp_path):
    _, options = create(tmp_path)
    catalog = Path(options.pop("catalog"))
    options.update(manifest=str(catalog / "sources.json"), snapshot=str(catalog / "snapshot.json"))
    handle = native.open(options, "arrow")
    try:
        assert Path(native.string("raincloud_path", handle)).is_file()
        manifest = json.loads((catalog / "sources.json").read_text())
        manifest["datasets"][0]["description"] = "edited after the handle opened"
        (catalog / "sources.json").write_text(json.dumps(manifest))
        with pytest.raises(NativeError) as error:
            native.string("raincloud_path", handle)
        assert error.value.code == 2, error.value
    finally:
        native.close(handle)


# --- one error vocabulary across Python, Rust, C and Java ---------------

def _rust_codes():
    text = (ROOT / "clients/rust/src/error.rs").read_text()
    body = text[text.index("pub enum ErrorKind {"):text.index("}", text.index("pub enum ErrorKind {"))]
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", name).upper(): int(code)
            for name, code in re.findall(r"^\s*(\w+) = (\d+),", body, re.M)}


def _python_table():
    text = (ROOT / "clients/rust/src/error.rs").read_text()
    body = text[text.index("fn from_python_class"):]
    body = body[:body.index("_ => return None")]
    table = {}
    for names, kind in re.findall(r'^\s*((?:"\w+"(?: \| )?)+) => Self::(\w+),', body, re.M):
        for name in re.findall(r'"(\w+)"', names):
            table[name] = re.sub(r"(?<!^)(?=[A-Z])", "_", kind).upper()
    return table


def test_error_codes_agree_across_rust_c_and_java():
    rust = _rust_codes()
    header = (ROOT / "clients/c/include/raincloud.h").read_text()
    c_codes = {name: int(code) for name, code in re.findall(r"RAINCLOUD_(\w+)=(\d+)", header) if name != "OK"}
    java = (ROOT / "clients/java/src/main/java/dev/raincloud/RaincloudException.java").read_text()
    java_codes = {name: int(code) for name, code in re.findall(r"\b([A-Z_]+)\((\d+)\)", java)}
    assert rust == c_codes == java_codes
    assert sorted(rust.values()) == list(range(1, len(rust) + 1))


def test_every_reader_error_has_its_native_code():
    table = _python_table()
    for name in table:
        assert name in {"ValueError", "OSError"} or isinstance(getattr(exceptions, name, None), type), (
            f"error.rs maps {name}, which raincloud.exceptions does not define")
    # Raised only by builds, publishes or Python-only APIs: INTERNAL natively.
    internal = {"RaincloudError", "BuildToolingMissing", "BuildFailed", "UnknownColumn"}
    classes = [cls for cls in vars(exceptions).values()
               if isinstance(cls, type) and issubclass(cls, exceptions.RaincloudError)]
    for cls in classes:
        mapped = next((table[base.__name__] for base in cls.__mro__ if base.__name__ in table), "INTERNAL")
        if cls.__name__ in internal:
            assert mapped == "INTERNAL", cls
        else:
            assert cls.__name__ in table, f"{cls.__name__} reaches native callers as INTERNAL; map it in error.rs"
    assert table["MirrorUnavailable"] == "TRANSPORT"
    assert table["CorruptArtifact"] == "CORRUPT_ARTIFACT"


# --- one knob grammar in every lane --------------------------------------

def test_python_reads_the_shared_knob_cases(monkeypatch):
    # sidecars/knob_cases.json is also read by the Rust and Java lanes' tests.
    from raincloud.pipeline import spec

    cases = json.loads((ROOT / "sidecars/knob_cases.json").read_text())["cases"]
    for case in cases:
        monkeypatch.setenv("RAINCLOUD_TEST_KNOB", case["raw"])
        if "error" in case:
            with pytest.raises(ValueError) as error:
                spec._env_count("RAINCLOUD_TEST_KNOB", 7.0)
            message = str(error.value)
            assert "RAINCLOUD_TEST_KNOB=" in message and case["error"] in message, (case, message)
        else:
            assert spec._env_count("RAINCLOUD_TEST_KNOB", 7.0) == case["value"], case
    monkeypatch.delenv("RAINCLOUD_TEST_KNOB")
    assert spec._env_count("RAINCLOUD_TEST_KNOB", 7.0) == 7


# --- pins and versions ---------------------------------------------------

def test_cmake_package_version_is_derived_not_declared():
    text = (ROOT / "clients/c/CMakeLists.txt").read_text()
    assert "raincloud/__init__.py" in text
    assert not re.search(r"project\(raincloud VERSION \d", text), "CMakeLists.txt declares its own version"


def _gradle_property(name):
    # The JVM sidecars' dependency versions live in one file, read by every subproject.
    text = (ROOT / "sidecars/java/gradle.properties").read_text()
    match = re.search(rf"^{re.escape(name)}=(\S+)$", text, re.M)
    assert match, f"no {name} in sidecars/java/gradle.properties"
    return match.group(1)


def test_vortex_pins_agree_across_python_rust_and_jvm():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    specs = [spec for group in [pyproject["project"].get("dependencies", []),
                                *pyproject["project"].get("optional-dependencies", {}).values()]
             for spec in group if spec.startswith("vortex-data")]
    python = {re.search(r"==\s*([0-9][\w.]*)", spec).group(1) for spec in specs}
    rust = {tomllib.loads((ROOT / crate).read_text())["dependencies"]["vortex"]["version"].lstrip("=")
            for crate in ("clients/rust/Cargo.toml", "sidecars/rust/Cargo.toml")}
    assert "dev.vortex:vortex-jni:$vortexJniVersion" in (ROOT / "sidecars/java/vortex-jni-reader/build.gradle.kts").read_text()
    jvm = {_gradle_property("vortexJniVersion")}
    assert len(python) == 1 and python == rust == jvm, (python, rust, jvm)


# --- recipes name the writer the catalog recorded --------------------------

def test_recipe_writer_priority_names_the_recorded_writer():
    # A recipe whose priority puts another writer first would change its
    # artifact (and sha) on any machine where that writer is installed.
    from raincloud import _formats

    manifest = json.loads((ROOT / "sources.json").read_text())
    snapshot = json.loads((ROOT / f"docs/v{manifest['schema_version']}/snapshot.json").read_text())["slugs"]
    wrong = []
    for spec in manifest["datasets"]:
        for fmt in ("parquet", "vortex"):
            recorded = snapshot.get(spec["slug"], {}).get(f"{fmt}_writer")
            if recorded is None:
                continue
            priority = _formats.export_priority(spec, manifest, None, fmt=fmt)
            first = next((w for w in priority if w in _formats.WRITERS[fmt]), None)
            if first != recorded:
                wrong.append((spec["slug"], fmt, first, recorded))
    assert not wrong, wrong[:10]
