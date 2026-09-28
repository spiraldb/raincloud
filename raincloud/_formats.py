# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Lightweight manifest export policy, shared by the loader and build pipeline."""
from __future__ import annotations

from ._registry import exporter_cells

# Formats a dataset exports unless its spec says otherwise. Canonical Arrow is
# not among them: it is what every exporter reads.
DEFAULT_FORMATS = ("parquet", "vortex")

# Which writer produces a format, when neither the spec, the catalog nor the
# machine names one. Python first: it is always installed, so by default a
# dataset's bytes do not depend on which sidecars a machine happens to have. A
# spec that wants another writer says so (`export.priority`), and a machine
# without that writer falls through to the next one. Whichever writer ran, the
# catalog records it beside the file's sha256; the file itself always lives at
# `<fmt>/`, so the writer is provenance, not part of the address.
#
# Names a writer per entry, not a cell: the same order serves every format, and
# a writer absent for a format (`java` has no vortex cell) is simply skipped. At
# run time an unknown name is skipped too, so a machine's
# RAINCLOUD_EXPORT_PRIORITY may name a writer this release does not ship. A
# manifest is stricter: validate_manifest rejects a name with no export cell,
# since there a typo would silently fall through to the next writer.
DEFAULT_EXPORT_PRIORITY = ("py", "rs", "java", "canonical")


def priority_shape_error(value, where: str) -> str | None:
    """Why `value` is not a writer order, or None when it is (or is absent).

    The one shape rule for `export.priority` and a catalog's `export_priority`:
    a non-empty list of writer names, which serves every format, or a non-empty
    map from exported format to such a list. Empty is an error rather than
    "fall through": omitting the key already says that, so an empty one is a
    mistake. Writer NAMES are not checked here; validate_manifest checks them
    for a manifest being authored, and at run time an unknown one is skipped.
    """
    if value is None:
        return None
    orders = [value]
    if isinstance(value, dict):
        if not value or not set(value) <= set(EXPORTED_FORMATS):
            return f"{where} map must name exported formats ({', '.join(EXPORTED_FORMATS)})"
        orders = list(value.values())
    if not all(isinstance(order, (list, tuple)) and order and all(isinstance(w, str) for w in order)
               for order in orders):
        return f"{where} must be a non-empty list of writer names or a {{format: [writers]}} map; got {value!r}"
    return None


def _priority_for(value, fmt: str | None, where: str) -> tuple[str, ...] | None:
    """One level's writer order for `fmt`, or None to fall through.

    A list applies to every format; a map `{format: [writers]}` applies per
    format, so a format it does not name -- or no format at all -- falls through.
    """
    error = priority_shape_error(value, where)
    if error:
        raise ValueError(error)
    if isinstance(value, dict):
        value = value.get(fmt) if fmt is not None else None
    return tuple(value) if value else None


def export_priority(spec: dict | None = None, manifest: dict | None = None,
                    config=None, *, fmt: str | None = None) -> tuple[str, ...]:
    """Writer preference for `fmt`, most specific source first.

    Three levels, because the right answer differs at each: one file may need a
    particular writer, a catalog may standardise on one, and a machine may only
    have one installed.

        spec["export"]["priority"]   per file
        manifest["export_priority"]  per catalog (the group of files)
        config.export_priority       per machine, from RAINCLOUD_EXPORT_PRIORITY
        DEFAULT_EXPORT_PRIORITY      built in

    The spec and catalog levels take either a list of WRITER names ("rs", "py"),
    which serves every format, or a map from format to such a list
    (`{"parquet": ["rs", "py"]}`), which serves only the formats it names; any
    other format -- or a call without `fmt` -- falls through to the next level.
    """
    slug = (spec or {}).get("slug", "<spec>")
    for value, where in (
        (((spec or {}).get("export") or {}).get("priority"), f"{slug}: export.priority"),
        ((manifest or {}).get("export_priority"), "catalog export_priority"),
        # The machine's list is a parsed setting; empty means unset.
        (getattr(config, "export_priority", None) or None, "RAINCLOUD_EXPORT_PRIORITY"),
    ):
        order = _priority_for(value, fmt, where)
        if order:
            return order
    return DEFAULT_EXPORT_PRIORITY


def resolve_export_cell(fmt: str, priority, *, is_available) -> str | None:
    """First installed writer for `fmt`, as a cell id, or None if none is.

    `priority` is the writer order (`export_priority(..., fmt=fmt)`);
    `is_available` decides whether a cell can run -- for a sidecar, whether its
    binary is on PATH (`raincloud.pipeline.export.cell_available`).
    """
    writers = WRITERS.get(fmt, ())
    for writer in priority:
        if writer not in writers:
            continue  # not a writer for this format, or not a writer at all
        cell = f"{fmt}@{writer}"
        if is_available(cell):
            return cell
    return None


def export_formats(spec: dict, version: int = 2) -> list[str]:
    """The formats `spec` exports, each written once, to `<fmt>/`.

    In schema_version 2 `export.formats` is the only declaration: omitted, a
    dataset exports DEFAULT_FORMATS. `convert.vortex` is the schema_version 1
    opt-in. The schema and validate_manifest reject it in a v2 manifest, but
    v2 catalogs released before that rule still carry it, and they must keep
    reading: there a `convert.vortex: false` without `export.formats` still
    means no Vortex.
    """
    if version < 2:
        return ["parquet", "vortex"] if (spec.get("convert") or {}).get("vortex") else ["parquet"]
    requested = (spec.get("export") or {}).get("formats")
    if requested is None and (spec.get("convert") or {}).get("vortex") is False:
        return ["parquet"]
    return list(requested) if requested is not None else list(DEFAULT_FORMATS)


def export_cells(spec: dict, manifest: dict | None = None) -> list[str]:
    """The writer each exported format is DECLARED to use: the first writer in
    that format's priority that exists for it.

    Declaration, not availability. What actually runs on a machine is
    `resolve_export_cell`, which also skips a writer that is not installed.
    The schema_version is the manifest's (v2 without one).
    """
    version = (manifest or {}).get("schema_version", 2)
    cells = []
    for fmt in export_formats(spec, version):
        # A priority naming no writer for this format falls back to the built-in
        # order, which starts with a writer every format has.
        priority = (*export_priority(spec, manifest, fmt=fmt), *DEFAULT_EXPORT_PRIORITY)
        writer = next(w for w in priority if w in WRITERS[fmt])
        cells.append(f"{fmt}@{writer}")
    return cells


def vortex_cells(spec: dict, version: int, manifest: dict | None = None) -> list[str]:
    """Effective Vortex writers: from export.formats in v2, convert.vortex in v1.

    Pass the catalog's `manifest` when there is one: its `export_priority` can
    choose the writer. Without it, the answer is still right about WHETHER the
    dataset has Vortex, but names the writer the spec alone would pick.
    """
    if version < 2:
        return ["vortex@py"] if "vortex" in export_formats(spec, version) else []
    manifest = {**(manifest or {}), "schema_version": version}
    return [cell for cell in export_cells(spec, manifest) if cell.startswith("vortex@")]


def vortex_skip_reason(spec: dict, version: int, snapshot_entry: dict | None = None) -> str | None:
    """Why `spec` has no Vortex file, or None when it has one.

    A MEASURED reason first: the catalog snapshot's `vortex_unavailable`, the
    build that found the writer could not produce the file (`describe_unavailable`).
    Otherwise, for a policy that leaves Vortex out, what the recipe says: v1's
    convert.vortex_skip_reason, or v2's export.notes, which a deliberate
    omission may carry and a released catalog may still hold.
    """
    measured = (snapshot_entry or {}).get("vortex_unavailable")
    if isinstance(measured, dict) and vortex_cells(spec, version):
        return describe_unavailable(measured)
    if vortex_cells(spec, version):
        return None
    if version < 2:
        return (spec.get("convert") or {}).get("vortex_skip_reason")
    return (spec.get("export") or {}).get("notes") or "the export policy leaves Vortex out"


def describe_unavailable(measurement: dict) -> str:
    """One line for an "unavailable" measurement: the writer, the toolchain it
    ran with, when, and its error. Never a remedy: the measurement is a fact
    about that toolchain, and only a different one can change it."""
    toolchain = ", ".join(f"{name} {version}" for name, version in (measurement.get("toolchain") or {}).items()
                          if name != "python")
    when = measurement.get("measured_at")
    return (f"{measurement.get('cell', 'the writer')} could not write it"
            + (f" ({toolchain})" if toolchain else "") + (f", measured {when}" if when else "")
            + f": {measurement.get('error') or 'no error recorded'}")


def buildable_formats(spec: dict, version: int) -> set[str]:
    if version < 2:
        # Legacy catalogs precede the canonical spine and export policy.
        return set(export_formats(spec, version))
    return {"arrow", *export_formats(spec, version)}


def _writers() -> dict[str, tuple[str, ...]]:
    """fmt -> writer names, in declaration order, from `raincloud._registry`'s
    exporter cells, plus the canonical Arrow every exporter reads. The order
    decides nothing: which writer runs is `export_priority`'s answer."""
    writers: dict[str, tuple[str, ...]] = {}
    for cell in exporter_cells():
        base, _, writer = cell.partition("@")
        writers[base] = (*writers.get(base, ()), writer)
    return {**writers, "arrow": ("canonical",)}


WRITERS = _writers()
# The formats a dataset can export (those with an exporter cell), and every
# artifact format: those plus the canonical Arrow they are all written from.
EXPORTED_FORMATS = tuple(fmt for fmt in WRITERS if fmt != "arrow")
ALL_FORMATS = (*EXPORTED_FORMATS, "arrow")


def base_format(fmt: str) -> str:
    return fmt.split("@", 1)[0]


def split_cell(cell: str) -> tuple[str, str]:
    """`parquet@rs` -> ("parquet", "rs"), validating both halves."""
    base, sep, writer = cell.partition("@")
    if not sep or base not in WRITERS or writer not in WRITERS[base]:
        raise ValueError(f"unknown writer cell {cell!r}")
    return base, writer


def select_format(formats, requested: str = "auto") -> str:
    """The format to open: `requested`, or the first present of vortex, parquet,
    arrow for "auto". Each format is one file; which writer made it is recorded
    in the catalog, not chosen here."""
    from .exceptions import FormatUnavailable
    if "@" in requested:
        raise FormatUnavailable(
            f"format {requested!r}: a dataset has one file per format; ask for "
            f"{base_format(requested)!r} (`raincloud describe` shows which writer made it)"
        )
    bases = ("vortex", "parquet", "arrow") if requested == "auto" else (requested,)
    for base in bases:
        if base in formats:
            return base
    raise FormatUnavailable(f"artifact {requested!r} unavailable; have {sorted(formats)}")
