# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Ledger — persist the compliance matrix + the additive-only immutable oracle.

Turns an in-memory `ComplianceReport` into the verbose ledger plus a regression
gate. The serializer core reads no clock (the caller stamps `generated_at` and
`versions`) and writes nothing except the one explicit atomic write in
`write_compliance_json`. It does read the configuration, to rewrite every
configured data root in paths and free-text notes to a stable token, so a
tracked ledger never carries the machine that measured it.

``to_compliance_json`` — the verbose matrix in the selected catalog's
observation directory (or an explicit CLI PATH): an envelope (`generated_at` +
`versions`) wrapping per-slug `write` / `read` / `rollup`, with every
`ReadResult` dumped as a flat per-cell record (iceberg `spec/dimensions.md`).
Checkout defaults use `docs/v{n}/compliance.json`; installed, pinned and custom
catalogs use `<data_dir>/.raincloud/observations/<catalog-revision>/compliance.json`.

The additive-only immutable oracle: a cell `(slug, artifact_cell, reader_id)`
(a write verdict uses reader `WRITE_CELL_READER`) may be **added** but never
**removed** or **mutated**. ``diff_against_oracle`` classifies each cell;
``oracle_gate`` turns the classification into an exit status:

- no oracle (a seed or plain measuring run): fail on any read `fail` and any
  write `roundtrip=False`;
- with an oracle: fail on any status change (pass -> skip that a reader
  returned after running included), on any removed cell, and on any failing
  cell the oracle does not record. A `fail` the oracle records is a known
  state and passes. Cells this profile could not measure — a reader whose
  toolchain is absent, a write-cell whose sidecar is absent, or a cell outside
  the run's requested cells/readers — are `skipped`, never violations.

A slug block may also carry `stale_opt_outs`: formats the selected catalog
records as unavailable that a writer round-tripped in the run. It is
informational, never an oracle cell, so the gate neither adds nor compares it.

The compliance CLI chooses observation destinations; this serializer writes
only the path supplied by its caller.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from raincloud._locking import atomic_write

from .spec import display_path, published_path, scrub_published_text

if TYPE_CHECKING:  # avoid a runtime import cycle — compliance imports ledger
    from .compliance import ComplianceReport, SlugCompliance

# Stable status order for every rollup/tally (self-contained: `base.VERDICT_STATUSES`
# is an unordered frozenset, and importing compliance would cycle).
_STATUS_ORDER = ("pass", "fail", "skip", "na", "spec_ambiguous")


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _status_counts(read_results) -> dict[str, int]:
    """Per-status read-verdict counts over `read_results`, in stable order."""
    counts = {s: 0 for s in _STATUS_ORDER}
    for rr in read_results:
        counts[rr.verdict.status] = counts.get(rr.verdict.status, 0) + 1
    return counts


def _scrub_paths(text: str) -> str:
    """Rewrite configured roots and host paths in a free-text note/detail.

    A reader/sidecar error quotes the absolute artifact path (e.g. pyarrow's
    "Could not open ... '/abs/outputs/v2/...'"); serializing that verbatim bakes
    the measuring machine into the committed oracle. Every configured root
    (data, raw, scratch, cache, home; resolved and as configured) becomes its
    stable token and any other host path or credential is redacted — see
    `spec.scrub_published_text`. Status (what the oracle gate compares) is
    untouched, so this is cosmetic w.r.t. the diff.
    """
    return scrub_published_text(text)


def _slug_block(sc: "SlugCompliance") -> dict:
    """The verbose per-slug block: canonical relpath + write + read + rollup."""
    write = [
        {
            "cell": r.format_id,
            "roundtrip": r.compliance.roundtrip,
            "variant_faithful": r.compliance.variant_faithful,
            "note": _scrub_paths(r.compliance.note),
        }
        for r in sc.write_results
    ]
    read = [
        {
            "artifact_cell": rr.artifact_cell,
            "reader_id": rr.reader_id,
            "status": rr.verdict.status,
            # Serialized only for the benign never-invoked skip, so the diff can
            # tell it from a `skip` a reader returned after running. Omitted when
            # false to keep the committed ledger diff-quiet.
            **(
                {"toolchain_absent": True}
                if rr.verdict.toolchain_absent
                else {}
            ),
            "note": _scrub_paths(rr.verdict.note),
            "detail": _scrub_paths(rr.verdict.detail),
        }
        for rr in sc.read_results
    ]
    # Write-cells that produced no artifact this run (sidecar binary absent /
    # unregistered) — serialized so a cross-profile `diff_against_oracle` can
    # tell "this profile couldn't produce the artifact" (benign) from "the cell
    # was deleted" (a violation).
    skipped_cells = [{"cell": cell, "reason": reason} for cell, reason in sc.skipped_cells]
    # Formats the catalog records as unavailable that a writer round-tripped in
    # this run. Informational, never an oracle cell (the write rows above carry
    # the verdicts); omitted when empty to keep the committed ledger diff-quiet.
    stale = [{**s, "catalog_recorded": {**s["catalog_recorded"],
                                        "error": _scrub_paths(str(s["catalog_recorded"].get("error", "")))}}
             for s in sc.stale_opt_outs]
    return {
        "canonical": published_path(sc.canonical),
        "write": write,
        "read": read,
        "skipped_cells": skipped_cells,
        **({"stale_opt_outs": stale} if stale else {}),
        "rollup": _status_counts(sc.read_results),
    }


def _skipped_slug_block(reason: str) -> dict:
    """A slug deliberately not measured (`--skip-slug`) — a known-brutal build
    that OOMs/crashes the harness. Recorded as an explicit skip (never silently
    omitted) so the oracle spans the whole catalog and a reader sees why it's
    absent. Carries no write/read cells, so it contributes nothing to the oracle
    diff (no cells to remove/mutate) and never gates.
    """
    return {
        "canonical": None,
        "skipped": True,
        "skip_reason": reason,
        "write": [],
        "read": [],
        "skipped_cells": [],
        "rollup": _status_counts([]),
    }


def to_compliance_json(
    report: "ComplianceReport",
    *,
    generated_at: str,
    versions: dict | None,
    scope: dict | None = None,
) -> dict:
    """Serialize `report` to the verbose compliance-matrix JSON (dict).

    `generated_at` (an ISO-8601 string) and `versions` are passed in — the
    serializer core never reads a clock or probes the environment; the caller
    stamps them. Every `ReadResult` is dumped verbatim (flat per-cell record).
    Per-slug `rollup` + the catalog `rollup` are the full five-status counts.
    Slugs marked `--skip-slug` are serialized as explicit skip blocks (recorded,
    not omitted).
    """
    slugs = {sc.slug: _slug_block(sc) for sc in report.slugs}
    for slug, reason in report.skipped_slugs:
        slugs.setdefault(slug, _skipped_slug_block(reason))
    out = {
        "generated_at": generated_at,
        "versions": dict(versions or {}),
        # What the run was asked to measure (slugs/cells/readers requested, and
        # whether `--all` was used). Without it a ledger cannot distinguish an
        # intended partial campaign from a silent shrink: a file covering 128 of
        # 250 slugs looks identical either way, so "additive-only" could not be
        # audited against the invocation that produced it.
        **({"scope": scope} if scope is not None else {}),
        "slugs": slugs,
        "rollup": report.counts(),
    }
    return out


def write_compliance_json(
    report: "ComplianceReport",
    path: str | Path,
    *,
    generated_at: str,
    versions: dict | None = None,
    scope: dict | None = None,
) -> Path:
    """Atomically write the verbose compliance JSON to `path`.

    `path` is explicit — the writer never picks its own destination, so a normal
    measuring run never touches `docs/v{n}/` as a side effect and tests stay
    under tmp. `generated_at` is passed through to `to_compliance_json` (caller
    stamps the clock). Returns the written path.
    """
    path = Path(path)
    payload = to_compliance_json(
        report, generated_at=generated_at, versions=versions, scope=scope
    )
    text = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    atomic_write(path, text.encode())
    return path


# ---------------------------------------------------------------------------
# Additive-only immutable oracle
# ---------------------------------------------------------------------------


class MalformedOracle(Exception):
    """The `--check-oracle` file is not a well-formed compliance ledger.

    Raised by `load_oracle`, never by the tolerant `_read_cells` flatteners. The
    distinction matters: the flatteners degrade a malformed shape to "fewer known
    cells", which for the oracle side means every current cell looks merely
    *added* and the additive-only gate goes green — the gate failing open on a
    corrupt baseline. The oracle is a committed, machine-generated artifact, so
    anything unparseable in it is an error to surface, not damage to route around.
    """


def load_oracle(path: str | Path) -> dict:
    """Read + strictly validate an oracle compliance ledger. Raises `MalformedOracle`.

    Validates the envelope, every slug block, and every read/write/skipped-cell
    record, and rejects duplicate `(artifact_cell, reader_id)` read keys or
    duplicate write cells within a slug (a duplicate silently shadows its twin in
    any flattened map, so one of the two verdicts would never be compared).
    """
    path = Path(path)
    try:
        raw = path.read_text()
    except OSError as exc:
        raise MalformedOracle(f"{display_path(path)}: unreadable ({exc})") from exc
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise MalformedOracle(f"{display_path(path)}: invalid JSON ({exc})") from exc
    _validate_oracle(data, display_path(path))
    return data


def _validate_oracle(data: object, where: str) -> None:
    """Raise `MalformedOracle` unless `data` is a well-formed verbose ledger."""
    if not isinstance(data, dict):
        raise MalformedOracle(f"{where}: top level must be an object, got {type(data).__name__}")
    slugs = data.get("slugs")
    if not isinstance(slugs, dict):
        raise MalformedOracle(
            f"{where}: 'slugs' must be an object, got {type(slugs).__name__}"
        )
    for slug, block in slugs.items():
        at = f"{where}: slugs[{slug!r}]"
        if not isinstance(block, dict):
            raise MalformedOracle(f"{at} must be an object, got {type(block).__name__}")
        # A deliberately-skipped slug (`--skip-slug`) carries no cells.
        if block.get("skipped") is True:
            continue
        _validate_read_rows(block.get("read"), at)
        _validate_write_rows(block.get("write"), at)
        _validate_skipped_cells(block.get("skipped_cells"), at)
        _validate_stale_opt_outs(block.get("stale_opt_outs", []), at)


def _validate_read_rows(reads: object, at: str) -> None:
    if not isinstance(reads, list):
        raise MalformedOracle(f"{at}.read must be a list, got {type(reads).__name__}")
    seen: set[tuple[str, str]] = set()
    for i, r in enumerate(reads):
        if not isinstance(r, dict):
            raise MalformedOracle(f"{at}.read[{i}] must be an object")
        cell, reader, status = r.get("artifact_cell"), r.get("reader_id"), r.get("status")
        if not isinstance(cell, str) or not cell:
            raise MalformedOracle(f"{at}.read[{i}].artifact_cell must be a non-empty string")
        if not isinstance(reader, str) or not reader:
            raise MalformedOracle(f"{at}.read[{i}].reader_id must be a non-empty string")
        if reader == WRITE_CELL_READER:
            raise MalformedOracle(
                f"{at}.read[{i}].reader_id {reader!r} is reserved for write verdicts"
            )
        if status not in _STATUS_ORDER:
            raise MalformedOracle(
                f"{at}.read[{i}].status {status!r} is not one of {list(_STATUS_ORDER)}"
            )
        absent = r.get("toolchain_absent", False)
        if not isinstance(absent, bool):
            raise MalformedOracle(f"{at}.read[{i}].toolchain_absent must be a boolean")
        if absent and status != "skip":
            raise MalformedOracle(
                f"{at}.read[{i}] sets toolchain_absent with status {status!r} (skip only)"
            )
        if (cell, reader) in seen:
            raise MalformedOracle(
                f"{at}.read has duplicate cell ({cell!r}, {reader!r}) — "
                "one verdict would shadow the other and never be compared"
            )
        seen.add((cell, reader))


def _validate_write_rows(writes: object, at: str) -> None:
    if not isinstance(writes, list):
        raise MalformedOracle(f"{at}.write must be a list, got {type(writes).__name__}")
    seen: set[str] = set()
    for i, w in enumerate(writes):
        if not isinstance(w, dict):
            raise MalformedOracle(f"{at}.write[{i}] must be an object")
        cell = w.get("cell")
        if not isinstance(cell, str) or not cell:
            raise MalformedOracle(f"{at}.write[{i}].cell must be a non-empty string")
        # An explicit `null` is legal and meaningful: written but round-trip not
        # measured. A missing key is not: a truncated or hand-edited oracle would
        # otherwise drop measured write cells from the comparison.
        if "roundtrip" not in w:
            raise MalformedOracle(
                f"{at}.write[{i}] has no 'roundtrip' (use null for unmeasured)"
            )
        if not isinstance(w["roundtrip"], bool) and w["roundtrip"] is not None:
            raise MalformedOracle(
                f"{at}.write[{i}].roundtrip must be a boolean or null (unmeasured)"
            )
        if cell in seen:
            raise MalformedOracle(f"{at}.write has duplicate cell {cell!r}")
        seen.add(cell)


def _validate_skipped_cells(skipped: object, at: str) -> None:
    if not isinstance(skipped, list):
        raise MalformedOracle(
            f"{at}.skipped_cells must be a list, got {type(skipped).__name__}"
        )
    for i, s in enumerate(skipped):
        if not isinstance(s, dict):
            raise MalformedOracle(f"{at}.skipped_cells[{i}] must be an object")
        if not isinstance(s.get("cell"), str) or not s.get("cell"):
            raise MalformedOracle(
                f"{at}.skipped_cells[{i}].cell must be a non-empty string"
            )


def _validate_stale_opt_outs(stale: object, at: str) -> None:
    """`stale_opt_outs` is informational (no oracle cell reads it), but a
    committed ledger is still machine-generated: a malformed one is an error."""
    if not isinstance(stale, list):
        raise MalformedOracle(f"{at}.stale_opt_outs must be a list, got {type(stale).__name__}")
    for i, s in enumerate(stale):
        if not (isinstance(s, dict) and isinstance(s.get("format"), str)
                and isinstance(s.get("cells"), list) and all(isinstance(c, str) for c in s["cells"])
                and isinstance(s.get("catalog_recorded"), dict)):
            raise MalformedOracle(
                f"{at}.stale_opt_outs[{i}] must be an object with a format, a list of cells "
                "and the catalog_recorded measurement")


@dataclass(frozen=True)
class OracleDiff:
    """Classification of every `(slug, artifact_cell, reader_id)` cell vs an
    oracle. `added` is fine; `removed` + `mutated` are violations.

    Each entry is a tuple keyed `(slug, artifact_cell, reader_id)`; `mutated`
    additionally carries `(old_status, new_status)`.

    `skipped` holds cells whose oracle verdict is a real measurement but which
    this run did not measure: the reader's toolchain is absent (the cell is a
    `toolchain_absent` skip in `new`), the artifact's write-cell was skipped so
    the artifact was never produced (no read rows, and the write-cell is listed
    in `new`'s `skipped_cells`), or the cell lies outside the run's requested
    cells/readers. All are benign, never a mutation or removal — they let one
    committed oracle stay valid across machines with different sidecar
    toolchains and across subset runs.
    """

    added: list[tuple[str, str, str]] = field(default_factory=list)
    removed: list[tuple[str, str, str]] = field(default_factory=list)
    mutated: list[tuple[str, str, str, str, str]] = field(default_factory=list)
    unchanged: list[tuple[str, str, str]] = field(default_factory=list)
    skipped: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def has_violations(self) -> bool:
        return bool(self.removed or self.mutated)

    def violation_reasons(self) -> list[str]:
        """Human-readable one-liners for each removed/mutated cell."""
        reasons: list[str] = []
        for slug, cell, reader in self.removed:
            reasons.append(
                f"removed cell {slug}/{cell}/{reader} "
                f"(present in oracle, absent now — additive-only violation)"
            )
        for slug, cell, reader, old, new in self.mutated:
            reasons.append(
                f"mutated cell {slug}/{cell}/{reader}: {old} -> {new} "
                f"(oracle status is immutable — additive-only violation)"
            )
        return reasons


def _read_cells(compliance_json: dict) -> dict[tuple[str, str, str], str]:
    """Flatten a verbose compliance-json into a `(slug, cell, reader) -> status` map.

    Precondition: an oracle has been through `load_oracle`, and `new` comes from
    `to_compliance_json`. The shape checks here only keep a hand-built dict from
    raising a bare KeyError; they are not validation — a malformed oracle that
    reached this point would read as "fewer known cells" and fail the gate open.
    """
    out: dict[tuple[str, str, str], str] = {}
    if not isinstance(compliance_json, dict):
        return out
    slugs = compliance_json.get("slugs")
    if not isinstance(slugs, dict):
        return out
    for slug, block in slugs.items():
        if not isinstance(block, dict):
            continue
        reads = block.get("read")
        if not isinstance(reads, list):
            continue
        for r in reads:
            if not isinstance(r, dict):
                continue
            cell = r.get("artifact_cell")
            reader = r.get("reader_id")
            status = r.get("status")
            if cell is None or reader is None or status is None:
                continue
            out[(slug, cell, reader)] = status
    return out


def _skipped_cells(compliance_json: dict) -> dict[str, set[str]]:
    """Flatten a verbose compliance-json into a `slug -> {skipped write-cell}` map.

    A write-cell in a slug's `skipped_cells` produced no artifact this run (its
    sidecar binary was absent, or it is unregistered), so there are zero read
    rows over it — that is exactly what lets the diff distinguish "this profile
    couldn't produce the artifact" (benign) from "the cell was deleted" (a
    violation). Same precondition as `_read_cells`.
    """
    out: dict[str, set[str]] = {}
    if not isinstance(compliance_json, dict):
        return out
    slugs = compliance_json.get("slugs")
    if not isinstance(slugs, dict):
        return out
    for slug, block in slugs.items():
        if not isinstance(block, dict):
            continue
        skipped = block.get("skipped_cells")
        if not isinstance(skipped, list):
            continue
        cells = {
            s["cell"]
            for s in skipped
            if isinstance(s, dict) and isinstance(s.get("cell"), str)
        }
        if cells:
            out[slug] = cells
    return out


#: Sentinel `reader_id` for a write-cell verdict in the flattened cell space.
#: Write verdicts are real oracle cells (a `roundtrip` True->False is a
#: regression), but they have no reader — this keeps them in the same
#: `(slug, cell, reader)` key space as read rows without colliding with any real
#: reader id (no reader id contains '<').
WRITE_CELL_READER = "<write>"


def _measured_slugs(compliance_json: dict) -> set[str]:
    """Slugs this run actually measured — from the slug blocks themselves.

    Deliberately not derived from emitted read rows. Scoping `removed` by
    "slugs that produced read rows" meant a slug whose every write-cell failed
    (no artifact -> zero read rows) counted as never-measured, so all of its
    oracle cells were out of scope and the gate went green on a total
    writer-side collapse. A slug block exists iff the slug was measured; a
    deliberate `--skip-slug` block carries `skipped: true` and is not measured.
    """
    slugs = compliance_json.get("slugs")
    if not isinstance(slugs, dict):
        return set()
    return {
        slug
        for slug, block in slugs.items()
        if isinstance(block, dict) and block.get("skipped") is not True
    }


def _absent_read_cells(compliance_json: dict) -> set[tuple[str, str, str]]:
    """Read cells whose `skip` is the benign never-invoked kind.

    Only a reader that was never invoked (binary absent on this profile) says
    nothing about the artifact. A `skip` a reader returned after running is a
    measured verdict and must compare like any other status.
    """
    out: set[tuple[str, str, str]] = set()
    slugs = compliance_json.get("slugs")
    if not isinstance(slugs, dict):
        return out
    for slug, block in slugs.items():
        if not isinstance(block, dict):
            continue
        reads = block.get("read")
        if not isinstance(reads, list):
            continue
        for r in reads:
            if not isinstance(r, dict) or r.get("toolchain_absent") is not True:
                continue
            cell, reader = r.get("artifact_cell"), r.get("reader_id")
            if isinstance(cell, str) and isinstance(reader, str):
                out.add((slug, cell, reader))
    return out


def _write_cells(compliance_json: dict) -> dict[tuple[str, str, str], str]:
    """Flatten write verdicts into the same cell space as reads.

    `roundtrip` True -> ``pass``, False -> ``fail``, keyed
    `(slug, cell, WRITE_CELL_READER)`, so a writer regressing to
    `roundtrip=False` is a mutation like any read regression.

    An unmeasured write (`roundtrip: null`) yields no cell: there is nothing to
    compare. It is deliberately not treated as a pass — that is the whole point
    of the tri-state. Note the consequence, which is the desired one: a cell the
    oracle recorded as measured that comes back unmeasured shows up as `removed`,
    so losing a measurement is itself a gate violation.
    """
    out: dict[tuple[str, str, str], str] = {}
    slugs = compliance_json.get("slugs")
    if not isinstance(slugs, dict):
        return out
    for slug, block in slugs.items():
        if not isinstance(block, dict):
            continue
        writes = block.get("write")
        if not isinstance(writes, list):
            continue
        for w in writes:
            if not isinstance(w, dict):
                continue
            cell, roundtrip = w.get("cell"), w.get("roundtrip")
            if isinstance(cell, str) and isinstance(roundtrip, bool):
                out[(slug, cell, WRITE_CELL_READER)] = "pass" if roundtrip else "fail"
    return out


def _out_of_scope(key: tuple[str, str, str], scope: dict | None) -> bool:
    """Whether a cell lies outside what the run was asked to measure.

    `scope` is the ledger's scope block. An explicit `--cells`/`--readers`
    subset leaves every other cell of a measured slug absent from `new` by
    request, not by deletion. A full run records `None` for both ("every
    registered id"), and nothing is out of its scope: an oracle cell of a writer
    or reader that is no longer registered is then a removal. A list here cannot
    tell "not requested" from "no longer exists", so only a subset may set one.
    """
    if not isinstance(scope, dict):
        return False
    _slug, cell, reader = key
    cells = scope.get("requested_cells")
    if isinstance(cells, list) and cell not in cells:
        return True
    readers = scope.get("requested_readers")
    return (reader != WRITE_CELL_READER and isinstance(readers, list)
            and reader not in readers)


def diff_against_oracle(new: dict, oracle: dict, *, scope: dict | None = None) -> OracleDiff:
    """Classify each cell of `new` vs `oracle` (both verbose compliance-json).

    Read and write verdicts share one key space (`_write_cells`). A cell present
    only in `new` is **added**; present in both with the same status is
    **unchanged**; with a different status it is **mutated** (a violation),
    except:

    - `new` holds a never-invoked skip (`toolchain_absent`: the reader's binary
      is absent on this profile) over an oracle verdict -> **skipped**. A skip
      a reader returned after running is a measured verdict and still mutates;
    - the oracle holds a never-invoked skip and `new` carries a real verdict ->
      **added**: the oracle never measured the cell, so this is its first
      measurement. `oracle_gate` still fails it if that verdict is `fail`.

    A cell present only in the oracle is **removed** (a violation), except when
    `new` did not measure it: its slug was not measured at all, its write-cell
    is in `new`'s `skipped_cells` with no read rows over it (sidecar absent), or
    it is outside `scope` (default: `new["scope"]`), the explicitly requested
    cells and readers of a subset run (see `_out_of_scope`). Those are **skipped**. A write measurement that
    comes back unmeasured (`roundtrip: null`) yields no cell, so it is removed:
    losing a measurement is a violation. A write-cell's `variant_faithful`
    flipping is informational and never compared.
    """
    if scope is None:
        scope = new.get("scope") if isinstance(new, dict) else None
    new_cells = {**_read_cells(new), **_write_cells(new)}
    oracle_cells = {**_read_cells(oracle), **_write_cells(oracle)}
    new_absent = _absent_read_cells(new)
    oracle_absent = _absent_read_cells(oracle)

    diff = OracleDiff()
    for key, status in new_cells.items():
        if key not in oracle_cells:
            diff.added.append(key)
        elif oracle_cells[key] == status:
            diff.unchanged.append(key)
        elif status == "skip" and key in new_absent:
            # Gated on the serialized `toolchain_absent` flag, not on
            # `status == "skip"` alone: a sidecar that ran and returned `skip`
            # (unsupported type, comparator gap) measured this artifact, and
            # excusing every skip lets comparator coverage rot under a green gate.
            diff.skipped.append(key)
        elif key in oracle_absent:
            # The oracle was seeded where this reader was absent; the cell had
            # no measurement to mutate.
            diff.added.append(key)
        else:
            slug, cell, reader = key
            diff.mutated.append((slug, cell, reader, oracle_cells[key], status))
    measured_slugs = _measured_slugs(new)
    measured_cells = {(slug, cell) for slug, cell, _ in new_cells}
    new_skipped = _skipped_cells(new)
    for key in oracle_cells:
        slug, cell, _reader = key
        if slug not in measured_slugs or key in new_cells:
            continue
        # A skipped write-cell excuses its read rows only when `new` has no read
        # row over it at all: a produced-and-skipped cell (impossible from the
        # compliance runner, constructible by hand) is a removal, so failing
        # closed does not depend on an invariant kept in another module.
        if ((cell in new_skipped.get(slug, set()) and (slug, cell) not in measured_cells)
                or _out_of_scope(key, scope)):
            diff.skipped.append(key)
        else:
            diff.removed.append(key)
    return diff


def oracle_gate(
    report: "ComplianceReport", oracle: dict | None, *, scope: dict | None = None
) -> tuple[bool, list[str]]:
    """Gate `report`: return `(exit_ok, reasons)`.

    raincloud's matrix legitimately carries known non-conformances (e.g. the
    pinned `vortex@py` binding can't encode the shredded VARIANT struct) — those
    are measured data, not "must fix" — so the gate has two modes:

    - **With an oracle (`--check-oracle`, the regression gate):** fail on every
      violation `diff_against_oracle` reports — any status change except a
      toolchain-absent skip, and any removed cell — and on any `fail` in a cell
      the oracle does not record (a first measurement, a newly enabled cell). A
      `fail` the oracle records is a known state and passes. `scope` is the
      run's explicit `--cells`/`--readers` subset (None for a full run), so a
      subset run does not read as removals and a full one narrows nothing.
    - **Without an oracle (seed / plain measurement run):** no baseline can
      excuse a failure, so any read `fail` or write-cell `roundtrip=False`
      fails. `--write-ledger` writes before this gate, so a seed run still
      produces its ledger when it exits non-zero.

    In neither mode does a `skip` / `na` / `spec_ambiguous` verdict fail by
    itself; with an oracle, changing to or from one does.
    """
    reasons: list[str] = []

    if oracle is not None:
        # Regression gate: deviation from the recorded oracle...
        new = to_compliance_json(report, generated_at="", versions=None, scope=scope)
        diff = diff_against_oracle(new, oracle)
        reasons.extend(diff.violation_reasons())
        # ...plus any failure that is new: an `added` cell (never recorded, or
        # recorded only as a toolchain-absent skip) carrying `fail`. Only a
        # failure the oracle already records is a known state; an unrecorded one
        # has to be either fixed or consciously re-seeded.
        added = set(diff.added)
        new_cells = {**_read_cells(new), **_write_cells(new)}
        for key in sorted(added):
            if new_cells.get(key) != "fail":
                continue
            slug, cell, reader = key
            if reader == WRITE_CELL_READER:
                reasons.append(
                    f"new write fail {slug}/{cell} (not recorded in the oracle — "
                    "fix it or re-seed deliberately)"
                )
            else:
                reasons.append(
                    f"new read fail {slug}/{cell}/{reader} (not recorded in the "
                    "oracle — fix it or re-seed deliberately)"
                )
        return (not reasons, reasons)

    # No baseline: strict absolute check on measured failures.
    for sc in report.slugs:
        for rr in sc.read_results:
            if rr.verdict.status == "fail":
                note = rr.verdict.note or "read fail"
                reasons.append(f"read fail {sc.slug}/{rr.artifact_cell}/{rr.reader_id}: {note}")
        for r in sc.write_results:
            # `is False` — an unmeasured (`None`) round-trip is not a failure.
            if r.compliance.roundtrip is False:
                note = r.compliance.note or "write-cell roundtrip=False"
                reasons.append(f"write fail {sc.slug}/{r.format_id}: {note}")

    return (not reasons, reasons)
