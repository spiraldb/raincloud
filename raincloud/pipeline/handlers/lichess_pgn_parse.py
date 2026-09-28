# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse a Lichess monthly PGN.zst dump into a streamed parquet.

Source: https://database.lichess.org/standard/lichess_db_standard_rated_YYYY-MM.pgn.zst

Each game in the PGN is a header block (tag pairs in [Name "value"] form)
followed by a blank line, then the move text terminated by the result
(1-0 / 0-1 / 1/2-1/2 / *). Games are separated by a blank line.

We stream zstandard-decompress, parse games one at a time via a small
state-machine (no dependency on python-chess — we don't need to interpret
the moves, just record them), and flush `batch_size` games at a time to the
canonical Arrow writer. Peak memory is bounded regardless of input size.

Schema: one column per PGN tag the dumps carry (`_TAGS`), plus the move text:
    event, site, white, black, result, utc_date, utc_time,
    white_elo, black_elo, white_rating_diff, black_rating_diff,
    white_title, black_title, eco, opening, time_control, termination, moves
A header line that is not a tag pair, a tag with no column, a non-integer
Elo/diff other than `?`, or two games without a blank line between them
fails the build, naming the line.
"""
from __future__ import annotations

import io
import re
from pathlib import Path

import pyarrow as pa
import zstandard as zstd

from ..canonical import open_canonical_writer

_TAG_RE = re.compile(r'\[(\w+)\s+"(.*)"\]')

# PGN tag -> column. A tag outside this map fails the build rather than being
# discarded.
_TAGS = {
    "Event": "event", "Site": "site", "White": "white", "Black": "black",
    "Result": "result", "UTCDate": "utc_date", "UTCTime": "utc_time",
    "WhiteElo": "white_elo", "BlackElo": "black_elo",
    "WhiteRatingDiff": "white_rating_diff", "BlackRatingDiff": "black_rating_diff",
    "WhiteTitle": "white_title", "BlackTitle": "black_title",
    "ECO": "eco", "Opening": "opening", "TimeControl": "time_control",
    "Termination": "termination",
}

_SCHEMA = pa.schema([
    ("event", pa.string()),
    ("site", pa.string()),
    ("white", pa.string()),
    ("black", pa.string()),
    ("result", pa.string()),
    ("utc_date", pa.string()),        # PGN's "2012.12.31" — kept as string, not coerced
    ("utc_time", pa.string()),
    ("white_elo", pa.int32()),
    ("black_elo", pa.int32()),
    ("white_rating_diff", pa.int32()),
    ("black_rating_diff", pa.int32()),
    ("white_title", pa.string()),     # BOT, GM, ... on the few titled players' games
    ("black_title", pa.string()),
    ("eco", pa.string()),
    ("opening", pa.string()),
    ("time_control", pa.string()),
    ("termination", pa.string()),
    ("moves", pa.string()),
])


def _coerce_int(v: str | None):
    """An integer tag; `?` (PGN for unknown) and an absent tag are null."""
    if v is None or v == "" or v == "?": return None
    try:
        return int(v.lstrip("+"))  # rating diffs come with a leading +
    except ValueError:
        raise ValueError(f"{v!r} is not an integer") from None


def _iter_games(text_stream):
    """Yield (headers_dict, moves_str, first_line_number) for each game.

    Fails, naming the line, on a header line that is not a tag pair and on a
    tag pair inside move text (two games not separated by a blank line)."""
    headers: dict[str, str] = {}
    moves_lines: list[str] = []
    state = "headers"
    start = 1
    for number, line in enumerate(text_stream, 1):
        line = line.rstrip("\n").rstrip("\r")
        if not line:
            if state == "headers" and headers:
                state = "moves"
            elif state == "moves":
                yield headers, " ".join(moves_lines).strip(), start
                headers = {}
                moves_lines = []
                state = "headers"
            continue
        m = _TAG_RE.fullmatch(line)
        if state == "headers":
            if not m:
                raise ValueError(f"line {number:,} is in a header block but is not a tag pair: {line[:80]!r}")
            if not headers:
                start = number
            headers[m.group(1)] = m.group(2)
        elif m:
            raise ValueError(f"line {number:,} is a tag pair inside move text; the game before "
                             f"is not ended by a blank line: {line[:80]!r}")
        else:
            moves_lines.append(line)
    if headers or moves_lines:
        yield headers, " ".join(moves_lines).strip(), start


def lichess_pgn_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                      batch_size: int = 50_000) -> list[tuple[str, pa.Table]]:
    pbf_files = [p for p, _ in parsed if str(p).lower().endswith(".pgn.zst")]
    if not pbf_files:
        raise ValueError("lichess_pgn_parse: no .pgn.zst files in extracted output")
    if len(pbf_files) > 1:
        raise ValueError(f"lichess_pgn_parse: multiple .pgn.zst files found: {[p.name for p in pbf_files]}")
    path = pbf_files[0]
    print(f"  streaming {path.name} ({path.stat().st_size / 1e6:.1f} MB compressed)")

    cols: dict[str, list] = {name: [] for name in _SCHEMA.names}
    ints = {f.name for f in _SCHEMA if pa.types.is_integer(f.type)}
    count = 0

    def flush(writer):
        nonlocal count, cols
        if not cols["event"]: return
        batch = pa.record_batch([pa.array(cols[f.name], type=f.type) for f in _SCHEMA], schema=_SCHEMA)
        writer.write_batch(batch)
        count += len(cols["event"])
        for k in cols: cols[k] = []

    with open_canonical_writer(spec["slug"], _SCHEMA) as writer:
        with open(path, "rb") as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as reader:
                # Strict UTF-8, and lines end only at \n.
                text_stream = io.TextIOWrapper(reader, encoding="utf-8", newline="\n")
                try:
                    for headers, moves, start in _iter_games(text_stream):
                        unknown = headers.keys() - _TAGS.keys()
                        if unknown:
                            raise ValueError(f"line {start:,}: tag(s) {sorted(unknown)} have no column")
                        row = {_TAGS[tag]: value for tag, value in headers.items()}
                        for name in _TAGS.values():
                            value = row.get(name)
                            if name in ints:
                                try:
                                    value = _coerce_int(value)
                                except ValueError as exc:
                                    raise ValueError(f"line {start:,}: {name} {exc}") from None
                            cols[name].append(value)
                        cols["moves"].append(moves)
                        if len(cols["event"]) >= batch_size:
                            flush(writer)
                            if count % (batch_size * 2) == 0:
                                print(f"    {count:,} games flushed")
                except (ValueError, UnicodeDecodeError) as exc:
                    raise ValueError(f"lichess_pgn_parse: {path.name}: {exc}") from None
        flush(writer)
    print(f"    total: {count:,} games written")
    return []
