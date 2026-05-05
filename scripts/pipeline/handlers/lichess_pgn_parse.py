# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse a Lichess monthly PGN.zst dump into a streamed parquet.

Source: https://database.lichess.org/standard/lichess_db_standard_rated_YYYY-MM.pgn.zst

Each game in the PGN is a header block (tag pairs in [Name "value"] form)
followed by a blank line, then the move text terminated by the result
(1-0 / 0-1 / 1/2-1/2 / *). Games are separated by a blank line.

We stream zstandard-decompress, parse games one at a time via a small
state-machine (no dependency on python-chess — we don't need to interpret
the moves, just record them), and flush `batch_size` games at a time to a
ParquetWriter. Peak memory is bounded regardless of input size.

Schema:
    event, site, white, black, result, utc_date, utc_time,
    white_elo, black_elo, white_rating_diff, black_rating_diff,
    eco, opening, time_control, termination, moves
"""
from __future__ import annotations

import io
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

_TAG_RE = re.compile(r'\[(\w+)\s+"(.*)"\]')

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
    ("eco", pa.string()),
    ("opening", pa.string()),
    ("time_control", pa.string()),
    ("termination", pa.string()),
    ("moves", pa.string()),
])


def _coerce_int(v: str | None):
    if v is None or v == "" or v == "?": return None
    try:
        return int(v.lstrip("+"))  # rating diffs come with a leading +
    except ValueError:
        return None


def _iter_games(text_stream):
    """Yield (headers_dict, moves_str) for each game."""
    headers: dict[str, str] = {}
    moves_lines: list[str] = []
    state = "headers"
    for line in text_stream:
        line = line.rstrip("\n").rstrip("\r")
        if not line:
            if state == "headers" and headers:
                state = "moves"
            elif state == "moves":
                yield headers, " ".join(moves_lines).strip()
                headers = {}
                moves_lines = []
                state = "headers"
            continue
        if state == "headers":
            if line.startswith("["):
                m = _TAG_RE.match(line)
                if m:
                    headers[m.group(1)] = m.group(2)
        else:
            moves_lines.append(line)
    if headers or moves_lines:
        yield headers, " ".join(moves_lines).strip()


def lichess_pgn_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                      batch_size: int = 50_000) -> list[tuple[str, pa.Table]]:
    pbf_files = [p for p, _ in parsed if str(p).lower().endswith(".pgn.zst")]
    if not pbf_files:
        raise ValueError("lichess_pgn_parse: no .pgn.zst files in extracted output")
    if len(pbf_files) > 1:
        raise ValueError(f"lichess_pgn_parse: multiple .pgn.zst files found: {[p.name for p in pbf_files]}")
    path = pbf_files[0]
    print(f"  streaming {path.name} ({path.stat().st_size / 1e6:.1f} MB compressed)")

    from ..spec import output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    cols: dict[str, list] = {name: [] for name in (
        "event", "site", "white", "black", "result", "utc_date", "utc_time",
        "white_elo", "black_elo", "white_rating_diff", "black_rating_diff",
        "eco", "opening", "time_control", "termination", "moves",
    )}
    count = 0

    def flush(writer):
        nonlocal count, cols
        if not cols["event"]: return
        batch = pa.record_batch([
            pa.array(cols["event"], type=pa.string()),
            pa.array(cols["site"], type=pa.string()),
            pa.array(cols["white"], type=pa.string()),
            pa.array(cols["black"], type=pa.string()),
            pa.array(cols["result"], type=pa.string()),
            pa.array(cols["utc_date"], type=pa.string()),
            pa.array(cols["utc_time"], type=pa.string()),
            pa.array(cols["white_elo"], type=pa.int32()),
            pa.array(cols["black_elo"], type=pa.int32()),
            pa.array(cols["white_rating_diff"], type=pa.int32()),
            pa.array(cols["black_rating_diff"], type=pa.int32()),
            pa.array(cols["eco"], type=pa.string()),
            pa.array(cols["opening"], type=pa.string()),
            pa.array(cols["time_control"], type=pa.string()),
            pa.array(cols["termination"], type=pa.string()),
            pa.array(cols["moves"], type=pa.string()),
        ], schema=_SCHEMA)
        writer.write_batch(batch)
        count += len(cols["event"])
        for k in cols: cols[k] = []

    with pq.ParquetWriter(out_path, _SCHEMA, compression=compression) as writer:
        with open(path, "rb") as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as reader:
                text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
                for headers, moves in _iter_games(text_stream):
                    cols["event"].append(headers.get("Event"))
                    cols["site"].append(headers.get("Site"))
                    cols["white"].append(headers.get("White"))
                    cols["black"].append(headers.get("Black"))
                    cols["result"].append(headers.get("Result"))
                    cols["utc_date"].append(headers.get("UTCDate"))
                    cols["utc_time"].append(headers.get("UTCTime"))
                    cols["white_elo"].append(_coerce_int(headers.get("WhiteElo")))
                    cols["black_elo"].append(_coerce_int(headers.get("BlackElo")))
                    cols["white_rating_diff"].append(_coerce_int(headers.get("WhiteRatingDiff")))
                    cols["black_rating_diff"].append(_coerce_int(headers.get("BlackRatingDiff")))
                    cols["eco"].append(headers.get("ECO"))
                    cols["opening"].append(headers.get("Opening"))
                    cols["time_control"].append(headers.get("TimeControl"))
                    cols["termination"].append(headers.get("Termination"))
                    cols["moves"].append(moves)
                    if len(cols["event"]) >= batch_size:
                        flush(writer)
                        if count % (batch_size * 2) == 0:
                            print(f"    {count:,} games flushed")
        flush(writer)
    print(f"    total: {count:,} games written")
    return []
