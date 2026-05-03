#!/usr/bin/env python
"""
Strip invisible Unicode "Other" characters from a large text file.

The file is processed in streaming chunks. By default, newline and tab controls
are preserved by the shared converter normalizer.
"""

from __future__ import annotations

import argparse
import codecs
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from convert.html_normalizer import remove_unicode_other_chars


DEFAULT_CHUNK_SIZE = 1024 * 1024


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream a text file and write a copy with invisible Unicode Other chars removed."
    )
    parser.add_argument("input", type=Path, help="Input text file")
    parser.add_argument(
        "output",
        type=Path,
        nargs="?",
        default=None,
        help="Output text file (default: INPUT.without-invisible + original suffix)",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding for input and output (default: utf-8)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Read size in bytes (default: {DEFAULT_CHUNK_SIZE})",
    )
    return parser.parse_args(argv)


def default_output_path(input_path: Path) -> Path:
    if input_path.suffix:
        return input_path.with_name(f"{input_path.stem}.without-invisible{input_path.suffix}")
    return input_path.with_name(f"{input_path.name}.without-invisible")


def strip_file(input_path: Path, output_path: Path, encoding: str, chunk_size: int) -> None:
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    encoder = codecs.getincrementalencoder(encoding)(errors="strict")
    total_bytes = input_path.stat().st_size

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(binary_units=True),
        TransferSpeedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Stripping invisible chars", total=total_bytes)

        with input_path.open("rb") as infile, output_path.open("wb") as outfile:
            while True:
                chunk = infile.read(chunk_size)
                if not chunk:
                    break

                text = decoder.decode(chunk, final=False)
                stripped = remove_unicode_other_chars(text)
                outfile.write(encoder.encode(stripped, final=False))
                progress.advance(task, len(chunk))

            tail = decoder.decode(b"", final=True)
            if tail:
                outfile.write(encoder.encode(remove_unicode_other_chars(tail), final=False))
            outfile.write(encoder.encode("", final=True))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    input_path = args.input.expanduser().resolve()
    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1
    if not input_path.is_file():
        print(f"Error: input path is not a file: {input_path}", file=sys.stderr)
        return 1
    if args.chunk_size <= 0:
        print("Error: --chunk-size must be greater than 0", file=sys.stderr)
        return 1

    output_path = (args.output or default_output_path(input_path)).expanduser().resolve()
    if output_path == input_path:
        print("Error: output path must be different from input path", file=sys.stderr)
        return 1
    output_path.parent.mkdir(parents=True, exist_ok=True)

    strip_file(input_path, output_path, args.encoding, args.chunk_size)
    print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
