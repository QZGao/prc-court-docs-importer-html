"""
CLI entry point for the conversion phase.

Usage:
    python -m convert input.jsonl [--output converted.jsonl] [--errors failed.jsonl]
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

from .converter import process_jsonl_stream


def main():
    parser = argparse.ArgumentParser(
        description="Convert court documents from JSONL (HTML) to wikitext",
        prog="python -m convert"
    )
    
    parser.add_argument(
        "input",
        type=Path,
        help="Path to input JSONL file containing court documents"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Path to output JSONL file (default: <input>_converted.jsonl)"
    )
    
    parser.add_argument(
        "--errors", "-e",
        type=Path,
        default=None,
        help="Path to error log JSONL file (default: <input>_failed.jsonl)"
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    # Set default output paths if not specified
    input_stem = args.input.stem
    input_parent = args.input.parent
    
    output_path = args.output or (input_parent / f"{input_stem}_converted.jsonl")
    error_path = args.errors or (input_parent / f"{input_stem}_failed.jsonl")
    
    # Print configuration
    print("=" * 60)
    print("Court Document Converter")
    print("=" * 60)
    print(f"Input:   {args.input}")
    print(f"Output:  {output_path}")
    print(f"Errors:  {error_path}")
    print("=" * 60)
    print()
    
    # Run conversion
    start_time = datetime.now()
    print(f"Started at: {start_time.isoformat()}")
    print()
    
    try:
        success_count, error_count = process_jsonl_stream(
            args.input,
            output_path,
            error_path,
        )
    except KeyboardInterrupt:
        print("\n\nConversion interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}", file=sys.stderr)
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Print summary
    print()
    print("=" * 60)
    print("Conversion Complete")
    print("=" * 60)
    print(f"Total processed: {success_count + error_count}")
    print(f"Successful:      {success_count}")
    print(f"Failed:          {error_count}")
    print(f"Duration:        {duration}")
    print(f"Rate:            {(success_count + error_count) / duration.total_seconds():.1f} docs/sec")
    print("=" * 60)
    
    if error_count > 0:
        print(f"\nCheck {error_path} for details on failed conversions.")
    
    # Return non-zero if all conversions failed
    if success_count == 0 and error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
