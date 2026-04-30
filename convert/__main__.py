"""
CLI entry point for the conversion phase.

Usage:
    python -m convert input.jsonl [--output converted.jsonl] [--errors failed.jsonl]
    python -m convert input.jsonl --filter "*判决书;*裁定书"
    python -m convert input.jsonl --filter "*判决书" --limit 200
    python -m convert input.jsonl --resume
    python -m convert --resume checkpoint.json
"""

import argparse
import fnmatch
import json
import sys
from pathlib import Path
from datetime import datetime

from .converter import ConversionInterrupted, process_jsonl_stream


def make_type_filter(filter_patterns: str):
    """
    Create a filter function from glob patterns.
    
    Args:
        filter_patterns: Semicolon-separated glob patterns, e.g., "*判决书;*裁定书"
        
    Returns:
        Filter function that takes raw_json and returns True if document matches
    """
    patterns = [p.strip() for p in filter_patterns.split(';') if p.strip()]
    
    def filter_func(raw_json: dict) -> bool:
        # Get document type from s1 (title) field
        doc_title = raw_json.get('s1', '')
        return any(fnmatch.fnmatch(doc_title, pattern) for pattern in patterns)
    
    return filter_func


def save_checkpoint(checkpoint_path: Path, config: dict):
    """Save checkpoint file with current configuration and progress."""
    with open(checkpoint_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"\nCheckpoint saved: {checkpoint_path}")


def load_checkpoint(checkpoint_path: Path) -> dict:
    """Load checkpoint file."""
    with open(checkpoint_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def canonicalize_path(path: Path | str | None) -> str | None:
    """Normalize a path for checkpoint comparisons."""
    if path is None:
        return None
    return str(Path(path).expanduser().resolve())


def derive_output_paths(
    input_path: Path,
    output_arg: Path | None,
    errors_arg: Path | None,
    checkpoint_arg: Path | None,
) -> tuple[Path, Path, Path]:
    """Resolve output, error, and checkpoint paths for a run."""
    output_path = output_arg or (input_path.parent / f"{input_path.stem}_converted.jsonl")
    output_parent = output_path.parent
    output_stem = output_path.stem
    error_path = errors_arg or (output_parent / f"{output_stem}_failed.jsonl")
    checkpoint_path = checkpoint_arg or (output_parent / f"{output_stem}_checkpoint.json")
    return output_path, error_path, checkpoint_path


def checkpoint_matches_request(
    checkpoint: dict,
    *,
    input_path: Path | None = None,
    output_path: Path | None = None,
    error_path: Path | None = None,
    filter_pattern: str | None = None,
    check_filter: bool = False,
) -> bool:
    """Return True when requested fields match the checkpoint configuration."""
    if input_path is not None and canonicalize_path(checkpoint.get('input')) != canonicalize_path(input_path):
        return False
    if output_path is not None and canonicalize_path(checkpoint.get('output')) != canonicalize_path(output_path):
        return False
    if error_path is not None and canonicalize_path(checkpoint.get('errors')) != canonicalize_path(error_path):
        return False
    if check_filter and checkpoint.get('filter') != filter_pattern:
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Convert court documents from JSONL (HTML) to wikitext",
        prog="python -m convert"
    )
    
    parser.add_argument(
        "input",
        type=Path,
        nargs='?',
        default=None,
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
        help="Path to error log JSONL file (default: <output>_failed.jsonl)"
    )
    
    parser.add_argument(
        "--filter", "-f",
        type=str,
        default=None,
        help="Filter documents by type using glob patterns. Use ';' to separate multiple patterns. "
             "E.g., '*判决书;*裁定书' matches documents ending with 判决书 or 裁定书"
    )
    
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Stop after this many successful conversions"
    )
    
    parser.add_argument(
        "--resume", "-r",
        nargs='?',
        const='',
        default=None,
        help="Resume from a checkpoint. Optionally pass a checkpoint path; "
             "otherwise use --checkpoint or the default <output>_checkpoint.json"
    )
    
    parser.add_argument(
        "--checkpoint", "-c",
        type=Path,
        default=None,
        help="Path to checkpoint file (default: <output>_checkpoint.json)"
    )
    
    parser.add_argument(
        "--txt", "-t",
        type=Path,
        default=None,
        help="Path to human-readable TXT output file (optional)"
    )
    
    parser.add_argument(
        "--original",
        type=Path,
        default=None,
        help="Path to save original JSON for each processed document (optional)"
    )
    
    args = parser.parse_args()
    
    checkpoint = None
    resume_requested = args.resume is not None
    explicit_resume_path = Path(args.resume) if args.resume not in (None, '') else None

    # Handle resume mode
    if resume_requested:
        requested_input = None
        requested_output = None
        requested_errors = None

        if explicit_resume_path is not None:
            checkpoint_path = explicit_resume_path
        elif args.checkpoint is not None:
            checkpoint_path = args.checkpoint
            if args.input:
                if not args.input.exists():
                    print(f"Error: Input file not found: {args.input}", file=sys.stderr)
                    sys.exit(1)
                requested_input = args.input
                requested_output, requested_errors, _ = derive_output_paths(
                    args.input,
                    args.output,
                    args.errors,
                    args.checkpoint,
                )
        else:
            if not args.input:
                print(
                    "Error: input file is required when using --resume without an explicit checkpoint path",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not args.input.exists():
                print(f"Error: Input file not found: {args.input}", file=sys.stderr)
                sys.exit(1)
            requested_input = args.input
            requested_output, requested_errors, checkpoint_path = derive_output_paths(
                args.input,
                args.output,
                args.errors,
                args.checkpoint,
            )

        if not checkpoint_path.exists():
            print(f"Error: Checkpoint file not found: {checkpoint_path}", file=sys.stderr)
            sys.exit(1)

        checkpoint = load_checkpoint(checkpoint_path)
        if (
            requested_input is not None or
            requested_output is not None or
            requested_errors is not None or
            args.filter is not None
        ) and not checkpoint_matches_request(
            checkpoint,
            input_path=requested_input,
            output_path=requested_output,
            error_path=requested_errors,
            filter_pattern=args.filter,
            check_filter=args.filter is not None,
        ):
            print(
                f"Error: Checkpoint does not match the requested run: {checkpoint_path}",
                file=sys.stderr,
            )
            sys.exit(1)

        input_path = Path(checkpoint['input'])
        output_path = Path(checkpoint['output'])
        error_path = Path(checkpoint['errors'])
        filter_pattern = checkpoint.get('filter')
        start_from = checkpoint.get('last_doc_num', 0)
        total_success = checkpoint.get('total_success', 0)
        total_errors = checkpoint.get('total_errors', 0)
        if (start_from > 0 or total_success > 0 or total_errors > 0) and not output_path.exists():
            print(
                f"Error: Checkpoint {checkpoint_path} shows prior progress, but output file is missing: {output_path}",
                file=sys.stderr,
            )
            sys.exit(1)
        original_path = args.original
        if original_path is None and checkpoint.get('original'):
            original_path = Path(checkpoint['original'])
        txt_path = args.txt
        if txt_path is None and checkpoint.get('txt'):
            txt_path = Path(checkpoint['txt'])
        append_output = True

        # Allow overriding limit when resuming
        max_success = args.limit

        print("=" * 60)
        print("Court Document Converter - RESUMING")
        print("=" * 60)
        print(f"Checkpoint: {checkpoint_path}")
        print(f"Resuming from document: {start_from}")
        print(f"Previous success: {total_success}")
        print(f"Previous errors: {total_errors}")

    else:
        # Normal mode - require input file
        if not args.input:
            print("Error: input file is required (unless using --resume)", file=sys.stderr)
            parser.print_help()
            sys.exit(1)
        
        if not args.input.exists():
            print(f"Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)

        input_path = args.input
        output_path, error_path, checkpoint_path = derive_output_paths(
            input_path,
            args.output,
            args.errors,
            args.checkpoint,
        )
        filter_pattern = args.filter
        max_success = args.limit
        start_from = 0
        total_success = 0
        total_errors = 0
        original_path = args.original
        txt_path = args.txt
        append_output = False

    # Create filter function if specified
    doc_filter = None
    if filter_pattern:
        doc_filter = make_type_filter(filter_pattern)

    # Print configuration
    print("=" * 60)
    print("Court Document Converter")
    print("=" * 60)
    print(f"Input:      {input_path}")
    print(f"Output:     {output_path}")
    print(f"Errors:     {error_path}")
    print(f"Checkpoint: {checkpoint_path}")
    if original_path:
        print(f"Original:   {original_path}")
    if filter_pattern:
        print(f"Filter:     {filter_pattern}")
    if max_success:
        print(f"Limit:      {max_success} successful conversions")
    if start_from > 0:
        print(f"Start from: document #{start_from}")
    print("=" * 60)
    print()
    
    # Run conversion
    start_time = datetime.now()
    print(f"Started at: {start_time.isoformat()}")
    print()
    
    interrupted = False
    last_doc_num = start_from
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    try:
        success_count, error_count, skipped_count, last_doc_num = process_jsonl_stream(
            input_path,
            output_path,
            error_path,
            doc_filter,
            start_from=start_from,
            max_success=max_success,
            original_path=original_path,
            append_output=append_output,
        )
    except ConversionInterrupted as exc:
        print("\n\nConversion interrupted by user")
        interrupted = True
        success_count = exc.success_count
        error_count = exc.error_count
        skipped_count = exc.skipped_count
        last_doc_num = exc.last_doc_num
    except KeyboardInterrupt:
        print("\n\nConversion interrupted by user")
        interrupted = True
    except Exception as e:
        print(f"\nFatal error: {e}", file=sys.stderr)
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Update totals
    total_success += success_count
    total_errors += error_count
    
    # Save checkpoint
    checkpoint_data = {
        'input': str(input_path.absolute()),
        'output': str(output_path.absolute()),
        'errors': str(error_path.absolute()),
        'filter': filter_pattern,
        'last_doc_num': last_doc_num,
        'total_success': total_success,
        'total_errors': total_errors,
        'original': str(original_path.absolute()) if original_path else None,
        'txt': str(txt_path.absolute()) if txt_path else None,
        'last_run': end_time.isoformat(),
    }
    save_checkpoint(checkpoint_path, checkpoint_data)

    # Generate TXT output if requested
    if txt_path:
        txt_path = Path(txt_path)
        print(f"\nGenerating TXT output: {txt_path}")
        with open(txt_path, 'w', encoding='utf-8') as out:
            out.write('=' * 80 + '\n')
            out.write(f'CONVERTED WIKITEXT OUTPUT\n')
            out.write(f'Input: {input_path}\n')
            out.write(f'Filter: {filter_pattern or "None"}\n')
            out.write('=' * 80 + '\n')
            
            with open(output_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    result = json.loads(line)
                    out.write(f"\n{'─' * 80}\n")
                    out.write(f"Case {i}: {result.get('title', 'Unknown')}\n")
                    out.write(f"{'─' * 80}\n\n")
                    out.write(result.get('wikitext', ''))
                    out.write('\n')
            
            out.write(f"\n{'=' * 80}\n")
            out.write(f'SUMMARY: {total_success} documents\n')
            out.write('=' * 80 + '\n')
        print(f"TXT output written: {txt_path}")
    
    # Print summary
    print()
    print("=" * 60)
    if interrupted:
        print("Conversion Interrupted - Progress Saved")
    else:
        print("Conversion Complete")
    print("=" * 60)
    total_scanned = success_count + error_count + skipped_count
    print(f"This run:")
    print(f"  Scanned:     {total_scanned}")
    if skipped_count > 0:
        print(f"  Skipped:     {skipped_count}")
    print(f"  Processed:   {success_count + error_count}")
    print(f"  Successful:  {success_count}")
    print(f"  Failed:      {error_count}")
    print(f"  Duration:    {duration}")
    if total_scanned > 0:
        print(f"  Rate:        {total_scanned / duration.total_seconds():.1f} docs/sec")
    
    if start_from > 0 or total_success > success_count:
        print()
        print(f"Cumulative totals:")
        print(f"  Total success: {total_success}")
        print(f"  Total errors:  {total_errors}")
        print(f"  Last document: #{last_doc_num}")
    
    print("=" * 60)
    
    if interrupted:
        print(f"\nTo resume: python -m convert --resume \"{checkpoint_path}\"")
        sys.exit(130)
    
    if error_count > 0:
        print(f"\nCheck {error_path} for details on failed conversions.")
    
    # Return non-zero if all conversions failed
    if success_count == 0 and error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
