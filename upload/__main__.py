"""
CLI entry point for the upload phase.

Usage:
    python -m upload converted.jsonl [--interval 3] [--max 100]
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

from .uploader import process_upload_batch, RateLimiter


def main():
    parser = argparse.ArgumentParser(
        description="Upload converted court documents to zhwikisource",
        prog="python -m upload"
    )
    
    parser.add_argument(
        "input",
        type=Path,
        help="Path to converted JSONL file"
    )
    
    parser.add_argument(
        "--interval", "-i",
        type=float,
        default=3.0,
        help="Minimum seconds between edits (default: 3)"
    )
    
    parser.add_argument(
        "--maxlag", "-m",
        type=int,
        default=5,
        help="Maxlag parameter for API (default: 5)"
    )
    
    parser.add_argument(
        "--max", "-n",
        type=int,
        default=None,
        help="Maximum number of documents to upload (default: all)"
    )
    
    parser.add_argument(
        "--no-resolve",
        action="store_true",
        help="Disable conflict resolution (skip all conflicts)"
    )
    
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help="Directory for log files (default: same as input)"
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    # Set up log paths
    log_dir = args.log_dir or args.input.parent
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    uploaded_log = log_dir / f"uploaded_{timestamp}.log"
    failed_log = log_dir / f"upload_failed_{timestamp}.jsonl"
    skipped_log = log_dir / f"skipped_{timestamp}.log"
    
    # Print configuration
    print("=" * 60)
    print("Court Document Uploader")
    print("=" * 60)
    print(f"Input:          {args.input}")
    print(f"Edit interval:  {args.interval}s")
    print(f"Maxlag:         {args.maxlag}")
    print(f"Max documents:  {args.max or 'all'}")
    print(f"Resolve:        {not args.no_resolve}")
    print(f"Uploaded log:   {uploaded_log}")
    print(f"Failed log:     {failed_log}")
    print(f"Skipped log:    {skipped_log}")
    print("=" * 60)
    print()
    
    # Create rate limiter
    rate_limiter = RateLimiter(
        min_interval=args.interval,
        maxlag=args.maxlag,
    )
    
    # Run upload
    start_time = datetime.now()
    print(f"Started at: {start_time.isoformat()}")
    print()
    
    try:
        uploaded, failed, skipped, resolved = process_upload_batch(
            input_path=args.input,
            uploaded_log=uploaded_log,
            failed_log=failed_log,
            skipped_log=skipped_log,
            rate_limiter=rate_limiter,
            resolve_conflicts=not args.no_resolve,
            max_documents=args.max,
        )
    except KeyboardInterrupt:
        print("\n\nUpload interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}", file=sys.stderr)
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    total = uploaded + failed + skipped + resolved
    
    # Print summary
    print()
    print("=" * 60)
    print("Upload Complete")
    print("=" * 60)
    print(f"Total processed:     {total}")
    print(f"Successfully uploaded: {uploaded}")
    print(f"Conflicts resolved:  {resolved}")
    print(f"Skipped:             {skipped}")
    print(f"Failed:              {failed}")
    print(f"Duration:            {duration}")
    if duration.total_seconds() > 0:
        print(f"Rate:                {total / duration.total_seconds():.2f} docs/sec")
    print("=" * 60)
    
    if failed > 0:
        print(f"\nCheck {failed_log} for details on failed uploads.")
    
    if uploaded == 0 and resolved == 0 and failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
