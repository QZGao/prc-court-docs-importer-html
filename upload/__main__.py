"""
CLI entry point for the upload phase.

Usage:
    python -m upload converted.jsonl [--interval 3] [--max 100]
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# Suppress pywikibot verbose output before importing it
logging.getLogger('pywiki').setLevel(logging.WARNING)
logging.getLogger('pywikibot').setLevel(logging.WARNING)

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from rich.console import Console

from .mediawiki import configure_throttle
from .uploader import process_upload_batch

console = Console()


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
        type=int,
        default=10,
        help="Minimum seconds between edits (default: 10, pywikibot's default)"
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
        console.print(f"[red]Error: Input file not found: {args.input}[/red]", file=sys.stderr)
        sys.exit(1)
    
    # Set up log paths
    log_dir = args.log_dir or args.input.parent
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    uploaded_log = log_dir / f"uploaded_{timestamp}.log"
    failed_log = log_dir / f"upload_failed_{timestamp}.jsonl"
    skipped_log = log_dir / f"skipped_{timestamp}.log"
    
    # Print configuration
    console.print("=" * 60)
    console.print("[bold cyan]Court Document Uploader[/bold cyan]")
    console.print("=" * 60)
    console.print(f"Input:          {args.input}")
    console.print(f"Edit interval:  {args.interval}s (pywikibot put_throttle)")
    console.print(f"Maxlag:         {args.maxlag}")
    console.print(f"Max documents:  {args.max or 'all'}")
    console.print(f"Resolve:        {not args.no_resolve}")
    console.print(f"Uploaded log:   {uploaded_log}")
    console.print(f"Failed log:     {failed_log}")
    console.print(f"Skipped log:    {skipped_log}")
    console.print("=" * 60)
    console.print()
    
    # Configure pywikibot's built-in rate limiting
    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    
    # Run upload
    start_time = datetime.now()
    console.print(f"Started at: {start_time.isoformat()}")
    console.print()
    
    try:
        uploaded, failed, skipped, resolved = process_upload_batch(
            input_path=args.input,
            uploaded_log=uploaded_log,
            failed_log=failed_log,
            skipped_log=skipped_log,
            resolve_conflicts=not args.no_resolve,
            max_documents=args.max,
        )
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Upload interrupted by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        console.print(f"\n[red]Fatal error: {e}[/red]", file=sys.stderr)
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    total = uploaded + failed + skipped + resolved
    
    # Print summary
    console.print()
    console.print("=" * 60)
    console.print("[bold green]Upload Complete[/bold green]")
    console.print("=" * 60)
    console.print(f"Total processed:       {total}")
    console.print(f"Successfully uploaded: {uploaded}")
    console.print(f"Conflicts resolved:    {resolved}")
    console.print(f"Skipped:               {skipped}")
    console.print(f"Failed:                {failed}")
    console.print(f"Duration:              {duration}")
    if duration.total_seconds() > 0:
        console.print(f"Rate:                  {total / duration.total_seconds():.2f} docs/sec")
    console.print("=" * 60)
    
    if failed > 0:
        console.print(f"\n[yellow]Check {failed_log} for details on failed uploads.[/yellow]")
    
    if uploaded == 0 and resolved == 0 and failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
