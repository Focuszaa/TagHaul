#!/usr/bin/env python3
"""
AI Photo Tagger
---------------
Walks a directory of images, describes each one using Ollama (llava:13b),
and writes the description into EXIF/IPTC/XMP metadata via exiftool.
A local SQLite database (indexing.db) tracks processed files so re-runs
only touch new or changed images.

Usage:
    python tagger.py [OPTIONS]

Options:
    --path PATH     Root directory to scan  (default: /mnt/synology)
    --model MODEL   Ollama model to use     (default: llava:13b)
    --db DB         Path to SQLite DB       (default: ./indexing.db)
    --log LOG       Path to log file        (default: ./tagger.log)
    --dry-run       Print actions only; do not write metadata or update DB
"""

import argparse
import sys
from pathlib import Path
from tagger_backend import DEFAULT_DB
from tagger_backend import DEFAULT_LOG
from tagger_backend import DEFAULT_MODEL
from tagger_backend import DEFAULT_PHOTO_ROOT
from tagger_backend import logger
from tagger_backend import process_images
from tagger_backend import setup_logging


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tag photos on your NAS with AI-generated descriptions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--path",
        default=DEFAULT_PHOTO_ROOT,
        help=f"Root directory to scan (default: {DEFAULT_PHOTO_ROOT})",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Ollama model name (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help=f"Path to the SQLite tracking database (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--log",
        default=DEFAULT_LOG,
        help=f"Path to the log file (default: {DEFAULT_LOG})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without writing anything",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log)

    if not Path(args.path).is_dir():
        logger.error("Photo root '%s' is not a directory or is not mounted.", args.path)
        sys.exit(1)

    logger.info("AI Photo Tagger")
    logger.info("  Photo root : %s", args.path)
    logger.info("  Model      : %s", args.model)
    logger.info("  Database   : %s", args.db)
    logger.info("  Log file   : %s", args.log)
    logger.info("  Dry run    : %s", args.dry_run)
    logger.info("")

    process_images(
        photo_root=args.path,
        model=args.model,
        db_path=args.db,
        dry_run=args.dry_run,
    )
    logger.info("Log saved to: %s", args.log)


if __name__ == "__main__":
    main()
