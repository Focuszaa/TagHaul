import base64
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ollama

logger = logging.getLogger("tagger")

DEFAULT_PHOTO_ROOT = "/mnt/synology/photos"
DEFAULT_DASHBOARD_ROOT = "/mnt/synology/photos"
DEFAULT_MODEL = "llava:13b"
DEFAULT_DB = "indexing.db"
DEFAULT_LOG = "tagger.log"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 5000
DEFAULT_MAX_WORKERS = 2
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png"}
IGNORED_DIRECTORY_NAMES = {"@eaDir", "#recycle", "@Recycle"}

OLLAMA_PROMPT = (
    "Analyze this image for high-precision search indexing. Provide a concise, descriptive string focusing on: 1.Main Subject (e.g., specific vehicle type, person, object)2.Setting (e.g., port, warehouse, outdoors, office) 3.Colors and Lighting (e.g., bright sunlight, neon, dominant blue) 4.Key Details (e.g., license plates, logos, weather). Format: [Subject] [Action] at [Setting], [Colors]. No introductory text. Max 20 words."
)


def setup_logging(log_path: str) -> None:
    """Configure the root logger to write to both the terminal and a log file."""
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    try:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except OSError as exc:
        console_only = logging.StreamHandler(sys.stderr)
        console_only.setFormatter(fmt)
        logger.addHandler(console_only)
        logger.warning("Could not open log file '%s': %s - logging to terminal only.", log_path, exc)

    logger.addHandler(console)


def open_db(db_path: str) -> sqlite3.Connection:
    """Open (or create) the SQLite tracking database and return the connection."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path    TEXT    NOT NULL UNIQUE,
            mtime        REAL    NOT NULL,
            processed_at TEXT    NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def open_task_db(db_path: str) -> sqlite3.Connection:
    """Open (or create) the shared task database; safe to use across threads."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path    TEXT    NOT NULL UNIQUE,
            mtime        REAL    NOT NULL,
            processed_at TEXT    NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id             TEXT    PRIMARY KEY,
            root           TEXT    NOT NULL,
            selected_paths TEXT    NOT NULL,
            model          TEXT    NOT NULL,
            prompt         TEXT    NOT NULL,
            temperature    REAL    NOT NULL,
            dry_run        INTEGER NOT NULL DEFAULT 0,
            status         TEXT    NOT NULL DEFAULT 'queued',
            total          INTEGER NOT NULL DEFAULT 0,
            processed      INTEGER NOT NULL DEFAULT 0,
            skipped        INTEGER NOT NULL DEFAULT 0,
            errors         INTEGER NOT NULL DEFAULT 0,
            completed      INTEGER NOT NULL DEFAULT 0,
            avg_seconds    REAL    NOT NULL DEFAULT 0.0,
            created_at     TEXT    NOT NULL,
            started_at     TEXT,
            completed_at   TEXT
        )
        """
    )
    conn.commit()
    return conn


def is_processed(conn: sqlite3.Connection, file_path: str, mtime: float) -> bool:
    """Return True when the stored record matches the current path and mtime."""
    row = conn.execute(
        "SELECT mtime FROM processed_files WHERE file_path = ?",
        (file_path,),
    ).fetchone()
    return row is not None and row[0] == mtime


def mark_processed(conn: sqlite3.Connection, file_path: str, mtime: float) -> None:
    """Upsert a record into the DB to mark this file as successfully processed."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO processed_files (file_path, mtime, processed_at)
        VALUES (?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET mtime = excluded.mtime,
                                             processed_at = excluded.processed_at
        """,
        (file_path, mtime, now),
    )
    conn.commit()


def iter_images(root: str):
    """Yield Path objects for every supported image under *root*."""
    for current_root, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in IGNORED_DIRECTORY_NAMES]
        for filename in filenames:
            path = Path(current_root) / filename
            if path.suffix.lower() in SUPPORTED_EXTENSIONS:
                yield path


def is_visible_directory(path: Path) -> bool:
    """Return True when *path* should appear in the dashboard explorer."""
    return path.is_dir() and path.name not in IGNORED_DIRECTORY_NAMES


def directory_has_visible_children(path: Path) -> bool:
    """Return True when a directory contains visible folders or supported images."""
    try:
        for child in path.iterdir():
            if is_visible_directory(child) or is_supported_image(child):
                return True
    except OSError:
        return False
    return False


def is_supported_image(path: Path) -> bool:
    """Return True when *path* is a supported image file."""
    return path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS


def resolve_relative_path(root: str, relative_path: str = "") -> Path:
    """Resolve a relative path under *root* and reject traversal outside it."""
    root_path = Path(root).resolve()
    target_path = (root_path / relative_path).resolve()

    try:
        target_path.relative_to(root_path)
    except ValueError as exc:
        raise ValueError("Path escapes the configured photo root.") from exc

    return target_path


def to_relative_path(root: str, target_path: Path) -> str:
    """Return a POSIX-style relative path from *root* to *target_path*."""
    root_path = Path(root).resolve()
    relative = target_path.resolve().relative_to(root_path)
    return relative.as_posix()


def list_directory(root: str, relative_path: str = "") -> dict:
    """List immediate child directories and supported images for the given directory."""
    current_path = resolve_relative_path(root, relative_path)
    if not current_path.exists():
        raise FileNotFoundError(f"Path does not exist: {relative_path or '.'}")
    if not current_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {relative_path or '.'}")

    folders = []
    files = []

    for child in sorted(current_path.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower())):
        if is_visible_directory(child):
            folders.append(
                {
                    "name": child.name,
                    "path": to_relative_path(root, child),
                    "has_children": directory_has_visible_children(child),
                }
            )
        elif is_supported_image(child):
            stat_result = child.stat()
            files.append(
                {
                    "name": child.name,
                    "path": to_relative_path(root, child),
                    "size": stat_result.st_size,
                    "mtime": stat_result.st_mtime,
                }
            )

    parent = None
    if current_path != Path(root).resolve():
        parent = to_relative_path(root, current_path.parent)

    return {
        "root": root,
        "current": relative_path,
        "parent": parent,
        "folders": folders,
        "files": files,
    }


def expand_selection(root: str, selected_paths) -> list[Path]:
    """Expand selected file and directory paths under *root* into unique image files."""
    image_paths = set()

    for raw_path in selected_paths:
        target_path = resolve_relative_path(root, raw_path)
        if target_path.is_dir():
            for image_path in iter_images(str(target_path)):
                image_paths.add(image_path.resolve())
        elif is_supported_image(target_path):
            image_paths.add(target_path.resolve())

    return sorted(image_paths)


def serialize_event(payload: dict) -> str:
    """Serialize an event payload for server-sent events."""
    event_name = payload.get("event", "message")
    return f"event: {event_name}\ndata: {json.dumps(payload)}\n\n"


def describe_image(
    image_path: Path,
    model: str,
    prompt: str = OLLAMA_PROMPT,
    temperature: float = 0.2,
) -> str:
    """Send *image_path* to Ollama and return the generated description string."""
    with open(image_path, "rb") as fh:
        image_data = base64.b64encode(fh.read()).decode("utf-8")

    try:
        response = ollama.chat(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                    "images": [image_data],
                }
            ],
            options={"temperature": temperature},
        )
    except Exception as exc:
        raise RuntimeError(f"Ollama error: {exc}") from exc

    description = response["message"]["content"].strip()
    if not description:
        raise RuntimeError("Ollama returned an empty description.")
    return description


def write_metadata(image_path: Path, description: str) -> None:
    """Write *description* into the image metadata using exiftool."""
    cmd = [
        "exiftool",
        "-overwrite_original",
        "-m",
        f"-Description={description}",
        f"-ImageDescription={description}",
        f"-XPComment={description}",
        f"-Keywords={description}",
        f"-Subject={description}",
        str(image_path),
    ]

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"exiftool failed (exit {result.returncode}): {result.stderr.strip()}"
        )


def read_existing_metadata(image_path: Path) -> dict[str, str]:
    """Read selected metadata fields from *image_path* using exiftool."""
    cmd = [
        "exiftool",
        "-j",
        "-m",
        "-Description",
        "-UserComment",
        "-ImageDescription",
        "-XPComment",
        str(image_path),
    ]

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"exiftool metadata read failed (exit {result.returncode}): {result.stderr.strip()}"
        )

    try:
        payload: list[dict[str, Any]] = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse exiftool metadata output: {exc}") from exc

    if not payload:
        return {}

    values = payload[0]
    return {
        "Description": str(values.get("Description", "") or "").strip(),
        "UserComment": str(values.get("UserComment", "") or "").strip(),
        "ImageDescription": str(values.get("ImageDescription", "") or "").strip(),
        "XPComment": str(values.get("XPComment", "") or "").strip(),
    }


def has_existing_description(image_path: Path) -> bool:
    """Return True if the image already contains a manual description field."""
    metadata = read_existing_metadata(image_path)
    for key in ("Description", "UserComment", "ImageDescription", "XPComment"):
        if metadata.get(key):
            return True
    return False


def should_skip_file(
    conn: sqlite3.Connection,
    file_path: str,
    mtime: float,
    image_path: Path,
) -> tuple[bool, str | None]:
    """Return whether a file should be skipped and the reason for the skip."""
    if is_processed(conn, file_path, mtime):
        return True, "db"

    if has_existing_description(image_path):
        return True, "metadata"

    return False, None


def process_single_image(
    image_path: str | Path,
    model: str,
    db_path: str,
    dry_run: bool,
    *,
    prompt: str = OLLAMA_PROMPT,
    temperature: float = 0.2,
) -> dict[str, Any]:
    """Process exactly one image and return a structured result."""
    path = Path(image_path)
    file_path_str = str(path)

    try:
        mtime = os.path.getmtime(path)
    except OSError as exc:
        return {
            "event": "error",
            "path": file_path_str,
            "message": f"cannot read mtime: {exc}",
        }

    if dry_run:
        logger.info("[DRY-RUN] %s", file_path_str)
        return {
            "event": "dry-run",
            "path": file_path_str,
            "description": None,
        }

    conn = open_db(db_path)
    try:
        skip, reason = should_skip_file(conn, file_path_str, mtime, path)
        if skip:
            if reason == "metadata":
                logger.info("Skipping: Manual description already exists in file. %s", file_path_str)
                return {
                    "event": "skip",
                    "path": file_path_str,
                    "message": "Skipping: Manual description already exists in file.",
                    "skip_reason": reason,
                }

            logger.debug("[CACHED] %s", file_path_str)
            return {
                "event": "skip",
                "path": file_path_str,
                "skip_reason": reason,
            }

        description = describe_image(
            path,
            model,
            prompt=prompt,
            temperature=temperature,
        )

        write_metadata(path, description)

        try:
            written_mtime = os.path.getmtime(path)
        except OSError:
            written_mtime = mtime

        mark_processed(conn, file_path_str, written_mtime)
        logger.info("metadata written: %s", file_path_str)
        return {
            "event": "written",
            "path": file_path_str,
            "description": description,
        }
    except RuntimeError as exc:
        return {
            "event": "error",
            "path": file_path_str,
            "message": str(exc),
        }
    finally:
        conn.close()


def process_image_paths(
    image_paths,
    model: str,
    db_path: str,
    dry_run: bool,
    *,
    prompt: str = OLLAMA_PROMPT,
    temperature: float = 0.2,
    event_callback=None,
    stop_event: "threading.Event | None" = None,
) -> dict:
    """Process a concrete list of image paths and return a summary dictionary."""
    images = [Path(image_path) for image_path in image_paths]
    total = len(images)

    if total == 0:
        logger.info("No supported images found.")
        summary = {"completed": 0, "processed": 0, "skipped": 0, "errors": 0, "total": 0}
        if event_callback:
            event_callback({"event": "complete", **summary})
        return summary

    if event_callback:
        event_callback({"event": "start", "total": total})

    skipped = 0
    processed = 0
    errors = 0

    was_stopped = False
    for idx, image_path in enumerate(images, start=1):
        if stop_event and stop_event.is_set():
            was_stopped = True
            break

        file_path_str = str(image_path)
        if event_callback and not dry_run:
            event_callback({
                "event": "processing",
                "current": idx,
                "total": total,
                "path": file_path_str,
            })

        result = process_single_image(
            image_path,
            model,
            db_path,
            dry_run,
            prompt=prompt,
            temperature=temperature,
        )

        event_name = result["event"]
        payload = {
            "event": event_name,
            "current": idx,
            "total": total,
            "path": result.get("path", file_path_str),
        }

        if result.get("description"):
            logger.info("[%d/%d] DESCRIBE  %s", idx, total, image_path.name)
            logger.info("            %s", result["description"])
            if event_callback:
                event_callback({
                    "event": "describe",
                    "current": idx,
                    "total": total,
                    "path": file_path_str,
                    "description": result["description"],
                })

        if result.get("message"):
            payload["message"] = result["message"]
        if result.get("skip_reason"):
            payload["skip_reason"] = result["skip_reason"]

        if event_name == "written":
            processed += 1
        elif event_name == "skip":
            skipped += 1
        elif event_name == "error":
            logger.error("[%d/%d] ERROR  %s - %s", idx, total, file_path_str, result.get("message", "unknown error"))
            errors += 1

        if event_callback:
            event_callback(payload)

    total_handled = processed + skipped + errors
    summary = {
        "completed": total_handled,
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "total": total,
    }
    logger.info("-" * 60)
    if was_stopped:
        logger.info("Stopped. Processed: %d  Skipped: %d  Errors: %d", processed, skipped, errors)
    elif dry_run:
        logger.info("Dry-run complete. %d file(s) would be evaluated.", total)
    else:
        logger.info(
            "Done. Processed: %d  Skipped (cached): %d  Errors: %d",
            processed,
            skipped,
            errors,
        )
    if event_callback:
        event_name = "stopped" if was_stopped else "complete"
        event_callback({"event": event_name, **summary})
    return summary


def process_images(
    photo_root: str,
    model: str,
    db_path: str,
    dry_run: bool,
    *,
    prompt: str = OLLAMA_PROMPT,
    temperature: float = 0.2,
    event_callback=None,
) -> dict:
    """Process all supported images under *photo_root*."""
    images = list(iter_images(photo_root))
    if not images:
        logger.info("No supported images found under '%s'.", photo_root)
        summary = {"completed": 0, "processed": 0, "skipped": 0, "errors": 0, "total": 0}
        if event_callback:
            event_callback({"event": "complete", **summary})
        return summary

    logger.info("Found %d image(s) under '%s'. Scanning...", len(images), photo_root)
    return process_image_paths(
        images,
        model,
        db_path,
        dry_run,
        prompt=prompt,
        temperature=temperature,
        event_callback=event_callback,
    )