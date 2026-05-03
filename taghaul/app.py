import json
import os
import queue
import threading
import uuid
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from flask import Flask
from flask import Response
from flask import jsonify
from flask import render_template
from flask import request

from .tagger_backend import DEFAULT_DASHBOARD_ROOT
from .tagger_backend import DEFAULT_DB
from .tagger_backend import DEFAULT_HOST
from .tagger_backend import DEFAULT_LOG
from .tagger_backend import DEFAULT_MAX_WORKERS
from .tagger_backend import DEFAULT_MODEL
from .tagger_backend import DEFAULT_PORT
from .tagger_backend import OLLAMA_PROMPT
from .tagger_backend import expand_selection
from .tagger_backend import list_directory
from .tagger_backend import logger
from .tagger_backend import open_task_db
from .tagger_backend import process_single_image
from .tagger_backend import serialize_event
from .tagger_backend import setup_logging

APP_ROOT = Path(__file__).resolve().parent
DASHBOARD_ROOT_ENV_VAR = "PHOTO_TAGGER_DASHBOARD_ROOT"
DB_PATH_ENV_VAR = "PHOTO_TAGGER_DB_PATH"
HOST_ENV_VAR = "PHOTO_TAGGER_HOST"
PORT_ENV_VAR = "PHOTO_TAGGER_PORT"
SETTINGS_PATH_ENV_VAR = "PHOTO_TAGGER_SETTINGS_PATH"
MAX_WORKERS_ENV_VAR = "PHOTO_TAGGER_MAX_WORKERS"

app = Flask(__name__)
setup_logging(str(APP_ROOT / DEFAULT_LOG))


def get_dashboard_root() -> str:
    configured = os.environ.get(DASHBOARD_ROOT_ENV_VAR, DEFAULT_DASHBOARD_ROOT)
    return str(Path(configured).expanduser().resolve())


def get_db_path() -> str:
    configured = os.environ.get(DB_PATH_ENV_VAR, DEFAULT_DB)
    return str(Path(configured).expanduser().resolve())


def get_settings_path() -> str:
    configured = os.environ.get(SETTINGS_PATH_ENV_VAR, APP_ROOT / "tagger_settings.json")
    return str(Path(configured).expanduser().resolve())


def resolve_dashboard_root(root_value: str | None) -> str:
    candidate = root_value or get_dashboard_root()
    root_path = Path(candidate).expanduser().resolve()
    if not root_path.exists():
        raise ValueError(f"Photo root does not exist: {root_path}")
    if not root_path.is_dir():
        raise ValueError(f"Photo root is not a directory: {root_path}")
    return str(root_path)


def normalize_selected_paths(root: str, selected_paths: list[str]) -> list[str]:
    """Keep the most specific selections to avoid accidental broad scans."""
    root_path = Path(root).resolve()
    resolved_paths: list[tuple[str, Path]] = []
    seen: set[str] = set()

    for raw_path in selected_paths:
        normalized = raw_path.strip().strip("/")
        if normalized in seen:
            continue

        resolved_path = (root_path / normalized).resolve() if normalized else root_path
        try:
            resolved_path.relative_to(root_path)
        except ValueError as exc:
            raise ValueError("Selected path escapes the configured photo root.") from exc

        seen.add(normalized)
        resolved_paths.append((normalized, resolved_path))

    kept: list[tuple[str, Path]] = []
    for candidate in sorted(resolved_paths, key=lambda item: (-len(item[1].parts), item[0])):
        _, candidate_path = candidate
        if any(existing_path.is_relative_to(candidate_path) for _, existing_path in kept):
            continue
        kept.append(candidate)

    return [raw_path for raw_path, _ in sorted(kept, key=lambda item: (len(item[1].parts), item[0]))]


class TaskManager:
    """Global multi-task scheduler with shared worker concurrency across all tasks."""

    def __init__(self, db_path: str, settings_path: str) -> None:
        self._db_path = db_path
        self._settings_path = Path(settings_path)
        self._lock = threading.Lock()
        self._tasks: dict[str, dict] = {}
        self._task_order: list[str] = []
        self._dispatch_cursor = 0
        self._conn = open_task_db(db_path)
        self._future_jobs: dict[Future, dict] = {}
        self._global_sum_seconds = 0.0
        self._global_processed_count = 0
        self._max_workers = self._load_max_workers()
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="taghaul")
        self._prepare_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="taghaul-prepare")
        self._preparation_future: Future | None = None
        self._preparation_task_id: str | None = None
        self._load_tasks_from_db()
        self._scheduler = threading.Thread(target=self._schedule_loop, daemon=True)
        self._scheduler.start()

    def _load_max_workers(self) -> int:
        default_value = int(os.environ.get(MAX_WORKERS_ENV_VAR, DEFAULT_MAX_WORKERS))
        default_value = max(1, min(4, default_value))

        if not self._settings_path.exists():
            return default_value

        try:
            payload = json.loads(self._settings_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return default_value

        stored = int(payload.get("max_workers", default_value))
        return max(1, min(4, stored))

    def _save_settings_locked(self) -> None:
        self._settings_path.parent.mkdir(parents=True, exist_ok=True)
        self._settings_path.write_text(
            json.dumps({"max_workers": self._max_workers}, indent=2),
            encoding="utf-8",
        )

    def get_settings(self) -> dict:
        with self._lock:
            return {
                "max_workers": self._max_workers,
                "global_eta_seconds": self._compute_global_eta_locked(),
            }

    def update_max_workers(self, new_value: int) -> dict:
        if new_value < 1:
            raise ValueError("Max parallel workers must be at least 1.")
        if new_value > 4:
            raise ValueError("Max parallel workers cannot exceed 4 because of VRAM limits.")

        with self._lock:
            if new_value == self._max_workers:
                settings = {
                    "max_workers": self._max_workers,
                    "global_eta_seconds": self._compute_global_eta_locked(),
                }
            else:
                old_executor = self._executor
                self._max_workers = new_value
                self._executor = ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="taghaul")
                self._save_settings_locked()
                self._recalculate_global_eta_locked()
                settings = {
                    "max_workers": self._max_workers,
                    "global_eta_seconds": self._compute_global_eta_locked(),
                }
                old_executor.shutdown(wait=False, cancel_futures=False)

        return settings

    def _load_tasks_from_db(self) -> None:
        cursor = self._conn.execute(
            "SELECT id, root, selected_paths, model, prompt, temperature, dry_run, "
            "status, total, processed, skipped, errors, completed, avg_seconds, "
            "created_at, started_at, completed_at FROM tasks ORDER BY created_at ASC"
        )
        rows = cursor.fetchall()
        for row in rows:
            (
                task_id,
                root,
                selected_paths_json,
                model,
                prompt,
                temperature,
                dry_run,
                status,
                total,
                processed,
                skipped,
                errors,
                completed,
                avg_seconds,
                created_at,
                started_at,
                completed_at,
            ) = row
            if status in ("queued", "running"):
                status = "stopped"
                self._conn.execute("UPDATE tasks SET status='stopped' WHERE id=?", (task_id,))
            task = self._make_task_dict(
                task_id=task_id,
                root=root,
                selected_paths=json.loads(selected_paths_json),
                model=model,
                prompt=prompt,
                temperature=float(temperature),
                dry_run=bool(dry_run),
                status=status,
                total=int(total or 0),
                processed=int(processed or 0),
                skipped=int(skipped or 0),
                errors=int(errors or 0),
                completed=int(completed or 0),
                avg_seconds=float(avg_seconds or 0.0),
                created_at=created_at,
                started_at=started_at,
                completed_at=completed_at,
            )
            self._tasks[task_id] = task
            self._task_order.append(task_id)
        self._conn.commit()
        self._recalculate_global_eta_locked()

    @staticmethod
    def _make_task_dict(
        *,
        task_id: str,
        root: str,
        selected_paths: list,
        model: str,
        prompt: str,
        temperature: float,
        dry_run: bool,
        status: str,
        total: int,
        processed: int,
        skipped: int,
        errors: int,
        completed: int,
        avg_seconds: float,
        created_at: str,
        started_at: str | None,
        completed_at: str | None,
    ) -> dict:
        return {
            "id": task_id,
            "root": root,
            "selected_paths": selected_paths,
            "model": model,
            "prompt": prompt,
            "temperature": temperature,
            "dry_run": dry_run,
            "status": status,
            "total": total,
            "processed": processed,
            "skipped": skipped,
            "errors": errors,
            "completed": completed,
            "avg_seconds": avg_seconds,
            "sum_seconds": 0.0,
            "count_processed": 0,
            "eta_seconds": None,
            "created_at": created_at,
            "started_at": started_at,
            "completed_at": completed_at,
            "stop_event": threading.Event(),
            "listeners": [],
            "history": [],
            "pending_paths": None,
            "preparing": False,
            "active_futures": 0,
            "terminal_emitted": False,
        }

    def _snapshot_from_task(self, task_id: str, task: dict) -> dict:
        return {
            "id": task_id,
            "root": task["root"],
            "selected_paths": list(task["selected_paths"]),
            "model": task["model"],
            "prompt": task["prompt"],
            "temperature": task["temperature"],
            "dry_run": task["dry_run"],
            "status": task["status"],
            "total": task["total"],
            "processed": task["processed"],
            "skipped": task["skipped"],
            "errors": task["errors"],
            "completed": task["completed"],
            "avg_seconds": round(task["avg_seconds"], 2),
            "eta_seconds": task["eta_seconds"],
            "created_at": task["created_at"],
            "started_at": task["started_at"],
            "completed_at": task["completed_at"],
            "max_workers": self._max_workers,
        }

    def _compute_global_eta_locked(self) -> int | None:
        if self._global_processed_count > 0:
            avg_seconds = self._global_sum_seconds / self._global_processed_count
        else:
            per_task_averages = [task["avg_seconds"] for task in self._tasks.values() if task["avg_seconds"] > 0]
            avg_seconds = (sum(per_task_averages) / len(per_task_averages)) if per_task_averages else 0.0

        if avg_seconds <= 0:
            return None

        remaining = 0
        for task in self._tasks.values():
            if task["status"] in ("queued", "running"):
                remaining += max(task["total"] - task["completed"], 0)

        if remaining <= 0:
            return 0

        return int((remaining / max(self._max_workers, 1)) * avg_seconds)

    def _next_unprepared_task_locked(self) -> tuple[str | None, dict | None]:
        for task_id in self._task_order:
            task = self._tasks.get(task_id)
            if not task:
                continue
            if task["status"] not in ("queued", "running"):
                continue
            if task["stop_event"].is_set() or task["preparing"]:
                continue
            if task["pending_paths"] is None:
                task["preparing"] = True
                return task_id, task
        return None, None

    def _recalculate_global_eta_locked(self) -> int | None:
        global_eta = self._compute_global_eta_locked()
        for task in self._tasks.values():
            if task["status"] in ("queued", "running"):
                task["eta_seconds"] = global_eta
            elif task["status"] == "stopped":
                task["eta_seconds"] = None
            elif task["status"] in ("completed", "failed"):
                task["eta_seconds"] = 0
        return global_eta

    def _publish(self, task_id: str, payload: dict) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            event_text = serialize_event(payload)
            task["history"].append(event_text)
            listeners = list(task["listeners"])

        for listener in listeners:
            listener.put(event_text)

    def _next_dispatchable_task_locked(self) -> tuple[str | None, dict | None]:
        if not self._task_order:
            return None, None

        count = len(self._task_order)
        for offset in range(count):
            index = (self._dispatch_cursor + offset) % count
            task_id = self._task_order[index]
            task = self._tasks.get(task_id)
            if not task:
                continue
            if task["status"] not in ("queued", "running"):
                continue
            if task["stop_event"].is_set():
                continue
            if task["pending_paths"]:
                self._dispatch_cursor = (index + 1) % count
                return task_id, task

        return None, None

    def _finalize_task_locked(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if not task or task["terminal_emitted"]:
            return
        if task["active_futures"] > 0 or task["pending_paths"]:
            return

        if task["stop_event"].is_set() or task["status"] == "stopped":
            task["status"] = "stopped"
            task["eta_seconds"] = None
            self._conn.execute(
                "UPDATE tasks SET status='stopped', processed=?, skipped=?, errors=?, completed=? WHERE id=?",
                (task["processed"], task["skipped"], task["errors"], task["completed"], task_id),
            )
            self._conn.commit()
            task["terminal_emitted"] = True
            self._recalculate_global_eta_locked()
            payload = {
                "event": "stopped",
                "task_id": task_id,
                **self._snapshot_from_task(task_id, task),
            }
        else:
            task["status"] = "completed" if task["errors"] == 0 else "failed"
            task["completed_at"] = datetime.now(timezone.utc).isoformat()
            task["eta_seconds"] = 0
            self._conn.execute(
                "UPDATE tasks SET status=?, completed_at=?, processed=?, skipped=?, errors=?, completed=?, avg_seconds=? WHERE id=?",
                (
                    task["status"],
                    task["completed_at"],
                    task["processed"],
                    task["skipped"],
                    task["errors"],
                    task["completed"],
                    task["avg_seconds"],
                    task_id,
                ),
            )
            self._conn.commit()
            task["terminal_emitted"] = True
            self._recalculate_global_eta_locked()
            payload = {
                "event": "complete",
                "task_id": task_id,
                "completed": task["completed"],
                "processed": task["processed"],
                "skipped": task["skipped"],
                "errors": task["errors"],
                "total": task["total"],
                "eta_seconds": task["eta_seconds"],
            }

        listeners = list(task["listeners"])
        event_text = serialize_event(payload)
        task["history"].append(event_text)

        for listener in listeners:
            listener.put(event_text)

    def _handle_worker_result_locked(self, task_id: str, result: dict, started_at: float) -> list[dict]:
        task = self._tasks.get(task_id)
        if not task:
            return []

        duration = monotonic() - started_at
        events: list[dict] = []
        event_name = result.get("event")
        task["completed"] += 1

        if event_name == "written":
            task["processed"] += 1
            task["sum_seconds"] += duration
            task["count_processed"] += 1
            task["avg_seconds"] = task["sum_seconds"] / task["count_processed"]
            self._global_sum_seconds += duration
            self._global_processed_count += 1
            self._recalculate_global_eta_locked()
            self._conn.execute(
                "UPDATE tasks SET processed=?, completed=?, avg_seconds=? WHERE id=?",
                (task["processed"], task["completed"], task["avg_seconds"], task_id),
            )
            self._conn.commit()
            if result.get("description"):
                events.append(
                    {
                        "event": "describe",
                        "task_id": task_id,
                        "path": result.get("path"),
                        "description": result.get("description"),
                    }
                )
            events.append(
                {
                    "event": "written",
                    "task_id": task_id,
                    "path": result.get("path"),
                    "description": result.get("description"),
                    "current": task["completed"],
                    "total": task["total"],
                    "eta_seconds": task["eta_seconds"],
                    "avg_seconds": round(task["avg_seconds"], 2),
                }
            )
        elif event_name == "skip":
            task["skipped"] += 1
            self._recalculate_global_eta_locked()
            self._conn.execute(
                "UPDATE tasks SET skipped=?, completed=? WHERE id=?",
                (task["skipped"], task["completed"], task_id),
            )
            self._conn.commit()
            events.append(
                {
                    "event": "skip",
                    "task_id": task_id,
                    "path": result.get("path"),
                    "message": result.get("message"),
                    "skip_reason": result.get("skip_reason"),
                    "current": task["completed"],
                    "total": task["total"],
                    "eta_seconds": task["eta_seconds"],
                }
            )
        elif event_name == "dry-run":
            self._recalculate_global_eta_locked()
            self._conn.execute(
                "UPDATE tasks SET completed=? WHERE id=?",
                (task["completed"], task_id),
            )
            self._conn.commit()
            events.append(
                {
                    "event": "dry-run",
                    "task_id": task_id,
                    "path": result.get("path"),
                    "current": task["completed"],
                    "total": task["total"],
                    "eta_seconds": task["eta_seconds"],
                }
            )
        else:
            task["errors"] += 1
            self._recalculate_global_eta_locked()
            self._conn.execute(
                "UPDATE tasks SET errors=?, completed=? WHERE id=?",
                (task["errors"], task["completed"], task_id),
            )
            self._conn.commit()
            events.append(
                {
                    "event": "error",
                    "task_id": task_id,
                    "path": result.get("path"),
                    "message": result.get("message", "unknown error"),
                    "current": task["completed"],
                    "total": task["total"],
                    "eta_seconds": task["eta_seconds"],
                }
            )

        return events

    def create_task(
        self,
        *,
        selected_paths: list[str],
        root: str,
        model: str,
        dry_run: bool,
        prompt: str,
        temperature: float,
    ) -> dict:
        normalized_paths = normalize_selected_paths(root, selected_paths)
        if not normalized_paths:
            raise ValueError("No supported images found in the selected paths.")

        task_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()

        task = self._make_task_dict(
            task_id=task_id,
            root=root,
            selected_paths=normalized_paths,
            model=model,
            prompt=prompt,
            temperature=temperature,
            dry_run=dry_run,
            status="queued",
            total=0,
            processed=0,
            skipped=0,
            errors=0,
            completed=0,
            avg_seconds=0.0,
            created_at=now,
            started_at=None,
            completed_at=None,
        )

        with self._lock:
            self._conn.execute(
                "INSERT INTO tasks (id, root, selected_paths, model, prompt, temperature, dry_run, "
                "status, total, processed, skipped, errors, completed, avg_seconds, created_at, "
                "started_at, completed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id,
                    root,
                    json.dumps(normalized_paths),
                    model,
                    prompt,
                    temperature,
                    int(dry_run),
                    "queued",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0.0,
                    now,
                    None,
                    None,
                ),
            )
            self._conn.commit()
            self._tasks[task_id] = task
            self._task_order.append(task_id)
            self._recalculate_global_eta_locked()
            snapshot = self._snapshot_from_task(task_id, task)

        return snapshot

    def get_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            return self._snapshot_from_task(task_id, task) if task else None

    def list_tasks(self) -> list[dict]:
        with self._lock:
            return [self._snapshot_from_task(task_id, task) for task_id, task in self._tasks.items()]

    def stop_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            task["stop_event"].set()
            task["status"] = "stopped"
            task["pending_paths"] = []
            task["preparing"] = False
            self._conn.execute("UPDATE tasks SET status='stopped' WHERE id=?", (task_id,))
            self._conn.commit()
            self._recalculate_global_eta_locked()
            snapshot = self._snapshot_from_task(task_id, task)
            if task["active_futures"] == 0:
                self._finalize_task_locked(task_id)
            return snapshot

    def resume_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            if task["status"] != "stopped":
                return self._snapshot_from_task(task_id, task)
            if task["active_futures"] > 0:
                raise ValueError("Wait for in-flight work to finish before resuming this task.")

        with self._lock:
            task = self._tasks[task_id]
            task["stop_event"] = threading.Event()
            task["status"] = "queued"
            task["processed"] = 0
            task["skipped"] = 0
            task["errors"] = 0
            task["completed"] = 0
            task["sum_seconds"] = 0.0
            task["count_processed"] = 0
            task["avg_seconds"] = 0.0
            task["eta_seconds"] = None
            task["pending_paths"] = None
            task["preparing"] = False
            task["total"] = 0
            task["completed_at"] = None
            task["history"] = []
            task["terminal_emitted"] = False
            self._conn.execute(
                "UPDATE tasks SET status='queued', total=?, processed=0, skipped=0, errors=0, completed=0, avg_seconds=0, completed_at=NULL WHERE id=?",
                (task["total"], task_id),
            )
            self._conn.commit()
            self._recalculate_global_eta_locked()
            return self._snapshot_from_task(task_id, task)

    def clear_completed(self) -> None:
        with self._lock:
            to_remove = []
            for task_id, task in self._tasks.items():
                if task["status"] in ("completed", "failed") and task["active_futures"] == 0:
                    to_remove.append(task_id)

            for task_id in to_remove:
                self._tasks.pop(task_id, None)
                if task_id in self._task_order:
                    self._task_order.remove(task_id)

    def attach_listener(self, task_id: str) -> tuple[dict, queue.Queue]:
        listener: queue.Queue = queue.Queue()
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                raise KeyError(task_id)
            for event_text in task["history"]:
                listener.put(event_text)
            task["listeners"].append(listener)
            return self._snapshot_from_task(task_id, task), listener

    def detach_listener(self, task_id: str, listener: queue.Queue) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task and listener in task["listeners"]:
                task["listeners"].remove(listener)

    # ------------------------------------------------------------------
    # Scheduler support methods
    # ------------------------------------------------------------------

    def _get_default_root(self) -> str:
        """Return the default photo root path for scheduler batch execution."""
        return get_dashboard_root()

    def create_scheduled_batch(
        self, name, schedule_type, schedule_value, selected_paths,
        model=None, prompt=None, temperature=0.2, dry_run=False,
        tags=None, notifications=None,
    ) -> dict:
        from .scheduler import ScheduleParser

        batch_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)

        if model is None:
            model = DEFAULT_MODEL
        if prompt is None:
            prompt = OLLAMA_PROMPT

        next_run = ScheduleParser.calculate_next_run(schedule_type, schedule_value)
        if not next_run and schedule_type == 'once':
            raise ValueError("Schedule time is in the past")

        with self._lock:
            self._conn.execute("""
                INSERT INTO scheduled_batches
                (id, name, enabled, schedule_type, schedule_value, selected_paths,
                 model, prompt, temperature, dry_run, next_run_at, created_at, updated_at, tags, notifications)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                batch_id, name, 1, schedule_type, json.dumps(schedule_value),
                json.dumps(selected_paths), model, prompt, temperature,
                int(dry_run), next_run.isoformat(), now.isoformat(), now.isoformat(),
                json.dumps(tags or []), json.dumps(notifications or {}),
            ))
            self._conn.commit()

        return self.get_scheduled_batch(batch_id)

    def get_scheduled_batch(self, batch_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM scheduled_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if not row:
                return None
            return {
                'id': row[0],
                'name': row[1],
                'enabled': bool(row[2]),
                'schedule_type': row[3],
                'schedule_value': json.loads(row[4]),
                'selected_paths': json.loads(row[5]),
                'model': row[6],
                'prompt': row[7],
                'temperature': row[8],
                'dry_run': bool(row[9]),
                'last_run_at': row[10],
                'next_run_at': row[11],
                'created_at': row[12],
                'updated_at': row[13],
                'tags': json.loads(row[14]) if row[14] else [],
                'notifications': json.loads(row[15]) if row[15] else {},
            }

    def get_scheduled_batches(self, enabled_only=False):
        with self._lock:
            query = "SELECT * FROM scheduled_batches"
            if enabled_only:
                query += " WHERE enabled = 1"
            query += " ORDER BY next_run_at ASC"

            rows = self._conn.execute(query).fetchall()
            batches = []
            for row in rows:
                batches.append({
                    'id': row[0],
                    'name': row[1],
                    'enabled': bool(row[2]),
                    'schedule_type': row[3],
                    'schedule_value': json.loads(row[4]),
                    'selected_paths': json.loads(row[5]),
                    'model': row[6],
                    'prompt': row[7],
                    'temperature': row[8],
                    'dry_run': bool(row[9]),
                    'last_run_at': row[10],
                    'next_run_at': row[11],
                    'created_at': row[12],
                    'updated_at': row[13],
                    'tags': json.loads(row[14]) if row[14] else [],
                    'notifications': json.loads(row[15]) if row[15] else {},
                })
            return batches

    def update_scheduled_batch(self, batch_id, **updates):
        from .scheduler import ScheduleParser

        with self._lock:
            batch = self.get_scheduled_batch(batch_id)
            if not batch:
                raise KeyError(f"Batch {batch_id} not found")

            allowed_fields = ['name', 'enabled', 'schedule_type', 'schedule_value',
                             'selected_paths', 'model', 'prompt', 'temperature',
                             'dry_run', 'tags', 'notifications']

            for key, value in updates.items():
                if key in allowed_fields:
                    batch[key] = value

            if 'schedule_type' in updates or 'schedule_value' in updates:
                next_run = ScheduleParser.calculate_next_run(
                    batch['schedule_type'], batch['schedule_value']
                )
                batch['next_run_at'] = next_run.isoformat() if next_run else None

            batch['updated_at'] = datetime.now(timezone.utc).isoformat()

            self._conn.execute("""
                UPDATE scheduled_batches
                SET name=?, enabled=?, schedule_type=?, schedule_value=?,
                    selected_paths=?, model=?, prompt=?, temperature=?,
                    dry_run=?, next_run_at=?, updated_at=?, tags=?, notifications=?
                WHERE id=?
            """, (
                batch['name'], int(batch['enabled']), batch['schedule_type'],
                json.dumps(batch['schedule_value']), json.dumps(batch['selected_paths']),
                batch['model'], batch['prompt'], batch['temperature'],
                int(batch['dry_run']), batch['next_run_at'], batch['updated_at'],
                json.dumps(batch['tags']), json.dumps(batch['notifications']),
                batch_id,
            ))
            self._conn.commit()

        return batch

    def delete_scheduled_batch(self, batch_id):
        with self._lock:
            result = self._conn.execute(
                "DELETE FROM scheduled_batches WHERE id = ?", (batch_id,)
            )
            self._conn.commit()
            return result.rowcount > 0

    def get_batch_history(self, batch_id):
        with self._lock:
            rows = self._conn.execute("""
                SELECT * FROM batch_history
                WHERE batch_id = ?
                ORDER BY started_at DESC
            """, (batch_id,)).fetchall()

            history = []
            for row in rows:
                history.append({
                    'id': row[0],
                    'batch_id': row[1],
                    'task_id': row[2],
                    'started_at': row[3],
                    'completed_at': row[4],
                    'status': row[5],
                    'summary': json.loads(row[6]) if row[6] else None,
                })
            return history

    def execute_batch_now(self, batch_id):
        batch = self.get_scheduled_batch(batch_id)
        if not batch:
            raise KeyError(f"Batch {batch_id} not found")

        from .scheduler import BatchScheduler
        temp_scheduler = BatchScheduler(self._db_path, self)
        temp_scheduler._execute_batch({
            'id': batch['id'],
            'selected_paths': json.dumps(batch['selected_paths']),
            'model': batch['model'],
            'dry_run': batch['dry_run'],
            'prompt': batch['prompt'],
            'temperature': batch['temperature'],
            'schedule_type': batch['schedule_type'],
            'schedule_value': json.dumps(batch['schedule_value']),
        })

        return batch['id']

    def _schedule_loop(self) -> None:
        while True:
            completed_preparation = None
            completed_preparation_task_id = None
            with self._lock:
                while len(self._future_jobs) < self._max_workers:
                    task_id, task = self._next_dispatchable_task_locked()
                    if not task_id or not task:
                        break
                    if task["pending_paths"] is None:
                        break
                    if not task["pending_paths"]:
                        self._finalize_task_locked(task_id)
                        continue

                    image_path = task["pending_paths"].pop(0)
                    if task["status"] == "queued":
                        task["status"] = "running"
                        if not task["started_at"]:
                            task["started_at"] = datetime.now(timezone.utc).isoformat()
                        self._conn.execute(
                            "UPDATE tasks SET status='running', started_at=? WHERE id=?",
                            (task["started_at"], task_id),
                        )
                        self._conn.commit()
                        self._recalculate_global_eta_locked()
                        running_event = {
                            "event": "running",
                            "task_id": task_id,
                            "total": task["total"],
                            "eta_seconds": task["eta_seconds"],
                            "max_workers": self._max_workers,
                        }
                    else:
                        running_event = None

                    task["active_futures"] += 1
                    started_at = monotonic()
                    future = self._executor.submit(
                        process_single_image,
                        image_path,
                        task["model"],
                        self._db_path,
                        task["dry_run"],
                        prompt=task["prompt"],
                        temperature=task["temperature"],
                    )
                    self._future_jobs[future] = {
                        "task_id": task_id,
                        "path": image_path,
                        "started_at": started_at,
                    }
                    processing_event = {
                        "event": "processing",
                        "task_id": task_id,
                        "path": image_path,
                        "current": min(task["completed"] + task["active_futures"], task["total"]),
                        "total": task["total"],
                        "eta_seconds": task["eta_seconds"],
                        "max_workers": self._max_workers,
                    }

                    listeners = list(task["listeners"])
                    to_send = []
                    if running_event:
                        task["history"].append(serialize_event(running_event))
                        to_send.append(running_event)
                    task["history"].append(serialize_event(processing_event))
                    to_send.append(processing_event)

                    for event in to_send:
                        event_text = serialize_event(event)
                        for listener in listeners:
                            listener.put(event_text)

                futures = list(self._future_jobs.keys())
                if self._preparation_future and self._preparation_future.done():
                    completed_preparation = self._preparation_future
                    completed_preparation_task_id = self._preparation_task_id
                    self._preparation_future = None
                    self._preparation_task_id = None

                if self._preparation_future is None:
                    unprepared_task_id, unprepared_task = self._next_unprepared_task_locked()
                    if unprepared_task_id and unprepared_task:
                        self._preparation_future = self._prepare_executor.submit(
                            expand_selection,
                            unprepared_task["root"],
                            list(unprepared_task["selected_paths"]),
                        )
                        self._preparation_task_id = unprepared_task_id

            if completed_preparation:
                try:
                    image_paths = completed_preparation.result()
                    preparation_error = None
                except Exception as exc:  # pragma: no cover - scheduler safeguard
                    image_paths = []
                    preparation_error = str(exc)

                with self._lock:
                    task = self._tasks.get(completed_preparation_task_id)
                    if task:
                        if task["stop_event"].is_set() or task["status"] == "stopped":
                            task["preparing"] = False
                            task["pending_paths"] = []
                            self._finalize_task_locked(completed_preparation_task_id)
                            continue

                        task["preparing"] = False
                        task["pending_paths"] = [str(path) for path in image_paths]
                        task["total"] = len(image_paths)
                        self._conn.execute(
                            "UPDATE tasks SET total=? WHERE id=?",
                            (task["total"], completed_preparation_task_id),
                        )
                        self._conn.commit()

                        if preparation_error or not image_paths:
                            task["status"] = "failed"
                            task["errors"] = 1
                            task["eta_seconds"] = 0
                            task["completed_at"] = datetime.now(timezone.utc).isoformat()
                            task["terminal_emitted"] = True
                            self._conn.execute(
                                "UPDATE tasks SET status='failed', errors=?, completed_at=? WHERE id=?",
                                (task["errors"], task["completed_at"], completed_preparation_task_id),
                            )
                            self._conn.commit()
                            self._recalculate_global_eta_locked()

                            failure_message = preparation_error or "No supported images found in the selected paths."
                            listeners = list(task["listeners"])
                            failure_events = [
                                {
                                    "event": "error",
                                    "task_id": completed_preparation_task_id,
                                    "message": failure_message,
                                    "current": 0,
                                    "total": 0,
                                    "eta_seconds": 0,
                                },
                                {
                                    "event": "complete",
                                    "task_id": completed_preparation_task_id,
                                    "completed": 0,
                                    "processed": 0,
                                    "skipped": 0,
                                    "errors": 1,
                                    "total": 0,
                                    "eta_seconds": 0,
                                },
                            ]
                            for event in failure_events:
                                event_text = serialize_event(event)
                                task["history"].append(event_text)
                                for listener in listeners:
                                    listener.put(event_text)
                        else:
                            self._recalculate_global_eta_locked()
                continue

            if futures:
                done, _ = wait(futures, timeout=0.25, return_when=FIRST_COMPLETED)
            else:
                threading.Event().wait(0.1)
                continue

            for future in done:
                with self._lock:
                    job = self._future_jobs.pop(future, None)
                if not job:
                    continue

                task_id = job["task_id"]
                try:
                    result = future.result()
                except Exception as exc:
                    logger.exception("Worker failed for task %s: %s", task_id, exc)
                    result = {
                        "event": "error",
                        "path": job["path"],
                        "message": str(exc),
                    }

                with self._lock:
                    task = self._tasks.get(task_id)
                    if not task:
                        continue
                    task["active_futures"] = max(task["active_futures"] - 1, 0)
                    events = self._handle_worker_result_locked(task_id, result, job["started_at"])
                    should_finalize = task["active_futures"] == 0 and not task["pending_paths"]

                for event in events:
                    self._publish(task_id, event)

                if should_finalize:
                    with self._lock:
                        self._finalize_task_locked(task_id)


from .scheduler import BatchScheduler

task_manager = TaskManager(get_db_path(), get_settings_path())
batch_scheduler = BatchScheduler(get_db_path(), task_manager)
batch_scheduler.start()

import atexit
def cleanup_scheduler():
    batch_scheduler.stop()
atexit.register(cleanup_scheduler)


@app.get("/")
def index():
    settings = task_manager.get_settings()
    return render_template(
        "index.html",
        photo_root=get_dashboard_root(),
        root_env_var=DASHBOARD_ROOT_ENV_VAR,
        default_model=DEFAULT_MODEL,
        prompt=OLLAMA_PROMPT,
        max_workers=settings["max_workers"],
    )


@app.get("/api/tree")
def api_tree():
    relative_path = request.args.get("path", "")
    root_value = request.args.get("root")
    try:
        dashboard_root = resolve_dashboard_root(root_value)
        payload = list_directory(dashboard_root, relative_path)
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(payload)


@app.get("/api/health")
def api_health():
    root_value = request.args.get("root")
    dashboard_root = str(Path(root_value or get_dashboard_root()).expanduser().resolve())
    root_exists = Path(dashboard_root).is_dir()
    return jsonify(
        {
            "ok": True,
            "photo_root": dashboard_root,
            "root_exists": root_exists,
            "root_env_var": DASHBOARD_ROOT_ENV_VAR,
        }
    )


@app.get("/api/settings")
def api_get_settings():
    return jsonify(task_manager.get_settings())


@app.post("/api/settings")
def api_update_settings():
    payload = request.get_json(silent=True) or {}
    try:
        max_workers = int(payload.get("max_workers"))
        settings = task_manager.update_max_workers(max_workers)
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(settings)


@app.post("/api/tasks")
def api_create_task():
    payload = request.get_json(silent=True) or {}
    selected_paths = payload.get("paths") or []
    if not isinstance(selected_paths, list) or not all(isinstance(path, str) for path in selected_paths):
        return jsonify({"error": "Request must include a list of relative paths."}), 400
    if not selected_paths:
        return jsonify({"error": "Select at least one folder or image."}), 400

    model = str(payload.get("model") or DEFAULT_MODEL)
    dry_run = bool(payload.get("dry_run", False))
    prompt = str(payload.get("prompt") or OLLAMA_PROMPT)
    temperature = float(payload.get("temperature", 0.2))
    root_value = payload.get("root")

    try:
        dashboard_root = resolve_dashboard_root(root_value)
        snapshot = task_manager.create_task(
            selected_paths=selected_paths,
            root=dashboard_root,
            model=model,
            dry_run=dry_run,
            prompt=prompt,
            temperature=temperature,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(snapshot), 202


@app.get("/api/tasks")
def api_list_tasks():
    status_filter = request.args.get("status")
    tasks = task_manager.list_tasks()
    if status_filter:
        allowed = {status.strip() for status in status_filter.split(",")}
        tasks = [task for task in tasks if task["status"] in allowed]
    return jsonify({"tasks": tasks, "settings": task_manager.get_settings()})


@app.get("/api/tasks/<task_id>")
def api_get_task(task_id: str):
    snapshot = task_manager.get_task(task_id)
    if not snapshot:
        return jsonify({"error": "Task not found."}), 404
    return jsonify(snapshot)


@app.post("/api/tasks/<task_id>/stop")
def api_stop_task(task_id: str):
    snapshot = task_manager.stop_task(task_id)
    if not snapshot:
        return jsonify({"error": "Task not found."}), 404
    return jsonify(snapshot)


@app.post("/api/tasks/<task_id>/resume")
def api_resume_task(task_id: str):
    try:
        snapshot = task_manager.resume_task(task_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not snapshot:
        return jsonify({"error": "Task not found."}), 404
    return jsonify(snapshot)


@app.delete("/api/tasks/completed")
def api_clear_completed():
    task_manager.clear_completed()
    return jsonify({"ok": True})


@app.get("/api/tasks/<task_id>/events")
def api_task_events(task_id: str):
    try:
        snapshot, listener = task_manager.attach_listener(task_id)
    except KeyError:
        return jsonify({"error": "Task not found."}), 404

    def generate():
        yield serialize_event({"event": "snapshot", "task_id": task_id, **snapshot})
        try:
            while True:
                try:
                    event_text = listener.get(timeout=15)
                    yield event_text
                    if '"event": "complete"' in event_text or '"event": "stopped"' in event_text:
                        break
                except queue.Empty:
                    yield ": keep-alive\n\n"
        finally:
            task_manager.detach_listener(task_id, listener)

    return Response(generate(), mimetype="text/event-stream")


# ------------------------------------------------------------------
# Scheduled Batches API
# ------------------------------------------------------------------


@app.get("/api/scheduled-batches")
def api_list_scheduled_batches():
    enabled_only = request.args.get('enabled_only', 'false').lower() == 'true'
    batches = task_manager.get_scheduled_batches(enabled_only)
    return jsonify({"batches": batches})


@app.post("/api/scheduled-batches")
def api_create_scheduled_batch():
    payload = request.get_json(silent=True) or {}

    required = ['name', 'schedule_type', 'schedule_value', 'paths']
    for field in required:
        if field not in payload:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        batch = task_manager.create_scheduled_batch(
            name=payload['name'],
            schedule_type=payload['schedule_type'],
            schedule_value=payload['schedule_value'],
            selected_paths=payload['paths'],
            model=payload.get('model', DEFAULT_MODEL),
            prompt=payload.get('prompt', OLLAMA_PROMPT),
            temperature=float(payload.get('temperature', 0.2)),
            dry_run=bool(payload.get('dry_run', False)),
            tags=payload.get('tags', []),
            notifications=payload.get('notifications', {}),
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to create batch: {str(e)}"}), 500

    return jsonify(batch), 201


@app.get("/api/scheduled-batches/<batch_id>")
def api_get_scheduled_batch(batch_id):
    batch = task_manager.get_scheduled_batch(batch_id)
    if not batch:
        return jsonify({"error": "Batch not found"}), 404
    return jsonify(batch)


@app.put("/api/scheduled-batches/<batch_id>")
def api_update_scheduled_batch(batch_id):
    payload = request.get_json(silent=True) or {}
    try:
        batch = task_manager.update_scheduled_batch(batch_id, **payload)
    except KeyError:
        return jsonify({"error": "Batch not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(batch)


@app.delete("/api/scheduled-batches/<batch_id>")
def api_delete_scheduled_batch(batch_id):
    success = task_manager.delete_scheduled_batch(batch_id)
    if not success:
        return jsonify({"error": "Batch not found"}), 404
    return jsonify({"ok": True})


@app.post("/api/scheduled-batches/<batch_id>/toggle")
def api_toggle_scheduled_batch(batch_id):
    payload = request.get_json(silent=True) or {}
    enabled = payload.get('enabled', True)
    try:
        batch = task_manager.update_scheduled_batch(batch_id, enabled=enabled)
    except KeyError:
        return jsonify({"error": "Batch not found"}), 404
    return jsonify(batch)


@app.get("/api/scheduled-batches/<batch_id>/history")
def api_batch_history(batch_id):
    history = task_manager.get_batch_history(batch_id)
    return jsonify({"history": history})


@app.post("/api/scheduled-batches/<batch_id>/run-now")
def api_run_batch_now(batch_id):
    try:
        task_manager.execute_batch_now(batch_id)
        return jsonify({"message": "Batch execution started"})
    except KeyError:
        return jsonify({"error": "Batch not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    host = os.environ.get(HOST_ENV_VAR, DEFAULT_HOST)
    port = int(os.environ.get(PORT_ENV_VAR, DEFAULT_PORT))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host=host, port=port, use_reloader=False)
