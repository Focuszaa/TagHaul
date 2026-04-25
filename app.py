import json
import os
import queue
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from flask import Flask
from flask import Response
from flask import jsonify
from flask import render_template
from flask import request

from tagger_backend import DEFAULT_DB
from tagger_backend import DEFAULT_DASHBOARD_ROOT
from tagger_backend import DEFAULT_LOG
from tagger_backend import DEFAULT_MODEL
from tagger_backend import OLLAMA_PROMPT
from tagger_backend import expand_selection
from tagger_backend import list_directory
from tagger_backend import logger
from tagger_backend import open_task_db
from tagger_backend import process_image_paths
from tagger_backend import serialize_event
from tagger_backend import setup_logging

APP_ROOT = Path(__file__).resolve().parent
DASHBOARD_ROOT_ENV_VAR = "PHOTO_TAGGER_DASHBOARD_ROOT"

app = Flask(__name__)
setup_logging(str(APP_ROOT / DEFAULT_LOG))


def get_dashboard_root() -> str:
    configured = os.environ.get(DASHBOARD_ROOT_ENV_VAR, DEFAULT_DASHBOARD_ROOT)
    return str(Path(configured).expanduser().resolve())


def resolve_dashboard_root(root_value: str | None) -> str:
    candidate = root_value or get_dashboard_root()
    root_path = Path(candidate).expanduser().resolve()
    if not root_path.exists():
        raise ValueError(f"Photo root does not exist: {root_path}")
    if not root_path.is_dir():
        raise ValueError(f"Photo root is not a directory: {root_path}")
    return str(root_path)


class TaskManager:
    """Multi-task queue: one worker thread processes tasks FIFO; each task can be stopped and resumed."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._tasks: dict[str, dict] = {}
        self._task_queue: queue.Queue[str] = queue.Queue()
        self._conn = open_task_db(db_path)
        self._load_tasks_from_db()
        worker = threading.Thread(target=self._worker_loop, daemon=True)
        worker.start()

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    def _load_tasks_from_db(self) -> None:
        """Load persisted tasks; mark any that were running/queued as stopped."""
        cursor = self._conn.execute(
            "SELECT id, root, selected_paths, model, prompt, temperature, dry_run, "
            "status, total, processed, skipped, errors, completed, avg_seconds, "
            "created_at, started_at, completed_at FROM tasks ORDER BY created_at ASC"
        )
        rows = cursor.fetchall()
        for row in rows:
            (
                task_id, root, selected_paths_json, model, prompt, temperature, dry_run,
                status, total, processed, skipped, errors, completed, avg_seconds,
                created_at, started_at, completed_at,
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
        self._conn.commit()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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
        image_paths = expand_selection(root, selected_paths)
        if not image_paths:
            raise ValueError("No supported images found in the selected paths.")

        task_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()
        total = len(image_paths)

        task = self._make_task_dict(
            task_id=task_id,
            root=root,
            selected_paths=selected_paths,
            model=model,
            prompt=prompt,
            temperature=temperature,
            dry_run=dry_run,
            status="queued",
            total=total,
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
                    task_id, root, json.dumps(selected_paths), model, prompt, temperature,
                    int(dry_run), "queued", total, 0, 0, 0, 0, 0.0, now, None, None,
                ),
            )
            self._conn.commit()
            self._tasks[task_id] = task
            snapshot = self._snapshot_from_task(task_id, task)

        self._task_queue.put(task_id)
        return snapshot

    def get_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            return self._snapshot_from_task(task_id, task) if task else None

    def list_tasks(self) -> list[dict]:
        with self._lock:
            return [self._snapshot_from_task(tid, t) for tid, t in self._tasks.items()]

    def stop_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            if task["status"] == "running":
                task["stop_event"].set()
            return self._snapshot_from_task(task_id, task)

    def resume_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            if task["status"] != "stopped":
                return self._snapshot_from_task(task_id, task)
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
            task["history"] = []
            self._conn.execute(
                "UPDATE tasks SET status='queued', processed=0, skipped=0, errors=0, "
                "completed=0, avg_seconds=0 WHERE id=?",
                (task_id,),
            )
            self._conn.commit()
            snapshot = self._snapshot_from_task(task_id, task)

        self._task_queue.put(task_id)
        return snapshot

    def clear_completed(self) -> None:
        with self._lock:
            to_remove = [
                tid for tid, t in self._tasks.items()
                if t["status"] in ("completed", "failed")
            ]
            for tid in to_remove:
                del self._tasks[tid]

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
    # Internal event publishing
    # ------------------------------------------------------------------

    def _publish(self, task_id: str, payload: dict) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return

            event_name = payload.get("event")

            if event_name == "written":
                task["completed"] += 1
                task["processed"] += 1
                delta = payload.pop("_delta", None)
                if delta is not None:
                    task["sum_seconds"] += delta
                    task["count_processed"] += 1
                    task["avg_seconds"] = task["sum_seconds"] / task["count_processed"]
                remaining = task["total"] - task["completed"]
                task["eta_seconds"] = (
                    int(remaining * task["avg_seconds"]) if task["avg_seconds"] > 0 else None
                )
                payload["eta_seconds"] = task["eta_seconds"]
                payload["avg_seconds"] = round(task["avg_seconds"], 2)
                self._conn.execute(
                    "UPDATE tasks SET processed=?, completed=?, avg_seconds=? WHERE id=?",
                    (task["processed"], task["completed"], task["avg_seconds"], task_id),
                )
                self._conn.commit()

            elif event_name == "skip":
                task["completed"] += 1
                task["skipped"] += 1

            elif event_name == "dry-run":
                task["completed"] += 1

            elif event_name == "error":
                task["completed"] += 1
                task["errors"] += 1
                self._conn.execute(
                    "UPDATE tasks SET errors=?, completed=? WHERE id=?",
                    (task["errors"], task["completed"], task_id),
                )
                self._conn.commit()

            elif event_name == "stopped":
                task["status"] = "stopped"
                task["eta_seconds"] = None
                self._conn.execute(
                    "UPDATE tasks SET status='stopped', processed=?, skipped=?, errors=?, completed=? WHERE id=?",
                    (task["processed"], task["skipped"], task["errors"], task["completed"], task_id),
                )
                self._conn.commit()

            elif event_name == "complete":
                task["status"] = "completed" if payload.get("errors", 0) == 0 else "failed"
                task["completed_at"] = datetime.now(timezone.utc).isoformat()
                task["completed"] = payload.get("completed", task["completed"])
                task["processed"] = payload.get("processed", task["processed"])
                task["skipped"] = payload.get("skipped", task["skipped"])
                task["errors"] = payload.get("errors", task["errors"])
                task["eta_seconds"] = 0
                self._conn.execute(
                    "UPDATE tasks SET status=?, completed_at=?, processed=?, skipped=?, "
                    "errors=?, completed=? WHERE id=?",
                    (
                        task["status"], task["completed_at"], task["processed"],
                        task["skipped"], task["errors"], task["completed"], task_id,
                    ),
                )
                self._conn.commit()

            event_text = serialize_event(payload)
            task["history"].append(event_text)
            listeners = list(task["listeners"])

        for listener in listeners:
            listener.put(event_text)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    def _worker_loop(self) -> None:
        while True:
            task_id = self._task_queue.get()

            with self._lock:
                task = self._tasks.get(task_id)
                if not task or task["status"] != "queued":
                    continue
                task["status"] = "running"
                if not task["started_at"]:
                    task["started_at"] = datetime.now(timezone.utc).isoformat()
                root = task["root"]
                selected_paths = list(task["selected_paths"])
                model = task["model"]
                dry_run = task["dry_run"]
                prompt = task["prompt"]
                temperature = task["temperature"]
                stop_event = task["stop_event"]
                self._conn.execute(
                    "UPDATE tasks SET status='running', started_at=? WHERE id=?",
                    (task["started_at"], task_id),
                )
                self._conn.commit()

            # Re-expand selection so is_processed filters already-tagged files on resume
            try:
                image_paths = expand_selection(root, selected_paths)
            except Exception as exc:
                self._publish(task_id, {
                    "event": "error", "task_id": task_id,
                    "message": f"Failed to expand selection: {exc}",
                })
                self._publish(task_id, {
                    "event": "complete", "task_id": task_id,
                    "completed": 0, "processed": 0, "skipped": 0, "errors": 1, "total": 0,
                })
                continue

            with self._lock:
                task = self._tasks.get(task_id)
                if task:
                    task["total"] = len(image_paths)
                    self._conn.execute(
                        "UPDATE tasks SET total=? WHERE id=?", (len(image_paths), task_id)
                    )
                    self._conn.commit()

            self._publish(task_id, {
                "event": "running", "task_id": task_id, "total": len(image_paths),
            })

            # Build a timing-aware callback for ETA computation
            timing: dict = {}

            def make_callback(tid: str, timing_dict: dict):
                def callback(event: dict) -> None:
                    ev = event.get("event")
                    if ev == "processing":
                        timing_dict["t_start"] = monotonic()
                    elif ev == "written":
                        t = timing_dict.pop("t_start", None)
                        if t is not None:
                            event = dict(event)
                            event["_delta"] = monotonic() - t
                    elif ev in ("error", "stopped", "complete"):
                        timing_dict.pop("t_start", None)
                    self._publish(tid, {"task_id": tid, **event})
                return callback

            try:
                process_image_paths(
                    image_paths,
                    model,
                    self._db_path,
                    dry_run,
                    prompt=prompt,
                    temperature=temperature,
                    event_callback=make_callback(task_id, timing),
                    stop_event=stop_event,
                )
            except Exception as exc:
                logger.exception("Task %s failed: %s", task_id, exc)
                snap = self.get_task(task_id) or {}
                self._publish(task_id, {
                    "event": "error", "task_id": task_id, "message": str(exc),
                })
                self._publish(task_id, {
                    "event": "complete", "task_id": task_id,
                    "completed": snap.get("completed", 0),
                    "processed": snap.get("processed", 0),
                    "skipped": snap.get("skipped", 0),
                    "errors": snap.get("errors", 0),
                    "total": snap.get("total", 0),
                })


task_manager = TaskManager(str(APP_ROOT / DEFAULT_DB))


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------


@app.get("/")
def index():
    return render_template(
        "index.html",
        photo_root=get_dashboard_root(),
        root_env_var=DASHBOARD_ROOT_ENV_VAR,
        default_model=DEFAULT_MODEL,
        prompt=OLLAMA_PROMPT,
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
    return jsonify({
        "ok": True,
        "photo_root": dashboard_root,
        "root_exists": root_exists,
        "root_env_var": DASHBOARD_ROOT_ENV_VAR,
    })


@app.post("/api/tasks")
def api_create_task():
    payload = request.get_json(silent=True) or {}
    selected_paths = payload.get("paths") or []
    if not isinstance(selected_paths, list) or not all(isinstance(p, str) for p in selected_paths):
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
        allowed = {s.strip() for s in status_filter.split(",")}
        tasks = [t for t in tasks if t["status"] in allowed]
    return jsonify({"tasks": tasks})


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
    snapshot = task_manager.resume_task(task_id)
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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)