# Changelog

### v0.5.0 — Scheduler & Batch Automation — 2026-04-26
- Added scheduled batch capability with a web UI to create, edit, enable/disable, run-now, and view history.
- Implemented `BatchScheduler` for background execution of scheduled batches.
- Added TaskManager methods: `create_scheduled_batch`, `get_scheduled_batches`, `update_scheduled_batch`, `delete_scheduled_batch`, `execute_batch_now`, and history retrieval.
- Persisted scheduled batches and execution history via new SQLite tables: `scheduled_batches`, `batch_history`.
- Exposed REST endpoints under `/api/scheduled-batches` for full CRUD and control.
- Added scheduler UI tab, modal forms, and JS handlers to the dashboard (`templates/index.html`, `app.py`).
- Added `croniter` dependency for cron schedule support.
- Changed default dashboard port to `5001` (see `tagger_backend.py`).
- Minor README wording updates and moved changelog out of README into `CHANGELOG.md`.

### v0.4.0 — Global Concurrency And Metadata Guard — 2026-04-25
- Added a shared global worker pool with live `1..4` runtime concurrency control in the dashboard.
- Added double-guard skip logic using both the SQLite registry and on-file metadata detection before Ollama runs.
- Added Docker packaging, persistent settings storage, and env-based runtime configuration.
