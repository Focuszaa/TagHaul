# TagHaul

Automatically describe photos using a local LLM (Ollama `llava:13b`) and write the descriptions into image metadata so they are searchable in **Synology Photos**, Windows Explorer, and macOS Finder. The repo now includes both the original CLI workflow and a Flask dashboard with multi-task scheduling, shared worker concurrency, and live progress streaming.

---

## How it works

1. Walks `/mnt/synology/photos` (or any directory you specify) recursively
2. Skips images already processed (tracked by `indexing.db`, keyed on path + modification time)
3. Sends each new image to Ollama (`llava:13b`) and gets a ≤40-word description
4. Writes the description to **5 metadata fields** via `exiftool`:

| Field | Standard | Read by |
|---|---|---|
| `Description` | XMP | Synology Photos search |
| `ImageDescription` | EXIF | Universal (every viewer) |
| `XPComment` | EXIF/Windows | Windows Explorer |
| `Keywords` | IPTC | Tag-based search |
| `Subject` | XMP | Synology tag cloud |

The dashboard uses the same backend logic, but now applies a double-guard skip rule before sending an image to Ollama:

1. Skip if `processed_files` already contains the current `file_path` plus `mtime`
2. Skip if the file already contains a non-empty `Description`, `UserComment`, `ImageDescription`, or `XPComment`

---

## Prerequisites

### 1. Install system dependencies

```bash
sudo apt update && sudo apt install -y exiftool
```

### 2. Install Ollama and pull the model

Follow the official install guide at https://ollama.com, then:

```bash
ollama pull llava:13b
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

If you prefer direct install commands:

```bash
pip install ollama flask
```

---

## Usage

### Start the Phase 2 dashboard

```bash
python app.py
```

Then open `http://127.0.0.1:5000` in your browser.

Dashboard notes:

- The dashboard defaults to `/mnt/synology/photos`, but you can change the server photo root in the UI.
- Multiple tasks can run at once, but the global worker pool is capped by the **Max Parallel Workers** setting.
- You can select both folders and individual images.
- Progress and descriptions stream live with Server-Sent Events.
- The default worker count is `2`, adjustable live in the UI from `1` to `4`.
- If your photos live somewhere else, set the `Server photo root` field to that absolute path.

### Environment file

Copy [.env.example](/home/user/Documents/projects/auto-gen-description/.env.example) to `.env` and adjust paths as needed:

```bash
cp .env.example .env
```

Available settings:

```bash
PHOTO_TAGGER_DASHBOARD_ROOT=/mnt/synology/photos
PHOTO_TAGGER_DB_PATH=/data/indexing.db
PHOTO_TAGGER_SETTINGS_PATH=/data/tagger_settings.json
PHOTO_TAGGER_HOST=0.0.0.0
PHOTO_TAGGER_PORT=5000
PHOTO_TAGGER_MAX_WORKERS=2
OLLAMA_HOST=http://host.docker.internal:11434
```

`PHOTO_TAGGER_MAX_WORKERS` is the startup default. The current runtime value is also persisted in `PHOTO_TAGGER_SETTINGS_PATH` so UI changes survive restarts.
`OLLAMA_HOST` is passed through to the `ollama` Python client. For Docker on Linux, point it at the host gateway as shown below.

### Docker

Build the image:

```bash
docker build -t taghaul .
```

Run it with a persistent volume for the SQLite database and saved worker setting:

```bash
docker run --rm -p 5000:5000 \
	--env-file .env \
	--add-host=host.docker.internal:host-gateway \
	-v $(pwd)/data:/data \
	-v /mnt/synology:/mnt/synology \
	taghaul
```

Notes:

- The image includes `exiftool`.
- `/data` stores `indexing.db` and `tagger_settings.json`.
- Mount your NAS path read/write into the container at the same path used by `PHOTO_TAGGER_DASHBOARD_ROOT`.
- On Linux, `--add-host=host.docker.internal:host-gateway` makes the host Ollama endpoint reachable at the default `OLLAMA_HOST` value from `.env.example`.

### Dry run (safe preview — no files touched)

```bash
python tagger.py --dry-run
```

### Tag all photos on the NAS

```bash
python tagger.py
```

### Tag a specific album

```bash
python tagger.py --path /mnt/synology/photos/2024-holidays
```

### Use a different model

```bash
python tagger.py --model llava:7b
```

### All options

```
--path PATH     Root directory to scan        (default: /mnt/synology/photos)
--model MODEL   Ollama model name             (default: llava:13b)
--db DB         Path to SQLite tracking DB    (default: ./indexing.db)
--dry-run       Preview only; no writes
```

---

## Verifying the result

```bash
# Check that metadata was written to a specific file
exiftool -Description -ImageDescription -Keywords /mnt/synology/photos/IMG_1234.jpg
```

---

## Re-running / incremental updates

Re-run `python tagger.py` or trigger the same selection from the dashboard at any time. Only images that are **new** or whose **modification time has changed** will be processed. Everything else is skipped instantly.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Ollama error: connection refused` | Make sure `ollama serve` is running |
| `exiftool: command not found` | Run `sudo apt install exiftool` |
| `Photo root is not a directory` | Check your NAS mount with `ls /mnt/synology` |
| File processed but description not visible in Synology Photos | Trigger a re-index in Synology Photos > Settings > Re-index |
| Dashboard explorer returns an error | Confirm `/mnt/synology/photos` is mounted and readable by the Flask process |

## Changelog

### v0.4.0 — Phase 4 — Global Concurrency And Metadata Guard — 2026-04-25
- Added a shared global worker pool with live `1..4` runtime concurrency control in the dashboard.
- Added double-guard skip logic using both the SQLite registry and on-file metadata detection before Ollama runs.
- Added Docker packaging, persistent settings storage, and env-based runtime configuration.
