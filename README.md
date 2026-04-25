# TagHaul

Automatically describe photos using a local LLM (Ollama `llava:13b`) and write the descriptions into image metadata so they are searchable in **Synology Photos**, Windows Explorer, and macOS Finder. The repo now includes both the original CLI workflow and a Phase 2 Flask dashboard for browsing folders, selecting files, and running one tagging job at a time with live progress.

---

## How it works

1. Walks `/mnt/synology` (or any directory you specify) recursively
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

The dashboard uses the same backend logic, but fixes the skip check to compare both `file_path` and `mtime`, so changed files are reprocessed while unchanged files are skipped.

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
- Only one tagging job is allowed at a time.
- You can select both folders and individual images.
- Progress and descriptions stream live with Server-Sent Events.
- If your photos live somewhere else, set the `Server photo root` field to that absolute path.

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
--path PATH     Root directory to scan        (default: /mnt/synology)
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
