---
Phase 1
### The "AI Photo Tagger" Script Requirements

**Core Logic:**
1.  **The Registry:** Use a local `indexing.db` (SQLite) to store the `file_path` and `file_hash` (or last modified timestamp) of every processed image.
2.  **The Model:** Interface with **Ollama** using the `llava` model (since you have 16GB VRAM).
3.  **The Writer:** Use `exiftool` to write the description into the `-Description`, `-ImageDescription`, and `-XPComment` fields. This ensures Synology Photos, Windows, and Mac can all "see" the search terms.
4.  **The Target:** A mounted directory (your Synology NAS share).

---

### Copy-Paste this Prompt to Generate the Script:

> **System Context:** I am running Ubuntu with an NVIDIA GPU (16GB VRAM). I have Ollama installed with the 'llava' model. My photos are stored on a Synology NAS mounted at `/mnt/synology/photos`.
>
> **Task:** Write a Python script to automate AI image tagging with the following features:
>
> 1. **Database Tracking:** Create a SQLite database to keep track of processed files. Before processing an image, check if its path and modification time exist in the DB. If yes, skip it.
> 2. **Ollama Integration:** Use the `ollama` python library. For each new image, send it to the `llava` model with the prompt: *"Describe this image in detail for search indexing. Focus on objects, colors, and setting. Keep it under 40 words."*
> 3. **Metadata Writing:** Use the `subprocess` module to call `exiftool`. Write the AI-generated description into the following metadata tags: `Description`, `UserComment`, and `Keywords`. 
> 4. **Safety:** >    - Only process common image formats (.jpg, .jpeg, .png).
>    - Include a "dry run" mode where it just prints what it *would* do without writing to files.
>    - Handle errors gracefully (e.g., if Ollama is busy or a file is locked).
> 5. **Performance:** Process images one by one to respect VRAM limits.
>
> **Output:** Provide the full Python code and the shell command to install the necessary dependencies (`pip install ollama` and `sudo apt install exiftool`).

---

Photo is at path mnt/synology.

---
## Phase 2 

> **Task:** Create a web-based "AI Photo Tagging Dashboard" using Python (Flask), HTML/CSS (Bootstrap), and SQLite.
>
> **System Context:**
> - **OS:** Ubuntu
> - **AI:** Ollama running `llava:13b`
> - **Storage:** Synology NAS mounted at `/mnt/synology/photos`
>
> **Requirements:**
> 1. **Backend (Flask):**
>    - A function to **scan the directory tree** of the NAS and return a list of folders and image files.
>    - An API endpoint to **trigger the tagging script** for a specific selected folder or file.
>    - Integrate the **SQLite indexing logic** we discussed: Check if a file is already tagged in the DB before processing.
>    - Use `ollama` python library and `exiftool` (via subprocess) to write metadata.
>
> 2. **Frontend (HTML/CSS):**
>    - A **File Explorer UI:** A sidebar or list where I can browse folders on my NAS.
>    - **Checkboxes:** Ability to select specific folders or individual images.
>    - **Action Bar:** A "Run AI Tagging" button that starts the process.
>    - **Progress Bar:** A simple visual indicator showing "Processing X of Y images."
>    - **Logs View:** A scrolling text area showing the AI descriptions as they are generated in real-time.
>
> 3. **AI Logic:**
>    - Use the prompt: *"Analyze this image for high-precision search indexing. Format: [Subject] [Action] at [Setting], [Colors]. No introductory text. Max 20 words."*
>    - Set Ollama temperature to 0.2.
>
> 4. **Script Safety:** >    - Ensure the script runs in a separate thread so the web UI doesn't freeze while the AI is working.
>
> **Output:** Provide the `app.py` code and a `templates/index.html` file. Include the `pip install` commands for flask, ollama, and any other dependencies.

----
## Phase 3 

### The "Task Manager" Logic
1.  **Unique Task ID:** Each time you select a folder and click "Run," a unique ID is created.
2.  **Persistent Status:** The `is_processed` check must happen *inside* the loop. If you stop Task A and start it again later, the script should query the DB: "Give me all files in Folder A that are NOT in the `processed_files` table."
3.  **The Stop Button:** This sends a "kill" signal to that specific Task ID thread.


> **Task:** Build a Multi-Tasking AI Photo Tagging Dashboard with Resume/Stop capabilities.
>
> **System Context:** Ubuntu, 16GB VRAM, Ollama (`llava:13b`), Synology NAS mount.
>
> **Architecture Requirements:**
> 1. **Task Queue System:** >    - Users can select a folder and "Create Task." Each task appears as a row in a "Active Tasks" table.
>    - Support multiple tasks. While multiple can be "Queued," the backend should process them **sequentially** (one image at a time across all tasks) to protect VRAM.
>
> 2. **Resume & `is_processed` Logic:**
>    - **Check:** Before processing any file, the script must check the SQLite DB for the `file_path` AND `file_hash` (or `last_modified`). 
>    - **Resume:** If a task is stopped and restarted, the script should simply "re-scan" the folder, compare against the DB, and only send the "missing" files to Ollama.
>
> 3. **UI Elements (Bootstrap):**
>    - **Task Cards:** Each task shows: Folder Name, Progress Bar, ETA, and Status (Running, Paused, Completed).
>    - **Control Buttons:** Each task needs a **Stop/Pause** button and a **Resume** button.
>    - **Global Clear:** A button to remove "Completed" tasks from the UI view.
>
> 4. **Backend Implementation (Flask + Threading):**
>    - Use a `threading.Event()` for each task to handle the "Stop" signal gracefully.
>    - Calculate **ETA** based on the remaining files in that specific task multiplied by the average processing time.
>    - Ensure the SQLite connection is "thread-safe" (use `check_same_thread=False` or a scoped session).
>
> **Output:** Provide the complete `app.py` with the Task Manager class and the updated `index.html` with the multi-tasking UI.

***
## Phase 4

> **Task:** Finalize the "PortMaster AI" Photo Tagger with **Adjustable Parallelism** and **Deep Metadata Protection**.
>
> **1. Configurable Concurrency (The "Throttle"):**
> * **UI Control:** Add a "Settings" section (or a slider/input) in the Web UI to set **"Max Parallel Workers"**. 
> * **Behavior:** Default this to `2`. The user should be able to change this (e.g., from 1 to 4) without restarting the app. 
> * **Implementation:** Use a `ThreadPoolExecutor` where the number of workers is dynamically updated based on the UI setting.
>
> **2. Smart Skip Logic (The "Double Guard"):**
> * **Function:** Before processing any file, the app must return `True` for skipping if:
>     - **Condition A (Database):** The file path/hash exists in the `is_processed` SQLite table.
>     - **Condition B (Inside File):** `exiftool` detects that the `Description`, `UserComment`, or `ImageDescription` fields are NOT empty.
> * **Logging:** If a file is skipped due to "Existing Metadata," log it as: *"Skipping: Manual description already exists in file."*
>
> **3. Multi-Task Management:**
> * **Active Tasks:** The UI must display multiple "Task Cards" for each folder being processed.
> * **Concurrency across Tasks:** The "Max Workers" limit should be global. (e.g., If Max Workers is 2, and I have 3 tasks, it will process 2 images from Task 1, OR 1 image from Task 1 and 1 from Task 2 simultaneously).
> * **Stop/Resume:** Each task card must have independent controls to Pause or Stop.
>
> **4. Docker & Persistence (12-Factor):**
> * Use a `.env` file for initial setup.
> * Store the SQLite database and the current "Concurrency Setting" in a persistent volume.
> * Ensure `exiftool` is part of the Docker image.
>
> **5. Technical Refinement:**
> * **Ollama Integration:** Use `llava:13b` as the default model.
> * **ETA Logic:** Calculate the ETA based on the total remaining images across all active tasks divided by the current worker count.
> 
> **6. UI enhancement :**
> * **Ollama Integration:** Use `llava:13b` as the default model.
> * **ETA Logic:** Calculate the ETA based on the total remaining images across all active tasks divided by the current worker count.
> **Output:** Provide the updated `app.py`, the `index.html` with a "Concurrency Slider," and the `Dockerfile`.

***
