# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Master Node** service for a distributed audio transcription system. It orchestrates the workflow of splitting audio files and transcribing them using Whisper models. The master node:

1. Exposes a REST API for clients to request transcription of audio files
2. Communicates with downstream services via RabbitMQ message queues:
   - **audio-splitter**: Splits large audio files into chunks
   - **whisper workers** (2 instances): Perform actual transcription on chunks
   - **file-storage-service**: Stores split audio files and original files
   - **MariaDB**: Persists task metadata, progress tracking, and transcription results

## Architecture

### Component Overview

- **[src/main.py](src/main.py)**: FastAPI application with REST endpoints
  - `POST /transcribe`: Start a new transcription task
  - `GET /status/{task_id}`: Poll task status
  - `GET /health`: Health check endpoint
  - Recovers incomplete tasks on startup

- **[src/database.py](src/database.py)**: Database layer using PyMySQL
  - Manages task lifecycle (create, update status)
  - Tracks transcription parts and their results
  - Stores final transcription segments with timing offsets

- **[src/rabbitmq.py](src/rabbitmq.py)**: Message queue orchestration
  - Separate connections for consuming (blocking) and publishing (non-blocking)
  - Two consumer threads: one for split responses, one for transcription results
  - One publisher thread for async message publishing from internal queue
  - Handles task state transitions based on message responses
  - Deletes split audio files from file-storage-service after successful transcription

- **[src/models.py](src/models.py)**: Pydantic request/response models

### Message Flows

**Splitting (split_in → split_out):**
- Master sends: task_id, URL, max_duration, save_to_storage=true
- Splitter returns: list of split files with duration_msec for each
- Master creates transcription_parts records with offset_ms calculated as cumulative durations

**Transcription (wisper_in → wisper_out):**
- Master sends: task_id, correlation_id (task_id_part_index), file_url, model_size, format
- Worker returns: JSON segments with start (sec), end (sec), text, or error message
- Master converts start/end times to milliseconds and adds offset_ms from the part

**File Cleanup (after transcription success):**
- Master sends: DELETE `/api/files/{file_id}` request to file-storage-service for each completed transcription part
- Removes split audio files from storage after successful processing
- Response (200): `{"message": "File deleted successfully", "id": "file_id"}`

**Task State Transitions:**
```
pending → splitting (after split queued)
       → transcribing (after all transcription messages published)
       → completed (when all parts succeed and cleanup performed)
       → error (if any part fails or splitting fails)
```

## Development Commands

### Build & Run Locally

**With Docker Compose** (recommended - includes all dependent services):
```bash
docker-compose up --build
```

**Without Docker** (requires MariaDB and RabbitMQ running elsewhere):
```bash
pip install -r requirements.txt
MARIADB_HOST=localhost MARIADB_USER=root MARIADB_PASSWORD=root \
RABBITMQ_HOST=localhost RABBITMQ_USER=guest RABBITMQ_PASSWORD=guest \
python src/main.py
```

The API will be available at `http://localhost:8000`.

### Database

Initialize database manually if needed:
```bash
mysql -h localhost -u root -proot_password < db_init/init-db.sql
```

### Testing

**Check Health:**
```bash
curl http://localhost:8000/health
```

**Start a Transcription Task:**
```bash
curl -X POST http://localhost:8000/transcribe \
  -H "Content-Type: application/json" \
  -d '{"file_id": 555, "url": "http://file-storage-service:3001/api/files/audio_short.mp3", "max_duration": 60, "model_size": "small", "format": "json"}'
```

**Check Task Status:**
```bash
curl http://localhost:8000/status/{task_id}
```

## Key Implementation Details

### Database Schema

**transcribtion_tasks** (note: table name has typo - "transcribtion"):
- Stores master task records
- Tracks overall task status, model_size, and output format
- Linked to files table by file_id

**transcription_parts**:
- One record per split audio chunk
- Stores offset_ms (cumulative duration of previous parts) for timing adjustment
- duration_msec stores the chunk's length
- correlation_id uniquely identifies each part processing request

**transcription_results**:
- Final transcription segments with timing adjusted for file position
- start/end stored in milliseconds (converted from seconds in response)
- offset_ms added to segment timing to reflect position in original file

### Task Recovery

On startup, the master queries for tasks not in terminal states and republishes their incomplete parts. This ensures no work is lost if the service crashes mid-transcription.

### Threading Model

Three daemon threads run during consume:
1. **Consumer**: Processes split_out and wisper_out messages with 1-second timeouts
2. **Publisher**: Publishes queued messages (from publish_queue) with 1-second timeouts
3. Main thread: Handles FastAPI requests

RabbitMQ connections use heartbeat=600s for stability with long-running tasks.

### Error Handling

- Split errors: Task marked as "error" with error message
- Transcription errors: Individual parts marked "error", task marked "error" if any part fails
- Missing data: Warnings logged but processing continues (e.g., empty transcription results)

## Key Files

| File | Purpose |
|------|---------|
| [docker-compose.yml](docker-compose.yml) | Service definitions; sets environment for all containers |
| [Dockerfile](Dockerfile) | Python 3.11.15 slim image; runs main.py on port 8000 |
| [db_init/init-db.sql](db_init/init-db.sql) | Database schema and sample test data (file_id=555) |
| [requirements.txt](requirements.txt) | Python dependencies |
