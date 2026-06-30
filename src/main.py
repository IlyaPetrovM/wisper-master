import logging
import os

from fastapi import FastAPI, HTTPException

from database import Database
from models import TranscribeRequest, LoadModelRequest
from rabbitmq import RabbitMQConnection
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Master Node")
rmq = None

# A task that has been sitting in 'pending'/'splitting' without producing any
# transcription parts for longer than this is considered stuck and marked failed.
PENDING_TASK_TIMEOUT_SECONDS = int(os.getenv("PENDING_TASK_TIMEOUT_SECONDS", 3600))


@app.on_event("startup")
def startup_event():
    global rmq
    rmq = RabbitMQConnection()
    rmq.connect()

    logger.info("Recovering incomplete tasks...")
    incomplete_tasks = Database.get_incomplete_tasks()
    for task_info in incomplete_tasks:
        task_id = task_info["task_id"]
        age_seconds = task_info.get("age_seconds", 0)
        logger.info(f"Recovering task {task_id} (status: {task_info.get('status')}, age: {age_seconds}s)")

        full_task_info = Database.get_task_info(task_id)
        model_size = full_task_info.get("model_size", "bzikst/faster-whisper-large-v3-russian-int8") if full_task_info else "bzikst/faster-whisper-large-v3-russian-int8"
        format_type = full_task_info.get("format", "json") if full_task_info else "json"

        parts = Database.get_task_parts(task_id)

        if not parts:
            # No parts means the task never got past the split stage (still pending).
            if age_seconds >= PENDING_TASK_TIMEOUT_SECONDS:
                logger.warning(f"  Task {task_id} stuck without splitting for {age_seconds}s, marking as failed")
                Database.update_task_status(
                    task_id, "failed",
                    f"Task stuck in '{task_info.get('status')}' state without splitting for {age_seconds}s"
                )
            else:
                logger.info(f"  Re-publishing split task for {task_id}")
                rmq.publish_split_task(task_id, task_info["url"], task_info["max_duration"])
            continue

        for part in parts:
            if part["status"] in ("pending", "processing"):
                logger.info(f"  Republishing part {part['part_index']} (correlation_id: {part['correlation_id']})")
                rmq.publish_transcribe_task(task_id, part["file_url"], part["part_index"], model_size, format_type)

    rmq.start_consuming()


@app.post("/transcribe")
def start_transcription(request: TranscribeRequest):
    try:
        task_id = str(uuid.uuid4())
        Database.create_task(request.file_id, request.url, task_id, request.model_size, request.format, request.min_mark_duration_ms, request.max_duration)
        rmq.publish_split_task(task_id, request.url, request.max_duration)
        return {"task_id": task_id, "status": "started"}
    except Exception as e:
        logger.error(f"Error starting transcription: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/load_model")
def load_model(request: LoadModelRequest):
    try:
        rmq.publish_load_model(request.model_size)
        return {"status": "model_load_queued", "model_size": request.model_size}
    except Exception as e:
        logger.error(f"Error queuing model load: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{task_id}")
def get_transcription_status(task_id: str):
    status = Database.get_task_status(task_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"task_id": task_id, "status": status}


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
