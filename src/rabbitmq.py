import json
import logging
import os
import queue
import re
import threading
import uuid
import time
from typing import Dict, List

import pika
import pymysql

from database import DB_CONFIG, Database

logger = logging.getLogger(__name__)

RABBITMQ_CONFIG = {
    "host": os.getenv("RABBITMQ_HOST", "rabbitmq"),
    "port": int(os.getenv("RABBITMQ_PORT", 5672)),
    "user": os.getenv("RABBITMQ_USER", "guest"),
    "password": os.getenv("RABBITMQ_PASSWORD", "guest"),
}

QUEUE_SPLIT_IN = "split_in"
QUEUE_SPLIT_OUT = "split_out"
QUEUE_WISPER_IN = "wisper_in"
QUEUE_WISPER_OUT = "wisper_out"


class RabbitMQConnection:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.publish_queue = queue.Queue()

    def connect(self):
        credentials = pika.PlainCredentials(
            RABBITMQ_CONFIG["user"], RABBITMQ_CONFIG["password"]
        )
        params = pika.ConnectionParameters(
            host=RABBITMQ_CONFIG["host"],
            port=RABBITMQ_CONFIG["port"],
            credentials=credentials,
            connection_attempts=5,
            retry_delay=2,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self._declare_queues()
        logger.info("Connected to RabbitMQ")

    def _declare_queues(self):
        for queue in [QUEUE_SPLIT_IN, QUEUE_SPLIT_OUT, QUEUE_WISPER_IN, QUEUE_WISPER_OUT]:
            self.channel.queue_declare(queue=queue, durable=True)

    def publish_split_task(self, task_id: str, url: str, max_duration: int):
        message = {
            "task_id": task_id,
            "url": url,
            "max_duration": max_duration,
            "save_to_storage": True,
        }
        self.publish_queue.put(("split_in", message))
        logger.info(f"Queued split task {task_id}")

    def publish_transcribe_task(self, task_id: str, file_url: str, part_index: int):
        message = {
            "command": "transcribe",
            "correlation_id": f"{task_id}_{part_index}",
            "model_size": "small",
            "format": "srt",
            "file_url": file_url,
        }
        self.publish_queue.put(("wisper_in", message))
        logger.info(f"Queued transcribe task for {file_url}")

    def start_consuming(self):
        self.channel.basic_consume(
            queue=QUEUE_SPLIT_OUT,
            on_message_callback=self._handle_split_response,
            auto_ack=True,
        )
        self.channel.basic_consume(
            queue=QUEUE_WISPER_OUT,
            on_message_callback=self._handle_transcription_response,
            auto_ack=True,
        )

        def consume_with_timeout():
            try:
                while True:
                    try:
                        self.connection.process_data_events(time_limit=0.1)
                    except Exception as e:
                        logger.error(f"Error processing data events: {e}")
                        time.sleep(0.5)
            except KeyboardInterrupt:
                pass

        def publish_from_queue():
            try:
                while True:
                    try:
                        queue_name, message = self.publish_queue.get(timeout=1)
                        self.channel.basic_publish(
                            exchange="",
                            routing_key=queue_name,
                            body=json.dumps(message),
                            properties=pika.BasicProperties(delivery_mode=2),
                        )
                        logger.info(f"Published message to {queue_name}")
                    except queue.Empty:
                        pass
                    except Exception as e:
                        logger.error(f"Error publishing message: {e}", exc_info=True)
            except KeyboardInterrupt:
                pass

        consumer_thread = threading.Thread(
            target=consume_with_timeout, daemon=True
        )
        consumer_thread.start()

        publisher_thread = threading.Thread(
            target=publish_from_queue, daemon=True
        )
        publisher_thread.start()

        logger.info("Started RabbitMQ consumers and publisher")

    def _handle_split_response(self, ch, method, properties, body):
        try:
            response = json.loads(body)
            task_id = response.get("task_id")

            task_info = Database.get_task_info(task_id)
            if not task_info:
                logger.warning(f"Unknown task {task_id}")
                return

            if response.get("success"):
                files = response.get("storage_files", [])
                splitted_file_id = str(uuid.uuid4())

                logger.info(f"Split response for task {task_id}: {len(files)} files received")

                for idx, file_info in enumerate(files):
                    file_path = file_info.get("path")
                    file_url = f"http://file-storage-service:3001/api/files/{file_path}"
                    correlation_id = f"{task_id}_{idx}"
                    offset_ms = idx * 60 * 1000

                    Database.create_transcription_part(
                        task_id, idx, file_path, file_url, correlation_id
                    )
                    logger.info(f"  Saved part {idx}: {file_path} (correlation_id: {correlation_id}, offset: {offset_ms}ms)")

                    self.publish_transcribe_task(task_id, file_url, idx)

                Database.update_split_task(task_id, splitted_file_id, "splitting")
                logger.info(f"Task {task_id} split and transcription tasks queued")
            else:
                error_msg = response.get("error", "Unknown error")
                Database.update_task_status(task_id, "error", error_msg)
                logger.error(f"Split failed for {task_id}: {error_msg}")
        except Exception as e:
            logger.error(f"Error handling split response: {e}")
            if task_id:
                Database.update_task_status(task_id, "error", str(e))

    def _handle_transcription_response(self, ch, method, properties, body):
        try:
            response = json.loads(body)
            correlation_id = response.get("correlation_id")

            part_info = Database.get_part_by_correlation_id(correlation_id)
            if not part_info:
                logger.warning(f"Unknown part {correlation_id}")
                return

            task_id = part_info["task_id"]
            part_index = part_info["part_index"]
            file_url = response.get("file_url", "unknown")

            if response.get("success", True):
                transcript = response.get("transcript", "")
                Database.update_part_status(correlation_id, "completed", transcript=transcript)
                logger.info(f"Received transcription for task {task_id} part {part_index}: {file_url}")

                self._save_transcription_to_marks(task_id, part_index, transcript)
            else:
                error_msg = response.get("error", "Unknown error")
                Database.update_part_status(correlation_id, "error", error_msg=error_msg)
                logger.error(f"Transcription failed for task {task_id} part {part_index} ({file_url}): {error_msg}")

            self._check_task_completion(task_id)
        except Exception as e:
            logger.error(f"Error handling transcription response: {e}")

    def _save_transcription_to_marks(self, task_id: str, part_index: int, transcript_srt: str):
        try:
            task_info = Database.get_task_info(task_id)
            if not task_info:
                logger.error(f"Task {task_id} not found")
                return

            file_id = task_info["file_id"]
            offset_ms = part_index * 60 * 1000

            subtitles = self._parse_srt(transcript_srt)

            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            for subtitle in subtitles:
                start_ms = subtitle["start_ms"] + offset_ms
                start_time = self._format_time(start_ms)

                check_sql = "SELECT describtion FROM marks WHERE file_id = %s AND time_msec = %s"
                cursor.execute(check_sql, (file_id, start_ms))
                result = cursor.fetchone()

                if result:
                    update_sql = "UPDATE marks SET describtion = %s WHERE file_id = %s AND time_msec = %s"
                    new_desc = f"{result[0]} {subtitle['text']}"
                    cursor.execute(update_sql, (new_desc, file_id, start_ms))
                else:
                    insert_sql = """
                        INSERT INTO marks (file_id, time_msec, start_time, describtion)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(
                        insert_sql,
                        (file_id, start_ms, start_time, subtitle["text"]),
                    )

            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Saved transcription for task {task_id} part {part_index} to marks")
        except Exception as e:
            logger.error(f"Error saving transcription to marks: {e}")

    def _check_task_completion(self, task_id: str):
        try:
            parts = Database.get_task_parts(task_id)
            if not parts:
                return

            completed_count = sum(1 for p in parts if p["status"] == "completed")
            error_count = sum(1 for p in parts if p["status"] == "error")
            total_count = len(parts)

            logger.info(f"Task {task_id}: {completed_count} completed, {error_count} error, {total_count} total")

            if completed_count + error_count == total_count:
                if error_count > 0:
                    failed_indices = [str(p["part_index"]) for p in parts if p["status"] == "error"]
                    Database.update_task_status(task_id, "error",
                        f"Failed parts: {', '.join(failed_indices)}")
                    logger.error(f"Task {task_id} failed - {error_count} parts failed")
                else:
                    Database.update_task_status(task_id, "completed")
                    logger.info(f"Task {task_id} completed successfully")
        except Exception as e:
            logger.error(f"Error checking task completion: {e}")


    def _parse_srt(self, srt_content: str) -> List[Dict]:
        subtitles = []
        blocks = srt_content.strip().split("\n\n")

        for block in blocks:
            lines = block.strip().split("\n")
            if len(lines) < 3:
                continue

            time_line = lines[1]
            text = " ".join(lines[2:])

            try:
                start_str = time_line.split(" --> ")[0]
                start_ms = self._srt_time_to_ms(start_str)

                subtitles.append({"start_ms": start_ms, "text": text})
            except Exception as e:
                logger.warning(f"Error parsing SRT line: {e}")

        return subtitles

    def _srt_time_to_ms(self, time_str: str) -> int:
        pattern = r"(\d{2}):(\d{2}):(\d{2}),(\d{3})"
        match = re.match(pattern, time_str)
        if match:
            hours, minutes, seconds, millis = match.groups()
            total_ms = (
                int(hours) * 3600000
                + int(minutes) * 60000
                + int(seconds) * 1000
                + int(millis)
            )
            return total_ms
        return 0

    def _format_time(self, milliseconds: int) -> str:
        seconds = milliseconds // 1000
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
