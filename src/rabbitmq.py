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
        self.publish_connection = None
        self.publish_channel = None
        self.publish_queue = queue.Queue()
        self.wisper_tasks_pending = {}  # Отслеживание кол-ва сообщений wisper_in для каждого task_id

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
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        # Отдельное соединение для потребления сообщений
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self._declare_queues()

        # Отдельное соединение для публикации сообщений
        self.publish_connection = pika.BlockingConnection(params)
        self.publish_channel = self.publish_connection.channel()

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

    def publish_transcribe_task(self, task_id: str, file_url: str, part_index: int, model_size: str = "small", format: str = "json"):
        message = {
            "command": "transcribe",
            "task_id": task_id,
            "correlation_id": f"{task_id}_{part_index}",
            "model_size": model_size,
            "format": format,
            "file_url": file_url,
        }
        self.publish_queue.put(("wisper_in", message))
        if task_id not in self.wisper_tasks_pending:
            self.wisper_tasks_pending[task_id] = 0
        self.wisper_tasks_pending[task_id] += 1
        logger.info(f"Queued transcribe task for {file_url} (model_size: {model_size}, format: {format})")

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
                        self.connection.process_data_events(time_limit=1)
                    except Exception as e:
                        error_msg = str(e)
                        if not any(skip in error_msg for skip in ["Timeout", "closed"]):
                            logger.error(f"Error processing data events: {e}")
            except KeyboardInterrupt:
                pass

        def publish_from_queue():
            try:
                while True:
                    try:
                        queue_name, message = self.publish_queue.get(timeout=1)
                        self.publish_channel.basic_publish(
                            exchange="",
                            routing_key=queue_name,
                            body=json.dumps(message),
                            properties=pika.BasicProperties(delivery_mode=2),
                        )
                        logger.info(f"Published message to {queue_name}")

                        # Update part status to transcribing when publishing to wisper_in
                        if queue_name == QUEUE_WISPER_IN and "correlation_id" in message:
                            correlation_id = message.get("correlation_id")
                            Database.update_part_status(correlation_id, "transcribing")

                            # Check if all wisper_in messages for this task have been published
                            task_id = message.get("task_id")
                            if task_id in self.wisper_tasks_pending:
                                self.wisper_tasks_pending[task_id] -= 1
                                if self.wisper_tasks_pending[task_id] == 0:
                                    Database.update_task_status(task_id, "transcribing")
                                    logger.info(f"All transcribe tasks for {task_id} have been published, status changed to transcribing")
                                    del self.wisper_tasks_pending[task_id]
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
            status = response.get("status")

            task_info = Database.get_task_info(task_id)
            if not task_info:
                logger.warning(f"Unknown task {task_id}")
                return

            if status == "success":
                # Support both storage_files (uploaded) and files (local storage) formats
                files = response.get("storage_files") or response.get("files", [])
                splitted_file_id = str(uuid.uuid4())

                logger.info(f"Split response for task {task_id}: {len(files)} files received")

                offset_ms = 0
                model_size = task_info.get("model_size", "small")
                format_type = task_info.get("format", "json")
                for idx, file_info in enumerate(files):
                    file_path = file_info.get("path")
                    file_url = f"http://file-storage-service:3001/api/files/{file_path}" if response.get("storage_files") else file_path
                    correlation_id = f"{task_id}_{idx}"
                    duration_msec = file_info.get("duration_msec", 0)

                    Database.create_transcription_part(
                        task_id, idx, file_path, file_url, correlation_id, duration_msec, offset_ms
                    )
                    logger.info(f"  Saved part {idx}: {file_path} (correlation_id: {correlation_id}, offset: {offset_ms}ms, duration: {duration_msec}ms)")

                    self.publish_transcribe_task(task_id, file_url, idx, model_size, format_type)
                    offset_ms += duration_msec

                Database.update_split_task(task_id, splitted_file_id, "splitting")
                logger.info(f"Task {task_id} split and transcription tasks queued")
            elif status == "error":
                error_msg = response.get("error", "Unknown error")
                Database.update_task_status(task_id, "error", error_msg)
                logger.error(f"Split failed for {task_id}: {error_msg}")
            else:
                logger.warning(f"Unknown status '{status}' for task {task_id}")
                Database.update_task_status(task_id, "error", f"Unknown split response status: {status}")
        except Exception as e:
            logger.error(f"Error handling split response: {e}")
            if task_id:
                Database.update_task_status(task_id, "error", str(e))

    def _handle_transcription_response(self, ch, method, properties, body):
        try:
            response = json.loads(body)
            task_id = response.get("task_id")
            correlation_id = response.get("correlation_id")
            status = response.get("status")
            logger.info(f"Transcription response received: task_id={task_id}, correlation_id={correlation_id}, status={status}")
            logger.debug(f"Full response: {json.dumps(response, ensure_ascii=False)}")

            part_info = Database.get_part_by_correlation_id(correlation_id)
            if not part_info:
                logger.warning(f"Unknown part {correlation_id}")
                return

            part_index = part_info["part_index"]
            # Verify task_id matches (safety check)
            if part_info["task_id"] != task_id:
                logger.error(f"Task ID mismatch for correlation_id {correlation_id}: expected {part_info['task_id']}, got {task_id}")
                return
            file_url = response.get("file_url", "unknown")
            status = response.get("status", "unknown")

            if status == "error":
                error_msg = response.get("error", "Unknown error")
                worker_id = response.get("worker_id", "unknown")
                filename = response.get("filename", "unknown")
                logs = response.get("logs", [])

                # Логируем информацию об ошибке с деталями
                logger.error(f"Transcription failed for task {task_id} part {part_index} ({file_url}) from {worker_id}: {error_msg}")

                # Логируем логи воркера если они есть
                if logs:
                    logger.error(f"Worker logs for {correlation_id}:")
                    for log_line in logs:
                        logger.error(f"  {log_line}")

                Database.update_part_status(correlation_id, "error", error_msg=error_msg, worker_id=worker_id, filename=filename)
            elif status == "success":
                # Получаем результат - это может быть JSON массив или SRT строка
                result = response.get("result", [])
                worker_id = response.get("worker_id", "unknown")
                filename = response.get("filename", "unknown")

                # Получаем file_id из информации о задаче
                task_info = Database.get_task_info(task_id)
                file_id = task_info.get("file_id") if task_info else None

                if not result:
                    logger.warning(f"Empty result for task {task_id} part {part_index} from {worker_id}")
                    Database.update_part_status(correlation_id, "completed", transcript="", worker_id=worker_id, filename=filename, file_id=file_id)
                else:
                    result_size = len(result) if isinstance(result, list) else len(result) if isinstance(result, str) else 0
                    logger.info(f"Received transcription for task {task_id} part {part_index} from {worker_id}: {filename} ({result_size} {'segments' if isinstance(result, list) else 'chars'})")

                    # Сохраняем результат как JSON строка в БД
                    if isinstance(result, list):
                        transcript = json.dumps(result, ensure_ascii=False)
                    else:
                        transcript = result

                    Database.update_part_status(correlation_id, "completed", transcript=transcript, worker_id=worker_id, filename=filename, file_id=file_id)
                    self._save_transcription_to_marks(task_id, part_index, transcript)
            else:
                logger.warning(f"Unknown status for task {task_id} part {part_index}: {status}")
                worker_id = response.get("worker_id", "unknown")
                filename = response.get("filename", "unknown")
                Database.update_part_status(correlation_id, "error", error_msg=f"Unknown status: {status}", worker_id=worker_id, filename=filename)

            self._check_task_completion(task_id)
        except Exception as e:
            logger.error(f"Error handling transcription response: {e}", exc_info=True)

    def _save_transcription_to_marks(self, task_id: str, part_index: int, transcript_data: str):
        try:
            task_info = Database.get_task_info(task_id)
            if not task_info:
                logger.error(f"Task {task_id} not found")
                return

            file_id = task_info["file_id"]
            min_mark_duration_ms = task_info.get("min_mark_duration_ms", 60000)

            # Get offset from task parts (offset = sum of all previous parts' durations)
            parts = Database.get_task_parts(task_id)
            offset_ms = 0
            if parts and part_index < len(parts):
                offset_ms = parts[part_index].get("offset_ms", 0)

            logger.debug(f"Parsing transcript for task {task_id} part {part_index}, data length: {len(transcript_data)}, offset: {offset_ms}ms")
            subtitles = self._parse_transcript_json(transcript_data)
            logger.info(f"Parsed {len(subtitles)} segments from transcript (task {task_id} part {part_index})")

            if not subtitles:
                logger.warning(f"No segments parsed from transcript for task {task_id} part {part_index}")
                return

            # Group segments by min_mark_duration_ms
            grouped_subtitles = self._group_segments_by_duration(subtitles, min_mark_duration_ms)
            logger.info(f"Grouped {len(subtitles)} segments into {len(grouped_subtitles)} marks")

            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            inserted_count = 0
            updated_count = 0

            for group in grouped_subtitles:
                start_ms = group["start_ms"] + offset_ms
                start_time = self._format_time(start_ms)
                text = group["text"]

                check_sql = "SELECT id, hide FROM marks WHERE file_id = %s AND time_msec = %s"
                cursor.execute(check_sql, (file_id, start_ms))
                result = cursor.fetchone()

                if result:
                    hide = result[1]
                    # Если запись скрыта (hide не пусто), то добавляем новую вместо обновления
                    if hide is not None and str(hide) == '1':
                        insert_sql = """
                            INSERT INTO marks (file_id, time_msec, start_time, describtion)
                            VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(
                            insert_sql,
                            (file_id, start_ms, start_time, text),
                        )
                        inserted_count += 1
                        logger.debug(f"Added new record for file_id={file_id}, time_msec={start_ms} - existing record is hidden")
                    else:
                        # Если запись не скрыта оставляем как есть
                        logger.debug(f"Pass record for file_id={file_id}, time_msec={start_ms} - record exists ")
                        updated_count += 1
                else:
                    insert_sql = """
                        INSERT INTO marks (file_id, time_msec, start_time, describtion)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(
                        insert_sql,
                        (file_id, start_ms, start_time, text),
                    )
                    inserted_count += 1

            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Saved transcription for task {task_id} part {part_index} to marks: {inserted_count} inserted, {updated_count} updated")
        except Exception as e:
            logger.error(f"Error saving transcription to marks: {e}", exc_info=True)

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


    def _group_segments_by_duration(self, segments: List[Dict], min_duration_ms: int) -> List[Dict]:
        """Groups segments so that each group has duration >= min_duration_ms.

        The last incomplete group is merged with the previous group.

        Args:
            segments: List of segments with start_ms, end_ms and text
            min_duration_ms: Minimum duration for each group

        Returns:
            List of grouped segments with combined text
        """
        if not segments:
            return []

        logger.info(f"Grouping {len(segments)} segments with min_duration_ms={min_duration_ms}")

        # Calculate duration for each segment
        segments_with_duration = []
        for seg in segments:
            duration_ms = seg.get("end_ms", seg["start_ms"]) - seg["start_ms"]
            if duration_ms < 0:
                duration_ms = 0

            segments_with_duration.append({
                "start_ms": seg["start_ms"],
                "text": seg["text"],
                "duration_ms": duration_ms
            })
            logger.debug(f"  Segment: start={seg['start_ms']}ms, end={seg.get('end_ms', seg['start_ms'])}ms, duration={duration_ms}ms, text='{seg['text'][:50]}'")

        # Group segments by minimum duration
        groups = []
        current_group = []
        current_duration = 0

        for seg in segments_with_duration:
            current_group.append(seg)
            current_duration += seg["duration_ms"]

            if current_duration >= min_duration_ms:
                # Create a group
                group_text = " ".join([s["text"] for s in current_group])
                groups.append({
                    "start_ms": current_group[0]["start_ms"],
                    "text": group_text,
                    "duration_ms": current_duration
                })
                logger.debug(f"Created group: start={current_group[0]['start_ms']}ms, duration={current_duration}ms, text_length={len(group_text)}")
                current_group = []
                current_duration = 0

        # Handle remainder
        if current_group:
            remainder_text = " ".join([s["text"] for s in current_group])
            remainder_duration = sum([s["duration_ms"] for s in current_group])

            if groups:
                # Merge remainder with last group
                groups[-1]["text"] += " " + remainder_text
                groups[-1]["duration_ms"] += remainder_duration
                logger.debug(f"Merged remainder ({remainder_duration}ms) with last group. New duration: {groups[-1]['duration_ms']}ms")
            else:
                # Only remainder exists, create single group
                groups.append({
                    "start_ms": current_group[0]["start_ms"],
                    "text": remainder_text,
                    "duration_ms": remainder_duration
                })
                logger.debug(f"Created single group from remainder: start={current_group[0]['start_ms']}ms, duration={remainder_duration}ms")

        logger.info(f"Grouping complete: {len(segments)} segments → {len(groups)} groups")
        for i, g in enumerate(groups):
            logger.info(f"  Group {i}: start={g['start_ms']}ms, duration={g['duration_ms']}ms, text='{g['text'][:80]}'")

        return groups

    def _parse_transcript_json(self, json_data: str) -> List[Dict]:
        """Парсит транскрипт в JSON формате от whisper-service

        Ожидается JSON массив с объектами:
        [
            {"id": 0, "start": 0.0, "end": 3.5, "text": "Текст"},
            {"id": 1, "start": 3.5, "end": 7.2, "text": "Текст"}
        ]
        """
        subtitles = []
        try:
            data = json.loads(json_data)

            # Если это массив объектов (формат JSON от whisper-service)
            if isinstance(data, list):
                for segment in data:
                    if isinstance(segment, dict):
                        start_ms = int(float(segment.get("start", 0)) * 1000)
                        end_ms = int(float(segment.get("end", 0)) * 1000)
                        text = segment.get("text", "").strip()

                        if text:
                            subtitles.append({"start_ms": start_ms, "end_ms": end_ms, "text": text})

            # Если это объект с полями segments
            elif isinstance(data, dict):
                if "segments" in data:
                    for segment in data["segments"]:
                        start_ms = int(float(segment.get("start", 0)) * 1000)
                        end_ms = int(float(segment.get("end", 0)) * 1000)
                        text = segment.get("text", "").strip()

                        if text:
                            subtitles.append({"start_ms": start_ms, "end_ms": end_ms, "text": text})

                # Если есть просто поле text (полная транскрипция)
                elif "text" in data:
                    text = data.get("text", "").strip()
                    if text:
                        subtitles.append({"start_ms": 0, "text": text})

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing transcript JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error parsing transcript: {e}")

        return subtitles

    def _parse_srt(self, srt_content: str) -> List[Dict]:
        """Парсит SRT формат (оставлено для обратной совместимости)"""
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
