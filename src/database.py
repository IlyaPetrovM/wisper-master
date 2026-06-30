import json
import logging
import os
from typing import Optional

import pymysql

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("MARIADB_HOST", "mariadb"),
    "port": int(os.getenv("MARIADB_PORT", 3306)),
    "user": os.getenv("MARIADB_USER", "mediarch_user"),
    "password": os.getenv("MARIADB_PASSWORD", "mediarch_password"),
    "database": os.getenv("MARIADB_DATABASE", "mediarch"),
}


class Database:
    @staticmethod
    def create_task(file_id: str, url: str, task_id: str, model_size: str = "bzikst/faster-whisper-large-v3-russian-int8", format: str = "json", min_mark_duration_ms: int = 60000, max_duration: int = 60):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            insert_sql = """
                INSERT INTO transcribtion_tasks (task_id, file_id, url, model_size, format, min_mark_duration_ms, max_duration, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', NOW())
            """

            cursor.execute(insert_sql, (task_id, file_id, url, model_size, format, min_mark_duration_ms, max_duration))
            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Created task {task_id} (model_size: {model_size}, format: {format}, min_mark_duration_ms: {min_mark_duration_ms}, max_duration: {max_duration})")
        except Exception as e:
            logger.error(f"Error creating task: {e}")
            raise

    @staticmethod
    def update_split_task(task_id: str, splitted_file_id: str, status: str):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            update_sql = """
                UPDATE transcribtion_tasks
                SET splitted_file_id = %s, status = %s
                WHERE task_id = %s
            """

            cursor.execute(update_sql, (splitted_file_id, status, task_id))
            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Updated task {task_id} with splitted_file_id {splitted_file_id}")
        except Exception as e:
            logger.error(f"Error updating split task: {e}")

    @staticmethod
    def update_task_status(task_id: str, status: str, error_msg: str = None):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            if error_msg:
                update_sql = """
                    UPDATE transcribtion_tasks
                    SET status = %s, error_message = %s
                    WHERE task_id = %s
                """
                cursor.execute(update_sql, (status, error_msg, task_id))
            else:
                update_sql = """
                    UPDATE transcribtion_tasks
                    SET status = %s
                    WHERE task_id = %s
                """
                cursor.execute(update_sql, (status, task_id))

            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Updated task {task_id} status to {status}")
        except Exception as e:
            logger.error(f"Error updating task status: {e}")

    @staticmethod
    def get_task_status(task_id: str) -> Optional[str]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute(
                "SELECT status FROM transcribtion_tasks WHERE task_id = %s", (task_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            db.close()

            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return None

    @staticmethod
    def create_transcription_part(task_id: str, part_index: int, file_path: str,
                                  file_url: str, correlation_id: str, duration_msec: int = 0, offset_ms: int = 0):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            insert_sql = """
                INSERT INTO transcription_parts
                (task_id, part_index, file_path, file_url, correlation_id, duration_msec, offset_ms, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
            """
            cursor.execute(insert_sql, (task_id, part_index, file_path, file_url, correlation_id, duration_msec, offset_ms))
            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Created transcription part {correlation_id} (offset: {offset_ms}ms, duration: {duration_msec}ms)")
        except Exception as e:
            logger.error(f"Error creating transcription part: {e}")

    @staticmethod
    def update_part_status(correlation_id: str, status: str, transcript: str = None,
                          error_msg: str = None, worker_id: str = None, filename: str = None, file_id: str = None, offset_ms: int = 0):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            if error_msg:
                update_sql = """
                    UPDATE transcription_parts
                    SET status = %s, error_message = %s, worker_id = %s, filename = %s
                    WHERE correlation_id = %s
                """
                cursor.execute(update_sql, (status, error_msg, worker_id, filename, correlation_id))
                logger.info(f"Updated part {correlation_id} status to {status} with error: {error_msg}")
            elif transcript:
                cursor.execute(
                    "SELECT task_id, offset_ms FROM transcription_parts WHERE correlation_id = %s",
                    (correlation_id,)
                )
                result = cursor.fetchone()
                if result:
                    task_id = result[0]
                    offset_ms = result[1]  # Получаем offset_ms из таблицы части
                    update_sql = """
                        UPDATE transcription_parts
                        SET status = %s, worker_id = %s, filename = %s
                        WHERE correlation_id = %s
                    """
                    cursor.execute(update_sql, (status, worker_id, filename, correlation_id))

                    # Get file_id from task if not provided
                    if not file_id:
                        cursor.execute(
                            "SELECT file_id FROM transcribtion_tasks WHERE task_id = %s",
                            (task_id,)
                        )
                        file_result = cursor.fetchone()
                        if file_result:
                            file_id = file_result[0]

                    try:
                        segments = json.loads(transcript)
                        if not isinstance(segments, list):
                            segments = [segments]
                    except:
                        segments = []

                    for segment in segments:
                        # Конвертируем start и end из секунд в миллисекунды
                        start_ms = int(float(segment.get("start", 0)) * 1000)
                        end_ms = int(float(segment.get("end", 0)) * 1000)

                        insert_sql = """
                            INSERT INTO transcription_results
                            (task_id, file_id, correlation_id, segment_id, start, end, offset_ms, text, avg_logprob, compression_ratio, no_speech_prob, filename)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(insert_sql, (
                            task_id,
                            file_id,
                            correlation_id,
                            segment.get("id", 0),
                            start_ms,
                            end_ms,
                            offset_ms,
                            segment.get("text", ""),
                            segment.get("avg_logprob"),
                            segment.get("compression_ratio"),
                            segment.get("no_speech_prob"),
                            filename
                        ))
                    logger.info(f"Updated part {correlation_id} status to {status} with {len(segments)} segments")
            else:
                update_sql = """
                    UPDATE transcription_parts
                    SET status = %s, worker_id = %s, filename = %s
                    WHERE correlation_id = %s
                """
                cursor.execute(update_sql, (status, worker_id, filename, correlation_id))
                logger.info(f"Updated part {correlation_id} status to {status}")

            db.commit()
            cursor.close()
            db.close()
        except Exception as e:
            logger.error(f"Error updating part status: {e}", exc_info=True)

    @staticmethod
    def get_task_parts(task_id: str) -> Optional[list]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT part_index, file_path, file_url, correlation_id, status, duration_msec, offset_ms
                FROM transcription_parts
                WHERE task_id = %s
                ORDER BY part_index
            """, (task_id,))

            results = cursor.fetchall()
            cursor.close()
            db.close()

            if results:
                return [
                    {
                        "part_index": r[0],
                        "file_path": r[1],
                        "file_url": r[2],
                        "correlation_id": r[3],
                        "status": r[4],
                        "duration_msec": r[5],
                        "offset_ms": r[6]
                    }
                    for r in results
                ]
            return []
        except Exception as e:
            logger.error(f"Error getting task parts: {e}")
            return []

    @staticmethod
    def get_incomplete_tasks() -> Optional[list]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT task_id, file_id, url, status,
                       COALESCE(max_duration, 60) AS max_duration,
                       TIMESTAMPDIFF(SECOND, created_at, NOW()) AS age_seconds
                FROM transcribtion_tasks
                WHERE status NOT IN ('completed', 'error', 'failed')
                ORDER BY created_at
            """)

            results = cursor.fetchall()
            cursor.close()
            db.close()

            if results:
                return [
                    {
                        "task_id": r[0],
                        "file_id": r[1],
                        "url": r[2],
                        "status": r[3],
                        "max_duration": r[4],
                        "age_seconds": r[5] if r[5] is not None else 0
                    }
                    for r in results
                ]
            return []
        except Exception as e:
            logger.error(f"Error getting incomplete tasks: {e}")
            return []

    @staticmethod
    def get_part_by_correlation_id(correlation_id: str) -> Optional[dict]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT task_id, part_index, file_path, file_url, status, duration_msec, offset_ms, worker_id, filename
                FROM transcription_parts
                WHERE correlation_id = %s
            """, (correlation_id,))

            result = cursor.fetchone()
            cursor.close()
            db.close()

            if result:
                return {
                    "task_id": result[0],
                    "part_index": result[1],
                    "file_path": result[2],
                    "file_url": result[3],
                    "status": result[4],
                    "duration_msec": result[5],
                    "offset_ms": result[6],
                    "worker_id": result[7],
                    "filename": result[8]
                }
            return None
        except Exception as e:
            logger.error(f"Error getting part by correlation_id: {e}")
            return None

    @staticmethod
    def get_task_info(task_id: str) -> Optional[dict]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT task_id, file_id, url, status, model_size, format, min_mark_duration_ms
                FROM transcribtion_tasks
                WHERE task_id = %s
            """, (task_id,))

            result = cursor.fetchone()
            cursor.close()
            db.close()

            if result:
                return {
                    "task_id": result[0],
                    "file_id": result[1],
                    "url": result[2],
                    "status": result[3],
                    "model_size": result[4],
                    "format": result[5],
                    "min_mark_duration_ms": result[6] if result[6] else 60000
                }
            return None
        except Exception as e:
            logger.error(f"Error getting task info: {e}")
            return None

    @staticmethod
    def get_transcription_result(correlation_id: str) -> Optional[dict]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT file_id, filename, segment_id, start, end, offset_ms, text, avg_logprob, compression_ratio, no_speech_prob, created_at
                FROM transcription_results
                WHERE correlation_id = %s
                ORDER BY segment_id
            """, (correlation_id,))

            results = cursor.fetchall()
            cursor.close()
            db.close()

            if results:
                return {
                    "correlation_id": correlation_id,
                    "file_id": results[0][0],
                    "filename": results[0][1],
                    "segments": [
                        {
                            "id": r[2],
                            "start": r[3],
                            "end": r[4],
                            "offset_ms": r[5],
                            "text": r[6],
                            "avg_logprob": r[7],
                            "compression_ratio": r[8],
                            "no_speech_prob": r[9],
                            "created_at": r[10]
                        }
                        for r in results
                    ]
                }
            return None
        except Exception as e:
            logger.error(f"Error getting transcription result: {e}")
            return None

    @staticmethod
    def get_task_results(task_id: str) -> Optional[list]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT DISTINCT correlation_id, file_id, filename
                FROM transcription_results
                WHERE task_id = %s
                ORDER BY correlation_id
            """, (task_id,))

            correlations = cursor.fetchall()
            results = []

            for (correlation_id, file_id, filename) in correlations:
                cursor.execute("""
                    SELECT segment_id, start, end, offset_ms, text, avg_logprob, compression_ratio, no_speech_prob, created_at
                    FROM transcription_results
                    WHERE correlation_id = %s
                    ORDER BY segment_id
                """, (correlation_id,))

                segments = cursor.fetchall()
                if segments:
                    results.append({
                        "correlation_id": correlation_id,
                        "file_id": file_id,
                        "filename": filename,
                        "segments": [
                            {
                                "id": s[0],
                                "start": s[1],
                                "end": s[2],
                                "offset_ms": s[3],
                                "text": s[4],
                                "avg_logprob": s[5],
                                "compression_ratio": s[6],
                                "no_speech_prob": s[7],
                                "created_at": s[8]
                            }
                            for s in segments
                        ]
                    })

            cursor.close()
            db.close()
            return results
        except Exception as e:
            logger.error(f"Error getting task results: {e}")
            return []
