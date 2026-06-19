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
    def create_task(file_id: str, url: str, task_id: str):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            insert_sql = """
                INSERT INTO transcribtion_tasks (task_id, file_id, url, status, created_at)
                VALUES (%s, %s, %s, 'pending', NOW())
            """

            cursor.execute(insert_sql, (task_id, file_id, url))
            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Created task {task_id}")
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
                                  file_url: str, correlation_id: str):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            insert_sql = """
                INSERT INTO transcription_parts
                (task_id, part_index, file_path, file_url, correlation_id, status)
                VALUES (%s, %s, %s, %s, %s, 'pending')
            """
            cursor.execute(insert_sql, (task_id, part_index, file_path, file_url, correlation_id))
            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Created transcription part {correlation_id}")
        except Exception as e:
            logger.error(f"Error creating transcription part: {e}")

    @staticmethod
    def update_part_status(correlation_id: str, status: str, transcript: str = None,
                          error_msg: str = None):
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            if error_msg:
                update_sql = """
                    UPDATE transcription_parts
                    SET status = %s, error_message = %s
                    WHERE correlation_id = %s
                """
                cursor.execute(update_sql, (status, error_msg, correlation_id))
            elif transcript:
                update_sql = """
                    UPDATE transcription_parts
                    SET status = %s, transcript = %s
                    WHERE correlation_id = %s
                """
                cursor.execute(update_sql, (status, transcript, correlation_id))
            else:
                update_sql = """
                    UPDATE transcription_parts
                    SET status = %s
                    WHERE correlation_id = %s
                """
                cursor.execute(update_sql, (status, correlation_id))

            db.commit()
            cursor.close()
            db.close()
            logger.info(f"Updated part {correlation_id} status to {status}")
        except Exception as e:
            logger.error(f"Error updating part status: {e}")

    @staticmethod
    def get_task_parts(task_id: str) -> Optional[list]:
        try:
            db = pymysql.connect(**DB_CONFIG)
            cursor = db.cursor()

            cursor.execute("""
                SELECT part_index, file_path, file_url, correlation_id, status, transcript
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
                        "transcript": r[5]
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
                SELECT task_id, file_id, url
                FROM transcribtion_tasks
                WHERE status NOT IN ('completed', 'error')
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
                        "url": r[2]
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
                SELECT task_id, part_index, file_path, file_url, status, transcript
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
                    "transcript": result[5]
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
                SELECT task_id, file_id, url, status
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
                    "status": result[3]
                }
            return None
        except Exception as e:
            logger.error(f"Error getting task info: {e}")
            return None
