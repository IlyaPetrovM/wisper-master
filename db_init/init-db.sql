/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
USE mediarch;

CREATE TABLE IF NOT EXISTS `events` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date_start` date DEFAULT curdate(),
  `time_start` time DEFAULT NULL,
  `gps_str` char(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_created` char(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date_created` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `files` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `preview` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `tags` text COLLATE utf8mb4_unicode_ci DEFAULT '',
  `name` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `description` text COLLATE utf8mb4_unicode_ci DEFAULT '',
  `event_id` int(11) DEFAULT NULL,
  `recognizedText` text COLLATE utf8mb4_unicode_ci DEFAULT '',
  `recognitionStatus` char(50) COLLATE utf8mb4_unicode_ci DEFAULT 'Ещё не распознан',
  `operator` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'кто создал медиафайл',
  `date_upload_UTC` datetime DEFAULT NULL COMMENT 'локальное время сервера!',
  `date_upload_timezone` char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date_updated_UTC` datetime DEFAULT NULL COMMENT 'локальное время сервера!',
  `date_updated_timezone` char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'in +hh:mm format i.e.: +03:00',
  `oldName` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `fileExt` char(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filetype` char(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `file_created_UTC` datetime DEFAULT NULL,
  `file_created_LOCAL` datetime DEFAULT NULL,
  `file_updated_LOCAL` datetime DEFAULT NULL,
  `date_created_source` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'на основе каких данных нам известна дата',
  `hash_sha256` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `exif` mediumblob DEFAULT NULL COMMENT 'метаданные для фотографий',
  `deviceModel` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `duration_ms` int(11) DEFAULT NULL,
  `gps_str` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_created` char(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` char(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `annotation` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`annotation`)),
  PRIMARY KEY (`id`),
  KEY `FK_to_events` (`event_id`),
  CONSTRAINT `FK_to_events` FOREIGN KEY (`event_id`) REFERENCES `events` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `files_to_informants` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `inf_id` int(11) NOT NULL,
  `file_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_informant` (`inf_id`),
  KEY `FK_file` (`file_id`),
  CONSTRAINT `FK_file` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`),
  CONSTRAINT `FK_informant` FOREIGN KEY (`inf_id`) REFERENCES `informants` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `informants` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nickname` varchar(50) DEFAULT NULL,
  `first_name` varchar(512) DEFAULT '',
  `middle_name` varchar(512) DEFAULT '',
  `last_name` varchar(512) DEFAULT '',
  `last_name_at_birth` varchar(512) DEFAULT '',
  `birthYear` year(4) DEFAULT NULL,
  `birth` date DEFAULT NULL,
  `comments` text DEFAULT NULL,
  `contacts` varchar(255) DEFAULT NULL,
  `keywords` text DEFAULT NULL,
  `hide` int(1) DEFAULT 0,
  `reporter` varchar(512) DEFAULT NULL,
  `date_updated` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `date_created` datetime NOT NULL DEFAULT current_timestamp(),
  `user_created` char(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `interfaces` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tableName` char(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `col` char(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `editorHtml` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `viewHtml` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `marks` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start_time` time DEFAULT NULL,
  `tags` text DEFAULT NULL,
  `describtion` text DEFAULT NULL,
  `file_id` int(11) NOT NULL,
  `hide` char(1) NOT NULL DEFAULT '',
  `time_msec` int(11) DEFAULT 0,
  `recognition0` text DEFAULT NULL,
  `recognition1` text DEFAULT NULL,
  `recognition2` text DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `file_id` (`file_id`) USING BTREE,
  KEY `start_time` (`start_time`),
  FULLTEXT KEY `tags` (`tags`),
  FULLTEXT KEY `decription_of_file` (`describtion`),
  CONSTRAINT `marks_to_files_constr` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `photo_marks` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `rect_top` int(10) unsigned NOT NULL,
  `rect_left` int(10) unsigned NOT NULL,
  `rect_width` int(10) unsigned NOT NULL,
  `rect_height` int(10) unsigned NOT NULL,
  `inf_id` int(11) DEFAULT NULL,
  `file_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_to_file` (`file_id`),
  KEY `FK_to_inf` (`inf_id`),
  CONSTRAINT `FK_to_file` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`),
  CONSTRAINT `FK_to_inf` FOREIGN KEY (`inf_id`) REFERENCES `informants` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `transcribtion_tasks` (
  `file_id` int(11) NOT NULL,
  `task_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'pending',
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `model_size` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT 'small' COMMENT 'whisper model size: tiny, base, small, medium, large',
  `url` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `splitted_file_id` varchar(36) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `format` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT 'json' COMMENT 'output format: json, srt, vtt, txt',
  `min_mark_duration_ms` int(11) DEFAULT 60000 COMMENT 'minimum duration in ms for grouping transcription segments into marks',
  `error_message` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`task_id`),
  KEY `idx_file_id` (`file_id`),
  KEY `idx_status` (`status`),
  CONSTRAINT `fk_transcribtion_file` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `transcription_parts` (
  `filename` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `task_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `status` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT 'pending' COMMENT 'pending, processing, completed, error',
  `worker_id` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `part_index` int(11) NOT NULL,
  `duration_msec` int(11) DEFAULT 0 COMMENT 'длительность фрагмента в миллисекундах',
  `offset_ms` int(11) DEFAULT 0 COMMENT 'смещение начала фрагмента в исходном файле (сумма длительности предыдущих частей)',
  `file_path` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `file_url` text COLLATE utf8mb4_unicode_ci,
  `correlation_id` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL UNIQUE,
  `error_message` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_correlation_id` (`correlation_id`),
  KEY `idx_task_id` (`task_id`),
  KEY `idx_status` (`status`),
  CONSTRAINT `fk_transcription_parts_task` FOREIGN KEY (`task_id`) REFERENCES `transcribtion_tasks` (`task_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `transcription_results` (
  `file_id` int(11) NOT NULL,
  `offset_ms` int(11) DEFAULT 0 COMMENT 'смещение начала фрагмента в исходном файле (сумма длительности предыдущих частей)',
  `start` int(11) DEFAULT 0 COMMENT 'начало сегмента в миллисекундах',
  `end` int(11) DEFAULT 0 COMMENT 'конец сегмента в миллисекундах',
  `text` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `task_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `correlation_id` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `segment_id` int(11) NOT NULL COMMENT 'ID сегмента в результатах',
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `avg_logprob` float DEFAULT NULL,
  `compression_ratio` float DEFAULT NULL,
  `no_speech_prob` float DEFAULT NULL,
  `filename` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  KEY `idx_task_id` (`task_id`),
  KEY `idx_file_id` (`file_id`),
  KEY `idx_correlation_id` (`correlation_id`),
  CONSTRAINT `fk_results_task` FOREIGN KEY (`task_id`) REFERENCES `transcribtion_tasks` (`task_id`) ON DELETE CASCADE,
  CONSTRAINT `fk_results_correlation` FOREIGN KEY (`correlation_id`) REFERENCES `transcription_parts` (`correlation_id`) ON DELETE CASCADE,
  CONSTRAINT `fk_results_file` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DELIMITER //
CREATE FUNCTION IF NOT EXISTS `getServerTimezone`() RETURNS char(6) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci
    NO SQL
BEGIN
   IF (NOW() >= UTC_TIMESTAMP) THEN
      return CONCAT('+', SUBSTRING_INDEX(TIMEDIFF(NOW(), UTC_TIMESTAMP), ':', 2));
   ELSE
        return CONCAT('-', SUBSTRING_INDEX(TIMEDIFF(UTC_TIMESTAMP, NOW()), ':', 2));
   END IF;
END//
DELIMITER ;

SET @OLDTMP_SQL_MODE=@@SQL_MODE, SQL_MODE='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';
DELIMITER //
CREATE TRIGGER IF NOT EXISTS `files_insert` BEFORE INSERT ON `files` FOR EACH ROW BEGIN
   SET NEW.date_upload_UTC = UTC_TIMESTAMP();
   SET NEW.date_updated_UTC = UTC_TIMESTAMP();
    SET NEW.date_updated_timezone = getServerTimezone();
   SET NEW.date_upload_timezone = getServerTimezone();
END//
DELIMITER ;
SET SQL_MODE=@OLDTMP_SQL_MODE;

SET @OLDTMP_SQL_MODE=@@SQL_MODE, SQL_MODE='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';
DELIMITER //
CREATE TRIGGER IF NOT EXISTS `files_update` BEFORE UPDATE ON `files` FOR EACH ROW BEGIN
   SET NEW.date_updated_timezone = getServerTimezone();
   SET NEW.date_updated_UTC = UTC_TIMESTAMP();
END//
DELIMITER ;
SET SQL_MODE=@OLDTMP_SQL_MODE;


-- 1. Вставляем событие (опционально)
INSERT INTO `events` (`title`, `user_created`)
VALUES ('Test Event', 'test_user');

-- 2. Вставляем файл с id=555
INSERT INTO `files` (
  `id`,
  `name`,
  `description`,
  `operator`,
  `fileExt`,
  `filetype`,
  `duration_ms`,
  `user_created`,
  `status`
) VALUES (
  555,
  'audio_short.mp3',
  'Test audio file for transcription',
  'test_operator',
  'mp3',
  'audio',
  30000,
  'test_user',
  'ready'
);




/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;


