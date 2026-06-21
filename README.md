

Есть 4 сервиса:
- master
- rabbitMQ server
- audio splitter
- file-storage-service
- wisper workers (2 шт)

Алгоритм работы:

1. master получает по REST API название файла в хранилище который надо транскрибировать
2. master отправляет команду audio splitter через очередь split_in
3. audio-splitter возвращает master названия нескольких аудиофайлов через очередь split_out
4. master отправляет команду на транскрибацию в очередь wisper_in для каждого файла по отдельности
5. wisper workers возвращают транскрипции файлов в очередь wisper_out
6. master собирает все транскрипции в единый json (при этом меняет время начала и конца фраз в соответствии с тем к какой части аудио относится эта транскрипция)


# Audio Splitter Service

FastAPI микросервис для нарезания аудио файлов на части без переперекодирования.

## Поддерживаемые форматы

- Аудио: mp3, wav, m4a, flac, ogg
- Видео (извлечение аудиодорожки): mp4, mkv, webm

## API

### POST /split

Нарезать аудио файл по длительности или количеству частей.

**Параметры запроса:**
- `url` (string) - URL для скачивания файла
- `max_duration` (integer, опционально) - максимальная длительность каждого файла в секундах
- `save_to_storage` (boolean, по умолчанию false) - загрузить результирующие файлы в File Storage Service

**Примечание:** Должны быть указаны либо `filename`, либо `url` (но не оба одновременно). Должны быть указаны либо `max_duration`, либо `split_parts` (но не оба одновременно).

формат передачи задания в очередь split_in
```json
{
  "task_id": "task_uuid",
  "url": "http://file-storage-service:3001/api/files/audio_short.mp3",
  "max_duration": 60,
  "save_to_storage": true
}
```

### Формат исходящего сообщения (split_out)

#### При успехе без сохранения в хранилище (save_to_storage=false)

```json
{
  "status": "success",
  "task_id": "task_uuid",
  "files": [
    {
      "path": "/shared_storage/splitted/audio.mp3__part__0__30.mp3",
      "duration_msec": 30000
    }
  ]
}
```

#### При успехе с сохранением в хранилище (save_to_storage=true)

```json
{
  "status": "success",
  "task_id": "task_uuid",
  "storage_files": [
    {
      "path": "audio.mp3__part__0__30.mp3",
      "duration_msec": 30000,
      "uploadedAt": "2026-05-15T10:30:00.000Z"
    }
  ]
}
```

#### При ошибке

```json
{
  "status": "error",
  "task_id": "task_uuid",
  "error": "Error message"
}
```



# Транскрибирование с помощью Wisper Workers

### Формат исходящего сообщения (wisper_in)

```json
{
  "command": "transcribe",
  "task_id": "task_uuid",
  "correlation_id": "task_uuid_part_index",
  "model_size": "small",
  "format": "json",
  "file_url": "http://file-storage-service:3001/api/files/audio.mp3__part__0__30.mp3"
}
```

### Формат входящего сообщения (wisper_out)

#### При успехе

```json
{
  "status": "success",
  "task_id": "task_uuid",
  "correlation_id": "task_uuid_part_index",
  "result": [
    {"id": 0, "start": 0.0, "end": 3.5, "text": "Текст"},
    {"id": 1, "start": 3.5, "end": 7.2, "text": "Текст"}
  ],
  "worker_id": "worker-1",
  "filename": "audio.mp3__part__0__30.mp3"
}
```

#### При ошибке

```json
{
  "status": "error",
  "task_id": "task_uuid",
  "correlation_id": "task_uuid_part_index",
  "error": "Error message",
  "worker_id": "worker-1",
  "logs": ["log line 1", "log line 2"]
}
```

### Загрузка модели (load_model)

```json
{
  "command": "load_model",
  "model_size": "small",
  "correlation_id": "model-load-456",
  "task_id": "load_model-1"
}
```