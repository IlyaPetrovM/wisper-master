

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
  "url": "http://file-storage-service:3001/api/files/audio.mp3",
  "max_duration": 60,
  "save_to_storage": true
}
```

### Формат исходящего сообщения (split_out)

#### При успехе (save_to_storage=true)

```json
{
  "success": true,
  "task_id": "task_uuid",
  "storage_files": [
    {
      "path": "audio.mp3__part__0__30.mp3",
      "uploadedAt": "2026-05-15T10:30:00.000Z"
    }
  ]
}
```



# Транскрибирование с помощью Wisper Workers


## Транскрибация
```
{"command": "transcribe", "correlation_id": "task_uuid", "model_size": "small", "format": "srt", "file_url": "http://file-storage-service:3001/api/files/audio.mp3__part__0__30.mp3"}
```