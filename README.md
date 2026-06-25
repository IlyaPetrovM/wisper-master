

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
7. после успешного завершения транскрибации каждой части, master отправляет DELETE запрос в file-storage-service для удаления ненужного файла части

# Master Node API

## POST /load_model

Загрузить Whisper модель на рабочих узлах.

**Параметры запроса:**
- `model_size` (string) - размер модели: tiny, base, small, medium, large

**Пример запроса:**
```json
{
  "model_size": "small"
}
```

**Пример ответа:**
```json
{
  "status": "model_load_queued",
  "model_size": "small"
}
```

## POST /transcribe

Начать транскрибирование аудио файла.

**Параметры запроса:**
- `file_id` (integer) - ID файла в базе данных
- `url` (string) - URL аудио файла для скачивания
- `max_duration` (integer, по умолчанию 60) - максимальная длительность каждого фрагмента в секундах
- `model_size` (string, по умолчанию "small") - размер Whisper модели: tiny, base, small, medium, large
- `format` (string, по умолчанию "json") - формат вывода: json, srt, vtt, txt
- `min_mark_duration_ms` (integer, по умолчанию 60000) - минимальная длительность в миллисекундах для группировки распознанных фрагментов в marks

**Параметр min_mark_duration_ms:**

Управляет группировкой распознанных текстовых фрагментов при сохранении в таблицу marks:
- Распознанные сегменты объединяются в группы
- Каждая группа имеет суммарную длительность не менее `min_mark_duration_ms`
- Текст из нескольких сегментов объединяется в одну запись mark
- Последняя неполная группа присоединяется к предыдущей
- По умолчанию: 60000 мс (1 минута)

Примеры значений:
- `30000` - минимум 30 секунд на один mark
- `60000` - минимум 1 минута на один mark (по умолчанию)
- `120000` - минимум 2 минуты на один mark

**Пример запроса:**
```json
{
  "file_id": 555,
  "url": "http://file-storage-service:3001/api/files/audio_short.mp3",
  "max_duration": 60,
  "model_size": "small",
  "format": "json",
  "min_mark_duration_ms": 60000
}
```

**Пример ответа:**
```json
{
  "task_id": "768de42f-b6e4-44d5-adc8-5b87df64675c",
  "status": "started"
}
```

## GET /status/{task_id}

Получить статус задачи транскрибирования.

**Пример ответа при успехе:**
```json
{
  "task_id": "768de42f-b6e4-44d5-adc8-5b87df64675c",
  "status": "completed"
}
```

Возможные статусы:
- `pending` - задача создана, ожидает обработки
- `splitting` - аудио разбивается на части
- `transcribing` - идет транскрибирование
- `completed` - транскрибирование завершено успешно
- `error` - произошла ошибка при обработке

## Алгоритм группировки фрагментов (marks)

Master Node автоматически группирует распознанные текстовые фрагменты при сохранении в таблицу `marks`:

1. **Расчет длительности**: каждый сегмент получает длительность = `(end_time - start_time) * 1000` мс
2. **Накопление**: сегменты накапливаются в группу до достижения `min_mark_duration_ms`
3. **Сохранение группы**: когда сумма длительности >= `min_mark_duration_ms`, группа сохраняется в один mark с объединенным текстом
4. **Остаток**: последняя неполная группа присоединяется к предыдущей

**Пример:**
```
Сегменты Whisper (длительность):
- "Hello" (3.5 сек = 3500 мс)
- "world" (4.0 сек = 4000 мс)  
- "good" (2.0 сек = 2000 мс)
- "day" (0.5 сек = 500 мс)

min_mark_duration_ms = 6000

Результат:
- Mark 1: "Hello world" (7500 мс)
- Mark 2: "good day" (2500 мс) ← остаток присоединен к предыдущей группе
```


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