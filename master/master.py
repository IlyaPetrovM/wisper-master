import pika
import json
import time
import os
import uuid
from datetime import datetime

class MasterNode:
    def __init__(self):
        self.host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.user = os.getenv('RABBITMQ_USER', 'guest')
        self.password = os.getenv('RABBITMQ_PASSWORD', 'guest')

        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        """Подключение к RabbitMQ с повторными попытками"""
        max_retries = 10
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(self.user, self.password)
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        credentials=credentials,
                        connection_attempts=3,
                        retry_delay=2
                    )
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue='whisper_in', durable=True)
                self.channel.queue_declare(queue='whisper_out', durable=True)
                print(f"✓ Подключено к RabbitMQ ({self.host}:{self.port})")
                return
            except Exception as e:
                print(f"✗ Попытка подключения {attempt + 1}/{max_retries} не удалась: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise

    def send_message(self, command, model_size=None, format=None, file_url=None):
        """Отправка сообщения в очередь whisper_in"""
        correlation_id = str(uuid.uuid4())

        message = {
            "command": command,
            "correlation_id": correlation_id
        }

        if model_size:
            message["model_size"] = model_size
        if format:
            message["format"] = format
        if file_url:
            message["file_url"] = file_url

        try:
            self.channel.basic_publish(
                exchange='',
                routing_key='whisper_in',
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"\n📤 Сообщение отправлено:")
            print(f"   Команда: {command}")
            print(f"   Correlation ID: {correlation_id}")
            print(f"   Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return correlation_id
        except Exception as e:
            print(f"✗ Ошибка при отправке: {e}")
            return None

    def receive_response(self, correlation_id, timeout=30):
        """Получение ответа из очереди whisper_out по correlation_id"""
        print(f"\n⏳ Ожидание ответа (timeout: {timeout}s)...")

        start_time = time.time()

        def callback(ch, method, properties, body):
            try:
                response = json.loads(body)
                if response.get('correlation_id') == correlation_id:
                    self.channel.basic_ack(delivery_tag=method.delivery_tag)
                    print(f"\n📥 Ответ получен:")
                    print(json.dumps(response, indent=2, ensure_ascii=False))
                    # Остановить потребление
                    self.channel.stop_consuming()
                else:
                    # Переполучить в очередь если это не наш ответ
                    self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as e:
                print(f"✗ Ошибка при обработке ответа: {e}")
                self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        self.channel.basic_consume(queue='whisper_out', on_message_callback=callback)

        try:
            while True:
                if time.time() - start_time > timeout:
                    print(f"⏰ Timeout при ожидании ответа")
                    break
                self.channel.connection.process_data_events(time_limit=0.1)
        except KeyboardInterrupt:
            print("\n⚠ Прервано пользователем")
        except Exception as e:
            print(f"✗ Ошибка при получении ответа: {e}")

    def run_interactive(self):
        """Интерактивный режим для отправки сообщений"""
        print("\n" + "="*60)
        print("🐰 Master Node - RabbitMQ Message Publisher")
        print("="*60)

        while True:
            print("\n\nВыберите действие:")
            print("1. Загрузить модель (load_model)")
            print("2. Транскрибировать аудио (transcribe)")
            print("3. Выход")

            choice = input("\nВаш выбор (1-3): ").strip()

            if choice == "1":
                model_size = input("Размер модели (small/medium/large) [small]: ").strip() or "small"
                correlation_id = self.send_message("load_model", model_size=model_size)
                if correlation_id:
                    self.receive_response(correlation_id)

            elif choice == "2":
                model_size = input("Размер модели (small/medium/large) [small]: ").strip() or "small"
                format = input("Формат (srt/json) [srt]: ").strip() or "srt"
                file_url = input("URL аудио файла: ").strip()
                if file_url:
                    correlation_id = self.send_message(
                        "transcribe",
                        model_size=model_size,
                        format=format,
                        file_url=file_url
                    )
                    if correlation_id:
                        self.receive_response(correlation_id, timeout=60)

            elif choice == "3":
                print("\n👋 До свидания!")
                break
            else:
                print("❌ Неверный выбор")

    def run_demo(self):
        """Демо режим с автоматическими тестовыми сообщениями"""
        print("\n" + "="*60)
        print("🐰 Master Node - Demo Mode")
        print("="*60)

        # Тест 1: Загрузка модели
        print("\n[Тест 1] Загрузка модели small")
        correlation_id = self.send_message("load_model", model_size="small")
        if correlation_id:
            self.receive_response(correlation_id, timeout=10)

        time.sleep(2)

        # Тест 2: Попытка транскрибирования
        print("\n[Тест 2] Транскрибирование с моделью small")
        correlation_id = self.send_message(
            "transcribe",
            model_size="small",
            format="srt",
            file_url="https://example.com/audio.mp3"
        )
        if correlation_id:
            self.receive_response(correlation_id, timeout=30)

        time.sleep(2)

        # Тест 3: Загрузка большой модели
        print("\n[Тест 3] Загрузка модели medium")
        correlation_id = self.send_message("load_model", model_size="medium")
        if correlation_id:
            self.receive_response(correlation_id, timeout=10)

    def close(self):
        """Закрытие соединения"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            print("\n✓ Соединение закрыто")

def main():
    master = MasterNode()

    try:
        # Раскомментируйте одну из строк ниже:
        master.run_demo()      # Демо режим с автоматическими тестами
        # master.run_interactive()  # Интерактивный режим
    finally:
        master.close()

if __name__ == "__main__":
    main()
