from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import threading
import time

# Конфигурация
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'chat_topic'

# Producer (отправка сообщений)
def producer():
    conf = {'bootstrap.servers': KAFKA_SERVER}
    producer = Producer(conf)
    
    while True:
        message = input("Вы: ")
        if message.lower() == 'exit':
            break
        producer.produce(TOPIC, message)
        producer.flush()
        time.sleep(1)

# Consumer (получение сообщений)
def consumer():
    conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'chat_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        print(f"Собеседник: {msg.value().decode('utf-8')}")

# Запуск producer и consumer в отдельных потоках
def run_chat():
    producer_thread = threading.Thread(target=producer)
    consumer_thread = threading.Thread(target=consumer)
    
    producer_thread.start()
    consumer_thread.start()
    
    producer_thread.join()
    consumer_thread.join()

if __name__ == "__main__":
    run_chat()
