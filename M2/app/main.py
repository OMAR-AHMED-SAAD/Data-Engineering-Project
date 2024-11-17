from src import clean
from scripts.run_producer import start_producer, stop_container
from scripts.run_consumer import start_consumer, consume_until_eof

ID = "52_4509"
KAFKA_URL = "localhost:9092"
KAFKA_INTERNAL_URL = "kafka:29092"
TOPIC_NAME = "fintech-topic"

if __name__ == "__main__":
    try:
        clean.main()
        consumer = start_consumer(KAFKA_INTERNAL_URL, TOPIC_NAME)
        id = start_producer(ID, KAFKA_URL, TOPIC_NAME)
        consume_until_eof(consumer)
        consumer.close()
    except Exception as e:
        print(f"An error{e} occurred")
        exit(1)
    finally:
        stop_container(id)
