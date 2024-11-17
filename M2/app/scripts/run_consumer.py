import time
import json
import pandas as pd
from kafka import KafkaConsumer
from src.clean import streamed_main



def start_consumer(KAFKA_INTERNAL_URL: str, TOPIC_NAME: str) -> KafkaConsumer:
    """
    Start a Kafka Consumer

    Args:
    KAFKA_URL (str): Kafka URL
    TOPIC_NAME (str): Kafka Topic Name

    Returns:
    KafkaConsumer: Kafka Consumer
    """

    tries = 0
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_INTERNAL_URL,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            break
        except Exception as e:
            print(f"Error connecting to Kafka: {e} will retry again in 5 seconds")
            tries += 1
            if tries > 3:
                print("Max retries reached. Exiting. we will restart the container")
                raise ValueError("Max retries reached. Exiting.")
            time.sleep(5)

    return consumer


def consume_until_eof(consumer: KafkaConsumer) -> None:
    """
    Consume messages until EOF

    Args:
    consumer (KafkaConsumer): Kafka Consumer

    Returns:

    """

    while True:
        message_batch = consumer.poll()

        for _, messages in message_batch.items():
            for message in messages:
                if not message.value == "EOF":
                    new_rows = pd.DataFrame([message.value], columns=message.value.keys())
                    column_names = ['Customer Id','Emp Title','Emp Length','Home Ownership','Annual Inc','Annual Inc Joint','Verification Status','Zip Code','Addr State','Avg Cur Bal','Tot Cur Bal','Loan Id','Loan Status','Loan Amount','State','Funded Amount','Term','Int Rate','Grade','Issue Date','Pymnt Plan','Type','Purpose','Description']
                    for column in column_names:
                        if column not in new_rows.columns:
                            new_rows[column] = None
                    streamed_main(new_rows)
                else:   
                    print("EOF message received. Exiting..")
                    return