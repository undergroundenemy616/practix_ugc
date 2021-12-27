import logging
import uuid
from datetime import datetime
from time import sleep

import backoff
from clickhouse_driver import Client
from decouple import config
from kafka import KafkaConsumer
from memory_profiler import profile
from utils import coroutine

logging.basicConfig(level=logging.INFO)
fp = open('memory_profiler.log', 'w+')


class ETL:
    def __init__(self):
        self.kafka_consumer = KafkaConsumer(config('KAFKA_TOPIC'),
                                            bootstrap_servers=[config('KAFKA_BOOTSTRAP_SERVERS')],
                                            auto_offset_reset=config('AUTO_OFFSET_RESET'),
                                            api_version=(0, 11, 5),
                                            group_id=config('CONSUMER_GROUP'),
                                            enable_auto_commit=False,
                                            consumer_timeout_ms=5000,
                                            )
        self.clickhouse_client = Client(host=config('CLICKHOUSE_HOST1'), port=config('CLICKHOUSE_PORT1'),
                                        alt_hosts=f'{config("CLICKHOUSE_HOST2")}:{config("CLICKHOUSE_PORT2")}'
                                        )
        self.clickhouse_table = config('CLICKHOUSE_TABLE')

    @backoff.on_exception(backoff.expo, ConnectionError)
    def __get_new_messages(self) -> list:
        messages = []
        for message in self.kafka_consumer:
            messages.append(message)
            if len(messages) > 500:
                break
        return messages

    @backoff.on_exception(backoff.expo, ConnectionError)
    def __load_to_clickhouse(self, objects: list) -> None:
        self.clickhouse_client.execute(
            f'INSERT INTO {self.clickhouse_table} (id, user_login, film_id, event_time) VALUES',
            objects
        )

    @coroutine
    @profile(stream=fp)
    def extract(self, target):
        while True:
            sleep(5)
            new_messages = self.__get_new_messages()
            if new_messages:
                logging.info(f'Found {len(new_messages)}')
                target.send(new_messages)

    @coroutine
    @profile(stream=fp)
    def transform(self, target):
        while True:
            raw_new_messages = (yield)
            transformed_messages = []
            for message in raw_new_messages:
                partition_key = message.key.partition(b'+')
                transformed_message = [uuid.uuid4(),
                                       partition_key[0].decode('utf-8'),
                                       uuid.UUID(partition_key[2].decode('utf-8')),
                                       datetime.utcfromtimestamp(int(message.value))]
                transformed_messages.append(transformed_message)
            target.send(transformed_messages)

    @coroutine
    @profile(stream=fp)
    def load(self):
        while True:
            transformed_messages = (yield)
            self.__load_to_clickhouse(transformed_messages)
            self.kafka_consumer.commit()


if __name__ == '__main__':

    etl = ETL()
    logging.info("Let's go")
    load_process = etl.load()
    transform_process = etl.transform(load_process)
    extract_process = etl.extract(target=transform_process)
