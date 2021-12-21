from utils import coroutine
from kafka import KafkaConsumer
from decouple import config
from clickhouse_driver import Client
from time import sleep
import logging
import backoff
import uuid

logging.basicConfig(level=logging.INFO)


class ETL:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 clickhouse_client: Client,
                 clickhouse_table: str):

        self.kafka_consumer = kafka_consumer,
        self.clickhouse_client = clickhouse_client,
        self.clickhouse_table = clickhouse_table

    @backoff.on_exception(backoff.expo, ConnectionError)
    def __get_new_messages(self) -> list:
        messages = []
        for message in self.kafka_consumer:
            messages.append(message)
        return messages

    @backoff.on_exception(backoff.expo, ConnectionError)
    def __load_to_clickhouse(self, objects: list) -> None:
        self.clickhouse_client.execute(
            f'INSERT INTO {self.clickhouse_table} (id, user_login, film_id, event_time) VALUES',
            objects
        )

    @coroutine
    def extract(self, target):
        while True:
            sleep(5)
            new_messages = self.__get_new_messages()
            if new_messages:
                logging.info(f'Found {len(new_messages)}')
                target.send(new_messages)

    @coroutine
    def transform(self, target):
        while True:
            raw_new_messages = (yield)
            transformed_messages = []
            for message in raw_new_messages:
                partition_key = message.key.partition(b'+')
                transformed_message = [uuid.uuid4(),
                                       partition_key[0],
                                       partition_key[2],
                                       message.value]
                transformed_messages.append(transformed_message)
            target.send(transformed_messages)

    @coroutine
    def load(self):
        while True:
            transformed_messages = (yield)
            self.__load_to_clickhouse(transformed_messages)


if __name__ == '__main__':
    consumer = KafkaConsumer(config('KAFKA_TOPIC'),
                             bootstrap_servers=[config('KAFKA_BOOTSTRAP_SERVERS')],
                             auto_offset_reset=config('AUTO_OFFSET_RESET'),
                             api_version=(0, 11, 5),
                             group_id=config('CONSUMER_GROUP')
                             )
    client = Client(host=config('CLICKHOUSE_HOST1'), port=config('CLICKHOUSE_PORT1'),
                    alt_hosts=f'{config("CLICKHOUSE_HOST2")}:{config("CLICKHOUSE_PORT2")}')
    etl = ETL(kafka_consumer=consumer,
              clickhouse_client=client,
              clickhouse_table=config('CLICKHOUSE_TABLE')
              )

    logging.info("Let's go")
    load_process = etl.load()
    transform_process = etl.transform(load_process)
    extract_process = etl.extract(target=transform_process)
