import json
from typing import Optional
from aiokafka import AIOKafkaProducer
import backoff as backoff

from core import config

kafka_producer: Optional[AIOKafkaProducer] = None


class KafkaProducer:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer
        self.views_topic = config.KAFKA_TOPIC

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def collect_view(self, film_id, user_login, timestamp):
        await self.producer.start()
        try:
            value_json = json.dumps(timestamp).encode('utf-8')
            key_json = json.dumps(user_login+film_id).encode('utf-8')
            await self.producer.send_and_wait(topic=self.views_topic, value=value_json, key=key_json)
        except Exception as e:
            print(e)
        finally:
            await self.producer.stop()


async def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(producer=kafka_producer)
