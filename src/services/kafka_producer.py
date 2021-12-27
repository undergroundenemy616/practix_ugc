import logging
import time
import uuid
from datetime import datetime
from typing import Optional

import backoff as backoff
from aiokafka import AIOKafkaProducer
from fastapi import Request

from core import config
from tracer import tracer

kafka_producer: Optional[AIOKafkaProducer] = None


class KafkaProducerAdapter:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer
        self.views_topic = config.KAFKA_TOPIC

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def collect_view(
        self, film_id: uuid.UUID, user_login: str, timestamp: datetime, request: Request
    ):
        with tracer.start_span('collect-view') as span:
            request_id = request.headers.get('X-Request-Id')
            span.set_tag('http.request_id', request_id)

            try:
                await self.producer.start()
                timestamp_unix = round(time.mktime(timestamp.timetuple()))
                value_json = str(timestamp_unix).encode('utf-8')
                key_json = f'{user_login}+{film_id}'.encode('utf-8')
                await self.producer.send_and_wait(
                    topic=self.views_topic, value=value_json, key=key_json
                )

                span.set_tag('result_status', 'ok')
            except Exception as e:
                logging.exception(f'{e}')
                span.set_tag('result_status', f'Error: {e}')

            finally:
                await self.producer.stop()


async def get_kafka_producer() -> KafkaProducerAdapter:
    return KafkaProducerAdapter(producer=kafka_producer)
