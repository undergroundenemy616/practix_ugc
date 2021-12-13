import asyncio

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import ORJSONResponse
from aiokafka import AIOKafkaProducer

from api.v1 import views
from core import config
from services.kafka_producer import kafka_producer
from tracer import tracer

app = FastAPI(
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    title="UGC API для онлайн-кинотеатра",
    description="Сервис для работы с данными для аналитики Онлайн-кинотеатра",
    version="1.0.0",
    default_response_class=ORJSONResponse,
)

loop = asyncio.get_event_loop()


@app.on_event('startup')
async def startup():
    kafka_producer.kafka_producer = AIOKafkaProducer(
        loop=loop, client_id=config.PROJECT_NAME, bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
    )


@app.middleware("http")
async def add_tracing(request: Request, call_next):
    request_id = request.headers.get('X-Request-Id')
    if not request_id:
        raise RuntimeError('request id is required')
    response = await call_next(request)
    with tracer.start_span(request.url.path) as span:
        request_id = request.headers.get('X-Request-Id')
        span.set_tag('http.request_id', request_id)
        span.set_tag('http.url', request.url)
        span.set_tag('http.method', request.method)
        span.set_tag('http.status_code', response.status_code)
    return response


app.include_router(views.router, prefix='/api/v1/views', tags=['views'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
    )
