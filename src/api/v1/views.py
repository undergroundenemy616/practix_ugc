import logging
import uuid
from datetime import datetime
from http import HTTPStatus

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from auth_grpc.auth_check import check_permission
from services.kafka_producer import KafkaProducerAdapter, get_kafka_producer

router = APIRouter()

logging.basicConfig(level=logging.INFO)


class View(BaseModel):
    film_id: uuid.UUID
    timestamp: datetime


@router.post('',
             summary="Точка сбора информации о просмотрах",
             description="Принимает id фильма и временную метку на которой окончился просмотр фильма",
             response_description="возвращается статус код",
             tags=['views']
             )
# @check_permission(roles=["Subscriber"])
async def collect_view(
        request: Request,
        view: View,
        kafka_service: KafkaProducerAdapter = Depends(get_kafka_producer)):

    token = request.headers.get('Authorization', None)
    token = token.replace('Bearer ', '')
    decoded_token = jwt.decode(
        token, options={"verify_signature": False}
    )
    login = decoded_token.get('sub', None)

    try:
        await kafka_service.collect_view(request=request, user_login=login, **view.dict())
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"{e}")
    return {
        'login': login,
        **view.dict()
    }
