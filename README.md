# Стек:
Python, Fast Api, Reids, Kafka, ClickHouse, MongoDB, Nginx, Docker, Gunicorn.

Этот сервис разрешает пользователям создавать свой контент в нашем кинотеатре — оставлять лайки, комментарии, отзывы к фильмам. 

## Как запустить проект:

После клонирования проекта локально необходимо выполнить команду:
```
cp template.env .env
```
И передать значения переменным, указанным в появившемся файле .env

Затем выполнить команду:
```
docker-compose up
```

Для настройки базы данных Clickhouse необходимо зайти в контейнеры нод Clickhouse:
```
docker-compose exec clickhouse-node1 bash
docker-compose exec clickhouse-node2 bash
```
и в каждом выполнить команду:
```
clickhouse-client --multiquery < /etc/clickhouse-server/create.sql
```

Репозиторий Auth:
https://github.com/undergroundenemy616/practix_auth

## Схема взаимодействия сервисов:
![Image alt](https://github.com/KonstantinChernov/practix_ugc/blob/master/scheme.jpg)

## Cхема работы ETL:
![Image alt](https://github.com/KonstantinChernov/practix_ugc/blob/master/scheme2.jpg)
