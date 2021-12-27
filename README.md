# practix_ugc

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
https://github.com/KonstantinChernov/practix_auth
