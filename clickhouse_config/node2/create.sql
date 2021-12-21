CREATE DATABASE IF NOT EXISTS shard;
CREATE DATABASE IF NOT EXISTS replica;


CREATE TABLE IF NOT EXISTS shard.ugc
(
   id UUID,
   user_login String,
   film_id UUID,
   event_time DateTime

)
Engine=ReplicatedMergeTree('/clickhouse/tables/shard2/test', 'replica_1')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY id;


CREATE TABLE IF NOT EXISTS replica.ugc
(
   id UUID,
   user_login String,
   film_id UUID,
   event_time DateTime
)
Engine=ReplicatedMergeTree('/clickhouse/tables/shard1/test', 'replica_2')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY id;


CREATE TABLE IF NOT EXISTS default.ugc
(
   id UUID,
   user_login String,
   film_id UUID,
   event_time DateTime
)
ENGINE = Distributed('company_cluster', '', ugc, rand());