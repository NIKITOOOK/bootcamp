"CREATE TABLE lakehouse.stage.t1_c13_matches (
   match_id bigint COMMENT 'Уникальный ID матча (Valve)',
   start_time timestamp(6) with time zone COMMENT 'Unix timestamp начала',
   match_hour timestamp(6) with time zone,
   duration_sec integer COMMENT 'Длительность (сек)',
   duration_min decimal(13, 1),
   game_mode integer COMMENT 'ID режима игры',
   radiant_win boolean COMMENT 'true если Radiant победил',
   loaded_at timestamp(6) with time zone COMMENT 'Время вставки в БД'
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/stage/t1_c13_matches'
)"
