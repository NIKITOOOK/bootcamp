"CREATE TABLE lakehouse.stage.t1_c13_tower_hits (
   match_id bigint COMMENT 'Уникальный ID матча (Valve)',
   hero_id integer COMMENT 'ID героя',
   is_radiant boolean COMMENT 'true если в Radiant',
   building_npc_id integer COMMENT 'ID башни',
   building_type varchar,
   lane varchar,
   source_type varchar,
   source_id varchar,
   source_name varchar,
   ability_level integer,
   time_window varchar,
   hits bigint,
   damage bigint COMMENT 'Количество урона',
   first_hit_sec integer,
   last_hit_sec integer
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/stage/t1_c13_tower_hits'
)"
