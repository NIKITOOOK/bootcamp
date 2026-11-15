"CREATE TABLE lakehouse.stage.t1_c13_building_falls (
   match_id bigint COMMENT 'Уникальный ID матча (Valve)',
   building_side varchar,
   building_code integer,
   building_type varchar,
   lane varchar,
   npc_id integer,
   available_from_sec integer,
   fall_time_sec integer,
   fall_time_source varchar,
   fell_by_15 integer,
   exposure_sec integer
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/stage/t1_c13_building_falls'
)"
