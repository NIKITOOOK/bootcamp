"CREATE TABLE lakehouse.mart.t1_c13_ability_building_damage (
   hero_id integer COMMENT 'ID героя',
   hero_name varchar COMMENT 'Имя героя',
   source_type varchar,
   source_group varchar,
   source_id varchar,
   source_name varchar,
   ability_level integer,
   building_type varchar,
   time_window varchar,
   match_phase varchar,
   is_talent integer,
   matches bigint,
   hits bigint,
   damage bigint COMMENT 'Количество урона',
   damage_hp_units double,
   dmg_per_hit double,
   dmg_per_match double,
   exposure_sec bigint,
   dmg_per_exposure_min double,
   ratio_to_t3 double,
   share_of_hero_damage double
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/mart/t1_c13_ability_building_damage'
)"
