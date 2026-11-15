"CREATE TABLE lakehouse.mart.t1_c13_hero_tower_damage (
   hero_id integer COMMENT 'ID героя',
   hero_name varchar COMMENT 'Имя героя',
   attack_type varchar COMMENT 'Тип атаки: Melee или Ranged',
   pos_proxy integer COMMENT 'Позиция по net worth внутри команды (1 = самый богатый)',
   pusher_allies_grp varchar,
   games bigint,
   tdpm_avg double,
   tdpm_median double,
   tdpm_expected double,
   tdpm_index double,
   tdpm_index_avg double,
   dmg_share_avg double,
   win_rate double,
   helm_rate double,
   share_phase_0_15 double,
   share_phase_15_30 double,
   share_phase_30_plus double,
   share_ability double,
   share_item double,
   share_autoattack double,
   share_summon_npc double,
   hp_units_per_game double
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/mart/t1_c13_hero_tower_damage'
)"
