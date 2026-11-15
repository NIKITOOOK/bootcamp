"CREATE TABLE lakehouse.mart.t1_c13_team_early_push_v2 (
   match_id bigint COMMENT 'Уникальный ID матча (Valve)',
   is_radiant boolean COMMENT 'true если в Radiant',
   match_hour timestamp(6) with time zone,
   duration_min decimal(13, 1),
   strong_pushers bigint,
   avg_rank_tier double,
   anonymous_players bigint,
   helm_players bigint,
   bld_dmg_10 bigint,
   bld_dmg_15 bigint,
   bld_hp_units_15 double,
   bld_dmg_15_30 bigint,
   bld_dmg_30_plus bigint,
   ability_dmg_15 bigint,
   item_dmg_15 bigint,
   autoattack_dmg_15 bigint,
   summon_npc_dmg_15 bigint,
   towers_down_15 bigint,
   first_enemy_t1_fall_sec integer,
   bld_dmg_15_diff bigint,
   towers_15_diff bigint,
   strong_pushers_diff bigint,
   team_net_worth bigint,
   net_worth_end_diff bigint,
   win integer COMMENT 'true если выиграл матч'
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/mart/t1_c13_team_early_push_v2'
)"
