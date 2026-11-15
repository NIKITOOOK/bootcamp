"CREATE TABLE lakehouse.stage.t1_c13_players (
   match_id bigint COMMENT 'Уникальный ID матча (Valve)',
   player_slot integer COMMENT 'Слот (0-127 Radiant, 128-255 Dire)',
   hero_id integer COMMENT 'ID героя',
   hero_name varchar COMMENT 'Имя героя',
   attack_type varchar COMMENT 'Тип атаки: Melee или Ranged',
   is_radiant boolean COMMENT 'true если в Radiant',
   win integer COMMENT 'true если выиграл матч',
   net_worth integer COMMENT 'Net worth',
   rank_tier integer COMMENT 'Ранг игрока (1-80), NULL если не определён',
   is_anonymous integer COMMENT '1 если анонимный профиль (account_id 4294967295)',
   tower_damage integer COMMENT 'Урон башням',
   duration_sec integer COMMENT 'Длительность матча, сек',
   pos_proxy integer COMMENT 'Позиция по net worth внутри команды (1 = самый богатый)',
   tdpm double COMMENT 'Урон башням в минуту',
   has_helm integer COMMENT '1 если игрок покупал Helm of the Dominator или Overlord',
   helm_time_sec integer COMMENT 'Время первой покупки шлема, сек',
   is_strong_pusher integer COMMENT '1 если герой из списка сильных пушеров',
   pusher_allies bigint COMMENT 'Число сильных пушеров среди союзников',
   team_tower_damage bigint COMMENT 'Суммарный урон башням команды',
   dmg_share double COMMENT 'Доля игрока в уроне башням своей команды'
)
WITH (
   format = 'PARQUET',
   format_version = 2,
   location = 's3://warehouse/stage/t1_c13_players'
)"
