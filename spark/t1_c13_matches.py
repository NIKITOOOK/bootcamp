# stage t1_c13_matches: инкремент. Окно HOURS часов по sys_insert_stamp в RAW, delete по match_id + append.
# Первый запуск (таблицы нет) = полная сборка. Режим «Spark script».
HOURS = 6

SQL = """
with m as (
    select match_id, start_time, game_mode, duration, radiant_win, human_players, sys_insert_stamp,
           row_number() over (partition by match_id order by sys_insert_stamp desc) as rn
    from lakehouse.raw.pg__matches
),
pc as (
    select match_id, count(*) as n_players, count(distinct hero_id) as n_heroes, count_if(hero_id <= 0) as n_bad_hero
    from lakehouse.raw.pg__match_players
    group by match_id
)
select m.match_id,
       cast(from_unixtime(m.start_time) as timestamp) as start_time,
       date_trunc('hour', cast(from_unixtime(m.start_time) as timestamp)) as match_hour,
       m.duration as duration_sec,
       round(m.duration / 60.0, 1) as duration_min,
       m.game_mode,
       m.radiant_win,
       m.sys_insert_stamp as loaded_at
from m
join pc on pc.match_id = m.match_id
where m.rn = 1 and m.game_mode <> 23 and m.duration >= 900 and m.human_players = 10
  and pc.n_players = 10 and pc.n_heroes = 10 and pc.n_bad_hero = 0
"""

if not spark.catalog.tableExists(FULL_NAME):
    result = spark.sql(SQL)
    result.writeTo(FULL_NAME).using("iceberg").createOrReplace()
else:
    delta = SQL.replace("    from lakehouse.raw.pg__matches\n",
        f"    from lakehouse.raw.pg__matches\n    where sys_insert_stamp >= current_timestamp() - interval {HOURS} hours\n")
    result = spark.sql(delta)
    result.createOrReplaceTempView("c13_delta")
    spark.sql(f"delete from {FULL_NAME} where match_id in (select match_id from c13_delta)")
    result.writeTo(FULL_NAME).append()
