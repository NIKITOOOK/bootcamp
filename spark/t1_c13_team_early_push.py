# mart t1_c13_team_early_push: инкремент. Скользящее окно HOURS часов, пересчёт только затронутых матчей,
# delete по match_id + append. Первый запуск (таблицы нет) = полная сборка. Режим «Spark script».
# FULL_NAME и spark даёт Портал. Собственное имя таблицы в коде не пишем (иначе поток зависит сам от себя).
HOURS = 6

SQL = """
with t as (
    select p.match_id, p.is_radiant, max(p.win) as win, sum(p.is_strong_pusher) as strong_pushers,
           avg(p.rank_tier) as avg_rank_tier, sum(p.is_anonymous) as anonymous_players
    from lakehouse.stage.t1_c13_players p
    group by 1, 2
),
dmg as (
    select match_id, is_radiant,
           sum(case when time_window = '00-10' then damage else 0 end) as bld_dmg_10,
           sum(case when time_window in ('00-10', '10-15') then damage else 0 end) as bld_dmg_15
    from lakehouse.stage.t1_c13_tower_hits
    group by 1, 2
),
tw as (
    select match_id, building_side, sum(fell_by_15) as towers_lost_15
    from lakehouse.stage.t1_c13_building_falls
    where building_type in ('T1', 'T2', 'T3', 'T4')
    group by 1, 2
),
gold as (
    select g.match_id, mp.is_radiant, sum(g.amount) as gold_15
    from lakehouse.raw.mongo__gold_events g
    join lakehouse.raw.pg__match_players mp on mp.match_id = g.match_id and mp.account_id = g.steam_account_id
    join t on t.match_id = g.match_id and t.is_radiant = mp.is_radiant and t.anonymous_players = 0
    where g.time <= 900 and g.amount > 0
    group by 1, 2
),
s as (
    select t.match_id, t.is_radiant, m.match_hour, m.duration_min, t.strong_pushers, round(t.avg_rank_tier, 1) as avg_rank_tier,
           coalesce(d.bld_dmg_10, 0) as bld_dmg_10, coalesce(d.bld_dmg_15, 0) as bld_dmg_15,
           coalesce(tw.towers_lost_15, 0) as towers_down_15,
           case when t.anonymous_players = 0 then gd.gold_15 end as gold_15,
           t.win
    from t
    join lakehouse.stage.t1_c13_matches m on m.match_id = t.match_id
    left join dmg d on d.match_id = t.match_id and d.is_radiant = t.is_radiant
    left join tw on tw.match_id = t.match_id and tw.building_side = case when t.is_radiant then 'dire' else 'radiant' end
    left join gold gd on gd.match_id = t.match_id and gd.is_radiant = t.is_radiant
)
select a.match_id, a.is_radiant, a.match_hour, a.duration_min, a.strong_pushers, a.avg_rank_tier,
       a.bld_dmg_10, a.bld_dmg_15, a.towers_down_15, a.gold_15,
       a.bld_dmg_15 - b.bld_dmg_15 as bld_dmg_15_diff,
       a.towers_down_15 - b.towers_down_15 as towers_15_diff,
       a.gold_15 - b.gold_15 as gold_15_diff,
       a.win
from s a
join s b on b.match_id = a.match_id and b.is_radiant <> a.is_radiant
"""

spark.sql(f"""create or replace temp view c13_new_ids as
    select match_id from lakehouse.stage.t1_c13_matches
    where loaded_at >= current_timestamp() - interval {HOURS} hours""")

if not spark.catalog.tableExists(FULL_NAME):
    result = spark.sql(SQL)
    result.writeTo(FULL_NAME).using("iceberg").createOrReplace()
else:
    delta = SQL
    delta = delta.replace("lakehouse.stage.t1_c13_players", "(select x.* from lakehouse.stage.t1_c13_players x join c13_new_ids n on n.match_id = x.match_id)")
    delta = delta.replace("lakehouse.stage.t1_c13_matches", "(select x.* from lakehouse.stage.t1_c13_matches x join c13_new_ids n on n.match_id = x.match_id)")
    delta = delta.replace("lakehouse.stage.t1_c13_tower_hits", "(select x.* from lakehouse.stage.t1_c13_tower_hits x join c13_new_ids n on n.match_id = x.match_id)")
    delta = delta.replace("lakehouse.stage.t1_c13_building_falls", "(select x.* from lakehouse.stage.t1_c13_building_falls x join c13_new_ids n on n.match_id = x.match_id)")
    result = spark.sql(delta)
    spark.sql(f"delete from {FULL_NAME} where match_id in (select match_id from c13_new_ids)")
    result.writeTo(FULL_NAME).append()
