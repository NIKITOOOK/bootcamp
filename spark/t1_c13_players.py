# stage t1_c13_players: инкремент. Скользящее окно HOURS часов, пересчёт только затронутых матчей,
# delete по match_id + append. Первый запуск (таблицы нет) = полная сборка. Режим «Spark script».
# FULL_NAME и spark даёт Портал. Собственное имя таблицы в коде не пишем (иначе поток зависит сам от себя).
HOURS = 6

SQL = """
with p as (
    select mp.match_id, mp.player_slot, mp.account_id, mp.hero_id, mp.is_radiant, mp.win, mp.rank,
           mp.net_worth, mp.tower_damage, m.duration_sec,
           row_number() over (partition by mp.match_id, mp.player_slot order by mp.sys_insert_stamp desc) as rn
    from lakehouse.raw.pg__match_players mp
    join lakehouse.stage.t1_c13_matches m on m.match_id = mp.match_id
),
helm as (
    select pe.match_id, pe.steam_account_id, min(pe.time) as helm_time_sec
    from lakehouse.raw.mongo__purchase_events pe
    join lakehouse.stage.t1_c13_matches m on m.match_id = pe.match_id
    where pe.itemid in (164, 635) and pe.steam_account_id <> 4294967295
    group by 1, 2
),
f as (
    select p.match_id, p.player_slot, p.hero_id, h.localized_name as hero_name, h.attack_type,
           p.is_radiant, case when p.win then 1 else 0 end as win, p.net_worth, nullif(p.rank, 0) as rank_tier,
           case when p.account_id = 4294967295 then 1 else 0 end as is_anonymous,
           p.tower_damage, p.duration_sec,
           cast(p.tower_damage as double) * 60 / p.duration_sec as tdpm,
           case when hl.match_id is not null then 1 else 0 end as has_helm, hl.helm_time_sec,
           case when p.hero_id in (45, 92, 27, 61, 77, 64, 82, 37, 56, 109, 48, 95, 10, 52, 21, 8, 74, 33, 43, 80, 53, 42,
                                   63, 1, 136, 49, 18, 81, 6, 59, 58, 138, 12, 54, 46, 78, 60, 35, 104, 20, 47, 113, 93)
                then 1 else 0 end as is_strong_pusher,
           row_number() over (partition by p.match_id, p.is_radiant order by p.net_worth desc, p.player_slot) as pos_proxy
    from p
    left join lakehouse.raw.pg__heroes h on h.hero_id = p.hero_id
    left join helm hl on hl.match_id = p.match_id and hl.steam_account_id = p.account_id and p.account_id <> 4294967295
    where p.rn = 1
)
select match_id, player_slot, hero_id, hero_name, attack_type, is_radiant, win, net_worth, rank_tier, is_anonymous,
       tower_damage, duration_sec, pos_proxy, tdpm, has_helm, helm_time_sec, is_strong_pusher,
       sum(is_strong_pusher) over (partition by match_id, is_radiant) - is_strong_pusher as pusher_allies,
       sum(tower_damage) over (partition by match_id, is_radiant) as team_tower_damage,
       cast(tower_damage as double) / nullif(sum(tower_damage) over (partition by match_id, is_radiant), 0) as dmg_share
from f
"""

spark.sql(f"""create or replace temp view c13_new_ids as
    select match_id from lakehouse.stage.t1_c13_matches
    where loaded_at >= current_timestamp() - interval {HOURS} hours""")

if not spark.catalog.tableExists(FULL_NAME):
    result = spark.sql(SQL)
    result.writeTo(FULL_NAME).using("iceberg").createOrReplace()
else:
    delta = SQL
    delta = delta.replace("lakehouse.stage.t1_c13_matches", "(select x.* from lakehouse.stage.t1_c13_matches x join c13_new_ids n on n.match_id = x.match_id)")
    result = spark.sql(delta)
    spark.sql(f"delete from {FULL_NAME} where match_id in (select match_id from c13_new_ids)")
    result.writeTo(FULL_NAME).append()
