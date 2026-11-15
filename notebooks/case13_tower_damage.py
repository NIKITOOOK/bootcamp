# Кейс 13. Способности и герои: урон по зданиям
# Прототип анализа (JupyterLab, Trino, каталог lakehouse). Выборка 5% матчей (match_id % 20 = 0).
# SELECT-запросы раздела 9 затем стали STAGE/MART-потоками (см. ../sql).
# Текстовые разделы ноутбука (цель, бизнес-вопрос, гипотезы, источники) сокращены до заголовков.

# %% ## 5. Загрузка
import os, warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import trino

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 200)
plt.rcParams["figure.figsize"] = (10, 4.5)

conn = trino.dbapi.connect(
    host=os.getenv("TRINO_HOST", "trino"),
    port=int(os.getenv("TRINO_PORT", "8080")),
    user=os.getenv("TRINO_USER", os.getenv("JUPYTERHUB_USER", "jupyter")),
    catalog="lakehouse",
)

import time

def q(sql, attempts=6, pause=30):
    """SELECT в Trino -> DataFrame; при перезапуске Trino ждём и повторяем."""
    for a in range(attempts):
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=[d[0] for d in cur.description])
        except Exception as e:
            msg = str(e)
            if a < attempts - 1 and ("SERVER_STARTING_UP" in msg or "Query not found" in msg or "Connection" in msg):
                print(f"Trino недоступен ({msg[:60]}...), повтор через {pause} c")
                time.sleep(pause)
                continue
            raise

SAMPLE = "match_id % 20 = 0"
q("select count(*) as matches from lakehouse.raw.pg__matches")

# %% ## 6. Первичное исследование
overview = q("""
select 'pg__matches' as t, count(*) as rows_, count(distinct match_id) as matches from lakehouse.raw.pg__matches
union all select 'pg__match_players', count(*), count(distinct match_id) from lakehouse.raw.pg__match_players
union all select 'tower_damage_events (5%)', count(*), count(distinct match_id) from lakehouse.raw.mongo__tower_damage_events where match_id % 20 = 0
union all select 'tower_death_events', count(*), count(distinct match_id) from lakehouse.raw.mongo__tower_death_events
""")
overview

# %%
quality = q("""
select
  (select count(*) from lakehouse.raw.pg__matches) as matches,
  (select count_if(game_mode = 23) from lakehouse.raw.pg__matches) as turbo,
  (select count_if(duration < 900) from lakehouse.raw.pg__matches where game_mode <> 23) as short_lt15,
  (select min(from_unixtime(start_time)) from lakehouse.raw.pg__matches) as first_start,
  (select max(from_unixtime(start_time)) from lakehouse.raw.pg__matches) as last_start,
  (select count(*) - count(distinct (match_id, player_slot)) from lakehouse.raw.pg__match_players) as dup_match_players,
  (select count_if(account_id = 4294967295) from lakehouse.raw.pg__match_players) as anon_players,
  (select count(*) from lakehouse.raw.pg__match_players) as players,
  (select count(*) - count(distinct id) from lakehouse.raw.mongo__tower_damage_events where match_id % 20 = 0) as dup_hits_5pct,
  (select min(level) from lakehouse.raw.mongo__ability_learn_events where match_id % 20 = 0) as learn_level_min,
  (select max(level) from lakehouse.raw.mongo__ability_learn_events where match_id % 20 = 0 and abilityid = 5186) as nether_blast_level_max
""")
quality.T

# %%
# attacker в tower_damage_events — это hero_id, а не steamAccountId (как написано в каталоге)
attacker_check = q("""
with e as (
  select match_id, steam_account_id, max(attacker) as attacker
  from lakehouse.raw.mongo__tower_damage_events
  where match_id % 200 = 0 and steam_account_id <> 4294967295
  group by 1, 2)
select count(*) as players, count_if(e.attacker = p.hero_id) as attacker_eq_hero_id
from e join lakehouse.raw.pg__match_players p on p.match_id = e.match_id and p.account_id = e.steam_account_id
""")
attacker_check

# %%
q_turbo = quality.loc[0, "turbo"] / quality.loc[0, "matches"]
issues = pd.DataFrame([
    ["Turbo — {:.0%} матчей".format(q_turbo), "режим с ускоренной экономикой", "урон за удар в 1,5–2 раза выше, другое время падения башен", "исключаем game_mode = 23"],
    ["Короткие матчи < 15 мин", "abandon / ранний gg", "TDPM и доли урона шумные", "duration >= 900"],
    ["attacker = hero_id", "ошибка описания в каталоге", "join событий с игроками по steam_account_id ломается для анонимов", "join по (match_id, hero_id)"],
    ["Анонимный account_id 4294967295 ({:.0%} игроков)".format(quality.loc[0, "anon_players"] / quality.loc[0, "players"]), "скрытый профиль", "нельзя приписать покупки и золото конкретному игроку", "Helm у анонимов = неизвестно (0); золото к 15 мин — только матчи без анонимов"],
    ["level в ability_learn_events с 0", "формат источника", "номинал способности сдвигается на уровень", "level + 1"],
    ["npcid = любое здание, не только башня", "описание «ID башни» неполное", "в «урон по башням» попадают бараки, трон, fillers", "тип здания по npc_id (pg__npcs), fillers отдельно"],
    ["Вторая T4 не пишется в маску", "в маске 17 бит: 11 башен-слотов без второй T4", "окно трона нельзя взять из маски", "оценка: max(бит 9, последний удар по T4)"],
    ["Данные за ~18 часов, один патч", "источники чистятся каждые 4 часа", "нет динамики по патчам", "выводы — только для текущего патча"],
], columns=["проблема", "причина", "влияние", "решение"])
issues

# %% ## 7. Предобработка
SQL_FILTER = """
select game_mode, lobby_type, count(*) as matches, round(avg(duration) / 60.0, 1) as avg_min
from lakehouse.raw.pg__matches
where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
group by 1, 2 order by 3 desc
"""
q(SQL_FILTER)

# %% ## 9. Аналитический набор
SQL_PLAYERS = """
-- Кейс 13. Игроки: позиция-прокси (ранг по net_worth в команде), TDPM, Helm у игрока
with m as (
    select match_id, duration
    from lakehouse.raw.pg__matches
    where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
),
helm as (
    select distinct match_id, steam_account_id
    from lakehouse.raw.mongo__purchase_events
    where match_id % 20 = 0 and itemid in (164, 635) and steam_account_id <> 4294967295
)
select
    mp.match_id, mp.player_slot, mp.hero_id, h.localized_name as hero,
    mp.is_radiant, case when mp.win then 1 else 0 end as win,
    nullif(mp.rank, 0) as rank, mp.net_worth, mp.tower_damage, m.duration,
    cast(mp.tower_damage as double) * 60 / m.duration as tdpm,
    case when mp.account_id = 4294967295 then 1 else 0 end as is_anon,
    case when hl.match_id is not null then 1 else 0 end as helm_self,
    row_number() over (partition by mp.match_id, mp.is_radiant order by mp.net_worth desc, mp.player_slot) as pos
from lakehouse.raw.pg__match_players mp
join m on m.match_id = mp.match_id
left join lakehouse.raw.pg__heroes h on h.hero_id = mp.hero_id
left join helm hl on hl.match_id = mp.match_id and hl.steam_account_id = mp.account_id and mp.account_id <> 4294967295
"""
players = q(SQL_PLAYERS)
print(players.shape)
players.head()

# %%
SQL_HITS = """
-- Кейс 13. Удары по зданиям: источник x тип здания x окно времени (агрегат), выборка 5%
-- Правило источника: hero_attack (нет byability/byitem/fromnpc); ability (byability>0, fromnpc=0);
-- item (byitem>0 или юнит Necronomicon); own_summon (призыв героя: fromnpc не нейтрал/крип,
-- а также нейтралы у Chen 66 и Enchantress 58); helm_unit (нейтрал под Helm у прочих героев); other_npc (лайн-крипы и прочее)
with m as (
    select match_id from lakehouse.raw.pg__matches
    where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
),
e as (
    select t.damage, t.time,
        case when t.npcid between 16 and 18 or t.npcid between 26 and 28 then 'T1'
             when t.npcid between 19 and 21 or t.npcid between 29 and 31 then 'T2'
             when t.npcid between 22 and 24 or t.npcid between 32 and 34 then 'T3'
             when t.npcid in (25, 35) then 'T4'
             when t.npcid in (38, 39, 40, 44, 45, 46) then 'rax_melee'
             when t.npcid in (41, 42, 43, 47, 48, 49) then 'rax_range'
             when t.npcid in (50, 51) then 'fort'
             when t.npcid in (36, 37) then 'filler' end as building_type,
        case when t.byitem > 0 or n.name like 'npc_dota_necronomicon%' then 'item'
             when t.fromnpc = 0 and t.byability > 0 then 'ability'
             when t.fromnpc = 0 then 'hero_attack'
             when (n.name like 'npc_dota_neutral%' or n.name = 'npc_dota_dark_troll_warlord_skeleton_warrior')
                  then case when t.attacker in (66, 58) then 'own_summon' else 'helm_unit' end
             when n.name like 'npc_dota_creep%' or n.name like 'npc_dota_goodguys%' or n.name like 'npc_dota_badguys%'
                  or n.name is null or n.name like 'npc_dota_unit%' then 'other_npc'
             else 'own_summon' end as src,
        case when t.time < 600 then '00-10' when t.time < 1200 then '10-20' when t.time < 1800 then '20-30'
             when t.time < 2400 then '30-40' else '40+' end as time_window
    from lakehouse.raw.mongo__tower_damage_events t
    join m on m.match_id = t.match_id
    left join lakehouse.raw.pg__npcs n on n.npc_id = t.fromnpc
)
select src, building_type, time_window,
    count(*) as hits, sum(damage) as damage_sum, round(avg(damage), 2) as avg_hit,
    approx_percentile(damage, array[0.05, 0.25, 0.5, 0.75, 0.95]) as q
from e
where building_type is not null
group by 1, 2, 3
order by 1, 2, 3
"""
hits = q(SQL_HITS)
print(hits.shape)
hits.head()

# %%
SQL_KAPPA = """
-- Кейс 13. Nether Blast (ability 5186): урон по зданию / номинал на уровне (95/170/245/320); level в learn_events с нуля
with m as (
    select match_id from lakehouse.raw.pg__matches
    where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
),
nb as (
    select t.id, t.match_id, t.steam_account_id, t.time, t.damage,
        case when t.npcid between 16 and 18 or t.npcid between 26 and 28 then 'T1'
             when t.npcid between 19 and 21 or t.npcid between 29 and 31 then 'T2'
             when t.npcid between 22 and 24 or t.npcid between 32 and 34 then 'T3'
             when t.npcid in (25, 35) then 'T4'
             when t.npcid between 38 and 49 then 'rax'
             when t.npcid in (50, 51) then 'fort' else 'filler' end as building_type
    from lakehouse.raw.mongo__tower_damage_events t
    join m on m.match_id = t.match_id
    where t.byability = 5186
),
lv as (
    select nb.id, max(l.level) + 1 as lvl
    from nb
    join lakehouse.raw.mongo__ability_learn_events l
      on l.match_id = nb.match_id and l.steam_account_id = nb.steam_account_id
     and l.abilityid = 5186 and l.time <= nb.time
    where l.match_id % 20 = 0
    group by nb.id
)
select nb.building_type, lv.lvl, count(*) as hits,
    approx_percentile(nb.damage, 0.5) as dmg_median,
    element_at(array[95, 170, 245, 320], lv.lvl) as nominal,
    round(cast(approx_percentile(nb.damage, 0.5) as double) / element_at(array[95, 170, 245, 320], lv.lvl), 3) as kappa_median
from nb join lv on lv.id = nb.id
where lv.lvl between 1 and 4
group by 1, 2
order by 1, 2
"""
kappa = q(SQL_KAPPA)
print(kappa.shape)
kappa.head()

# %%
SQL_ABIL = """
-- Кейс 13. Вклад способностей и призывов в урон по зданиям (все герои), выборка 5%
-- source_key: ability:<id> для byability>0 (в т.ч. способность призванного юнита), npc:<name> для призывов/helm, item, hero_attack
with m as (
    select match_id from lakehouse.raw.pg__matches
    where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
),
e as (
    select t.match_id, t.attacker as hero_id, t.damage, t.npcid,
        case when t.byitem > 0 or n.name like 'npc_dota_necronomicon%' then 'item'
             when t.fromnpc = 0 and t.byability > 0 then 'ability'
             when t.fromnpc = 0 then 'hero_attack'
             when (n.name like 'npc_dota_neutral%' or n.name = 'npc_dota_dark_troll_warlord_skeleton_warrior')
                  then case when t.attacker in (66, 58) then 'own_summon' else 'helm_unit' end
             when n.name like 'npc_dota_creep%' or n.name like 'npc_dota_goodguys%' or n.name like 'npc_dota_badguys%'
                  or n.name is null or n.name like 'npc_dota_unit%' then 'other_npc'
             else 'own_summon' end as src,
        case when t.byability > 0 then 'ability:' || cast(t.byability as varchar)
             when t.byitem > 0 then 'item:' || cast(t.byitem as varchar)
             when t.fromnpc > 0 then 'npc:' || coalesce(regexp_replace(n.name, '_[0-9]+$', ''), cast(t.fromnpc as varchar))
             else 'hero_attack' end as source_key,
        t.byability, t.byitem
    from lakehouse.raw.mongo__tower_damage_events t
    join m on m.match_id = t.match_id
    left join lakehouse.raw.pg__npcs n on n.npc_id = t.fromnpc
    where t.npcid between 16 and 51
)
select e.hero_id, h.localized_name as hero, e.src, e.source_key, coalesce(max(a.display_name), max(it.localized_name)) as source_name,
    count(distinct e.match_id) as n_matches, count(*) as hits, sum(e.damage) as damage_sum,
    round(avg(e.damage), 2) as avg_hit,
    round(avg(case when e.npcid between 16 and 18 or e.npcid between 26 and 28 then e.damage end), 2) as avg_hit_t1,
    round(avg(case when e.npcid between 22 and 24 or e.npcid between 32 and 34 then e.damage end), 2) as avg_hit_t3,
    round(avg(case when e.npcid in (25, 35) then e.damage end), 2) as avg_hit_t4,
    round(avg(case when e.npcid between 38 and 49 then e.damage end), 2) as avg_hit_rax
from e
left join lakehouse.raw.pg__heroes h on h.hero_id = e.hero_id
left join lakehouse.raw.pg__abilities a on a.ability_id = e.byability and e.byability > 0
left join lakehouse.raw.pg__items it on it.item_id = e.byitem and e.byitem > 0
group by 1, 2, 3, 4
"""
abil = q(SQL_ABIL)
print(abil.shape)
abil.head()

# %%
SQL_EXPO = """
-- Кейс 13. Экспозиция зданий («время появления объекта»): окно [t0, t1] каждого здания в матче, выборка 5%
-- Маска tower_death_events (radiant/dire) = накопленные биты разрушенных зданий стороны:
-- b0-2 T1 top/mid/bot, b3-5 T2, b6-8 T3, b9 первая T4, b10-12 melee rax top/mid/bot, b13-15 range rax, b16 трон (= конец матча).
-- t0: T1=0; T2/T3 = падение предыдущего тира линии; бараки = падение T3 линии; T4 = падение любой T3; трон = падение второй T4.
-- Падение второй T4 в маске нет: оценка greatest(b9, последний удар по T4), если трон атаковали.
WITH mt AS (SELECT match_id, duration, radiant_win FROM lakehouse.raw.pg__matches WHERE game_mode <> 23 AND duration >= 900 AND match_id % 20 = 0),
ev AS (
  SELECT d.match_id, d.time, s.side, s.mask,
         lag(s.mask, 1, 0) OVER (PARTITION BY d.match_id, s.side ORDER BY d.time, s.mask) prev
  FROM lakehouse.raw.mongo__tower_death_events d JOIN mt ON mt.match_id = d.match_id
  CROSS JOIN UNNEST(ARRAY['radiant','dire'], ARRAY[d.radiant, d.dire]) AS s(side, mask)
),
dt AS (SELECT match_id, side, CAST(b AS integer) b, min(time) t_death FROM ev CROSS JOIN UNNEST(sequence(0,16)) AS t(b)
       WHERE bitwise_and(mask, bitwise_left_shift(1, CAST(b AS integer))) <> 0
         AND bitwise_and(prev, bitwise_left_shift(1, CAST(b AS integer))) = 0
       GROUP BY 1,2,3),
hx AS (SELECT t.match_id, CASE WHEN t.npcid IN (25,50) THEN 'radiant' ELSE 'dire' END side,
              max(CASE WHEN t.npcid IN (25,35) THEN t.time END) last_t4_hit, min(CASE WHEN t.npcid IN (50,51) THEN t.time END) first_fort_hit
       FROM lakehouse.raw.mongo__tower_damage_events t JOIN mt ON mt.match_id = t.match_id
       WHERE t.match_id % 20 = 0 AND t.npcid IN (25,35,50,51) GROUP BY 1,2),
dp0 AS (SELECT mt.match_id, mt.duration, mt.radiant_win, s.side,
  max(CASE WHEN b=0 THEN t_death END) d0,
  max(CASE WHEN b=1 THEN t_death END) d1,
  max(CASE WHEN b=2 THEN t_death END) d2,
  max(CASE WHEN b=3 THEN t_death END) d3,
  max(CASE WHEN b=4 THEN t_death END) d4,
  max(CASE WHEN b=5 THEN t_death END) d5,
  max(CASE WHEN b=6 THEN t_death END) d6,
  max(CASE WHEN b=7 THEN t_death END) d7,
  max(CASE WHEN b=8 THEN t_death END) d8,
  max(CASE WHEN b=9 THEN t_death END) d9,
  max(CASE WHEN b=10 THEN t_death END) d10,
  max(CASE WHEN b=11 THEN t_death END) d11,
  max(CASE WHEN b=12 THEN t_death END) d12,
  max(CASE WHEN b=13 THEN t_death END) d13,
  max(CASE WHEN b=14 THEN t_death END) d14,
  max(CASE WHEN b=15 THEN t_death END) d15,
  max(CASE WHEN b=16 THEN t_death END) d16
  FROM mt CROSS JOIN UNNEST(ARRAY['radiant','dire']) AS s(side)
  LEFT JOIN dt ON dt.match_id = mt.match_id AND dt.side = s.side
  GROUP BY 1,2,3,4),
dp AS (SELECT dp0.*, CASE WHEN dp0.d9 IS NOT NULL AND hx.first_fort_hit IS NOT NULL
                          THEN least(greatest(dp0.d9, coalesce(hx.last_t4_hit, dp0.d9)), dp0.duration) END t4b
       FROM dp0 LEFT JOIN hx ON hx.match_id = dp0.match_id AND hx.side = dp0.side),
win AS (SELECT dp.match_id, dp.duration, dp.radiant_win, dp.side, w.slot, w.k, w.lane, w.t0, w.t1, w.t_dead,
               CASE WHEN w.t0 IS NULL THEN 0 ELSE greatest(w.t1 - w.t0, 0) END expo
  FROM dp CROSS JOIN UNNEST(ARRAY[0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,9,17,16], ARRAY['T1','T1','T1','T2','T2','T2','T3','T3','T3','rax_melee','rax_melee','rax_melee','rax_range','rax_range','rax_range','T4','T4','fort'], ARRAY[0,1,2,0,1,2,0,1,2,0,1,2,0,1,2,CAST(NULL AS integer),CAST(NULL AS integer),CAST(NULL AS integer)],
        ARRAY[CAST(0 AS integer),CAST(0 AS integer),CAST(0 AS integer),CAST(d0 AS integer),CAST(d1 AS integer),CAST(d2 AS integer),CAST(d3 AS integer),CAST(d4 AS integer),CAST(d5 AS integer),CAST(d6 AS integer),CAST(d7 AS integer),CAST(d8 AS integer),CAST(d6 AS integer),CAST(d7 AS integer),CAST(d8 AS integer),CAST(nullif(least(coalesce(d6,99999),coalesce(d7,99999),coalesce(d8,99999)),99999) AS integer),CAST(nullif(least(coalesce(d6,99999),coalesce(d7,99999),coalesce(d8,99999)),99999) AS integer),CAST(t4b AS integer)], ARRAY[CAST(coalesce(d0, duration) AS integer),CAST(coalesce(d1, duration) AS integer),CAST(coalesce(d2, duration) AS integer),CAST(coalesce(d3, duration) AS integer),CAST(coalesce(d4, duration) AS integer),CAST(coalesce(d5, duration) AS integer),CAST(coalesce(d6, duration) AS integer),CAST(coalesce(d7, duration) AS integer),CAST(coalesce(d8, duration) AS integer),CAST(coalesce(d10, duration) AS integer),CAST(coalesce(d11, duration) AS integer),CAST(coalesce(d12, duration) AS integer),CAST(coalesce(d13, duration) AS integer),CAST(coalesce(d14, duration) AS integer),CAST(coalesce(d15, duration) AS integer),CAST(coalesce(d9, duration) AS integer),CAST(coalesce(t4b, duration) AS integer),CAST(coalesce(d16, duration) AS integer)], ARRAY[CAST(d0 AS integer),CAST(d1 AS integer),CAST(d2 AS integer),CAST(d3 AS integer),CAST(d4 AS integer),CAST(d5 AS integer),CAST(d6 AS integer),CAST(d7 AS integer),CAST(d8 AS integer),CAST(d10 AS integer),CAST(d11 AS integer),CAST(d12 AS integer),CAST(d13 AS integer),CAST(d14 AS integer),CAST(d15 AS integer),CAST(d9 AS integer),CAST(t4b AS integer),CAST(d16 AS integer)])
        AS w(slot, k, lane, t0, t1, t_dead))
SELECT match_id, side, k AS building_type, count(*) AS buildings, count(t_dead) AS destroyed, sum(expo) AS exposure_sec, min(t0) AS first_t0, min(t_dead) AS first_death
FROM win GROUP BY 1, 2, 3
"""
expo = q(SQL_EXPO)
print(expo.shape)
expo.head()

# %%
SQL_BDMG = """
-- Кейс 13. Урон героя по зданиям противника: матч x герой x тип здания, выборка 5%
-- attacker в tower_damage_events = hero_id (в каталоге ошибочно описан как steamAccountId); сторона здания по npc_id
with m as (
    select match_id from lakehouse.raw.pg__matches
    where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
)
select t.match_id, t.attacker as hero_id,
    case when t.npcid between 16 and 25 or t.npcid between 38 and 43 or t.npcid = 50 then 'radiant' else 'dire' end as building_side,
    case when t.npcid between 16 and 18 or t.npcid between 26 and 28 then 'T1'
         when t.npcid between 19 and 21 or t.npcid between 29 and 31 then 'T2'
         when t.npcid between 22 and 24 or t.npcid between 32 and 34 then 'T3'
         when t.npcid in (25, 35) then 'T4'
         when t.npcid in (38, 39, 40, 44, 45, 46) then 'rax_melee'
         when t.npcid in (41, 42, 43, 47, 48, 49) then 'rax_range'
         else 'fort' end as building_type,
    count(*) as hits, sum(t.damage) as damage_sum,
    sum(case when t.time < 600 then t.damage else 0 end) as damage_lt10,
    sum(case when t.time < 900 then t.damage else 0 end) as damage_lt15
from lakehouse.raw.mongo__tower_damage_events t
join m on m.match_id = t.match_id
where t.npcid between 16 and 51 and t.npcid not in (36, 37)
group by 1, 2, 3, 4
"""
bdmg = q(SQL_BDMG)
print(bdmg.shape)
bdmg.head()

# %%
SQL_EARLY = """
-- Кейс 13. Ранний пуш команды (H3): урон по зданиям врага до 10/15 мин, разрушенные башни врага к 15 мин,
-- золото команды к 15 мин (gold_events, только матчи без анонимных игроков), победа. Выборка 5%
with m as (
    select match_id, radiant_win from lakehouse.raw.pg__matches
    where game_mode <> 23 and duration >= 900 and match_id % 20 = 0
),
mp as (
    select p.match_id, p.account_id, p.hero_id, p.is_radiant
    from lakehouse.raw.pg__match_players p join m on m.match_id = p.match_id
),
anon as (select match_id, count_if(account_id = 4294967295) n_anon from mp group by 1),
dmg as (
    select t.match_id,
        case when t.npcid between 16 and 25 or t.npcid between 38 and 43 or t.npcid = 50 then 'dire' else 'radiant' end as side,
        sum(case when t.time < 600 then t.damage else 0 end) as bdmg_10,
        sum(case when t.time < 900 then t.damage else 0 end) as bdmg_15
    from lakehouse.raw.mongo__tower_damage_events t join m on m.match_id = t.match_id
    where t.match_id % 20 = 0 and t.npcid between 16 and 51 and t.npcid not in (36, 37) and t.time < 900
    group by 1, 2
),
td as (  -- маска разрушенных зданий стороны к 15 мин; башни T1-T3 = биты 0..8
    select d.match_id, s.side_lost, max(s.mask) as mask15
    from lakehouse.raw.mongo__tower_death_events d join m on m.match_id = d.match_id
    cross join unnest(array['radiant', 'dire'], array[d.radiant, d.dire]) as s(side_lost, mask)
    where d.match_id % 20 = 0 and d.time <= 900
    group by 1, 2
),
gold as (
    select g.match_id, case when p.is_radiant then 'radiant' else 'dire' end as side, sum(g.amount) as gold_15
    from lakehouse.raw.mongo__gold_events g
    join mp p on p.match_id = g.match_id and p.account_id = g.steam_account_id
    join anon a on a.match_id = g.match_id and a.n_anon = 0
    where g.match_id % 20 = 0 and g.time <= 900 and g.amount > 0
    group by 1, 2
)
select m.match_id, s.side,
    case when (s.side = 'radiant') = m.radiant_win then 1 else 0 end as win,
    coalesce(dm.bdmg_10, 0) as bdmg_10, coalesce(dm.bdmg_15, 0) as bdmg_15,
    coalesce(bit_count(bitwise_and(te.mask15, 511), 32), 0) as enemy_towers_lost_15,
    coalesce(bit_count(bitwise_and(tw.mask15, 511), 32), 0) as own_towers_lost_15,
    g.gold_15, a.n_anon
from m
cross join unnest(array['radiant', 'dire']) as s(side)
join anon a on a.match_id = m.match_id
left join dmg dm on dm.match_id = m.match_id and dm.side = s.side
left join td te on te.match_id = m.match_id and te.side_lost <> s.side
left join td tw on tw.match_id = m.match_id and tw.side_lost = s.side
left join gold g on g.match_id = m.match_id and g.side = s.side
"""
early = q(SQL_EARLY)
print(early.shape)
early.head()

# %%
# признаки игрока: база позиции, индекс героя, верхний квартиль пушеров, состав команды, σ
P = players.copy()
P["is_radiant"] = P["is_radiant"].astype(bool)
base = P.groupby("pos")["tdpm"].mean().rename("base_pos")
P = P.join(base, on="pos")
hero_n = P.groupby("hero")["tdpm"].size()
hero_idx = (P.groupby("hero")["tdpm"].sum() / P.groupby("hero")["base_pos"].sum()).rename("idx")
MIN_GAMES = 50
eligible = hero_n[hero_n >= MIN_GAMES].index
topq = set(hero_idx[eligible][hero_idx[eligible] >= hero_idx[eligible].quantile(0.75)].index)
P["is_topq"] = P["hero"].isin(topq).astype(int)
team = P.groupby(["match_id", "is_radiant"]).agg(topq_cnt=("is_topq", "sum"), helm_cnt=("helm_self", "sum"),
                                                team_rank=("rank", "mean"), team_nw=("net_worth", "sum"),
                                                team_td=("tower_damage", "sum")).reset_index()
P = P.merge(team, on=["match_id", "is_radiant"])
enemy = team.rename(columns={c: "enemy_" + c for c in ["topq_cnt", "helm_cnt", "team_rank", "team_nw", "team_td"]})
enemy["is_radiant"] = ~enemy["is_radiant"]
P = P.merge(enemy, on=["match_id", "is_radiant"])
P["n_topq_mates"] = P["topq_cnt"] - P["is_topq"]
P["helm_mates"] = ((P["helm_cnt"] - P["helm_self"]) > 0).astype(int)
P["nw_diff_k"] = (P["team_nw"] - P["enemy_team_nw"]) / 1000
P["sigma"] = P["tower_damage"] / P["team_td"].replace(0, np.nan)
P["mates_bucket"] = pd.cut(P["n_topq_mates"], [-1, 0, 1, 10], labels=["0", "1", "2+"])
print(P.shape, "матчей:", P.match_id.nunique(), "| героев с >=", MIN_GAMES, "играми:", len(eligible))
print("верхний квартиль пушеров:", ", ".join(sorted(topq)))

# %%
# урон на секунду доступности здания противника (экспозиция)
E = expo.copy()
D = bdmg.copy()
pl = P[["match_id", "hero_id", "hero", "pos", "is_radiant"]].copy()
pl["enemy_side"] = np.where(pl["is_radiant"], "dire", "radiant")
pe = pl.merge(E.rename(columns={"side": "enemy_side"}), on=["match_id", "enemy_side"], how="inner")
pe = pe.merge(D.rename(columns={"building_side": "enemy_side"})[["match_id", "hero_id", "enemy_side", "building_type", "damage_sum"]],
              on=["match_id", "hero_id", "enemy_side", "building_type"], how="left").fillna({"damage_sum": 0})
pe = pe[pe["exposure_sec"] > 0]
pos_rate = pe.groupby(["pos", "building_type"]).apply(lambda g: g.damage_sum.sum() / g.exposure_sec.sum()).rename("pos_rate")
pe = pe.join(pos_rate, on=["pos", "building_type"])
expo_hero = pe.groupby("hero").apply(lambda g: pd.Series({
    "n": g.match_id.nunique(),
    "dmg_per_expo_sec": g.damage_sum.sum() / g.exposure_sec.sum(),
    "idx_expo": g.damage_sum.sum() / (g.exposure_sec * g.pos_rate).sum(),
}))
expo_hero = expo_hero[expo_hero.n >= MIN_GAMES].sort_values("idx_expo", ascending=False)
expo_hero.head(10)

# %% ## 10. EDA
# 10.1 Урон за удар по типам зданий (атака героя, окно 30–40 мин) — boxplot по квантилям из Trino
order = ["T1", "T2", "T3", "T4", "rax_melee", "rax_range", "fort", "filler"]
h = hits[(hits.src == "hero_attack") & (hits.time_window == "30-40")].set_index("building_type").reindex(order).dropna(subset=["hits"])
stats = [{"label": k, "whislo": r.q[0], "q1": r.q[1], "med": r.q[2], "q3": r.q[3], "whishi": r.q[4], "fliers": []} for k, r in h.iterrows()]
fig, ax = plt.subplots()
ax.bxp(stats, showfliers=False)
ax.plot(range(1, len(h) + 1), h.avg_hit, "o", label="среднее")
ax.set_title("Урон атаки героя за удар по типу здания, 30–40 мин (усы = p5–p95)")
ax.set_ylabel("damage за событие"); ax.legend(); plt.show()
# Вывод: тип здания (броня) меняет урон за удар — основа H2.

# %%
# 10.2 TDPM по позиции-прокси
fig, ax = plt.subplots()
data = [P.loc[P.pos == i, "tdpm"].clip(upper=P.tdpm.quantile(0.99)) for i in range(1, 6)]
ax.boxplot(data, showfliers=False)
ax.set_xticks(range(1, 6)); ax.set_xticklabels([f"pos {i}" for i in range(1, 6)])
ax.set_title("Урон по зданиям в минуту (TDPM) по позиции-прокси (ранг по net worth)")
ax.set_ylabel("TDPM"); plt.show()
P.groupby("pos")["tdpm"].describe()[["count", "mean", "50%", "75%"]]
# Вывод: TDPM резко падает от pos 1 к pos 5 — героев нельзя сравнивать без поправки на позицию.

# %%
# 10.3 Экспозиция: когда здания становятся доступны и падают
ex = E.groupby("building_type").agg(avg_exposure_min=("exposure_sec", lambda s: s.mean() / 60),
                                    median_first_death_min=("first_death", lambda s: s.median() / 60),
                                    destroyed_share=("destroyed", "sum"), buildings=("buildings", "sum"))
ex["destroyed_share"] = ex["destroyed_share"] / ex["buildings"]
ex = ex.reindex(["T1", "T2", "T3", "rax_melee", "rax_range", "T4", "fort"])
fig, ax = plt.subplots(1, 2, figsize=(12, 4))
ex["median_first_death_min"].plot.bar(ax=ax[0], title="Медианное время первого падения, мин")
ex["avg_exposure_min"].plot.bar(ax=ax[1], title="Средняя суммарная экспозиция на сторону, мин")
plt.tight_layout(); plt.show()
ex.round(2)
# Вывод: T1 доступны весь матч, T3/бараки/T4 — только последние 5–15 минут; урон по поздним зданиям делим на время доступности.

# %%
# 10.4 Структура урона по источникам: топ-20 героев по индексу
src_share = abil.groupby(["hero", "src"])["damage_sum"].sum().unstack(fill_value=0)
src_share = src_share.div(src_share.sum(axis=1), axis=0)
top20 = hero_idx[eligible].sort_values(ascending=False).head(20).index
cols = [c for c in ["hero_attack", "ability", "own_summon", "helm_unit", "item", "other_npc"] if c in src_share]
src_share.loc[top20, cols].plot.barh(stacked=True, figsize=(10, 7), title="Доля урона по зданиям по источникам (топ-20 по индексу)")
plt.gca().invert_yaxis(); plt.xlabel("доля"); plt.show()
# Вывод: у лидеров по индексу урон дают призывы (Shadow Shaman, Visage, Broodmother) или способности (Pugna, Luna).

# %%
# 10.5 Лестница урона: средний урон за удар относительно T3 (окно 30–40)
lad = hits[hits.time_window == "30-40"].pivot_table(index="src", columns="building_type", values="avg_hit")
rel = lad[["T1", "T2", "T3", "T4", "rax_melee", "rax_range", "fort"]].div(lad["T3"], axis=0)
ax = rel.loc[[s for s in ["hero_attack", "own_summon", "helm_unit", "ability", "item"] if s in rel.index]].T.plot(marker="o")
ax.axhline(1, color="grey", lw=0.8); ax.set_title("Урон за удар / урон по T3, окно 30–40 мин"); ax.set_ylabel("отношение"); plt.show()
rel.round(2)
# Вывод: у физических источников одинаковая лестница: T4 ≈ 0,8 от T3, range-бараки ≈ 1,3.

# %%
# 10.6 κ Nether Blast по уровню способности
k = kappa.groupby("lvl").apply(lambda g: pd.Series({"hits": g.hits.sum(), "kappa_median_w": np.average(g.kappa_median, weights=g.hits)}))
ax = k["kappa_median_w"].plot(marker="o", title="Nether Blast: медиана урона по зданию / номинал")
ax.axhline(0.65, ls="--", color="red", label="0,65 (множитель по зданиям)"); ax.legend(); ax.set_xlabel("уровень"); plt.show()
kappa.pivot_table(index="building_type", columns="lvl", values="kappa_median")
# Вывод: на уровнях 1–3 урон ровно 0,653 номинала — подтверждение множителя 0,65 по зданиям.

# %%
# 10.7 Доля в уроне команды σ в зависимости от числа сильных пушеров-союзников
sg = P[P.hero.isin(eligible)].groupby("mates_bucket")["sigma"].mean()
ex_heroes = ["Pugna", "Shadow Shaman", "Lycan", "Beastmaster", "Luna", "Juggernaut"]
tab = P[P.hero.isin(ex_heroes)].pivot_table(index="hero", columns="mates_bucket", values="sigma", aggfunc="mean")
tab.plot.bar(title="σ — доля героя в уроне команды по зданиям, по числу сильных пушеров-союзников")
plt.axhline(0.2, color="grey", ls="--"); plt.ylabel("σ"); plt.show()
print("все герои:", sg.round(3).to_dict()); tab.round(3)
# Вывод: с ростом числа сильных пушеров в команде σ падает у всех — союзники делят урон.

# %%
# 10.8 Ранний пуш и победа
T = early.copy()
wr = T.groupby(T.enemy_towers_lost_15.clip(upper=4))["win"].agg(["mean", "size"])
fig, ax = plt.subplots(1, 2, figsize=(12, 4))
wr["mean"].plot.bar(ax=ax[0], title="Доля побед по числу снесённых башен врага к 15 мин (4 = 4+)")
T["bdmg15_dec"] = pd.qcut(T.bdmg_15.rank(method="first"), 10, labels=False) + 1
T.groupby("bdmg15_dec")["win"].mean().plot(marker="o", ax=ax[1], title="Доля побед по децилю урона по зданиям к 15 мин")
plt.tight_layout(); plt.show()
wr
# Вывод: доля побед монотонно растёт с числом ранних башен и с ранним уроном; без контроля золота это может быть следствием общего преимущества — проверяем в H3.

# %% ## 11. Проверка гипотез
# ### H1. Часть героев дает непропорционально большой урон по зданиям
from scipy import stats as st

P["r"] = P["tdpm"] / P["base_pos"]
rng = np.random.default_rng(13)
rows = []
for hero in eligible:
    x = P.loc[P.hero == hero]
    y = P.loc[(P.hero != hero) & P.pos.isin(x.pos.unique())]
    u = st.mannwhitneyu(x.r, y.r, alternative="two-sided")
    t, b = x.tdpm.to_numpy(), x.base_pos.to_numpy()
    boots = [t[i].sum() / b[i].sum() for i in (rng.integers(0, len(t), len(t)) for _ in range(300))]
    rows.append([hero, len(x), x.tdpm.median(), t.sum() / b.sum(), *np.percentile(boots, [2.5, 97.5]),
                 x.sigma.mean(), u.pvalue])
h1 = pd.DataFrame(rows, columns=["hero", "n", "tdpm_median", "idx", "idx_lo", "idx_hi", "sigma", "p_mw"]).set_index("hero")
h1 = h1.join(expo_hero[["dmg_per_expo_sec", "idx_expo"]])
# поправка Бенджамини–Хохберга
p = h1.p_mw.to_numpy(); order_ = np.argsort(p); m_ = len(p)
q_ = np.empty(m_); q_[order_] = np.minimum.accumulate((p[order_] * m_ / np.arange(1, m_ + 1))[::-1])[::-1]
h1["q_bh"] = np.minimum(q_, 1)
h1 = h1.sort_values("idx", ascending=False)
print("значимо выше базы (q<0.05, idx_lo>1):", int(((h1.q_bh < 0.05) & (h1.idx_lo > 1)).sum()), "из", len(h1))
display(h1.head(20).round(3)); display(h1.tail(10).round(3))

# %%
fig, ax = plt.subplots(figsize=(10, 8))
sel = pd.concat([h1.head(20), h1.tail(10)])
ax.errorbar(sel.idx, range(len(sel)), xerr=[sel.idx - sel.idx_lo, sel.idx_hi - sel.idx], fmt="o")
ax.set_yticks(range(len(sel))); ax.set_yticklabels(sel.index); ax.invert_yaxis()
ax.axvline(1, color="grey", ls="--"); ax.set_title("Индекс TDPM к позиции: топ-20 и низ-10, 95% bootstrap-ДИ"); plt.show()

# %%
# регрессия log(1+TDPM): герой (Sum-кодирование, эффекты к среднему герою) с контролями и без
import statsmodels.formula.api as smf
R = P.copy()
R["hero_g"] = np.where(R.hero.isin(eligible), R.hero, "other")
R["team_rank_f"] = R["team_rank"].fillna(R["team_rank"].median())
R["log_dur"] = np.log(R["duration"])
R["y"] = np.log1p(R["tdpm"])
R["is_radiant_i"] = R["is_radiant"].astype(int)
f_full = "y ~ C(hero_g, Sum) + C(pos) + n_topq_mates + helm_mates + helm_self + team_rank_f + is_radiant_i + win + log_dur + nw_diff_k"
f_raw = "y ~ C(hero_g, Sum)"
cl = {"groups": R["match_id"]}
m_full = smf.ols(f_full, R).fit(cov_type="cluster", cov_kwds=cl)
m_raw = smf.ols(f_raw, R).fit(cov_type="cluster", cov_kwds=cl)

def hero_effects(m):
    names = [n for n in m.params.index if n.startswith("C(hero_g, Sum)")]
    levels = sorted(R.hero_g.unique())
    coef = {n.split("[S.")[1].rstrip("]"): m.params[n] for n in names}
    se = {n.split("[S.")[1].rstrip("]"): m.bse[n] for n in names}
    last = [l for l in levels if l not in coef][0]
    coef[last] = -sum(coef.values()); se[last] = np.nan
    return pd.DataFrame({"coef": coef, "se": se})

eff = hero_effects(m_full).join(hero_effects(m_raw), rsuffix="_raw")
eff["lo"], eff["hi"] = eff.coef - 1.96 * eff.se, eff.coef + 1.96 * eff.se
eff["x_vs_avg"] = np.exp(eff.coef)
eff = eff.drop(index="other", errors="ignore").sort_values("coef", ascending=False)
print("R² с контролями:", round(m_full.rsquared, 3), "| без контролей:", round(m_raw.rsquared, 3), "| n =", int(m_full.nobs))
ctrl = m_full.params.filter(regex="^(n_topq_mates|helm_mates|helm_self|team_rank_f|is_radiant_i|win|log_dur|nw_diff_k|C\\(pos)")
display(pd.DataFrame({"coef": ctrl, "se": m_full.bse[ctrl.index]}).round(4))
display(eff.head(20).round(3)); display(eff.tail(10).round(3))

# %%
sel = pd.concat([eff.head(15), eff.tail(5)])
fig, ax = plt.subplots(figsize=(9, 7))
ax.errorbar(sel.coef, range(len(sel)), xerr=1.96 * sel.se, fmt="o", label="с контролями")
ax.plot(sel.coef_raw, range(len(sel)), "x", label="без контролей")
ax.set_yticks(range(len(sel))); ax.set_yticklabels(sel.index); ax.invert_yaxis(); ax.axvline(0, color="grey")
ax.set_title("Эффект героя на log(1+TDPM) относительно среднего героя"); ax.legend(); plt.show()

# ### H2. Эффект способности зависит от стадии и типа здания
# %%
import statsmodels.api as sm
H = hits[(hits.building_type != "filler") & hits.src.isin(["hero_attack", "own_summon", "ability", "helm_unit"]) & (hits.hits >= 30)].copy()
H["log_hit"] = np.log(H["avg_hit"])
m_add = smf.wls("log_hit ~ C(src) + C(building_type) + C(time_window)", H, weights=H["hits"]).fit()
m_int = smf.wls("log_hit ~ C(src) * C(building_type) + C(src) * C(time_window)", H, weights=H["hits"]).fit()
f_test = m_int.compare_f_test(m_add)
print("F-тест взаимодействий источник×тип и источник×окно: F=%.1f, p=%.2g, клеток=%d" % (f_test[0], f_test[1], len(H)))
# магия против физики: Nether Blast против атаки героя, отношение T4/T3 и fort/T3 в одном окне
nb = abil[abil.source_key == "ability:5186"][["avg_hit_t1", "avg_hit_t3", "avg_hit_t4", "avg_hit_rax"]].sum()
lad30 = hits[hits.time_window == "30-40"].pivot_table(index="src", columns="building_type", values="avg_hit")
print("атака героя T4/T3 =", round(lad30.loc["hero_attack", "T4"] / lad30.loc["hero_attack", "T3"], 3))
print("Nether Blast T4/T3 (все окна) =", round(nb.avg_hit_t4 / nb.avg_hit_t3, 3))

# %%
# способности: топ-30 по урону по зданиям (все герои), Helm-юниты и предметы отдельно
A = abil[abil.src.isin(["ability", "own_summon"])].copy()
hero_tot = abil.groupby("hero")["damage_sum"].sum()
A["share_of_hero"] = A.damage_sum / A.hero.map(hero_tot)
A["damage_per_match"] = A.damage_sum / A.n_matches
top30 = A.sort_values("damage_sum", ascending=False).head(30)
display(top30[["hero", "src", "source_key", "source_name", "n_matches", "hits", "damage_sum", "damage_per_match", "avg_hit",
               "avg_hit_t1", "avg_hit_t3", "avg_hit_t4", "avg_hit_rax", "share_of_hero"]].round(3))
other = abil[abil.src.isin(["helm_unit", "item"])].groupby("src")[["damage_sum", "hits"]].sum()
print("Helm-юниты и предметы:"); display(other)

# ### H3. Ранний урон по зданиям связан с победой при контроле золота и состава
# %%
# одна строка на матч с точки зрения Radiant: разности Radiant − Dire
tm = P.groupby(["match_id", "is_radiant"]).agg(topq=("is_topq", "sum"), helm=("helm_self", "sum")).reset_index()
tm["side"] = np.where(tm.is_radiant, "radiant", "dire")
T2 = T.merge(tm[["match_id", "side", "topq", "helm"]], on=["match_id", "side"])
r_ = T2[T2.side == "radiant"].set_index("match_id"); d_ = T2[T2.side == "dire"].set_index("match_id")
M = pd.DataFrame({
    "win": r_.win,
    "d_towers15": r_.enemy_towers_lost_15 - d_.enemy_towers_lost_15,
    "d_bdmg15_k": (r_.bdmg_15 - d_.bdmg_15) / 1000,
    "d_gold15_k": (r_.gold_15 - d_.gold_15) / 1000,
    "d_topq": r_.topq - d_.topq,
    "d_helm": r_.helm - d_.helm,
}).dropna(subset=["win", "d_towers15", "d_bdmg15_k"])

def auc(y, s):
    rk = st.rankdata(s); n1 = y.sum(); n0 = len(y) - n1
    return (rk[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)

res = {}
for name, f in {"башни+урон": "win ~ d_towers15 + d_bdmg15_k",
                "башни+урон+состав": "win ~ d_towers15 + d_bdmg15_k + d_topq + d_helm",
                "золото": "win ~ d_gold15_k",
                "башни+урон+золото+состав": "win ~ d_towers15 + d_bdmg15_k + d_gold15_k + d_topq + d_helm"}.items():
    data = M.dropna() if "gold" in f else M
    m = smf.logit(f, data).fit(disp=0)
    res[name] = m
    print(f"{name:28s} n={int(m.nobs):5d}  AUC={auc(data.win.to_numpy(), m.predict(data).to_numpy()):.3f}  pseudoR2={m.prsquared:.3f}")
full = res["башни+урон+золото+состав"]
pd.DataFrame({"coef": full.params, "se": full.bse, "p": full.pvalues, "OR": np.exp(full.params)}).round(4)

# %% ## 12. Итоговые выводы и ограничения
v1 = int(((h1.q_bh < 0.05) & (h1.idx_lo > 1)).sum())
summary = pd.DataFrame([
    ["H1", "индекс TDPM к позиции, σ, OLS с контролями", f"{v1} героев значимо выше базы; лидеры: " + ", ".join(h1.head(5).index),
     "подтверждена" if v1 > 0 else "не подтверждена"],
    ["H2", "WLS урона за удар, F-тест взаимодействий", f"F={f_test[0]:.1f}, p={f_test[1]:.1g}; атака T4/T3={lad30.loc['hero_attack','T4']/lad30.loc['hero_attack','T3']:.2f}, Nether Blast T4/T3={nb.avg_hit_t4/nb.avg_hit_t3:.2f}",
     "подтверждена" if f_test[1] < 0.05 else "не подтверждена"],
    ["H3", "логит win ~ ранний пуш + золото + состав", "Δбашни15: OR={:.2f} (p={:.1g}); Δурон15: OR={:.2f} (p={:.1g})".format(
        np.exp(full.params["d_towers15"]), full.pvalues["d_towers15"], np.exp(full.params["d_bdmg15_k"]), full.pvalues["d_bdmg15_k"]),
     "подтверждена" if min(full.pvalues["d_towers15"], full.pvalues["d_bdmg15_k"]) < 0.05 else "не подтверждена"],
], columns=["гипотеза", "метод", "результат", "вердикт"])
summary
