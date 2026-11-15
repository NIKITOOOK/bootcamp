-- STAGE t1_c13_tower_hits: урон героев по зданиям противника, агрегат матч x герой x здание x источник x уровень x окно
-- attacker = hero_id; пустые byability/byitem/fromnpc хранятся как 0. Источник: item (byitem > 0 или Necronomicon),
-- ability (byability > 0 без юнита), hero_attack, own_summon (призыв героя; нейтралы у Chen 66 и Enchantress 58),
-- helm_unit (нейтрал под Helm), other_npc. Уровень способности: последний изученный уровень на момент удара (с 1)
-- check: rows > 0
-- check: unique(match_id, hero_id, building_npc_id, source_type, source_id, ability_level, time_window)
-- check: not_null(match_id, hero_id, building_npc_id, building_type, source_type, time_window, hits, damage)
-- check: accepted(source_type, hero_attack, ability, item, own_summon, helm_unit, other_npc)
-- check: accepted(time_window, 00-10, 10-15, 15-30, 30+)
with pl as (
    select match_id, hero_id, is_radiant from lakehouse.stage.t1_c13_players
),
e as (
    select t.match_id, t.attacker as hero_id, pl.is_radiant, t.steam_account_id, t.npcid as building_npc_id,
           t.time, t.damage, t.byability, t.byitem, t.fromnpc, n.name as npc_name
    from lakehouse.raw.mongo__tower_damage_events t
    join pl on pl.match_id = t.match_id and pl.hero_id = t.attacker
    left join lakehouse.raw.pg__npcs n on n.npc_id = t.fromnpc and t.fromnpc > 0
    where (t.npcid between 16 and 35 or t.npcid between 38 and 51)
      and (t.npcid between 16 and 25 or t.npcid between 38 and 43 or t.npcid = 50) <> pl.is_radiant
),
lv as (
    select le.match_id, le.steam_account_id, le.abilityid, le.level + 1 as ability_level, le.time as t_from,
           lead(le.time, 1, 1000000) over (partition by le.match_id, le.steam_account_id, le.abilityid order by le.time, le.level) as t_to
    from lakehouse.raw.mongo__ability_learn_events le
    join (select distinct match_id from pl) m on m.match_id = le.match_id
),
c as (
    select e.match_id, e.hero_id, e.is_radiant, e.building_npc_id, e.time, e.damage,
           case when e.building_npc_id between 16 and 18 or e.building_npc_id between 26 and 28 then 'T1'
                when e.building_npc_id between 19 and 21 or e.building_npc_id between 29 and 31 then 'T2'
                when e.building_npc_id between 22 and 24 or e.building_npc_id between 32 and 34 then 'T3'
                when e.building_npc_id in (25, 35) then 'T4'
                when e.building_npc_id in (38, 39, 40, 44, 45, 46) then 'rax_melee'
                when e.building_npc_id in (41, 42, 43, 47, 48, 49) then 'rax_range'
                else 'fort' end as building_type,
           case when e.building_npc_id in (16, 19, 22, 26, 29, 32, 38, 41, 44, 47) then 'top'
                when e.building_npc_id in (17, 20, 23, 27, 30, 33, 39, 42, 45, 48) then 'mid'
                when e.building_npc_id in (18, 21, 24, 28, 31, 34, 40, 43, 46, 49) then 'bot'
                else 'base' end as lane,
           case when e.byitem > 0 or e.npc_name like 'npc_dota_necronomicon%' then 'item'
                when e.fromnpc = 0 and e.byability > 0 then 'ability'
                when e.fromnpc = 0 then 'hero_attack'
                when e.npc_name like 'npc_dota_neutral%' or e.npc_name = 'npc_dota_dark_troll_warlord_skeleton_warrior'
                     then case when e.hero_id in (66, 58) then 'own_summon' else 'helm_unit' end
                when e.npc_name is null or e.npc_name like 'npc_dota_creep%' or e.npc_name like 'npc_dota_goodguys%'
                     or e.npc_name like 'npc_dota_badguys%' or e.npc_name like 'npc_dota_unit%' then 'other_npc'
                else 'own_summon' end as source_type,
           case when e.byability > 0 then 'ability:' || cast(e.byability as varchar(20))
                when e.byitem > 0 then 'item:' || cast(e.byitem as varchar(20))
                when e.fromnpc > 0 then 'npc:' || coalesce(regexp_replace(e.npc_name, '_[0-9]+$', ''), cast(e.fromnpc as varchar(20)))
                else 'hero_attack' end as source_id,
           coalesce(a.display_name, it.localized_name, regexp_replace(e.npc_name, '_[0-9]+$', ''), 'hero attack') as source_name,
           case when e.byability > 0 then coalesce(lv.ability_level, 0) else 0 end as ability_level,
           case when e.time < 600 then '00-10' when e.time < 900 then '10-15' when e.time < 1800 then '15-30' else '30+' end as time_window
    from e
    left join lv on e.byability > 0 and lv.match_id = e.match_id and lv.steam_account_id = e.steam_account_id
                and lv.abilityid = e.byability and e.time >= lv.t_from and e.time < lv.t_to
    left join lakehouse.raw.pg__abilities a on a.ability_id = e.byability and e.byability > 0
    left join lakehouse.raw.pg__items it on it.item_id = e.byitem and e.byitem > 0
)
select match_id, hero_id, is_radiant, building_npc_id, building_type, lane, source_type, source_id,
       max(source_name) as source_name, ability_level, time_window,
       count(*) as hits, sum(damage) as damage, min(time) as first_hit_sec, max(time) as last_hit_sec
from c
group by match_id, hero_id, is_radiant, building_npc_id, building_type, lane, source_type, source_id, ability_level, time_window
