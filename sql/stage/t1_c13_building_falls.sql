-- STAGE t1_c13_building_falls: здание в матче, когда стало атакуемым (available_from_sec) и когда пало (fall_sec)
-- портативный текст: один и тот же запрос идёт и в Trino (предпросмотр), и в Spark (запуск). Без unnest, bitwise_*, varchar без длины.
-- маска tower_death_events = накопленные биты разрушенных зданий стороны: b0-2 T1 top/mid/bot, b3-5 T2, b6-8 T3,
-- b9 первая T4, b10-12 melee rax, b13-15 range rax, b16 трон (= конец матча). Вторая T4: hp = 0 в building_events,
-- иначе последний удар по T4 (если трон атаковали)
-- check: rows > 0
-- check: unique(match_id, building_side, building_code)
-- check: not_null(match_id, building_side, building_code, building_type, exposure_sec)
-- check: accepted(building_type, T1, T2, T3, T4, rax_melee, rax_range, fort)
-- check: accepted(fall_time_source, mask, building_events, last_hit, none)
with mt as (
    select match_id, duration_sec as duration from lakehouse.stage.t1_c13_matches
),
ev0 as (
    select d.match_id, d.time, s.side, case when s.side = 'radiant' then d.radiant else d.dire end as mask
    from lakehouse.raw.mongo__tower_death_events d
    join mt on mt.match_id = d.match_id
    cross join (values ('radiant'), ('dire')) as s(side)
),
ev as (
    select match_id, time, side, mask, lag(mask, 1, 0) over (partition by match_id, side order by time, mask) as prev
    from ev0
),
dt as (
    select match_id, side, b, min(time) as t_death
    from ev
    cross join (values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16)) as t(b)
    where mod(cast(floor(cast(mask as double) / power(2, b)) as bigint), 2) = 1
      and mod(cast(floor(cast(prev as double) / power(2, b)) as bigint), 2) = 0
    group by 1, 2, 3
),
t4_be as (
    select bi.match_id, case when bi.isradiant then 'radiant' else 'dire' end as side, max(bd.t_fall) as t4_second_fall
    from (select match_id, indexid, bool_or(isradiant) as isradiant, max(npcid) as npcid
          from lakehouse.raw.mongo__building_events where type = 'TOWER' group by 1, 2) bi
    join (select match_id, indexid, min(time) as t_fall
          from lakehouse.raw.mongo__building_events where hp = 0 group by 1, 2) bd
      on bd.match_id = bi.match_id and bd.indexid = bi.indexid
    join mt on mt.match_id = bi.match_id
    where bi.npcid in (25, 35)
    group by 1, 2
    having count(*) = 2
),
hx as (
    select t.match_id, case when t.npcid in (25, 50) then 'radiant' else 'dire' end as side,
           max(case when t.npcid in (25, 35) then t.time end) as last_t4_hit,
           min(case when t.npcid in (50, 51) then t.time end) as first_fort_hit
    from lakehouse.raw.mongo__tower_damage_events t
    join mt on mt.match_id = t.match_id
    where t.npcid in (25, 35, 50, 51)
    group by 1, 2
),
dp0 as (
    select mt.match_id, mt.duration, s.side,
           max(case when b = 0 then t_death end) as d0,
           max(case when b = 1 then t_death end) as d1,
           max(case when b = 2 then t_death end) as d2,
           max(case when b = 3 then t_death end) as d3,
           max(case when b = 4 then t_death end) as d4,
           max(case when b = 5 then t_death end) as d5,
           max(case when b = 6 then t_death end) as d6,
           max(case when b = 7 then t_death end) as d7,
           max(case when b = 8 then t_death end) as d8,
           max(case when b = 9 then t_death end) as d9,
           max(case when b = 10 then t_death end) as d10,
           max(case when b = 11 then t_death end) as d11,
           max(case when b = 12 then t_death end) as d12,
           max(case when b = 13 then t_death end) as d13,
           max(case when b = 14 then t_death end) as d14,
           max(case when b = 15 then t_death end) as d15,
           max(case when b = 16 then t_death end) as d16
    from mt
    cross join (values ('radiant'), ('dire')) as s(side)
    left join dt on dt.match_id = mt.match_id and dt.side = s.side
    group by 1, 2, 3
),
dp as (
    select dp0.match_id, dp0.duration, dp0.side, dp0.d0, dp0.d1, dp0.d2, dp0.d3, dp0.d4, dp0.d5, dp0.d6, dp0.d7, dp0.d8, dp0.d9, dp0.d10, dp0.d11, dp0.d12, dp0.d13, dp0.d14, dp0.d15, dp0.d16,
           case when be.t4_second_fall is not null then be.t4_second_fall
                when dp0.d9 is not null and hx.first_fort_hit is not null
                     then least(greatest(dp0.d9, coalesce(hx.last_t4_hit, dp0.d9)), dp0.duration) end as t4b,
           case when be.t4_second_fall is not null then 'building_events'
                when dp0.d9 is not null and hx.first_fort_hit is not null then 'last_hit' else 'none' end as t4b_source
    from dp0
    left join t4_be be on be.match_id = dp0.match_id and be.side = dp0.side
    left join hx on hx.match_id = dp0.match_id and hx.side = dp0.side
),
w as (
    select match_id, duration, side, 0 as slot, 'T1' as building_type, 'top' as lane, cast(0 as integer) as available_from_sec, cast(d0 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 1 as slot, 'T1' as building_type, 'mid' as lane, cast(0 as integer) as available_from_sec, cast(d1 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 2 as slot, 'T1' as building_type, 'bot' as lane, cast(0 as integer) as available_from_sec, cast(d2 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 3 as slot, 'T2' as building_type, 'top' as lane, cast(d0 as integer) as available_from_sec, cast(d3 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 4 as slot, 'T2' as building_type, 'mid' as lane, cast(d1 as integer) as available_from_sec, cast(d4 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 5 as slot, 'T2' as building_type, 'bot' as lane, cast(d2 as integer) as available_from_sec, cast(d5 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 6 as slot, 'T3' as building_type, 'top' as lane, cast(d3 as integer) as available_from_sec, cast(d6 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 7 as slot, 'T3' as building_type, 'mid' as lane, cast(d4 as integer) as available_from_sec, cast(d7 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 8 as slot, 'T3' as building_type, 'bot' as lane, cast(d5 as integer) as available_from_sec, cast(d8 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 10 as slot, 'rax_melee' as building_type, 'top' as lane, cast(d6 as integer) as available_from_sec, cast(d10 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 11 as slot, 'rax_melee' as building_type, 'mid' as lane, cast(d7 as integer) as available_from_sec, cast(d11 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 12 as slot, 'rax_melee' as building_type, 'bot' as lane, cast(d8 as integer) as available_from_sec, cast(d12 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 13 as slot, 'rax_range' as building_type, 'top' as lane, cast(d6 as integer) as available_from_sec, cast(d13 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 14 as slot, 'rax_range' as building_type, 'mid' as lane, cast(d7 as integer) as available_from_sec, cast(d14 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 15 as slot, 'rax_range' as building_type, 'bot' as lane, cast(d8 as integer) as available_from_sec, cast(d15 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 9 as slot, 'T4' as building_type, 'base' as lane, cast(nullif(least(coalesce(d6, 99999), coalesce(d7, 99999), coalesce(d8, 99999)), 99999) as integer) as available_from_sec, cast(d9 as integer) as fall_sec, 'mask' as src from dp
    union all
    select match_id, duration, side, 17 as slot, 'T4' as building_type, 'base' as lane, cast(nullif(least(coalesce(d6, 99999), coalesce(d7, 99999), coalesce(d8, 99999)), 99999) as integer) as available_from_sec, cast(t4b as integer) as fall_sec, t4b_source as src from dp
    union all
    select match_id, duration, side, 16 as slot, 'fort' as building_type, 'base' as lane, cast(t4b as integer) as available_from_sec, cast(d16 as integer) as fall_sec, 'mask' as src from dp
)
select match_id, side as building_side, slot as building_code, building_type, lane,
       case when side = 'radiant'
            then case when slot <= 8 then 16 + slot when slot in (9, 17) then 25 when slot between 10 and 15 then 28 + slot else 50 end
            else case when slot <= 8 then 26 + slot when slot in (9, 17) then 35 when slot between 10 and 15 then 34 + slot else 51 end
       end as npc_id,
       available_from_sec, fall_sec as fall_time_sec,
       case when fall_sec is null then 'none' else src end as fall_time_source,
       case when fall_sec <= 900 then 1 else 0 end as fell_by_15,
       case when available_from_sec is null then 0
            else greatest(coalesce(fall_sec, duration) - available_from_sec, 0) end as exposure_sec
from w
