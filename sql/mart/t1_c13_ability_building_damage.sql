-- MART t1_c13_ability_building_damage (H2): герой x источник x уровень x тип здания x окно; урон за удар, экспозиция, отношение к T3
-- exposure_sec: сумма секунд, когда здания противника этого типа были доступны для атаки в этом окне, по матчам героя
-- check: rows > 0
-- check: unique(hero_id, source_type, source_id, ability_level, building_type, time_window)
-- check: not_null(hero_id, source_type, source_id, building_type, time_window, hits, damage, dmg_per_hit)
with w as (
    select time_window, w0, w1 from (values ('00-10', 0, 600), ('10-15', 600, 900), ('15-30', 900, 1800), ('30+', 1800, 1000000)) as t(time_window, w0, w1)
),
ex as (
    select f.match_id, f.building_side, f.building_type, w.time_window,
           sum(greatest(least(coalesce(f.fall_time_sec, m.duration_sec), w.w1) - greatest(f.available_from_sec, w.w0), 0)) as exposure_sec
    from lakehouse.stage.t1_c13_building_falls f
    join lakehouse.stage.t1_c13_matches m on m.match_id = f.match_id
    cross join w
    where f.available_from_sec is not null
    group by 1, 2, 3, 4
),
hero_ex as (
    select p.hero_id, ex.building_type, ex.time_window, sum(ex.exposure_sec) as exposure_sec
    from lakehouse.stage.t1_c13_players p
    join ex on ex.match_id = p.match_id and ex.building_side = case when p.is_radiant then 'dire' else 'radiant' end
    group by 1, 2, 3
),
h as (
    select hero_id, source_type, source_id, max(source_name) as source_name, ability_level, building_type, time_window,
           count(distinct match_id) as matches, sum(hits) as hits, sum(damage) as damage
    from lakehouse.stage.t1_c13_tower_hits
    group by hero_id, source_type, source_id, ability_level, building_type, time_window
),
r as (
    select h.hero_id, hr.localized_name as hero_name, h.source_type, h.source_id, h.source_name, h.ability_level,
           h.building_type, h.time_window, h.matches, h.hits, h.damage,
           cast(h.damage as double) / h.hits as dmg_per_hit,
           cast(h.damage as double) / h.matches as dmg_per_match,
           hx.exposure_sec,
           cast(h.damage as double) * 60 / nullif(hx.exposure_sec, 0) as dmg_per_exposure_min,
           cast(h.damage as double) / sum(h.damage) over (partition by h.hero_id) as share_of_hero_damage
    from h
    left join hero_ex hx on hx.hero_id = h.hero_id and hx.building_type = h.building_type and hx.time_window = h.time_window
    left join lakehouse.raw.pg__heroes hr on hr.hero_id = h.hero_id
)
select hero_id, hero_name, source_type, source_id, source_name, ability_level, building_type, time_window,
       matches, hits, damage, round(dmg_per_hit, 2) as dmg_per_hit, round(dmg_per_match, 2) as dmg_per_match,
       exposure_sec, round(dmg_per_exposure_min, 4) as dmg_per_exposure_min,
       round(dmg_per_hit / nullif(max(case when building_type = 'T3' then dmg_per_hit end)
             over (partition by hero_id, source_type, source_id, ability_level, time_window), 0), 3) as ratio_to_t3,
       round(share_of_hero_damage, 5) as share_of_hero_damage
from r
