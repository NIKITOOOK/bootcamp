-- MART t1_c13_team_early_push (H3): матч x сторона; ранний урон по зданиям, башни к 15 мин, золото к 15 мин, победа
-- gold_15: сумма положительных gold_events с time <= 900; только матчи без анонимных игроков (иначе NULL)
-- check: rows > 0
-- check: unique(match_id, is_radiant)
-- check: not_null(match_id, is_radiant, win, bld_dmg_15, towers_down_15)
-- check: range(win, 0, 1)
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
