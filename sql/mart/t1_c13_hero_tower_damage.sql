-- MART t1_c13_hero_tower_damage (H1): герой x позиция x число сильных пушеров-союзников; индекс урона по зданиям к позиции
-- check: rows > 0
-- check: unique(hero_id, pos_proxy, pusher_allies_grp)
-- check: not_null(hero_id, pos_proxy, pusher_allies_grp, games, tdpm_median, tdpm_expected)
-- check: accepted(pusher_allies_grp, 0, 1, 2+)
-- check: range(win_rate, 0, 1)
with p as (
    select hero_id, hero_name, attack_type, pos_proxy, tdpm, dmg_share, win, has_helm,
           case when pusher_allies = 0 then '0' when pusher_allies = 1 then '1' else '2+' end as pusher_allies_grp
    from lakehouse.stage.t1_c13_players
),
pos as (
    select pos_proxy, approx_percentile(tdpm, 0.5) as tdpm_expected, avg(tdpm) as tdpm_pos_avg
    from p group by pos_proxy
)
select p.hero_id, max(p.hero_name) as hero_name, max(p.attack_type) as attack_type, p.pos_proxy, p.pusher_allies_grp,
       count(*) as games,
       round(avg(p.tdpm), 2) as tdpm_avg,
       round(approx_percentile(p.tdpm, 0.5), 2) as tdpm_median,
       round(max(pos.tdpm_expected), 2) as tdpm_expected,
       round(approx_percentile(p.tdpm, 0.5) / nullif(max(pos.tdpm_expected), 0), 3) as tdpm_index,
       round(avg(p.tdpm) / nullif(max(pos.tdpm_pos_avg), 0), 3) as tdpm_index_avg,
       round(avg(p.dmg_share), 4) as dmg_share_avg,
       round(avg(cast(p.win as double)), 4) as win_rate,
       round(avg(cast(p.has_helm as double)), 4) as helm_rate
from p
join pos on pos.pos_proxy = p.pos_proxy
group by p.hero_id, p.pos_proxy, p.pusher_allies_grp
