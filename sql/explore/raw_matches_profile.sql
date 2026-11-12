-- Профиль raw.pg__matches: длительность и доля побед Radiant по дням
-- check: rows > 0
-- check: unique(day)
-- check: not_null(day, matches)
-- check: range(radiant_win_rate, 0, 1)
select
    date(from_unixtime(start_time)) as day,
    count(*) as matches,
    count(distinct match_id) as uniq_matches,
    round(avg(duration) / 60.0, 1) as avg_minutes,
    round(avg(case when radiant_win then 1.0 else 0.0 end), 3) as radiant_win_rate
from lakehouse.raw.pg__matches
where duration > 0
group by 1
order by 1
