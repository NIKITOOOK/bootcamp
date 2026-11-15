# Кейс 13 · проверка исправления team_early_push (H3)

Проблема: в витрине `mart.t1_c13_team_early_push` стоял `INNER JOIN` на башни, из-за чего матчи, где одна из сторон не потеряла ни одной башни, выпадали (199 889 матчей вместо 219 355). В ноутбуке проверяется исправленный SQL (`LEFT JOIN`) на всех данных STAGE и результат сравнивается с текущей витриной и с первым ноутбуком (выборка 5%).

```python
import os, warnings, time
import pandas as pd
import trino
conn = trino.dbapi.connect(host=os.getenv("TRINO_HOST", "trino"), port=int(os.getenv("TRINO_PORT", "8080")),
                           user=os.getenv("TRINO_USER", os.getenv("JUPYTERHUB_USER", "jupyter")), catalog="lakehouse")
def q(sql, attempts=6, pause=30):
    for a in range(attempts):
        try:
            cur = conn.cursor(); cur.execute(sql); rows = cur.fetchall()
            return pd.DataFrame(rows, columns=[d[0] for d in cur.description])
        except Exception as e:
            if a < attempts - 1 and any(k in str(e) for k in ("SERVER_STARTING_UP", "Query not found", "Connection")):
                time.sleep(pause); continue
            raise
```

Исправленный SQL (ядро): агрегаты игроков `t`, урон по зданиям до 15-й минуты `dmg`, снесённые башни `tw`, затем
`left join dmg` и `left join tw` по стороне соперника, самосоединение `s a join s b` для пары сторон матча.
Полный текст потока — в `sql/mart/t1_c13_team_early_push.sql`.

## Проверка качества (DQ)

- по две строки на матч (по одной на сторону), ключ матч×сторона уникален;
- пропусков в `win` и `towers_down_15` нет;
- пустое `first_enemy_t1_fall_sec` означает, что у стороны первая T1 за матч не упала.

**Вывод:** исправленные доли побед совпадают с ноутбуком на 5% выборке, значит прежний `INNER JOIN` занижал эффект раннего пуша.
