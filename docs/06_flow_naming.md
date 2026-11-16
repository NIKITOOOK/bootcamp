# День 2. DL: нейминг потока

**Статус: готово** для потоков 1–2 (созданы на платформе); остальные имена зафиксированы заранее и совпадают с SQL.

## Схема имён

```
<слой>__t1_c13_<объект>      имя DAG в Airflow
lakehouse.<слой>.t1_c13_<объект>    имя таблицы в каталоге
```

- `<слой>` = `stage` или `mart`. Слой RAW ведёт платформа (`raw.pg__<таблица>`, `raw.mongo__<коллекция>`).
- `t1` = команда 1, `c13` = кейс 13, затем объект латиницей в snake_case.
- Название DAG порождает Портал из слоя и имени таблицы, отдельно его не задаём.
- В SQL всегда полные имена `lakehouse.raw.*` и `lakehouse.stage.*`: по ним Портал находит входы, строит запуск по датасетам и lineage.
- Метрики: суффикс периода (`_10`, `_15`), разницы `_diff`, доли `_share`/`_rate`.

## Реестр потоков

| Слой | Таблица | DAG | Назначение | Статус |
|---|---|---|---|---|
| STAGE | `t1_c13_matches` | `stage__t1_c13_matches` | отобранные матчи | готово |
| STAGE | `t1_c13_players` | `stage__t1_c13_players` | игроки матча с метриками | готово |
| STAGE | `t1_c13_building_falls` | `stage__t1_c13_building_falls` | время падения зданий | предпросмотр пройден, создание подтверждается |
| STAGE | `t1_c13_tower_hits` | `stage__t1_c13_tower_hits` | свёртка ударов по зданиям | создан |
| MART | `t1_c13_hero_tower_damage` | `mart__t1_c13_hero_tower_damage` | H1 | запланировано |
| MART | `t1_c13_team_early_push` | `mart__t1_c13_team_early_push` | H3 | запланировано |
| MART | `t1_c13_ability_building_damage` | `mart__t1_c13_ability_building_damage` | H2 | запланировано |

Обслуживающие DAG платформы (`maintenance__iceberg`, `lineage_outbox_relay`) нашими не являются.

## Правила для столбцов

- snake_case, без пробелов; зарезервированные слова (`time`) в SQL берём в обратные кавычки.
- Ключи: `match_id`, `hero_id`, `player_slot`, `building_type`.
- Служебные поля RAW (`sys_insert_stamp`, `job_id`, `src_modified_stamp`) в MART не выводим, в STAGE используем для дедупликации.
- Идентификаторы игроков (`account_id`) в STAGE и MART не выводим.
