# Как платформы работают друг с другом

Описание составлено по артефактам проекта (`05_etl.md`, `07_data_governance.md`, `lineage_dg.html`, SQL и Spark-скрипты, ноутбуки, скриншоты Airflow, Marquez, Superset и Портала). Часть формулировок про сбои взята из практики запусков на платформе. Документы `docs/0*.md` — снимок на 14.11, статусы «запланировано» в них устарели: все потоки позже были созданы и запущены (см. скриншоты Airflow).

## Общая схема

```
PostgreSQL (dota)      MongoDB (dota_events)
        │                      │      копирование платформой, общие потоки администраторов
        └──────────┬───────────┘      (append; служебные поля job_id, sys_insert_stamp, src_modified_stamp)
                   ▼
        RAW  Iceberg на S3 (RustFS), REST-каталог `lakehouse`
                   │
   Портал ── генерирует DAG из SQL потока ──► Airflow ──► Spark 4.1.2 (spark-master:7077)
                                                                 │ пишет таблицы Iceberg
                   ┌─────────────────────────────────────────────┘
                   ▼
        STAGE  ──►  MART   (тот же каталог `lakehouse`)
                   │
                   ├──► Trino читает тот же каталог ──► Superset (графики и дашборд)
                   │                              └──► предпросмотр SQL в Портале
                   ├──► JupyterHub: lakehouse_spark.SparkSessionContext → Spark, и trino-клиент → Trino
                   └──► OpenLineage-события ──► Marquez (через lineage_outbox_relay)

Метаданные платформы (конфигурация потоков `meta.table_config`, `meta.column_metadata`, история запусков `meta.job_runs`) — в PostgreSQL (БД `airflow`).
```

## Связки: кто кого вызывает, через что, что при сбое

| Связка | Кто вызывает и через что | Что бывает при сбое / важные свойства |
|---|---|---|
| Источники → RAW | Общие потоки администраторов: Airflow + Spark копируют PostgreSQL и MongoDB в Iceberg, дописывая строки (append) с `job_id`, `sys_insert_stamp`. RAW обновляется раз в 2 часа, прогон около 1 часа | Дубли версий строк в RAW — норма; в STAGE берём последнюю версию (`row_number() over (partition by ключ order by sys_insert_stamp desc, job_id desc)`). Источники чистятся каждые 4 часа, поэтому история матчей живёт только в RAW |
| Портал → Airflow | Разработчик пишет SQL потока в Портале (слой, имя `t1_c13_<объект>`), Портал создаёт DAG (`stage__…`, `mart__…`) с запуском по datasets: поток стартует, когда обновились все входные таблицы | Поток нельзя править, только удалить и создать заново, и только если от него никто не зависит. Предпросмотр («Показать данные») идёт через Trino |
| Airflow → Spark | Задача DAG запускает Spark-приложение на `spark-master:7077` (Spark 4.1.2), которое выполняет SQL и пишет Iceberg | Режим SQL в Портале `CREATE OR REPLACE` — **полная пересборка** при каждом запуске. Инкремент возможен только через Spark-скрипт (`spark/`: окно по `sys_insert_stamp`, удаление по `match_id` и append). Ограничения ресурсов Spark (например, `OUT_OF_SPILL_SPACE` на тяжёлых соединениях, предел числа стадий в одном запуске, около 150) лечатся упрощением запроса и разбиением потока |
| Spark ↔ каталог Iceberg | Spark пишет в каталог `lakehouse` (REST-каталог, данные на S3/RustFS). Коммит Iceberg атомарный | При падении потока остаётся прошлая версия таблицы. Iceberg хранит 5 последних snapshot, откат дальше невозможен |
| Trino → каталог | Trino читает тот же каталог `lakehouse` (схемы `raw`, `stage`, `mart`), без пароля во внутренней сети | Данные видны сразу после коммита Spark. Это основа предпросмотра, ноутбуков и Superset |
| Superset → Trino | Датасеты Superset — SQL к `lakehouse.mart.*` через Trino; дашборд `team1-case13-tower-abilities` | Если поток MART не обновился, на дашборде видны прежние данные; время последней загрузки выводим отдельно |
| Airflow/Spark → Marquez | События OpenLineage собирает `lineage_outbox_relay` (примерно раз в 10 минут), пространства имён `lakehouse_raw`, `lakehouse_stage`, `lakehouse_mart` | Лайнэдж приходит с задержкой; Marquez очень медленный на больших графах (минуты на страницу). Superset в Marquez не попадает: связь «витрина → график» показываем схемой |
| JupyterHub → Spark и Trino | Ядро ноутбука создаёт сессию через `lakehouse_spark.SparkSessionContext(...)` к `spark://spark-master:7077`; для SQL-аналитики используется `trino.dbapi` (хост из `TRINO_HOST`) | Ноутбук держит ресурсы кластера, пока сессия открыта. Тяжёлые агрегаты считаем в Trino, в pandas берём только агрегаты |
| Метаданные платформы | PostgreSQL (БД `airflow`): `meta.table_config`, `meta.column_metadata`, `meta.job_runs` | Источник для Портала (каталог, история запусков) и для замера SLA |

## Что я проверил на практике

- Цепочка из 8 DAG: `matches` → `players`, `building_falls` → `tower_hits` → три витрины MART; запуск «по датасетам» срабатывает сам, когда обновились все входы (скриншот Airflow: «0 of 4 datasets updated» и т.п. на каждом DAG).
- Один и тот же каталог виден из Spark (запись), Trino (чтение, предпросмотр, Superset, ноутбуки) и Marquez (лайнэдж по событиям).
- Ошибка `INNER JOIN` в витрине `team_early_push` и её исправление до `LEFT JOIN`, проверка в ноутбуке на полном STAGE и на выборке 5%.
- Полная пересборка (`CREATE OR REPLACE`) против инкремента: поэтому написаны отдельные Spark-скрипты.
