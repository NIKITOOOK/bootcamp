# jsql

SQL запускается в ядре моего сервера JupyterHub, ядро ходит в Trino (`trino:8080`, каталог `lakehouse`). Результат возвращается в терминал в виде markdown-таблицы, полный результат сохраняется в `runs/<имя>.csv`, все запуски дописываются в `runs/log.md`.

## Установка

```
pip install -r tools/requirements.txt
```

`.env` в корне репозитория:

```
JUPYTER_URL=http://153.51.166.221:8888/user/maleychenko_n/
JUPYTER_TOKEN=...
```

Токен создаётся в JupyterHub: Token → scope `access:servers!server=maleychenko_n/`. Сервер должен быть запущен (Start My Server).

## Команды

```
python tools/jsql.py ping                          # ядро + Trino
python tools/jsql.py tables raw                    # список таблиц
python tools/jsql.py describe raw.pg__matches      # колонки + 5 строк
python tools/jsql.py query "select count(*) from lakehouse.raw.pg__matches"
python tools/jsql.py run sql/mart/case_05.sql      # файл с проверками
python tools/jsql.py run-all sql/stage sql/mart    # все файлы, итог OK/FAIL/ERROR
python tools/jsql.py exec "import pandas; print(pandas.__version__)"
python tools/jsql.py restart                       # удалить ядро
```

Код возврата 0, если все проверки OK.

## Проверки в .sql

```
-- check: rows > 0
-- check: unique(match_id, hero_id)
-- check: not_null(match_id, win_rate)
-- check: range(win_rate, 0, 1)
-- check: accepted(side, radiant, dire)
```

Проверки выполняются по возвращённым строкам (по умолчанию до 200 000, `--max-rows`). Для больших таблиц считать агрегат в самом SQL.

## Цикл

1. Гипотеза → `sql/explore/*.sql` с проверками.
2. `run` → читаю таблицу и проверки, правлю SQL, повторяю до OK.
3. Готовый SQL переносится в `sql/stage` / `sql/mart`, оттуда в пайплайн Портала.
4. `run-all sql/stage sql/mart` перед сдачей.

Пароли и токены только в `.env`, в git не попадают.
