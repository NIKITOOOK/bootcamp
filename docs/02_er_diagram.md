# День 1. ER-диаграмма

**Статус: готово.** Диаграмма и таблица связей проверены на данных RAW 13.11 (5% матчей).

## Диаграмма (Mermaid)

```mermaid
erDiagram
    game_modes ||--o{ matches : "game_mode = mode_id"
    matches ||--|{ match_players : "match_id"
    heroes ||--o{ match_players : "hero_id"
    heroes ||--o{ hero_abilities : "hero_id"
    abilities ||--o{ hero_abilities : "ability_id"
    matches ||--o{ tower_damage_events : "match_id"
    match_players ||--o{ tower_damage_events : "match_id + attacker = hero_id"
    npcs ||--o{ tower_damage_events : "npcid = npc_id"
    abilities |o--o{ tower_damage_events : "byability = ability_id (>0)"
    items |o--o{ tower_damage_events : "byitem = item_id (>0)"
    npcs |o--o{ tower_damage_events : "fromnpc = npc_id (>0)"
    matches ||--o{ tower_death_events : "match_id"
    match_players ||--o{ ability_learn_events : "match_id + steam_account_id = account_id"
    abilities |o--o{ ability_learn_events : "abilityid = ability_id (>0)"
    match_players ||--o{ purchase_events : "match_id + steam_account_id = account_id"
    items |o--o{ purchase_events : "itemid = item_id"
    match_players ||--o{ gold_events : "match_id + steam_account_id = account_id"
    matches {
        bigint match_id PK
        bigint start_time
        int duration
        int game_mode FK
        int human_players
        boolean radiant_win
    }
    match_players {
        bigint match_id PK
        int player_slot PK
        bigint account_id
        int hero_id FK
        boolean is_radiant
        boolean win
        int net_worth
        int tower_damage
        int rank
    }
    tower_damage_events {
        varchar id PK
        bigint match_id FK
        int time
        int attacker "hero_id"
        int npcid FK
        int damage
        int byability
        int byitem
        int fromnpc
    }
    tower_death_events {
        bigint match_id FK
        int time
        bigint radiant "маска"
        bigint dire "маска"
    }
    ability_learn_events {
        bigint match_id FK
        bigint steam_account_id
        int time
        int abilityid FK
        int level "с 0"
    }
    purchase_events {
        bigint match_id FK
        bigint steam_account_id
        int time
        int itemid FK
    }
    gold_events {
        bigint match_id FK
        bigint steam_account_id
        int time
        int amount
    }
    heroes { int hero_id PK
        string localized_name
        string attack_type }
    npcs { int npc_id PK
        string name
        int status_health }
    abilities { int ability_id PK
        string display_name }
    items { int item_id PK
        string localized_name }
    hero_abilities { int hero_id PK
        int ability_id PK }
    game_modes { int mode_id PK
        string name }
```

Источники: PostgreSQL, схема `dota` (справочники, `matches`, `match_players`) и MongoDB, база `dota_events` (события). В lakehouse они лежат как `lakehouse.raw.pg__*` и `lakehouse.raw.mongo__*`.

## Пути соединения и кратность

| Связь | Ключ | Кратность | Замечание |
|---|---|---|---|
| match_players → matches | `match_id` | N:1 | У 32% match_id из match_players нет строки в matches, поэтому только INNER JOIN. |
| matches → game_modes | `game_mode = mode_id` | N:1 | Режим 23 (Turbo) исключаем. |
| tower_damage_events → match_players | `(match_id, attacker = hero_id)` | N:1 | `attacker` содержит `hero_id`, а не steam id (в каталоге описано неверно). Пара не уникальна в 0,28% матчей (hero_id = 0, повтор героя), такие матчи исключаем. |
| tower_damage_events → npcs | `npcid = npc_id` | N:1 | 16–35 башни, 38–49 бараки, 50–51 трон. Тир только в имени (`tower1`…`tower4`). |
| tower_damage_events → abilities / items / npcs | `byability`, `byitem`, `fromnpc` | N:0..1 | Пустая ссылка = 0, не NULL. Справочники присоединяем по условию «> 0», LEFT JOIN. |
| ability_learn_events → abilities | `abilityid` | N:0..1 | `level` с 0; 1,5% строк с `abilityid = 0` отбрасываем. |
| tower_death_events → matches | `match_id` | N:1 | Состояние башен битовыми масками; упавшее здание = новый бит. |
| purchase_events → match_players | `(match_id, steam_account_id)` | N:1 | Нужна только покупка Helm of the Dominator (item 164) и Helm of the Overlord (635). |

## Замечания

- В MongoDB внешних ключей нет, связи логические.
- Названия полей Mongo в lakehouse записаны в нижнем регистре без подчёркиваний.
- Во всех RAW-таблицах есть `sys_insert_stamp`, `job_id`, `src_modified_stamp`.
- Анонимный игрок: `account_id = 4294967295`.
