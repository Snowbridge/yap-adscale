# Стратегия баз данных (Database per Service / Context)

Документ описывает, **где** и **почему** хранятся данные каждого ограниченного контекста AdScale в целевой микросервисной архитектуре. Решения зафиксированы в [ADR-009](ADR/ADR-009-database-per-bounded-context.md).

Опора: [Task 1 — bounded contexts](../task1/03-bounded-contexts.md), [C4 TO-BE](../task1/diagrams/05-c4-container-to-be.puml), [ADR-006](../task1/ADR/ADR-006-rtb-in-memory-hot-path.md), [ADR-003](../task1/ADR/ADR-003-billing-spend-aggregates-source.md).

---

## Сводная таблица по bounded context

| Bounded context                          | Хранилище                    | Source of truth                                                    | Профиль доступа                  |
|------------------------------------------|------------------------------|--------------------------------------------------------------------|----------------------------------|
| **Аукционы и Выдача рекламы** (RTB Core) | **Redis Cluster**            | Операционная **проекция**; истина — Kafka + PG соседних контекстов | Sub-ms read/write, Lua, hot path |
| **Управление кампаниями и ставками**     | **PostgreSQL** `campaign-db` | Кампании, правила, шаблоны, агрегаты расхода                       | OLTP, ACID, умеренный write      |
| **Финансы и Биллинг**                    | **PostgreSQL** `billing-db`  | Транзакции, балансы, инвойсы                                       | Строгий ACID, низкий TPS         |
| **Телеметрия и Отчётность**              | **ClickHouse**               | Сырые события + OLAP-агрегаты                                      | Write-heavy append, heavy read   |

Диаграмма: [diagrams/01-c4-data-stores.puml](diagrams/01-c4-data-stores.puml).

---

## 1. RTB Core & Delivery

**СУБД:** Redis Cluster (AOF `everysec`, RDB snapshots, репликация shard).

**Что хранится:**

- Материализованные кампании, bid rules, лимиты, шаблоны, индексы кандидатов.
- Проекция балансов рекламодателя для защиты от перерасхода на hot path.
- Операционные счётчики показов/кликов, dedup ответов по `request_id`.

**Почему Redis, а не PostgreSQL:**

- P95 ≤ 100 ms / ≤ 80 ms для DSP ([02-requirements.md](../task1/02-requirements.md)) недостижимы при синхронном SQL на каждый bid ([ADR-006](../task1/ADR/ADR-006-rtb-in-memory-hot-path.md)).
- Атомарные резервы и HINCRBY в одном round-trip ([bidding-service.md](../task2/bidding-service.md)).

**Микросервисы и БД:**

| Сервис                                | Своя БД?        | Примечание                    |
|---------------------------------------|-----------------|-------------------------------|
| Targeting, Bidding, Creative Delivery | Общий RTB Redis | Один data plane на подсистему |
| Cache Projector                       | Нет             | Пишет в Redis из Kafka        |

Изоляция от кэша кабинета: [ADR-010](ADR/ADR-010-rtb-redis-isolation.md).

---

## 2. Campaign & Bid Management

**СУБД:** отдельный инстанс PostgreSQL `campaign-db`.

**Схемы (логическое разделение):**

- `campaign` — кампании, line items, креативы, таргетинг, шаблоны, бюджеты, статусы.
- `spend_agg` — агрегаты расхода по окнам (кампания + период), источник для `campaign.spend` ([ADR-003](../task1/ADR/ADR-003-billing-spend-aggregates-source.md)).

**Почему PostgreSQL:**

- Транзакционное редактирование в кабинете (FR-10).
- Связность сущностей кампании; миграции схемы при эволюции без big bang.
- Умеренный объём write по сравнению с телеметрией.

**Микросервисы:**

| Сервис           | БД            | Write                                                                    |
|------------------|---------------|--------------------------------------------------------------------------|
| Campaign API     | `campaign-db` | CRUD кампаний, публикация `campaign.config`                              |
| Spend Aggregator | `campaign-db` | Читает `telemetry.events`, пишет `spend_agg`, публикует `campaign.spend` |

До выделения второго инстанса — **одна** физическая БД, две схемы (компромисс ADR-009).

**Интеграция:** не отдаёт прямой SQL-доступ RTB; только Kafka + REST кабинета.

---

## 3. Billing & Finance

**СУБД:** отдельный инстанс PostgreSQL `billing-db`.

**Что хранится:**

- Балансы рекламодателей, журнал транзакций (`transaction_id` уникален).
- Инвойсы, акты сверки, связь с платёжным шлюзом (FR-11).

**Почему отдельный инстанс от campaign-db:**

- P2 «без двойных списаний» и минимальный blast radius.
- Независимые RPO/RTO и sync replica ([ADR-014](ADR/ADR-014-rpo-rto-backup-twelve-factor.md)).
- Биллинг **не** подписан на `telemetry.events` — только `campaign.spend` ([ADR-003](../task1/ADR/ADR-003-billing-spend-aggregates-source.md)).

---

## 4. Telemetry & Analytics

**СУБД:** ClickHouse (кластер ReplicatedMergeTree + Distributed).

**Что хранится:**

- Каждое событие показа/клика (и при необходимости `auction.events`) — отдельная строка.
- Materialized views для отчётов: по кампании, креативу, гео, времени (FR-8, FR-9).

**Почему ClickHouse, а не PostgreSQL:**

- Columnar OLAP, сжатие, высокая скорость ingest с Kafka.
- Тяжёлые отчёты не конкурируют с OLTP ([03-bounded-contexts.md](../task1/03-bounded-contexts.md)).

**Переход с монолита:** [ADR-002](../task1/ADR/ADR-002-analytics-read-replica.md) (read replica PG) — **временный** шаг strangler; **целевой** read path отчётности — только ClickHouse.

**Семантика:** агрегаты отчётности ≠ агрегаты расхода кампаний для биллинга.

---

## 5. Edge (вне доменных контекстов)

| Компонент           | Хранилище         | Комментарий                                 |
|---------------------|-------------------|---------------------------------------------|
| Identity (Keycloak) | Встроенная БД IdP | Пользователи, роли OIDC; не на hot path RTB |
| Telemetry Ingest    | Stateless         | Только буфер в Kafka                        |
| RTB / UI Gateway    | Stateless         | Rate-limit state — отдельно от RTB Redis    |

---

## 6. Запреты и антипаттерны

1. Shared PostgreSQL между Campaign и Billing или с монолитом после выделения контекста.
2. Синхронный read-through из RTB в `campaign-db` / `billing-db` на bid request.
3. Запись сырой телеметрии в OLTP PostgreSQL в целевом состоянии.
4. Использование ClickHouse как источника списаний или лимитов кампаний.

---

## 7. Отказоустойчивость данных (кратко)

Детали: [ADR-014](ADR/ADR-014-rpo-rto-backup-twelve-factor.md), [scaling.md](scaling.md), [README.md](README.md).

| Хранилище   | RPO (ориентир) | RTO (ориентир) |
|-------------|----------------|----------------|
| RTB Redis   | 1–5 min        | &lt; 15 min    |
| campaign-db | 0–1 min        | 30–60 min      |
| billing-db  | ~0             | 30–60 min      |
| ClickHouse  | ~1 h           | 2–4 h          |

Порядок восстановления: [diagrams/05-backup-dr-flow.puml](diagrams/05-backup-dr-flow.puml).
