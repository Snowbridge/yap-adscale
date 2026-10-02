# Масштабирование баз данных и CQRS

Стратегия репликации, шардирования и разделения read/write моделей. Обоснование: [ADR-013](ADR/ADR-013-scaling-replica-vs-shard.md), [ADR-011](ADR/ADR-011-cqrs-projections.md).

Диаграммы: [02-replication-topology.puml](diagrams/02-replication-topology.puml), [03-cqrs-projections.puml](diagrams/03-cqrs-projections.puml).

---

## 1. PostgreSQL — Campaign и Billing

### Master–Replica (Primary + Standby)

| Роль                   | Campaign (`campaign-db`)                                    | Billing (`billing-db`)                |
|------------------------|-------------------------------------------------------------|---------------------------------------|
| **Primary**            | Все записи: CRUD кампаний, `spend_agg`                      | Все списания, пополнения, инвойсы     |
| **Async read replica** | Списки кампаний, история, тяжёлые read-only отчёты кабинета | Опционально: read-only сверки         |
| **Sync standby**       | Опционально                                                 | **Предпочтительно** (минимальный RPO) |

**Правила:**

- RTB **не** подключается к PG replicas.
- Запросы «создал кампанию → сразу прочитал детали» — только **primary** (lag replica недопустим).
- Мониторинг `replication_lag` и алерты (наследие [ADR-002](../task1/ADR/ADR-002-analytics-read-replica.md) для переходного монолита).

### Шардирование PostgreSQL

**Статус:** не на первом этапе TO-BE.

**Триггеры включения:**

- Primary CPU &gt; 70% sustained или размер данных &gt; ~2 TB на инстанс.
- Рост числа рекламодателей с горячими партициями агрегатов.

**Ключ шарда:** `advertiser_id` — кампании и биллинг colocated по рекламодателю; cross-shard транзакции не требуются при разделённых контекстах.

**Маршрутизация:** shard router в Campaign API / Billing API (или Citus / application-level).

---

## 2. Redis — RTB Cluster

### Шардирование

- **Redis Cluster** — hash slots по ключу.
- **Hash tag** `{campaign_id}` в именах ключей (`campaign:{id}:limits` → `campaign:{id}:limits` с tag) — все ключи одной кампании на одном shard для Lua-транзакций ([bidding-service.md](../task2/bidding-service.md)).

### Master–Replica внутри кластера

| Операция                                      | Куда                                 |
|-----------------------------------------------|--------------------------------------|
| Targeting: чтение индексов `idx:candidates:*` | Replica (допустим краткий staleness) |
| Bidding: резервы, dedup, HINCRBY              | Master shard                         |
| Cache Projector: swap конфигурации            | Master shard                         |

### Горизонтальное масштабирование RTB

- Микросервисы RTB — **stateless**; масштабирование pod count по CPU и p95 latency.
- Узкое место — Redis и сеть; добавление shards при memory &gt; 70% кластера.

---

## 3. ClickHouse — аналитика

### Целевой read path

- UI отчётности и scheduled reports читают **только ClickHouse**.
- Read replica PostgreSQL монолита ([ADR-002](../task1/ADR/ADR-002-analytics-read-replica.md)) — **не** целевое состояние после миграции ingest в CH.

### Партиционирование и шардирование

```sql
-- Локальная таблица (пример)
PARTITION BY toYYYYMM(event_time)
ORDER BY (advertiser_id, campaign_id, event_time)
```

- **Distributed** таблица: `SHARD BY cityHash64(advertiser_id)` (или `campaign_id` при перекосе нагрузки).
- TTL на сырых данных: согласовать с FR-9 и стоимостью хранения (например 13–24 мес. hot, далее cold storage).

### Read replicas / роли кластера

| Нагрузка                             | Размещение                |
|--------------------------------------|---------------------------|
| Ad-hoc отчёты в UI                   | Replica group `reporting` |
| Materialized views, фоновые агрегаты | Replica group `ingest_mv` |

Разделение снижает конкуренцию OLAP-запросов с записью из Kafka.

---

## 4. Kafka — масштабирование потока

- **Partitions** — единица параллелизма consumer group ([event-streaming.md](event-streaming.md)).
- Увеличение partition count при стабильном consumer lag &gt; порога SLO.
- **Replication factor** = 3 для production.

| Топик              | Partition key                  | Зачем                                                  |
|--------------------|--------------------------------|--------------------------------------------------------|
| `telemetry.events` | `campaign_id`                  | Упорядоченность событий кампании; параллель агрегатора |
| `campaign.config`  | `campaign_id`                  | Проекция в Redis по кампании                           |
| `campaign.spend`   | `campaign_id`                  | Идемпотентность окон                                   |
| `billing.balance`  | `advertiser_id`                | Баланс рекламодателя                                   |
| `auction.events`   | `campaign_id` или `request_id` | Баланс нагрузки vs локальность                         |

---

## 5. CQRS — где применяется

| Write model   | Read model     | Транспорт            | Масштабирование read                   |
|---------------|----------------|----------------------|----------------------------------------|
| `campaign-db` | RTB Redis      | `campaign.config`    | N экземпляров Projector (≤ partitions) |
| `billing-db`  | RTB Redis + UI | `billing.balance`    | Отдельные consumer groups              |
| Kafka events  | ClickHouse     | consumers телеметрии | Шарды CH + MV                          |
| `campaign-db` | Кабинет (PG)   | синхронный API       | PG read replica                        |

**Не CQRS:** прямое редактирование кампании в кабинете — read-after-write с primary PG.

**Обязательный CQRS:** любое состояние, нужное RTB на bid path — только через проекцию ([ADR-011](ADR/ADR-011-cqrs-projections.md)).

Диаграмма потоков: [03-cqrs-projections.puml](diagrams/03-cqrs-projections.puml).

---

## 6. Read replicas для сервиса аналитики

| Вопрос                                           | Ответ                                                                                       |
|--------------------------------------------------|---------------------------------------------------------------------------------------------|
| Нужна ли PG read replica для аналитики в TO-BE?  | **Нет** (целевой store — ClickHouse)                                                        |
| Нужны ли read replicas в ClickHouse?             | **Да** — для изоляции UI read от фоновых MV                                                 |
| Может ли аналитика читать `campaign-db` replica? | Только **справочники** (названия кампаний) через REST к Campaign API, не для метрик показов |

Метрики показов/кликов — из CH, заполненного из `telemetry.events` / `auction.events`.

---

## 7. Триггеры пересмотра архитектуры масштабирования

| Метрика                   | Действие                                                           |
|---------------------------|--------------------------------------------------------------------|
| Redis memory &gt; 70%     | Add shards / eviction policy review (без eviction доменных ключей) |
| CH insert lag &gt; 5 min  | + consumers, + shards, tuning batch size                           |
| Kafka max lag &gt; 10 min | + partitions или consumers                                         |
| PG primary CPU &gt; 70%   | Read replica traffic audit; план шардирования по `advertiser_id`   |
