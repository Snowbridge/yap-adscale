# Task 3 — данные, масштабирование и отказоустойчивость

Проектирование data-плоскости микросервисной архитектуры AdScale: изоляция хранилищ по bounded context, масштабирование, кэширование, Kafka и DR. Опирается на [Task 1](../task1/readme.md) (C4 TO-BE, ADR-001…006) и [Task 2](../task2/README.md) (RTB, Redis-модель, ADR-007…008).

## Индекс материалов

| Файл | Содержание |
|------|------------|
| [`database-strategy.md`](database-strategy.md) | Database per context: Redis / PG×2 / ClickHouse, микросервисы и запреты |
| [`scaling.md`](scaling.md) | Master–replica, шардирование, CQRS, read path аналитики |
| [`caching.md`](caching.md) | Слои кэша, RTB Redis, TTL, инвалидация, cache warming |
| [`event-streaming.md`](event-streaming.md) | Топики Kafka, Avro/JSON, consumer groups, retention |
| [`ADR/`](ADR/) | ADR-009 … ADR-014 |
| [`diagrams/01-c4-data-stores.puml`](diagrams/01-c4-data-stores.puml) | C4 — хранилища по контекстам |
| [`diagrams/02-replication-topology.puml`](diagrams/02-replication-topology.puml) | Репликация PG, Redis, CH, Kafka |
| [`diagrams/03-cqrs-projections.puml`](diagrams/03-cqrs-projections.puml) | Write/read модели |
| [`diagrams/04-kafka-topics-consumers.puml`](diagrams/04-kafka-topics-consumers.puml) | Топики и consumer groups |
| [`diagrams/05-backup-dr-flow.puml`](diagrams/05-backup-dr-flow.puml) | Backup и восстановление |

## Сводка решений

| Тема | Решение | ADR |
|------|---------|-----|
| Database per context | RTB → Redis; Campaign → `campaign-db`; Billing → `billing-db`; Telemetry → ClickHouse | [ADR-009](ADR/ADR-009-database-per-bounded-context.md) |
| RTB Redis | Выделенный кластер, operational store, не shared с UI | [ADR-010](ADR/ADR-010-rtb-redis-isolation.md) |
| CQRS | PG → Kafka → Redis / CH; биллинг не читает сырую телеметрию | [ADR-011](ADR/ADR-011-cqrs-projections.md) |
| Схемы Kafka | Avro (telemetry, auction); JSON (config, spend, balance) | [ADR-012](ADR/ADR-012-event-schema-avro-json.md) |
| Масштабирование | PG replica сейчас, shard по `advertiser_id` позже; Redis Cluster; CH Distributed | [ADR-013](ADR/ADR-013-scaling-replica-vs-shard.md) |
| DR / 12-Factor | Дифференцированные RPO/RTO, stateless apps, admin jobs | [ADR-014](ADR/ADR-014-rpo-rto-backup-twelve-factor.md) |

## RPO / RTO (ориентиры)

| Компонент | RPO | RTO | Backup |
|-----------|-----|-----|--------|
| RTB Redis | 1–5 min | &lt; 15 min | AOF + RDB; replay Kafka compacted |
| campaign-db | 0–1 min | 30–60 min | WAL PITR + daily full |
| billing-db | ~0 | 30–60 min | Sync standby + PITR |
| ClickHouse | ~1 h | 2–4 h | S3 backup; re-ingest из Kafka |
| Kafka | 0 в retention | 1–2 h | RF=3 |
| Stateless сервисы | — | &lt; 10 min | Redeploy |

Подробности: [ADR-014](ADR/ADR-014-rpo-rto-backup-twelve-factor.md), [database-strategy.md §7](database-strategy.md).

## Чеклист задания

| # | Требование | Где закрыто |
|---|------------|-------------|
| 1 | Database per Service / context | [database-strategy.md](database-strategy.md), [ADR-009](ADR/ADR-009-database-per-bounded-context.md) |
| 2 | Масштабирование БД, replica/shard, CQRS, аналитика | [scaling.md](scaling.md), [ADR-013](ADR/ADR-013-scaling-replica-vs-shard.md) |
| 3 | Кэширование (что, где Redis, TTL, warming) | [caching.md](caching.md), [ADR-010](ADR/ADR-010-rtb-redis-isolation.md) |
| 4 | Потоковая обработка Kafka | [event-streaming.md](event-streaming.md), [ADR-012](ADR/ADR-012-event-schema-avro-json.md) |
| 5 | Отказоустойчивость, RPO/RTO, backup, 12-factor | [ADR-014](ADR/ADR-014-rpo-rto-backup-twelve-factor.md), [README](README.md), [diagrams/05-backup-dr-flow.puml](diagrams/05-backup-dr-flow.puml) |

## Связанные ADR предыдущих задач

| Тема | Документ |
|------|----------|
| Read replica монолита (переходный этап) | [ADR-002](../task1/ADR/ADR-002-analytics-read-replica.md) |
| Источник агрегатов для биллинга | [ADR-003](../task1/ADR/ADR-003-billing-spend-aggregates-source.md) |
| Kafka как шина | [ADR-004](../task1/ADR/ADR-004-message-broker-kafka.md) |
| Hot path только Redis | [ADR-006](../task1/ADR/ADR-006-rtb-in-memory-hot-path.md) |
| JitterTopic / DLQ | [ADR-008](../task2/ADR/ADR-008-kafka-jitter-topic-at-least-once.md) |
