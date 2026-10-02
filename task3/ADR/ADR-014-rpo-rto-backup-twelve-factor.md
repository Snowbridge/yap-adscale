# ADR-014: RPO/RTO, резервное копирование и соответствие 12-Factor

**Статус**: ✅ Принято

## Контекст

AdScale имеет разные требования к потере данных: **P1** — доступность RTB; **P2** — финансовая согласованность; **FR-9** — допустимая задержка отчётности. Единая политика backup для всех компонентов неприемлема.

Микросервисная целевая модель предполагает **stateless** процессы и подключаемые ресурсы ([ADR-001](../../task1/ADR/ADR-001-evolution-strategy.md)); эксплуатация без выделенного SRE (AD-7) требует **простых** runbook и автоматизируемых бэкапов.

## Рассмотренные варианты

1. **Одинаковые RPO/RTO 15 минут для всего.** Отклонено: избыточная стоимость для CH; недостаточно строго для биллинга.
2. **Только репликация без backup.** Отклонено: реплика не защищает от логической порчи, ошибочного DELETE, ransomware.
3. **Дифференцированные RPO/RTO + PITR для PG, AOF+RDB+Kafka replay для RTB, CH backup + re-ingest.** **Принято.**

## Решение

### Целевые RPO / RTO (ориентиры TO-BE)

| Компонент / сервис                   | RPO                    | RTO         | Backup / восстановление                                                                      |
|--------------------------------------|------------------------|-------------|----------------------------------------------------------------------------------------------|
| RTB Redis Cluster                    | 1–5 min (AOF everysec) | &lt; 15 min | AOF + RDB snapshot; полное восстановление проекции из Kafka compacted                        |
| Campaign PostgreSQL                  | 0–1 min (WAL/PITR)     | 30–60 min   | Daily full + continuous WAL; promote replica                                                 |
| Billing PostgreSQL                   | ~0 (sync replica)      | 30–60 min   | То же + обязательная сверка балансов после restore                                           |
| ClickHouse                           | ~1 h (snapshot)        | 2–4 h       | ReplicatedMergeTree + backup в object storage; при катастрофе — re-ingest из Kafka retention |
| Kafka (бизнес-топики)                | 0 в пределах retention | 1–2 h       | RF=3; retention как буфер DR; мониторинг under-replicated partitions                         |
| Stateless сервисы (RTB, Ingest, API) | n/a                    | &lt; 10 min | Redeploy из образа; config из env/secrets                                                    |

### Стратегия резервного копирования

1. **PostgreSQL:** `pg_basebackup` + WAL archiving в object storage; ежемесячный тест restore на staging.
2. **Redis RTB:** RDB каждые N часов + AOF; документированный порядок: поднять новый кластер → запустить Cache Projector с начала compacted log.
3. **ClickHouse:** `BACKUP TO S3` (или `clickhouse-backup`); TTL на сырых данных согласован с FR-9.
4. **Kafka:** не заменяет БД; retention ≥ максимальный ожидаемый downtime consumer + запас на late events.

### 12-Factor App (применимость к AdScale)

| Фактор                | Применение                                                                                    |
|-----------------------|-----------------------------------------------------------------------------------------------|
| I Codebase            | Один repo/сервис на deployable unit внутри подсистемы                                         |
| II Dependencies       | Явные зависимости (Go modules, etc.)                                                          |
| III Config            | Строки подключения к PG/Redis/CH/Kafka — **env**, не в образе                                 |
| IV Backing services   | PG, Redis, CH, Kafka — attachable resources, сменяемы по endpoint                             |
| V Build, release, run | CI build → immutable image → run                                                              |
| VI Processes          | RTB/Ingest stateless; долгоживущее состояние только в backing services                        |
| VII Port binding      | Сервисы слушают порты контейнера                                                              |
| VIII Concurrency      | Scale out процессов; partitions Kafka / Redis slots                                           |
| IX Disposability      | Быстрый старт; graceful shutdown consumers; cache warming при старте Projector                |
| X Dev/prod parity     | Те же backing services (меньше scale в dev)                                                   |
| XI Logs               | stdout → централизованный агрегатор (FR-13)                                                   |
| XII Admin processes   | Миграции Flyway/Liquibase, CH backfill, reconciliation PG↔Redis — one-off jobs, не в hot path |

## Последствия

### Положительные

- Согласованность с приоритетами P1/P2/FR-9; понятные ожидания стейкхолдеров.
- Stateless RTB упрощает RTO вычислительного слоя.

### Отрицательные и компромиссы

- Sync replica billing увеличивает latency commit (приемлемо для низкого TPS биллинга).
- Полное восстановление CH из Kafka ограничено retention — длинный retention на `telemetry.events` увеличивает стоимость диска.
