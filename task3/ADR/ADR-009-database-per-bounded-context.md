# ADR-009: Database per bounded context — изоляция хранилищ по ограниченным контекстам

**Статус**: ✅ Принято

## Контекст

В AS-IS все ограниченные контексты AdScale ([03-bounded-contexts.md](../../task1/03-bounded-contexts.md)) используют **одну PostgreSQL**, что создаёт конфликт паттернов доступа: sub-ms RTB, write-heavy телеметрия, тяжёлый OLAP и строгий ACID биллинга конкурируют за одни ресурсы (AD-2, AD-4 в [01-drivers-and-constraints.md](../../task1/01-drivers-and-constraints.md)).

Целевая C4 ([05-c4-container-to-be.puml](../../task1/diagrams/05-c4-container-to-be.puml)) и [readme Task 1](../../task1/readme.md) фиксируют **database per subsystem**: Redis (RTB), PostgreSQL (кампании), PostgreSQL (биллинг), ClickHouse (телеметрия/отчётность).

## Рассмотренные варианты

1. **Одна PostgreSQL с отдельными схемами на контекст.** Отклонено: общий инстанс сохраняет конкуренцию за I/O и блокировки; единая точка отказа; невозможно независимо масштабировать write path телеметрии и OLTP.
2. **Shared PostgreSQL для Campaign + Billing.** Отклонено: финансовый контур требует изоляции списаний и независимых политик backup/RPO; смешение увеличивает blast radius при ошибке миграции или тяжёлом запросе кабинета.
3. **PostgreSQL на RTB hot path (read-through кэш).** Отклонено в [ADR-006](../../task1/ADR/ADR-006-rtb-in-memory-hot-path.md): несовместимо с P1 latency.
4. **Отдельное хранилище на контекст по профилю нагрузки: Redis Cluster (RTB), два инстанса PostgreSQL (campaign, billing), ClickHouse (analytics).** **Принято.**



## Решение

1. **Аукционы и Выдача рекламы (RTB Core):** единственное хранилище подсистемы — **Redis Cluster** (персистентность AOF/RDB). Собственной PostgreSQL **нет**. Долгоживущие сущности — в PostgreSQL соседних контекстов; операционное состояние торгов — проекция из Kafka ([ADR-006](../../task1/ADR/ADR-006-rtb-in-memory-hot-path.md)).
2. **Управление кампаниями и ставками:** **отдельный PostgreSQL** (`campaign-db`) — кампании, line items, креативы, шаблоны, бюджеты, правила ставок, **агрегаты расхода** для лимитов и публикации `campaign.spend` ([ADR-003](../../task1/ADR/ADR-003-billing-spend-aggregates-source.md)).
3. **Финансы и Биллинг:** **отдельный PostgreSQL** (`billing-db`) — балансы, журнал транзакций, инвойсы, акты сверки. Физически **другой** инстанс, не `campaign-db`.
4. **Телеметрия и Отчётность:** **ClickHouse** — сырые события и OLAP-агрегаты (materialized views). **Не** использовать общую OLTP PostgreSQL для записи показов/кликов в целевом состоянии.
5. **Микросервисы внутри подсистемы:**
  - RTB (Targeting, Bidding, Creative): **общий** Redis Cluster подсистемы; Cache Projector — без отдельной БД.
  - Campaign (Campaign API, Spend Aggregator): **одна** `campaign-db`, схемы `campaign` и `spend_agg` до выделения второго инстанса при росте нагрузки.
  - Edge Identity (Keycloak): встроенное хранилище IdP; **не** участвует в доменных транзакциях RTB.
6. **Запреты:** cross-context **синхронный** доступ к чужой БД; RTB **не** читает `campaign-db` / `billing-db` на bid path.



## Последствия



### Положительные

- Изоляция нагрузок: телеметрия и отчётность не «топят» OLTP аукциона.
- Независимое масштабирование и политики DR по критичности (деньги vs hot path).
- Явные границы владения данными при декомпозиции монолита ([ADR-001](../../task1/ADR/ADR-001-evolution-strategy.md)).



### Отрицательные и компромиссы

- Консистентность между PG и Redis/CH через Kafka возможна только в конечном итоге (в любой отдельный момент времени возможен рассинхрон — плата за изкий latency); нужны контракты и идемпотентность.
- Больше операционных объектов (четыре класса хранилищ + Kafka).
- Два PG-инстанса — выше базовая стоимость, чем одна БД со схемами.

