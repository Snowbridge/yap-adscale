# ADR-013: Стратегия масштабирования — репликация vs шардирование

**Статус**: ✅ Принято

## Контекст

Платформа должна выдержать рост с **≥ 18 000 RPS** RTB к **~50 000 RPS** ([02-requirements.md](../../task1/02-requirements.md)) и огромный поток телеметрии без деградации P1. Разные хранилища имеют разные узкие места: OLTP PG — умеренный объём; Redis — latency и память; ClickHouse и Kafka — throughput и диск.

Нужно зафиксировать, где достаточно **Master–Replica (read scaling / HA)**, где нужно **шардирование**, и нужны ли **read replicas** для аналитики.

## Рассмотренные варианты

1. **Шардировать PostgreSQL с первого дня по всем контекстам.** Отклонено: операционная сложность без доказанного объёма OLTP кампаний/биллинга; cross-shard транзакции не нужны при разделении контекстов.
2. **Только vertical scale всего.** Отклонено: не закрывает write-heavy телеметрию и горизонтальное масштабирование consumers.
3. **Поэтапно: репликация где OLTP/read split; шардирование где доказанный объём; Kafka partitions + Redis Cluster по умолчанию для hot path.** **Принято.**



## Решение



### PostgreSQL (`campaign-db`, `billing-db`)


| Механизм                        | Campaign    | Billing           | Назначение                                                      |
| ------------------------------- | ----------- | ----------------- | --------------------------------------------------------------- |
| Primary + async read replica    | ✅           | опционально       | Тяжёлые read-only запросы кабинета, отчёты по истории изменений |
| Sync standby (same region)      | опционально | ✅ предпочтительно | Минимальный RPO для денег                                       |
| Шардирование по `advertiser_id` | этап 2      | этап 2            | При > ~2 TB или CPU saturation primary                          |


**Master–Slave:** запись **только** на primary; RTB **не** использует PG replicas.

### Redis (RTB Cluster)

- **Шардирование:** hash slots Redis Cluster; ключи с `{campaign_id}` для colocation.
- **Read replicas:** чтение индексов кандидатов (Targeting) с replica; **запись** всегда в master shard.
- Масштабирование RTB-подов — stateless, N инстансов → один кластер.



### ClickHouse

- **Партиционирование:** `PARTITION BY toYYYYMM(event_time)` на локальных таблицах.
- **Шардирование:** `Distributed` по `cityHash64(advertiser_id)` или `campaign_id`.
- **Read replicas:** отдельные реплики для **Reporting API** vs фоновых **materialized views** — снижение конкуренции OLAP ([ADR-002](../../task1/ADR/ADR-002-analytics-read-replica.md) снимает нагрузку с PG монолита только на переходном этапе; **целевой** read path отчётности — ClickHouse, не PG replica).



### Kafka

- **Партиции** = единица параллелизма consumer group.
- Ключи: `campaign_id` (`telemetry.events`, `campaign.*`), `advertiser_id` (`billing.balance`).
- Увеличение partitions при росте lag и throughput; RF=3.



### CQRS и масштабирование

- Горизонтальное масштабирование **Cache Projector** и **Spend Aggregator** — увеличение consumer instances в пределах partitions топика.
- CQRS не требует шардирования write-модели PG до появления метрик saturation.



## Последствия



### Положительные

- Соответствие P2 масштабируемости без premature sharding OLTP.
- Предсказуемый путь роста с измеримыми триггерами шардирования PG.



### Отрицательные и компромиссы

- Replication lag на campaign read replica — не использовать для «прочитал сразу после записи» без primary.
- Redis read-your-writes на replica не гарантирован — Targeting должен учитывать staleness или читать master для критичных проверок.

