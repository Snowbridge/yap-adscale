# ADR-010: Изоляция Redis RTB и роль operational store

**Статус**: ✅ Принято

## Контекст

Redis в целевой архитектуре выполняет **две разные роли**, которые легко смешать:

1. **Operational store hot path RTB** — кампании, лимиты, балансы-проекции, счётчики, dedup ([ADR-006](../../task1/ADR/ADR-006-rtb-in-memory-hot-path.md), [bidding-service.md](../../task2/bidding-service.md)).
2. **Общесистемный кэш** — сессии кабинета, справочники UI, rate-limit counters на gateway.

[ADR-004](../../task1/ADR/ADR-004-message-broker-kafka.md) отверг использование Redis Streams как центральной шины событий, чтобы не размывать границы и не ставить durable log в тот же кластер, что и sub-ms торги.

При совместном кластере eviction, memory pressure, тяжёлые SCAN или админские FLUSH на кэше кабинета создают риск для P1 latency RTB.

## Рассмотренные варианты

1. **Один Redis для всей платформы (DB 0 RTB, DB 1 UI).** Отклонено: общий CPU/память и единый blast radius; нет жёсткой изоляции SLO.
2. **Только in-process кэш на RTB без Redis.** Отклонено: stateless горизонтальное масштабирование RTB требует общего операционного состояния и атомарных Lua между инстансами.
3. **Выделенный Redis Cluster для RTB; отдельный Redis (или только локальный кэш) для кабинета/edge.** **Принято.**



## Решение

1. **RTB Redis Cluster** — dedicated кластер в том же регионе, что поды RTB; private network; **только** ключи hot path и Cache Projector (паттерны из [bidding-service.md §4](../../task2/bidding-service.md)).
2. **Redis для RTB — operational store**, не «опциональный L2-кэш с fallback в SQL». Отсутствие в кэше или устаревшая версия конфигурации → **no-bid**, не read-through в PostgreSQL.
3. **Кабинет / UI:** опциональный **отдельный** Redis Sentinel/standalone или application-level cache (Caffeine); **запрещено** размещать session/cache кабинета в RTB Cluster.
4. **Персистентность RTB Redis:** AOF `everysec` + периодический RDB; после полной потери данных — **восстановление проекции** из compacted топиков `campaign.config`, `billing.balance` и replay при необходимости ([ADR-011](ADR-011-cqrs-projections.md)).
5. **Hash tags** `{campaign_id}` в ключах одной кампании для colocation в Redis Cluster ([ADR-013](ADR-013-scaling-replica-vs-shard.md)).



## Последствия



### Положительные

- Предсказуемая latency и изоляция SLO hot path от некритичного кэширования.
- Чёткая операционная граница: один runbook на RTB data plane.



### Отрицательные и компромиссы

- Два (или более) Redis-контура — дополнительная инфраструктура.
- Дублирование части справочных данных (кампания в PG и в Redis) — осознанная плата за CQRS.

