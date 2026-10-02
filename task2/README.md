# Task 2 — RTB-сервис ставок и интеграция с DSP

Детализация критичного компонента целевой архитектуры AdScale: **сервис ставок (Bidding Service)**, протоколы взаимодействия и **RTB / Telemetry Gateway** для новой DSP-площадки. Опирается на целевое видение [Task 1](../task1/readme.md) и ADR Task1 [ADR-005](../task1/ADR/ADR-005-dual-edge-gateway.md), [ADR-006](../task1/ADR/ADR-006-rtb-in-memory-hot-path.md), а также [ADR-007](ADR/ADR-007-grpc-critical-path-rest-elsewhere.md) (gRPC / REST).

## Индекс материалов

| Файл | Содержание |
| ------ | ------------ |
| [`bidding-service.md`](bidding-service.md) | Границы сервиса ставок, API, зависимости, модель данных Redis |
| [`interaction.md`](interaction.md) | Протоколы (gRPC / REST / Kafka), паттерны надёжности; gRPC vs REST — [ADR-007](ADR/ADR-007-grpc-critical-path-rest-elsewhere.md) |
| [`ADR/ADR-007-grpc-critical-path-rest-elsewhere.md`](ADR/ADR-007-grpc-critical-path-rest-elsewhere.md) | gRPC только на critical path; REST для всего остального sync |
| [`ADR/ADR-008-kafka-jitter-topic-at-least-once.md`](ADR/ADR-008-kafka-jitter-topic-at-least-once.md) | подход к реализации at-least-once при обработке сообщений Kafka |
| [`api-gateway.md`](api-gateway.md) | Дизайн RTB Gateway для DSP: маршрутизация, RL, auth, CB (circuit breaker), мониторинг |
| [`diagrams/01-component-bidding-rtb.puml`](diagrams/01-component-bidding-rtb.puml) | C4 Component — подсистема RTB |
| [`diagrams/02-sequence-dsp-bid-request.puml`](diagrams/02-sequence-dsp-bid-request.puml) | Sequence — успешный bid request |
| [`diagrams/03-sequence-reliability-degradation.puml`](diagrams/03-sequence-reliability-degradation.puml) | Sequence — деградация и идемпотентность |

## Сводка решения

**Hot path (sync):** DSP → **HTTPS OpenRTB** → **RTB Gateway** → **gRPC** (Targeting → Bidding → Creative Delivery) → **Redis** → ответ партнёру.

**Async:** показы/клики → Telemetry Ingest → Kafka `telemetry.events`; конфиг и балансы → Kafka → проекция в Redis; исход аукциона → Kafka `auction.events` (fire-and-forget, не блокирует ответ).

**Campaign / Billing:** только через Kafka-проекции и некритичный REST кабинета — **без** синхронных вызовов с пути ставки ([ADR-006](../task1/ADR/ADR-006-rtb-in-memory-hot-path.md)).

## Целевые показатели (P1)

Источник: [02-requirements.md](../task1/02-requirements.md).

| Метрика | Значение |
| --------- | ---------- |
| RPS на RTB-контуре (пик) | ≥ 18 000 (промежуточная цель) |
| P95 latency (внутренний SLA) | ≤ 100 ms |
| Устойчивый ответ DSP | ≤ 80 ms |
| Функциональность интеграции | FR-1 … FR-6, наблюдаемость FR-13 |

## Выделение из монолита

| AS-IS (монолит) | TO-BE (подсистема RTB) |
| ----------------- | ------------------------- |
| Ad Server | Targeting Service |
| Auction Engine | **Bidding Service** (фокус Task 2) |
| Delivery Service | Creative Delivery Service |

Парсинг OpenRTB и партнёрская безопасность — на **RTB Gateway**, не в Bidding Service.
