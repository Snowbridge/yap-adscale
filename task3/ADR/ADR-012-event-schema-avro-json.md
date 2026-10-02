# ADR-012: Схемы событий Kafka — Avro для high-volume, JSON для доменных команд

**Статус**: ✅ Принято

## Контекст

[ADR-004](../../task1/ADR/ADR-004-message-broker-kafka.md) зафиксировала Kafka и каталог топиков, но оставила сериализацию как «JSON или Protobuf». Task 3 требует выбрать **Avro или JSON** с обоснованием.

Нагрузка по топикам различается на порядки: `telemetry.events` — десятки тысяч сообщений/сек; `campaign.config` / `billing.balance` — низкий TPS, высокая ценность отладки и ручного разбора.

Команда разрабатывает на Go/C++/Python; Schema Registry совместим с Avro и обратной совместимостью полей (FR-9, P3 эволюция).

## Рассмотренные варианты

1. **JSON для всех топиков.** Отклонено для телеметрии: больший объём на диске и сеть; слабее контракт схемы при эволюции высокочастотных полей.
2. **Avro для всех топиков.** Отклонено для низкочастотных финансовых/config-сообщений: избыточная операционная сложность для редких ручных правок без выигрыша в объёме.
3. **Protobuf для всех.** Допустимо технически; для единообразия с экосистемой Kafka и явного schema evolution в задании выбираем **Avro** на high-volume, **JSON** на low-volume.
4. **Гибрид: Avro + Schema Registry для** `telemetry.events` **и** `auction.events`**; JSON envelope с `schema_version` для** `campaign.config`**,** `campaign.spend`**,** `billing.balance`**. **Принято.**

## Решение

1. **Avro + Confluent Schema Registry (или совместимый):**
   - `telemetry.events` — Impression/Click record (поля: `event_id`, `event_time`, `campaign_id`, `advertiser_id`, `creative_id`, `source`, `cost_micros`, …).
   - `auction.events` — BidOutcome record (`event_id`, `request_id`, `type`, `price_micros`, `campaign_id`, `timestamp`, `config_version`).
2. **JSON (UTF-8) с обязательными полями envelope:**
   - `schema_version` (int), `event_id` (UUID), `occurred_at` (ISO-8601), `payload` (object).
   - Топики: `campaign.config`, `campaign.spend`, `billing.balance`.
3. **Совместимость:** Avro — `BACKWARD` для consumers; JSON — только добавление optional полей в `payload`.
4. **Технические топики** JitterTopic/DLQ ([ADR-008](../../task2/ADR/ADR-008-kafka-jitter-topic-at-least-once.md)) наследуют формат исходного топика + metadata retry в headers.
5. **Этап 2 (опционально):** перевод `campaign.spend` на Avro при росте требований к аудиту и единому registry.

## Последствия

### Положительные

- Экономия диска и bandwidth на телеметрии; строгие контракты на потоке с наибольшим объёмом.
- Простая отладка редких финансовых сообщений в JSON.

### Отрицательные и компромиссы

- Два формата в эксплуатации; consumers должны знать deserializer per topic.
- Зависимость от Schema Registry (доступность, мониторинг).
