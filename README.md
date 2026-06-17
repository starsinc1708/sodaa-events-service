# Event Analytics Service

Сервис аналитики событий: принимает события по gRPC, публикует их в Kafka напрямую
после сохранения в БД, фоновый worker идемпотентно агрегирует счётчики в
PostgreSQL, а API отдаёт агрегированную статистику.


## Архитектура

Жизненный цикл события:

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as "api (gRPC)"
    participant DB as PostgreSQL
    participant K as Kafka
    participant W as worker

    C->>A: CreateEvent
    A->>DB: INSERT events
    A->>K: publish event
    A-->>C: event

    K->>W: deliver (consumer group, батч)
    W->>DB: BEGIN, batch dedup + UPSERT event_stats, COMMIT
    W->>K: commit offset (только после commit БД)
```

**Поток данных.** `CreateEvent` сохраняет событие в БД и сразу публикует его в Kafka.
Для аналитики потеря сообщения при сбое брокера не критична — событие сохранено,
уникальность не страдает. Worker читает из Kafka и идемпотентно обновляет `event_stats`.

## Запуск

Требуется только Docker. Миграции применяются автоматически (сервис `migrate`).

```bash
docker compose up
# или: make up
```

Поднимаются: `postgres`, `kafka` (KRaft, без Zookeeper), `clickhouse` (опционально,
используется при `CLICKHOUSE_ENABLED=true`), `migrate` (одноразово), `api` (gRPC
:50051), `worker`.

gRPC доступен на `localhost:50051`. Reflection отключён (прод-настройка), поэтому
для ручных вызовов используйте сгенерированные клиенты из `api/proto`.

Остановить и очистить тома: `docker compose down -v` (или `make down`).

## API (gRPC, `event.v1.EventService`)

| Метод | Описание |
| --- | --- |
| `CreateEvent` | Создать событие (валидация `user_id`/`event_type`) |
| `ListEvents` | Список с фильтрацией по `user_id`/`event_type` и keyset-пагинацией (`page_token` → `next_page_token`) |
| `GetStats` | Агрегаты: `total_events`, `by_type`, `unique_users` |
| `grpc.health.v1.Health` | Healthcheck (стандартный сервис, совместим с grpcurl/k8s) |

Контракт — в [api/proto/events.proto](api/proto/events.proto).

## Middleware (gRPC unary interceptors)

Порядок: `Logging` → `Recovery` → `RateLimit` (Logging внешний, чтобы access-лог
писался даже для panic→Internal и отказов rate-limit).

- **Logging** — структурный лог (slog) каждого вызова: метод, код ответа, длительность в ms.
- **RateLimit** — корзина токенов на клиента по IP, 60 запросов/мин (настраивается
  `RATE_LIMIT_PER_MINUTE`). Превышение → `codes.ResourceExhausted` (gRPC-аналог
  429). Неактивные клиенты выселяются из памяти.
- **Recovery** — паника → `codes.Internal`, сервер не падает.

## ClickHouse (опциональная часть ТЗ)

По умолчанию `GetStats` читает агрегаты из Postgres. Если выставить
`CLICKHOUSE_ENABLED=true`, включается аналитический путь на ClickHouse:

- **worker** пишет сырые события пачкой в ClickHouse-таблицу `events`
  (`MergeTree`, партиции по месяцам, `LowCardinality(event_type)`);
- materialized view `event_agg_mv` на каждой вставке поддерживает
  `AggregatingMergeTree` `event_agg` с HLL-состояниями `uniqState(id)` (на тип) и
  `uniqState(user_id)`;
- **api** считает `GetStats` через `uniqMerge` по `event_agg` (≈ число типов строк),
  а не сканом сырой таблицы — латентность не зависит от объёма данных
  (см. раздел про деградацию: ~46 ms на 20M против ~4 s при полном скане).

Идемпотентность: дубли строк (at-least-once повтор) не завышают счётчики, т.к.
`uniq` считает по значению; запись в ClickHouse при ошибке транзиентна (offset не
коммитится → повтор), что вместе с дедупом по `event_id` в Postgres даёт
согласованность обоих сторов. Цена `uniq` — приближённость (~2%); нужны точные
`total`/`by_type` — берите их из Postgres `event_stats`.

Схема ClickHouse (таблицы + MV) создаётся приложением идемпотентно
(`CREATE … IF NOT EXISTS` на старте) — отдельный golang-migrate для опционального
стора не заводился.

Включение: `CLICKHOUSE_ENABLED=true` в `.env`, затем `docker compose up`.

## Структура проекта

```
cmd/api          точка входа API (gRPC-сервер, health)
cmd/worker       точка входа worker (Kafka consumer)
internal/domain  сущности и интерфейсы (порты)
internal/service бизнес-логика
internal/repository/postgres   pgx-репозитории (events, stats)
internal/repository/clickhouse ClickHouse-стор (опционально): запись + GetStats
internal/broker/kafka          producer, consumer, DLQ
internal/transport/grpc        handler + интерсепторы
internal/config  конфигурация (env)
migrations       SQL-миграции (golang-migrate)
api/proto        proto-контракт и сгенерированный код
```

## Оценка Go-подхода

**Почему такая структура.** `cmd/` + `internal/` — стандартный лэйаут Go-проектов.
Два `cmd/` (api, worker) позволяют собрать два бинарника из одного модуля.

**Разделение слоёв.** Три слоя без фреймворков:
- `domain` — сущности и интерфейсы-порты (`EventRepository`, `EventPublisher`), не зависит ни от чего;
- `service` — бизнес-логика, зависит только от `domain`;
- `repository` / `broker` / `transport` — адаптеры, реализуют порты, зависят от инфраструктуры (pgx, kafka-go, gRPC).

Согласно чистой архитектуре, все зависимости направлены к домену: `transport → service → domain ← repository`.

**Работа с БД.** `pgxpool` — connection pool поверх `pgx`, прямой SQL без ORM.
Интерфейс `EventRepository` скрывает детали: сервис не знает про SQL,
тесты подставляют mock. Миграции — отдельный контейнер `golang-migrate`,
применяются один раз при старте.

**Работа с очередью.** `kafka-go` от segmentio, consumer group с ручным коммитом.
Offset коммитится только после commit БД — гарантия at-least-once без потерь.
Батчевая обработка снижает число транзакций. Подход основан на best practices
[Kafka consumer group patterns](https://www.confluent.io/blog/turning-the-database-inside-out-with-apache-samza/)
и [segmentio/kafka-go examples](https://github.com/segmentio/kafka-go/tree/main/examples/consumer-group).

**Обработка ошибок.** Домен возвращает Sentinel-ошибки (`ErrInvalidUserID`, …).
Сервис оборачивает их через `fmt.Errorf("…: %w", err)`. gRPC-слой маппит
их в статус-коды: известные → `InvalidArgument`, остальное →
`Internal` с логированием реальной причины.

**Миграции.** `golang-migrate` в отдельном Docker-сервисе (`migrate`). Нумерация
`000001`–`000005`, каждая миграция идемпотентна (`IF NOT EXISTS`). Зависимость
`depends_on: postgres(healthy)` гарантирует порядок.

**Тесты.** Unit-тесты через mock-реализации интерфейсов `domain` — быстрые,
без инфраструктуры. Интеграционные — build-tag `integration`, поднимаются на
реальной БД. Покрывают 5 ключевых сценариев ТЗ + изоляцию rate-limit + DLQ.

**Развитие до production:**
- retention-политика на `processed_events` (партиции по `processed_at`);
- `unique_users` в Postgres-пути через HLL вместо `COUNT(DISTINCT)`;
- mTLS на gRPC, сервис-меш;
- Prometheus-метрики (задержка, пропускная способность);
- партиционирование `events` по `created_at`.

**Компромиссы из-за ограничений:**
- нет метрик и трейсинга;
- `processed_events` растёт неограниченно (retention не реализован);
- postgres-путь `unique_users` деградирует на больших объёмах;
- параметры батчей/интервалов захардкожены, не вынесены в конфиг.

## Тесты

```bash
make test              # юнит-тесты (handler, rate limit 61-й запрос, worker)
make test-race         # то же с -race (нужен gcc/CGO)
make test-integration  # реальная агрегация/дедуп против Postgres и ClickHouse:
                       # TEST_POSTGRES_DSN=postgres://app:app@localhost:5432/events?sslmode=disable
                       # TEST_CLICKHOUSE_ADDR=localhost:9000
```

Покрытие ключевых сценариев ТЗ: создание+валидация, фильтрация списка, маппинг
статистики, срабатывание rate limit на 61-м запросе, обработка сообщения worker'ом
(валид/дубль/transient/poison→DLQ). Корректность реальной SQL-агрегации и
идемпотентности — в build-tagged интеграционном тесте.

### Деградация чтения по объёму данных

Латентность чтения (localhost, без сетевого RTT, `-c 1`) на росте данных:

| Строк | ListEvents (PG keyset) | GetStats — было (полный скан) | GetStats — стало (материализ. агрегат) |
| --- | --- | --- | --- |
| 1M | 1.7 ms | 186 ms | 27 ms |
| 5M | 1.5 ms | 888 ms | 38 ms |
| 20M | ~1.5 ms | **3 990 ms** | **46 ms** (~87× быстрее) |

- **ListEvents — деградации нет**: keyset-пагинация + составные индексы (миграция
  `000005`) дают O(log n) на seek независимо от объёма таблицы и глубины страницы.
- **GetStats**: раньше `uniqExact` по всей таблице рос линейно с объёмом; теперь
  читается из материализованного `event_agg` (`AggregatingMergeTree` + MV с
  HLL-состояниями `uniqState`) через `uniqMerge` по ~числу типов строк → латентность
  плоская. Цена — приближённые счётчики (HLL `uniq`, ~2%); для точных `total`/`by_type`
  остаётся материализованный `event_stats` в Postgres.

## Принятые технические решения (кратко)

- **Прямая публикация в Kafka** из обработчика после сохранения в БД — для
  аналитического сервиса потеря единичного сообщения при сбое брокера некритична,
  а накладные расходы transactional outbox неоправданы.
- **At-least-once + идемпотентность**: точный exactly-once между Kafka и внешней БД
  невозможен без CDC; worker дедуплицирует по `event_id` (таблица
  `processed_events`) и инкрементит счётчик в одной транзакции, offset Kafka
  коммитится только после commit БД. Дубль при повторной доставке безвреден.
- **Keyset-пагинация** (`(created_at, id)`-курсор) вместо OFFSET — стабильна и не
  деградирует на больших смещениях.
- **Kafka-топик создаётся явно на старте** (`EnsureTopics`), а не ленивым
  авто-созданием: иначе consumer, стартовавший раньше первой публикации, не
  подхватывает появившийся позже топик. Образ — официальный `apache/kafka` (KRaft).
- **GetStats** по умолчанию читает `total`/`by_type` из материализованной
  `event_stats` (поддерживается worker'ом), `unique_users` — из сырой `events`
  через `COUNT(DISTINCT user_id)`. Опционально источником может быть ClickHouse
  (см. раздел выше) — это плюс по ТЗ.

### Известные компромиссы / возможное развитие

- `processed_events` растёт неограниченно — в проде нужна retention/партиционирование
  по `processed_at` (есть индекс под очистку).
- Postgres-путь `GetStats`: `total`/`by_type` плоские (из `event_stats`), но
  `unique_users` через `COUNT(DISTINCT user_id)` сканирует `events` и деградирует
  с объёмом — для прода вынести в HLL/материализованный счётчик (в ClickHouse-пути
  это уже решено через `uniq`).
- ClickHouse-счётчики приближённые (HLL `uniq`, ~2%) — плата за плоскую latency
  на больших объёмах; нужны точные значения — Postgres `event_stats`.
- TLS на gRPC не настроен (вне scope ТЗ); для прода — mTLS/сервис-меш.

Доп. задания:
- 11 [модуль обработки результатов поставщика](11_module_realization_plan.md).
- 12 [Дополнительная часть: Модуль биллинга](12_billing_module.md)
