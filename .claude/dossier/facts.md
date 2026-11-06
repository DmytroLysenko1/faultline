# FACTS — детерминированный скан репозитория

HEAD `323dcb2fdd4f` · Go-файлов просканировано 47 · сгенерированных пропущено 0
module `github.com/DmytroLysenko1/faultline` · go 1.27.0

## Конвенции (счётчики — не мнение)
- Где объявлены интерфейсы: **consumer-side** 1 vs producer-side/ports 1 (50%) — конвенция СЛАБАЯ — спроси автора
- Видимость интерфейсов: **unexported (локальные порты)** 2 vs exported 0 (100%) — конвенция жёсткая
- Обёртка ошибок: **fmt.Errorf(%w)** 28 vs errors.Join 1 (97%) — конвенция жёсткая
- Ошибки прочее: sentinel `var Err… = errors.New` 10 · `fmt.Errorf` без %w 0 · `errors.Is` 2 · `errors.As/AsType` 0
- Ресиверы: **pointer** 44 vs value 25 (64%) — конвенция СЛАБАЯ — спроси автора
- Границы транзакций: нет данных

## Стек (по числу файлов с импортом)
- логирование: slog=4
- DI: —
- БД: —
- HTTP: —
- брокер/кэш: —
- наблюдаемость: —
- тесты: —
- прочее: samber_lo=9

## Конкурентность
- `go` стартов 0 · каналов 0 · sync 0 · atomic 1 · errgroup 0 · тикеров 0
- ctx: первым параметром 5 · WithTimeout 1 · Background/TODO 0 · ctx в поле структуры 0
- Точки старта горутин/тикеров (первые 40): —

## Тесты
- `_test.go`: table-driven 24 · t.Parallel 0
- race в Makefile/CI: True · make targets: btf-check, clean, cover-check, demo-capture, demo-down, fmt, gen-check, generate, help, kind-down, kind-up, lint, mod-check, proto-breaking, proto-lint
- CI workflows: ci.yml

## Риск-маркеры (эвристика — каждый проверяется глазами перед тем как верить)
- не найдено

## Внешние края (кандидаты на таймаут/ретрай/идемпотентность)
- —

## Крупнейшие пакеты (файлов)
- `internal/domain/topology/matcher` — 6
- `internal/domain/topology` — 5
- `internal/domain/topology/graph` — 5
- `internal/domain/topology/tombstone` — 3
- `internal/domain/telemetry/batch` — 2
- `internal/domain/telemetry/ids` — 2
- `internal/domain/telemetry/kind` — 2
- `internal/domain/telemetry/resource` — 2
- `internal/domain/telemetry/response` — 2
- `internal/domain/telemetry/span` — 2
- `internal/domain/topology/call` — 2
- `internal/domain/topology/clock` — 2
- `internal/domain/topology/role` — 2
- `hack/trimdump` — 1
- `internal/application/query` — 1
- `internal/domain/telemetry/telemetrytest` — 1
- `internal/domain/topology/topologytest` — 1
- `internal/infrastructure/memgraph` — 1
- `internal/interfaces/connectapi` — 1
- `internal/interfaces/connectapi/graphmap` — 1
