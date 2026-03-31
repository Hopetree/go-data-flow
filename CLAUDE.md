# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

go-data-flow is a lightweight, YAML-driven data pipeline framework in Go. Data flows through a `Source -> Processor(s) -> Sink` pipeline connected by buffered Go channels, with built-in backpressure. All component interfaces are generic over `Record` (`map[string]interface{}`).

## Tech Stack

- Go 1.25.0
- Logging: uber-go/zap + lumberjack (file rotation)
- Kafka: IBM/sarama
- Expression: expr-lang/expr, itchyny/gojq
- Metrics: prometheus/client_golang
- Config: gopkg.in/yaml.v3
- Database: ClickHouse (clickhouse-go/v2, in builtins/sink/clickhouse)

## Commands

- **Dev build**: `go build -o dataflow ./cmd/dataflow`
- **Cross-platform build**: `./scripts/build.sh -p <mac|linux|windows|all> -a <amd64|arm64> -v <version>`
- **Test**: `go test ./...` or `go test -v -race -coverprofile=coverage.out ./...`
- **Single test**: `go test -v -run TestFuncName ./pkg/dataflow/...`
- **Lint**: `golangci-lint run` (uses `.golangci.yml` — errcheck, staticcheck, govet, gofmt, goimports, revive)
- **Run example**: `./dataflow -a examples/standard/app.yaml -c examples/standard/flow/01-simple.yaml`
- **List components**: `./dataflow -l`

## Architecture

### Core Interfaces (`pkg/dataflow/`)

Three generic interfaces drive everything — `Source[T]`, `Processor[T]`, `Sink[T]` — each with `Init(config []byte) error` plus their primary method (`Read`/`Process`/`Consume`). `Flow[T]` in `flow.go` orchestrates them via channels. Source and Sink run as single goroutines; Processors can fan-out with N concurrent workers.

Optional interfaces components may implement:
- `ConcurrencyCapable` — declares worker concurrency (`BaseProcessor` = single-goroutine default; `StatelessProcessor` = concurrent)
- `Closer` — resource cleanup
- `MetricsRecorderAware` / `MetricsCapable` — Prometheus metrics injection and custom metrics

### Component Registry (`registry.go`)

Factory-based registration: `RegisterSource(name, builder)`, `RegisterProcessor(name, builder)`, `RegisterSink(name, builder)`. All built-ins wired in `pkg/dataflow/app/builtins.go` via `RegisterAllBuiltins()`.

### Application Framework (`pkg/dataflow/app/`)

`App` in `app.go` creates the registry, loads YAML config, manages lifecycle, and runs flows (single, parallel, or sequential). Global config (`app.yaml`) controls logging, Prometheus metrics HTTP server, parallel mode, and buffer sizes.

### Python Script System (`pkg/dataflow/builtins/python/`)

Python scripts act as Source/Processor/Sink via subprocess communication. `runner.Runner` manages Python process lifecycle using JSON lines over stdin/stdout.

### Built-in Components (`pkg/dataflow/builtins/`)

- **Sources**: static, generator-sequence, csv-file, json-file, kafka
- **Processors**: condition-filter, transform-field, expr, jq-transform, aggregate, python
- **Sinks**: output-console, collect, null, file, csv, json, clickhouse, python

### Config Structure

Flow YAML files define: `name`, `buffer_size`, `source` (single), `processors[]` (chain), `sink` (single). CLI flags: `-a` (app config), `-c` (single flow), `-d` (directory scan `*.yaml`/`*.yml`), `-C` (comma-separated flows), `-l`, `-v`.

## Project Structure

```
cmd/dataflow/           # 应用入口 (main.go)
pkg/
  dataflow/             # 核心框架 (interfaces, flow, registry, metrics)
    app/                # 应用框架 (config loading, lifecycle)
    builtins/           # 内置组件 (source/, processor/, sink/, python/, types/)
  logger/               # 日志模块 (zap + lumberjack)
docs/                   # 文档 (design/, guides/)
examples/
  standard/             # 内置组件示例 (flow/, scripts/, data/)
scripts/                # 构建脚本 (build.sh)
```

## Code Conventions

- All code comments must be in Chinese — package, function, struct, and inline
- Git commits follow `type(category): description` (feat/fix/docs/refactor/test/chore/perf/style)
- Only execute git operations when explicitly requested by the user
- New components embed `BaseProcessor[T]` (single-goroutine) or `StatelessProcessor[T]` (concurrent)
- Record type is `map[string]interface{}` — defined in `pkg/dataflow/builtins/types/record.go`
- Docs: `docs/design/` (developer docs), `docs/guides/` (user guides), `docs/requirements/` (read-only, for AI)
