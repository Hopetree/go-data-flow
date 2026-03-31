# go-data-flow

轻量级数据管道框架，通过简单的 YAML 配置实现数据读取、处理、输出的自动化流程。

## 概述

```
     +--------------------------------------------------------------------------------------+                         
     |                                                                                      |                         
     |                                   Flow Pipeline                                      |                         
     |                                                                                      |                         
     |                                                                                      |                         
     |                   +---------------------------------------------+                    |                         
     |                   |                  Processor                  |                    |                         
     |                   |                                             |                    |                         
     |  +----------+     | +-----------------+      +---------------+  |      +----------+  |                         
     |  |          |     | |                 |      |               |  |      |          |  |                         
     |  |  Source  |---->| |  Processor[0]   |----->|  Processor[n] |  |----->|   Sink   |  |                         
     |  |          |     | |  (worker x N)   |      |  (worker x N) |  |      |          |  |                         
     |  +----------+     | +-----------------+      +---------------+  |      +----------+  |                         
     |       x1          |                                             |           x1       |                         
     |                   +---------------------------------------------+                    |                         
     |                                      x(0~M)                                          |                         
     |                                                                                      |                         
     |                                                                                      |                         
     +--------------------------------------------------------------------------------------+        
```

- **Source / Sink**：每个 Flow 仅一个，单线程运行
- **Processor**：支持多步骤链式处理，每一步可独立配置并发 worker 数
- **Channel 连接**：组件间通过带缓冲的 Go Channel 传递数据，天然背压

**核心特性：**

- **配置驱动** - 通过 YAML 文件定义数据处理流程，无需编写代码
- **组件化设计** - Source/Processor/Sink 三种组件灵活组合
- **类型安全** - 基于 Go 泛型，编译期检查类型错误
- **可扩展** - 支持自定义组件，即插即用
- **可观测** - 内置统计和错误收集

## 快速开始

### 安装

```bash
git clone https://github.com/yourname/go-data-flow.git
cd go-data-flow
go build -o dataflow ./cmd/dataflow
```

### 运行示例

```bash
# 运行内置示例（开发环境）
./dataflow -a examples/standard/app.yaml -c examples/standard/flow/01-simple.yaml

# 查看版本
./dataflow -v

# 列出所有组件
./dataflow -l
```

### 配置示例

```yaml
name: example-flow
buffer_size: 100

# 数据源：从 JSON 文件读取
source:
  name: source-json-file
  config:
    file_path: ./data/input.json
    format: array

# 处理器：过滤和转换
processors:
  - name: processor-condition-filter
    config:
      field: status
      op: eq
      value: "active"
  - name: processor-transform-field
    config:
      add:
        processed_at: "2024-01-01"

# 输出器：写入控制台
sink:
  name: sink-output-console
  config:
    format: json
    limit: 100
```

## 内置组件

### Source（数据源）

| 组件 | 说明 |
|------|------|
| `source-static-data` | 静态数据源 |
| `source-generator-sequence` | 序列生成器（用于测试） |
| `source-csv-file` | CSV 文件读取 |
| `source-json-file` | JSON 文件读取（支持 JSON Lines 和数组格式） |
| `source-kafka-consumer` | Kafka 消费者 |
| `source-python-script` | Python 脚本数据源 |

### Processor（处理器）

| 组件 | 说明 |
|------|------|
| `processor-condition-filter` | 条件过滤 |
| `processor-expr-filter` | 表达式求值 |
| `processor-transform-field` | 字段转换（添加/删除/重命名字段） |
| `processor-jq-transform` | JQ 表达式转换 |
| `processor-aggregate-group` | 聚合计算 |
| `processor-python-script` | Python 脚本处理 |

### Sink（输出器）

| 组件 | 说明 |
|------|------|
| `sink-output-console` | 控制台输出 |
| `sink-collect-memory` | 内存收集（用于测试） |
| `sink-null-discard` | 空输出（用于测试） |
| `sink-csv-file` | CSV 文件输出 |
| `sink-json-file` | JSON 文件输出 |
| `sink-python-script` | Python 脚本数据输出 |
| `sink-clickhouse` | ClickHouse 批量写入 |

## 命令行参数

```bash
./bin/dataflow [options]

Options:
  -a <file>     应用配置文件路径 (默认: app.yaml)
  -c <file>     Flow 配置文件路径
  -d <dir>      配置文件目录
  -C <files>    多个配置文件，逗号分隔
  -l            列出所有组件
  -v            显示版本
```

## 项目结构

```
go-data-flow/
├── cmd/dataflow/           # 应用入口
├── pkg/
│   ├── dataflow/           # 核心框架
│   │   ├── app/            # 应用框架
│   │   ├── builtins/       # 内置组件
│   │   └── metrics/        # 监控指标
│   └── logger/             # 日志模块
├── docs/
│   ├── design/             # 设计文档
│   └── guides/             # 使用指南
├── examples/
│   └── standard/           # 示例 (flow/, scripts/, data/)
└── scripts/                # 构建脚本
```

## 使用场景

- **日志分析** - 从日志文件读取 → 解析过滤 → 写入数据库
- **数据同步** - 从 MySQL 读取 → 转换格式 → 写入 ClickHouse
- **监控统计** - 从 Trace 系统读取 → 聚合计算 → 写入时序数据库
- **ETL 任务** - 从多个数据源读取 → 合并清洗 → 写入数据仓库

## 文档

- **使用指南** (`docs/guides/`)
  - [配置说明](docs/guides/00_配置说明.md)
  - [Source 组件](docs/guides/01_Source组件.md)
  - [Processor 组件](docs/guides/02_Processor组件.md)
  - [Sink 组件](docs/guides/03_Sink组件.md)
  - [Python 脚本组件](docs/guides/04_Python脚本组件.md)
- **设计文档** (`docs/design/`)
  - [自定义组件开发指南](docs/design/01_自定义组件开发指南.md)

## 开发

### 环境要求

- Go 1.25+

### 构建

```bash
# 构建
go build -o dataflow ./cmd/dataflow

# 运行测试
go test ./...
```

### 添加自定义组件

```go
package main

import (
    "github.com/yourorg/go-data-flow/pkg/dataflow"
)

// 实现 Source 接口
type MySource struct{}

func (s *MySource) Open(ctx context.Context) (<-chan dataflow.Record, error) {
    // 实现数据读取逻辑
}

func (s *MySource) Close() error {
    // 清理资源
}

// 注册组件
func Register(registry *dataflow.Registry) {
    registry.RegisterSource("source-my-source", func() dataflow.Source {
        return &MySource{}
    })
}
```

## License

MIT
