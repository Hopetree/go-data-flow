# Source 组件

Source 组件是数据流的起点，负责从外部系统读取数据并发送到处理管道。

## 组件总览

| 组件名 | 目录 | 功能 | 数据源 |
|--------|------|------|--------|
| `source-static-data` | source/static | 静态内存数据 | 配置文件中的静态数据 |
| `source-generator-sequence` | source/generator | 序列生成器 | 自动生成的测试数据 |
| `source-csv-file` | source/csv | CSV 文件读取 | CSV 文件 |
| `source-json-file` | source/json | JSON 文件读取 | JSON 数组或 JSON Lines |
| `source-kafka-consumer` | source/kafka | Kafka 消费者 | Kafka Topic |

## 通用配置

所有 Source 组件都实现 `dataflow.Source` 接口：

```go
type Source[T any] interface {
    Init(config []byte) error
    Read(ctx context.Context, out chan<- T) (int64, error)
}
```

---

## source-static-data

### 作用

从配置中读取静态数据，适用于测试和演示场景。

### 数据源

配置文件中定义的静态数据数组。

### 输出格式

每条记录是一个 `map[string]interface{}` 对象。

### 配置项

| 配置项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| data | array | 是 | 静态数据数组 |

### 示例

```yaml
source:
  name: source-static-data
  config:
    data:
      - id: 1
        name: Alice
        score: 95
      - id: 2
        name: Bob
        score: 72
```

---

## source-generator-sequence

### 作用

生成测试数据，适用于性能测试和压力测试。

### 数据源

根据配置规则自动生成数据。

### 输出格式

每条记录包含 `id`, `timestamp` 等字段。

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| count | int | 否 | 0 | 生成记录数量，0 表示无限生成 |
| interval_ms | int | 否 | 0 | 生成间隔（毫秒），0 表示无间隔连续生成 |

### 示例

```yaml
# 生成 100 条数据，每条间隔 10ms
source:
  name: source-generator-sequence
  config:
    count: 100
    interval_ms: 10
```

```yaml
# 无限生成数据，每秒 1 条（用于持续测试）
source:
  name: source-generator-sequence
  config:
    count: 0
    interval_ms: 1000
```

---

## source-csv-file

### 作用

从 CSV 文件读取数据。

### 数据源

CSV 文件，- 第一行为表头
- 后续行为数据行

### 输出格式

每行转换为一个 `map[string]interface{}` 对象，字段名来自 CSV 表头。

### 配置项

| 配置项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| path | string | 是 | CSV 文件路径 |
| delimiter | string | 否 | 逗号 (,) | 字段分隔符 |
| skip_header | bool | 否 | false | 是否跳过表头 |

### 示例

```yaml
source:
  name: source-csv-file
  config:
    path: ./data/input.csv
    delimiter: ","
```

**CSV 文件示例：**
```csv
id,name,score
1,Alice,95
2,Bob,72
3,Charlie,88
```

---

## source-json-file

### 作用

从 JSON 文件读取数据。

### 数据源

JSON 文件，支持两种格式：
- **JSON 数组**: 整个文件是一个 JSON 数组 `[{...}, {...}]`
- **JSON Lines**: 每行一个独立的 JSON 对象

### 输出格式

每个 JSON 对象转换为一个 `map[string]interface{}` 对象。

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| file_path | string | 是 | - | JSON 文件路径，支持 glob 模式 |
| format | string | 否 | lines | 文件格式: `lines` (JSON Lines) 或 `array` (JSON 数组) |
| batch_size | int | 否 | 0 | 每次读取的批次大小，0 表示无限制 |

### 示例

**JSON 数组格式（需要指定 format: array）：**
```yaml
source:
  name: source-json-file
  config:
    file_path: ./data/input.json
    format: array  # 重要！JSON 数组格式必须指定
```

**JSON Lines 格式（默认）：**
```yaml
source:
  name: source-json-file
  config:
    file_path: ./data/input.jsonl
    # format: lines  # 默认值，可省略
```

**JSON 文件示例（数组格式）：**
```json
[
  {"id": 1, "name": "Alice", "score": 95},
  {"id": 2, "name": "Bob", "score": 72}
]
```

**JSON 文件示例（JSON Lines 格式）：**
```json
{"id": 1, "name": "Alice", "score": 95}
{"id": 2, "name": "Bob", "score": 72}
```

### 常见问题

**问题**: JSON 数组文件读取不到数据（输入=0, 输出=0）

**原因**: JSON 数组格式的文件默认会被当作 JSON Lines 处理，导致解析失败。

**解决**: 添加 `format: "array"` 配置项：
```yaml
source:
  name: source-json-file
  config:
    file_path: ./data/input.json
    format: array  # 关键配置
```

---

## source-kafka-consumer

### 作用

从 Kafka Topic 消费消息。

### 数据源

Kafka Topic 中的消息。

### 输出格式

每条 Kafka 消息转换为一个 Record 对象，包含以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| key | string | 消息键 |
| value | string | 消息值（原始内容） |
| topic | string | 来源 Topic |
| partition | int | 分区号 |
| offset | int64 | 偏移量 |
| timestamp | int64 | 时间戳（Unix 时间） |

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| brokers | []string | 是 | - | Kafka 集群地址列表 |
| topic | string | 是 | - | 消费的 Topic |
| group_id | string | 是 | - | 消费者组 ID |
| offset | string | 否 | newest | 起始位置: newest 或 oldest |

### 示例

```yaml
source:
  name: source-kafka-consumer
  config:
    brokers:
      - localhost:9092
      - localhost:9093
    topic: user-events
    group_id: dataflow-consumer
    offset: newest
```

### 持续运行

Kafka Source 是持续运行的组件，会一直消费消息直到收到中断信号（Ctrl+C）。

### 处理 JSON 消息

Kafka 消息的 `value` 字段通常是 JSON 字符串。如果需要解析，可以在 Processor 中处理：

```yaml
processors:
  - name: transform
    config:
      # 解析 value 字段中的 JSON
      parse_json: value
```

