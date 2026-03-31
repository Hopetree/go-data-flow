# Sink 组件

Sink 组件是数据流的终点，负责将处理后的数据输出到外部系统。

## 组件总览

| 组件名 | 目录 | 功能 | 数据目标 |
|--------|------|------|----------|
| `sink-output-console` | sink/output | 控制台输出 | 标准输出 (stdout) |
| `sink-collect-memory` | sink/collect | 内存收集 | 内存切片 |
| `sink-null-discard` | sink/null | 丢弃数据 | 无 |
| `sink-csv-file` | sink/csv | CSV 文件输出 | CSV 文件 |
| `sink-json-file` | sink/json | JSON 文件输出 | JSON 数组或 Lines 文件 |
| `sink-clickhouse` | sink/clickhouse | ClickHouse 批量写入 | ClickHouse 数据库 |

## 通用配置

所有 Sink 组件都实现 `dataflow.Sink` 接口：

```go
type Sink[T any] interface {
    Init(config []byte) error
    Consume(ctx context.Context, in <-chan T) error
}
```

---

## sink-output-console

### 作用

将数据输出到控制台（标准输出），主要用于调试和开发阶段。

### 数据目标

标准输出（stdout）

### 输入格式

`map[string]interface{}` 对象

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| format | string | 否 | json | 输出格式: json, line, table |
| limit | int | 否 | 0 | 输出条数限制，0 表示无限制 |

### 示例

**JSON 格式输出：**
```yaml
sink:
  name: sink-output-console
  config:
    format: json
```

输出示例：
```json
{"id":1,"name":"Alice","score":95}
{"id":2,"name":"Bob","score":72}
```

**限制输出条数：**
```yaml
sink:
  name: sink-output-console
  config:
    format: json
    limit: 10
```

### 使用场景

- 开发调试
- 快速验证数据处理逻辑
- 演示和测试

---

## sink-collect-memory

### 作用

将数据收集到内存中，主要用于单元测试。

### 数据目标

内存切片

### 输入格式

`map[string]interface{}` 对象

### 配置项

无

### 示例

```yaml
sink:
  name: sink-collect-memory
```

### 特点

- 数据保存在内存中
- 提供 `Data()` 方法获取收集的数据
- 提供 `Count()` 方法获取数据数量
- 提供 `Reset()` 方法清空数据

### 使用场景

- 单元测试
- 集成测试
- 数据验证

---

## sink-null-discard

### 作用

丢弃所有数据，用于性能测试和基准测试。

### 数据目标

无（数据被丢弃）

### 输入格式

任意类型

### 配置项

无

### 示例

```yaml
sink:
  name: sink-null-discard
```

### 使用场景

- 性能测试（排除 Sink 写入开销）
- 压力测试
- 吞吐量基准测试

---

## sink-csv-file

### 作用

将数据写入 CSV 文件。

### 数据目标

CSV 文件

### 输入格式

`map[string]interface{}` 对象

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| file_path | string | 是 | - | 输出文件路径 |
| separator | string | 否 | , | 字段分隔符 |
| headers | array | 否 | - | 指定列名顺序，不指定则按首次记录的字段顺序 |
| append | bool | 否 | false | 是否追加模式 |
| flush_every | int | 否 | 100 | 每写入多少条刷新一次 |

### 示例

**基本用法：**
```yaml
sink:
  name: sink-csv-file
  config:
    file_path: ./output/result.csv
```

**指定列顺序：**
```yaml
sink:
  name: sink-csv-file
  config:
    file_path: ./output/result.csv
    headers:
      - id
      - name
      - score
```

**使用分号分隔符：**
```yaml
sink:
  name: sink-csv-file
  config:
    file_path: ./output/result.csv
    separator: ";"
```

**追加模式：**
```yaml
sink:
  name: sink-csv-file
  config:
    file_path: ./output/result.csv
    append: true
```

### 输出示例

```csv
id,name,score
1,Alice,95
2,Bob,72
3,Charlie,88
```

### 注意事项

- 如果不指定 headers，列顺序按字段名字母排序
- 追加模式下，如果文件已存在且有内容，不会重复写入标题行
- 支持的字段类型：string, int, int64, float32, float64, bool, nil

---

## sink-json-file

### 作用

将数据写入 JSON 文件，支持 JSON Lines 和 JSON 数组两种格式。

### 数据目标

JSON 文件

### 输入格式

`map[string]interface{}` 对象

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| file_path | string | 是 | - | 输出文件路径 |
| format | string | 否 | lines | 格式: lines (JSON Lines) 或 array (JSON 数组) |
| indent | string | 否 | - | 缩进字符串，如 "  " 或 "\t" |
| append | bool | 否 | false | 是否追加模式 |

### 示例

**JSON Lines 格式（默认）：**
```yaml
sink:
  name: sink-json-file
  config:
    file_path: ./output/result.jsonl
```

输出：
```json
{"id":1,"name":"Alice","score":95}
{"id":2,"name":"Bob","score":72}
```

**JSON 数组格式：**
```yaml
sink:
  name: sink-json-file
  config:
    file_path: ./output/result.json
    format: array
    indent: "  "
```

输出：
```json
[
  {"id":1,"name":"Alice","score":95},
  {"id":2,"name":"Bob","score":72}
]
```

**带缩进的 JSON Lines：**
```yaml
sink:
  name: sink-json-file
  config:
    file_path: ./output/result.jsonl
    format: lines
    indent: "  "
```

### 注意事项

- JSON 数组格式不支持追加模式（会导致 JSON 格式错误）
- JSON Lines 格式适合大数据量和流式处理
- JSON 数组格式适合数据量较小且需要整体加载的场景

---

## sink-clickhouse

### 作用

将数据批量写入 ClickHouse 数据库，支持定时刷新和可选的字段映射。

### 数据目标

ClickHouse 数据库

### 输入格式

`map[string]interface{}` 对象

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| ck_address | []string | 是 | - | ClickHouse 服务器地址列表，格式 `host:port`，支持多个地址 |
| ck_user | string | 否 | default | 用户名 |
| ck_password | string | 否 | (空) | 密码 |
| ck_database | string | 否 | default | 数据库名 |
| table | string | 是 | - | 目标表名 |
| batch_size | int | 否 | 1000 | 批量写入行数，达到该数量触发刷新 |
| flush_interval_ms | int | 否 | 1000 | 刷新间隔（毫秒），定时触发刷新 |
| field_mapping | map | 否 | - | 字段映射，key 为 Record 源字段名，value 为 ClickHouse 表列名 |
| dial_timeout_sec | int | 否 | 60 | 连接超时（秒） |
| max_open_conns | int | 否 | 10 | 最大连接数 |
| max_idle_conns | int | 否 | 5 | 最大空闲连接数 |
| conn_max_lifetime_sec | int | 否 | 1800 | 连接最大生命周期（秒） |

### 字段映射

默认情况下，Record 的所有 key 按字母排序作为 ClickHouse 表的列名，直接写入。当 Record 的字段名与目标表列名不一致时，可通过 `field_mapping` 配置映射关系。

### 示例

**基本用法：**
```yaml
sink:
  name: sink-clickhouse
  config:
    ck_address:
      - localhost:9000
    ck_user: default
    ck_password: "your_password"
    ck_database: default
    table: my_table
    batch_size: 1000
    flush_interval_ms: 1000
```

**多节点高可用：**
```yaml
sink:
  name: sink-clickhouse
  config:
    ck_address:
      - ch-node1:9000
      - ch-node2:9000
    ck_user: default
    ck_password: "your_password"
    ck_database: default
    table: my_table
```

**使用字段映射：**
```yaml
sink:
  name: sink-clickhouse
  config:
    ck_address:
      - localhost:9000
    ck_user: default
    ck_password: "your_password"
    ck_database: default
    table: my_table
    batch_size: 500
    flush_interval_ms: 500
    field_mapping:
      service: service_name
      avg_latency: p50_latency
      max_tps: peak_tps
```

### 批量写入机制

数据通过以下两种条件触发写入：

1. **按数量**：累积到 `batch_size` 条记录时立即刷新
2. **按时间**：每隔 `flush_interval_ms` 毫秒定时刷新

两个条件满足其一即触发。当输入通道关闭或上下文取消时，会尝试将剩余数据刷新后退出。

### 错误处理

写入失败时立即终止 Sink，不进行重试。错误场景包括：
- ClickHouse 连接失败
- 批次准备失败
- 记录追加失败（如类型不匹配）
- 批次发送失败

### 注意事项

- `ck_address` 支持配置多个地址，按顺序尝试连接
- 不配置 `field_mapping` 时，列顺序按字段名字母排序确定（以首条记录为准）
- 配置 `field_mapping` 后，仅映射中指定的字段会被写入，其他字段忽略

---

## 组件选择指南

| 场景 | 推荐组件 | 说明 |
|------|----------|------|
| 开发调试 | sink-output-console | 快速查看处理结果 |
| 单元测试 | sink-collect-memory | 便于断言验证 |
| 性能测试 | sink-null-discard | 排除 IO 开销 |
| 数据导出（小数据量） | sink-json-file (array) | 格式美观，易于查看 |
| 数据导出（大数据量） | sink-json-file (lines) / sink-csv-file | 流式写入，内存友好 |
| 数据分析 | sink-csv-file | 兼容 Excel 和分析工具 |
| 日志收集 | sink-json-file (lines) | 流式写入，简单可靠 |
| 数据库写入 | sink-clickhouse | 高性能批量写入，支持字段映射 |
