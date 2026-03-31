# Python 脚本组件

通过 Python 脚本实现 Source / Processor / Sink 组件。Go 框架负责编排（通道、并发、指标、生命周期），Python 脚本负责实际的数据逻辑。

适用于熟悉 Python、需要快速实现数据处理逻辑的场景。

---

## 组件总览

| 组件名 | 类型 | 功能 |
|--------|------|------|
| `source-python-script` | Source | 从 Python 脚本的 stdout 读取 JSON lines 作为数据源 |
| `processor-python-script` | Processor | 通过 stdin/stdout 双向通信，Python 脚本处理数据 |
| `sink-python-script` | Sink | 将数据通过 stdin 发送给 Python 脚本写入外部系统 |

## 通信协议

所有组件通过 **JSON lines** 协议与 Python 脚本通信。每行一个 JSON 对象。

```
Go ←→ stdin（JSON lines）←→ Python 脚本 ←→ stdout（JSON lines）←→ Go
```

---

## source-python-script

### 作用

从 Python 脚本的 stdout 读取数据。Python 脚本负责从外部数据源读取数据，逐行输出 JSON。

### 工作原理

1. Go 启动 Python 子进程
2. Python 脚本向 stdout 输出 JSON lines
3. Go 逐行读取 stdout，解析为 Record
4. Python 脚本退出即表示数据结束

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `script` | string | 是 | - | Python 脚本路径（支持相对路径） |
| `python_exec` | string | 否 | `python3` | Python 可执行文件路径 |
| `args` | []string | 否 | - | 传递给脚本的额外命令行参数 |
| `env` | map | 否 | - | 额外的环境变量 |

### Python 脚本规范

```python
#!/usr/bin/env python3
import json
import sys

for record in my_data_source():
    json.dump(record, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()
# 脚本退出即表示数据结束
```

### 示例

**从 API 读取数据：**

```yaml
source:
  name: source-python-script
  config:
    script: ./scripts/read_api.py
    env:
      API_ENDPOINT: "https://api.example.com/data"
```

**指定 Python 解释器：**

```yaml
source:
  name: source-python-script
  config:
    script: ./scripts/source.py
    python_exec: /usr/bin/python3.12
```

---

## processor-python-script

### 作用

通过 Python 脚本处理数据。Go 从上游通道读取 Record，序列化为 JSON lines 写入 Python 的 stdin，Python 处理后从 stdout 输出结果。

### 工作原理

1. Go 启动 Python 子进程
2. Go 将上游数据逐条写入 Python 的 stdin
3. Python 从 stdin 逐行读取、处理，向 stdout 输出结果
4. 所有输入写完后，Go 关闭 stdin，Python 检测到 EOF 后退出
5. Go 读取所有输出并发送到下游通道

> **注意：** 采用批量模式——先写完所有输入，再读取所有输出。不适合需要逐条流式处理的场景。

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `script` | string | 是 | - | Python 脚本路径（支持相对路径） |
| `python_exec` | string | 否 | `python3` | Python 可执行文件路径 |
| `args` | []string | 否 | - | 传递给脚本的额外命令行参数 |
| `env` | map | 否 | - | 额外的环境变量 |

### 并发能力

不支持并发（`IsStateful: true`）。每个 Processor 实例对应一个 Python 子进程，单进程的 stdin/stdout 不支持并发访问。

### Python 脚本规范

```python
#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    record = json.loads(line.strip())
    # 处理逻辑
    result = {"name": record["name"].upper(), "processed": True}
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()
# stdin 关闭后，Python 脚本应自然退出
```

### 示例

**字段转换：**

```yaml
processors:
  - name: processor-python-script
    config:
      script: ./scripts/transform.py
```

**使用额外环境变量：**

```yaml
processors:
  - name: processor-python-script
    config:
      script: ./scripts/translate.py
      env:
        TRANSLATE_API_KEY: "sk-xxx"
        TARGET_LANG: "en"
```

---

## sink-python-script

### 作用

将数据通过 stdin 发送给 Python 脚本，由 Python 写入外部系统。

### 工作原理

1. Go 启动 Python 子进程
2. Go 将 Record 逐条序列化为 JSON lines，写入 Python 的 stdin
3. Python 从 stdin 逐行读取，写入目标系统
4. 所有数据写完后，Go 关闭 stdin
5. Python 检测到 EOF，flush 缓冲区后退出
6. Go 等待 Python 进程退出，确认数据写入完成

### 配置项

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `script` | string | 是 | - | Python 脚本路径（支持相对路径） |
| `python_exec` | string | 否 | `python3` | Python 可执行文件路径 |
| `args` | []string | 否 | - | 传递给脚本的额外命令行参数 |
| `env` | map | 否 | - | 额外的环境变量 |

### Python 脚本规范

```python
#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    record = json.loads(line.strip())
    # 写入目标系统
    write_to_database(record)

# stdin 关闭后，flush 缓冲区并退出
```

### 示例

**写入数据库：**

```yaml
sink:
  name: sink-python-script
  config:
    script: ./scripts/write_db.py
    env:
      DB_URL: "postgres://user:pass@localhost/mydb"
```

**写入文件：**

```yaml
sink:
  name: sink-python-script
  config:
    script: ./scripts/write_file.py
    args:
      - "--output"
      - "/tmp/result.jsonl"
```

---

## 完整示例

**一个 Python 端到端的 ETL 流水线：**

```yaml
name: python-etl-example

source:
  name: source-python-script
  config:
    script: ./scripts/source.py

processors:
  - name: processor-python-script
    config:
      script: ./scripts/transform.py

sink:
  name: sink-python-script
  config:
    script: ./scripts/sink.py
```

对应的 Python 脚本：

```python
# source.py —— 从内存生成数据
import json, sys
for i in range(5):
    json.dump({"id": i, "value": i * 10}, sys.stdout)
    sys.stdout.write("\n")
```

```python
# transform.py —— 过滤并转换
import json, sys
for line in sys.stdin:
    record = json.loads(line.strip())
    if record["value"] > 0:
        record["doubled"] = record["value"] * 2
        json.dump(record, sys.stdout)
        sys.stdout.write("\n")
```

```python
# sink.py —— 输出到控制台
import json, sys
for line in sys.stdin:
    record = json.loads(line.strip())
    print(f"Result: {record}")
```

---

## 日志配置

### 输出流分工

| 输出流 | 用途 | 说明 |
|--------|------|------|
| **stdout** | 数据传输 | 只能输出 JSON lines，不能混入日志 |
| **stderr** | 日志输出 | Go 框架捕获 stderr，解析后映射到 Go 日志级别 |

### 日志级别映射

Go 框架解析 stderr 每行的 `日志级别:` 前缀，自动映射到对应的 Go 日志级别：

| Python 日志级别 | Go 日志级别 | 格式要求 |
|-----------------|-------------|----------|
| `DEBUG` | `logger.Debug` | `DEBUG:message` |
| `INFO` | `logger.Info` | `INFO:message` |
| `WARNING` | `logger.Warn` | `WARNING:message` |
| `ERROR` | `logger.Error` | `ERROR:message` |
| `CRITICAL` | `logger.Error` | `CRITICAL:message` |
| 其他格式 | `logger.Warn` | 降级为 WARN（向后兼容） |

### 推荐配置

使用 Python 标准 `logging` 模块，将格式设为 `%(levelname)s:%(message)s`，输出到 stderr：

```python
import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s:%(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

# 使用示例
log.info("开始处理记录 id=%s", record.get("id"))   # → Go logger.Info
log.debug("记录详情: %s", record)                     # → Go logger.Debug
log.warning("分数偏低: %s", score)                     # → Go logger.Warn
log.error("处理失败: %s", reason)                      # → Go logger.Error
```

### 格式要求

日志格式**必须**以 `日志级别:` 开头（级别与消息之间用冒号分隔）。框架按第一个冒号拆分，前面匹配级别，后面作为消息内容。

以下格式可以正确解析：

```python
# 可以 ✅
"%(levelname)s:%(message)s"        # INFO:hello world
"%(levelname)s: %(message)s"       # INFO: hello world
```

以下格式**无法**正确解析，会降级为 WARN：

```python
# 不可以 ❌
"%(levelname)s - %(message)s"      # INFO - hello world（分隔符不是冒号）
"%(asctime)s %(levelname)s:%(message)s"  # 2026-03-20 INFO:hello（第一个冒号在时间戳里）
```

### print() 的使用

`print()` 默认输出到 **stdout**，会混入数据通道导致解析错误。如果需要用 `print()` 输出日志，必须指定 `file=sys.stderr`：

```python
# 错误 ❌ — 输出到 stdout，破坏数据通道
print("开始处理")

# 正确 ✅ — 输出到 stderr，Go 当作 WARN 日志
print("开始处理", file=sys.stderr)
```

推荐使用 `logging` 模块代替 `print()`，以获得日志级别区分能力（参见上文"推荐配置"）。

### 数据输出的推荐写法

框架通信协议为 JSON lines，数据本身就是 JSON 时，推荐直接用 `json.dump` 写入 stdout：

```python
import json
import sys

result = {"id": 1, "name": "Alice"}
json.dump(result, sys.stdout)
sys.stdout.write("\n")
sys.stdout.flush()
```

三步缺一不可：

| 步骤 | 作用 | 省略的后果 |
|------|------|------------|
| `json.dump(result, sys.stdout)` | 序列化 JSON | - |
| `sys.stdout.write("\n")` | 添加换行符分隔记录 | Go 读不到行结束，一直阻塞 |
| `sys.stdout.flush()` | 立即发送数据 | 管道模式下 stdout 是全缓冲的，数据会攒着不发送 |

相比 `print(json.dumps(result))`，`json.dump(result, sys.stdout)` 少一次中间的字符串转换，效率更高。

---

## 注意事项

- **stdout 只能输出数据**：Python 脚本的 stdout 专用于 JSON lines 数据通信，不要在其中输出日志或调试信息。日志请写入 stderr。
- **不要使用默认的 print()**：`print()` 默认输出到 stdout。需要输出日志时，使用 `logging` 模块或 `print(msg, file=sys.stderr)`。
- **必须 flush**：每行 JSON 输出后调用 `sys.stdout.flush()`，确保数据及时发送到 Go。Python 的 stdout 在管道模式下不会自动刷新。
- **错误处理**：如果 Python 脚本以非零退出码退出，Go 会报告错误并停止 Flow。
- **脚本路径**：支持相对路径（相对于程序工作目录）和绝对路径。`Init` 阶段会验证脚本文件是否存在。
- **Python 版本**：默认使用 `python3`，可通过 `python_exec` 指定其他版本或虚拟环境中的 Python（如 `./venv/bin/python`）。
- **性能**：每条数据需要 JSON 序列化/反序列化，有额外开销。适合中等数据量的 ETL 场景。大数据量建议使用 Go 原生组件。
