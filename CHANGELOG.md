# Changelog

所有格式变更遵循 [Conventional Commits](https://www.conventionalcommits.org/)。

版本号规则：`MAJOR.MINOR.PATCH`
- **MAJOR**: 破坏性变更
- **MINOR**: 新功能（向下兼容）
- **PATCH**: Bug 修复、文档、构建等

## [v0.2.6] - 2026-04-28

### 修复

- **flow.go**: 所有组件 goroutine 添加 panic recovery，防止 panic 导致应用挂死
- **flow.go**: 修复关闭超时路径死锁，超时后立即返回而非无限等待 errCh
- **flow.go**: 修复 sink wrapper 的 select 语义，sink 完成后正确排空上游数据
- **flow.go**: `Close()` 使用 `errors.Join` 聚合所有组件关闭错误
- **flow.go**: 修复 `Metrics().TotalOut` 计算错误，改为使用 sink 的 RecordsIn
- **app.go**: `runSingle` 错误时确保调用 `flow.Close()` 释放资源
- **app.go**: `runParallel` 使用 `errors.Join` 聚合所有并行 Flow 错误
- **dlq.go**: 添加 `sync.Once` 防止双重关闭 panic
- **dlq.go**: DLQ sink 使用 30s 超时 context，防止阻塞主流程关闭
- **metrics/recorder.go**: 修复 `AddCounter` 错误调用 `SetGauge` 的复制粘贴 bug
- **metrics/recorder.go**: 修复 `ObserveHistogram` 错误调用 `ObserveSummary` 的复制粘贴 bug
- **logger.go**: 移除 `sync.Once`，支持重新初始化
- **logger.go**: 所有包级日志函数添加 nil 保护，防止未初始化时 panic
- **logger.go**: `SetLevel()` 修复旧 logger 文件句柄泄漏
- **kafka.go**: 添加 `sync.Once` 防止 rebalance 时 `close(ready)` panic
- **kafka.go**: `Read()` 等待 ready 时增加 `ctx.Done()` 检查
- **kafka.go**: `Close()` 关闭 consumer 后置 nil 防止双重关闭
- **csv.go**: 修复 `has_header: false` 无法设置的问题（改用 `*bool`）
- **json/source**: JSON Lines scanner 缓冲区从默认 64KB 增加到 10MB
- **output/sink**: 达到输出限制时记录警告日志
- **transform**: `extract` 字段不存在/为空/非对象时跳过记录而非输出空 `{}`
- **prometheus**: `Server.Stop()` 使用 `Shutdown` 优雅关闭替代 `Close`
- **app.go**: `CheckEnvVars` 同时检查 `${VAR}` 和 `$VAR` 两种语法
- **config.go**: 移除未使用的 `ValidateBuild` 死代码
- **registry.go**: `ListSources/ListProcessors/ListSinks` 返回结果按名称排序
- **main.go**: 重构为 `run()` 模式，确保所有退出路径刷新日志缓冲区

## [v0.2.5] - 2026-04-23

### 新功能

- 优雅关闭（Graceful Shutdown）：收到 SIGINT/SIGTERM 后停止 source，等待 channel 中剩余数据被 processor 和 sink 消费完毕后再退出，支持 `shutdown_timeout` 配置（默认 30 秒）
- Flow 错误信息聚合：`flow.Run()` 现在收集所有组件错误并使用 `errors.Join()` 聚合返回，不再只返回第一个错误
- CLI 增强：新增 `--validate`（验证配置）、`--env-check`（检查环境变量）、`--list-flows`（列出配置文件）参数
- 死信队列（DLQ）：新增 `error_handling.dlq` 配置，processor/sink 错误不再中断 flow，而是路由到指定 sink 记录错误元数据（组件名、错误信息、时间戳）
- 健康检查端点：Prometheus 服务器新增 `/health` 端点，返回 `{"status":"ok"}`，用于容器编排

### 配置

- `FlowConfig` 新增 `shutdown_timeout` 字段（单位秒，默认 30），支持 YAML/JSON 配置
- `FlowConfig` 新增 `error_handling.dlq` 字段，支持配置 DLQ sink

## [v0.2.4] - 2026-04-23

### 修复

- 修复 `SetGauge` 错误调用 `AddCounter` 的 bug，补全 `PrometheusCollector` 接口缺失的 `SetGauge` 和 `ObserveSummary` 方法
- 修复 `ObserveSummary` 错误调用 `ObserveHistogram` 的 bug
- 修复 Prometheus 默认 namespace 从 `"procflow"` 更正为 `"dataflow"`

### 其他

- 统一日志调用：`prometheus.go` 和 `kafka.go` 中的 `fmt.Printf`/`log.Printf` 替换为框架 `logger`

## [v0.2.3] - 2026-04-22

### 修复

- 严格化 `.env` 文件加载行为：指定 `-e` 时文件必须存在，加载失败（权限、格式等）报错退出；未指定 `-e` 时 `.env` 不存在则跳过

## [v0.2.2] - 2026-04-20

### 修复

- 修复 debug 级别日志无法写入日志文件的问题：将应用配置的 debug 日志移到日志模块初始化之后输出，确保日志文件输出已就绪

## [v0.2.1] - 2026-04-20

### 修复

- 支持 `$$` 转义字面量 `$`，避免配置值中的 `$` 被误解析为环境变量引用（如正则表达式 `$^`、jq 表达式 `$in` 等）

## [v0.2.0] - 2026-04-20

### 新增

- **环境变量配置渲染** — 支持 `${VAR}` / `$VAR` 语法在 YAML 配置中引用环境变量，通过 `godotenv` 自动加载 `.env` 文件（默认加载当前目录，支持 `-e` 参数指定路径）。适用于 app.yaml 和 flow YAML，完全向后兼容
- **`-e` CLI 参数** — 指定 `.env` 文件路径
- **示例** — 新增 `05-env-config.yaml` 和 `.env.example` 演示环境变量用法
- **Taskfile** — 新增 `clean:log` 命令清理日志文件，构建产物输出到 `dist/` 目录

### 修复

- 移除测试中冗余的 nil 检查，消除 SA5011 lint 告警

## [v0.1.3] - 2026-04-15

### 新增

- **DefaultRegistry 全局默认注册表** — 支持 `init()` + blank import 自动注册组件模式，减少 main.go 样板代码。新增 `GetDefaultRegistry()` 和 `Registry` 构造函数

### 重构

- `types.Record` 从命名类型改为类型别名（`type Record = map[string]interface{}`），与框架内部 `map[string]interface{}` 类型完全一致

### 移除

- 移除 `processor-aggregate-group` 组件

## [v0.1.2] - 2026-04-01

### 文档

- 重写自定义组件开发指南，补充自定义指标（Prometheus）章节
- 修正租户开发指南流程图对齐问题
- 添加 MIT License

## [v0.1.1] - 2026-04-01

### 修复

- 修复 Python runner 在 Go 1.25 下的兼容性问题（`syscall` → `golang.org/x/sys/unix`）
- 统一二进制命名格式并修复 module 路径

### CI/CD

- 指定 Go 版本为 1.25.0
- 拆分独立 release workflow

### 文档

- 同步租户开发指南 main.go 模板与项目实际代码一致
- 标准化租户开发指南中的项目结构和打包说明

## [v0.1.0] - 2026-03-31

### 初始版本

- Source / Processor / Sink 组件化数据管道框架
- YAML 驱动配置
- 内置组件：static-data、generator-sequence、csv-file、json-file、kafka、condition-filter、expr-filter、transform-field、jq-transform、python、output-console、collect-memory、null-discard、csv-file、json-file、clickhouse
- 可选并发 Processor（`BaseProcessor` / `StatelessProcessor`）
- Prometheus 指标收集
- Python 脚本组件支持
- 租户二次开发文档
