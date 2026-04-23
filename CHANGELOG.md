# Changelog

所有格式变更遵循 [Conventional Commits](https://www.conventionalcommits.org/)。

版本号规则：`MAJOR.MINOR.PATCH`
- **MAJOR**: 破坏性变更
- **MINOR**: 新功能（向下兼容）
- **PATCH**: Bug 修复、文档、构建等

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
