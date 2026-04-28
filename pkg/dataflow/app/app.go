// Package app 提供应用程序框架。开发者只需关注组件注册
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"gopkg.in/yaml.v3"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/metrics"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// Version, BuildTime, GitCommit 由 cmd/main 设置
var (
	Version   string
	BuildTime string
	GitCommit string
)

// Record 通用数据记录类型
type Record = types.Record

// Options 应用程序选项
type Options struct {
	// AppConfFile 应用配置文件路径
	AppConfFile string
	// ConfigFile 单个配置文件路径
	ConfigFile string
	// ConfigDir 配置文件目录
	ConfigDir string
	// Configs 多个配置文件（逗号分隔）
	Configs string
	// EnvFile .env 文件路径，为空时默认加载当前目录 .env
	EnvFile string
}

// App 数据流应用
type App struct {
	options       Options
	registry      *dataflow.Registry[Record]
	appConfig     *Config
	metricsServer *metrics.Server
}

// NewApp 创建应用实例
func NewApp(opts Options) *App {
	app := &App{
		options:  opts,
		registry: dataflow.GetDefaultRegistry(),
	}

	// 加载 .env 环境变量（在读取配置文件之前）
	app.loadEnv(opts.EnvFile)

	// 加载应用配置
	if opts.AppConfFile != "" {
		cfg, err := LoadAppConfig(opts.AppConfFile)
		if err != nil {
			// 使用默认配置初始化日志后输出警告
			initDefaultLogger()
			logger.Warn("加载应用配置失败: %v, 使用默认配置", err)
		} else {
			app.appConfig = cfg
			// 初始化日志
			if err := initLogger(cfg); err != nil {
				logger.Warn("初始化日志失败: %v", err)
			} else {
				// 日志初始化成功后，输出渲染后的应用配置内容
				if rendered, err := os.ReadFile(opts.AppConfFile); err == nil {
					logger.Debug("加载应用配置: %s\n%s", opts.AppConfFile, string(expandEnvVars(rendered)))
				}
			}
		}
	} else {
		// 如果没有应用配置，使用默认配置初始化日志
		initDefaultLogger()
	}

	return app
}

// loadEnv 加载 .env 环境变量文件
// 指定 -e 时文件必须存在，未指定时 .env 不存在则跳过
func (a *App) loadEnv(envFile string) {
	target := envFile
	explicit := target != ""
	if target == "" {
		target = ".env"
	}

	// 检查文件是否存在
	if _, err := os.Stat(target); os.IsNotExist(err) {
		if explicit {
			fmt.Fprintf(os.Stderr, "环境变量文件不存在: %s\n", target)
			os.Exit(1)
		}
		return
	}

	// 加载失败则报错退出
	if err := LoadEnv(target); err != nil {
		fmt.Fprintf(os.Stderr, "加载环境变量文件失败 %s: %v\n", target, err)
		os.Exit(1)
	}
}

// initDefaultLogger 初始化默认日志配置
func initDefaultLogger() {
	if err := logger.Init(logger.Config{
		Level:      "info",
		Format:     "text",
		Output:     "stdout",
		Prefix:     "[dataflow]",
		MaxSize:    100,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
	}); err != nil {
		// 默认日志初始化失败时使用 zap 的默认日志器，不应中断程序
		_ = err
	}
}

// initLogger 初始化日志模块
func initLogger(cfg *Config) error {
	return logger.Init(logger.Config{
		Level:      cfg.Log.Level,
		Format:     cfg.Log.Format,
		Output:     cfg.Log.Output,
		Prefix:     cfg.Log.Prefix,
		MaxSize:    cfg.Log.MaxSize,
		MaxBackups: cfg.Log.MaxBackups,
		MaxAge:     cfg.Log.MaxAge,
		Compress:   cfg.Log.Compress,
	})
}

// Registry 返回组件注册表。用于注册自定义组件
func (a *App) Registry() *dataflow.Registry[Record] {
	return a.registry
}

// Run 运行应用（调用前需先注册组件）
func (a *App) Run(ctx context.Context) error {
	logger.Info("go-data-flow %s (built at %s)", Version, BuildTime)

	// 启动 Metrics 服务器
	if err := a.startMetricsServer(); err != nil {
		logger.Warn("启动 Metrics 服务器失败: %v", err)
	}
	defer a.stopMetricsServer()

	// 收集配置文件
	configFiles := a.CollectConfigFiles()
	if len(configFiles) == 0 {
		return fmt.Errorf("没有找到配置文件")
	}

	logger.Info("发现 %d 个配置文件", len(configFiles))

	// 监听中断信号
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 运行 Flow
	if len(configFiles) == 1 {
		return a.runSingle(ctx, configFiles[0])
	}

	// 从配置文件读取 parallel 设置，默认为 true
	parallel := true
	if a.appConfig != nil && a.appConfig.Parallel != nil {
		parallel = *a.appConfig.Parallel
	}

	if parallel {
		return a.runParallel(ctx, configFiles)
	}
	return a.runSequential(ctx, configFiles)
}

// CollectConfigFiles 收集所有配置文件路径
func (a *App) CollectConfigFiles() []string {
	var files []string

	// 单个配置文件
	if a.options.ConfigFile != "" {
		files = append(files, a.options.ConfigFile)
	}

	// 多个配置文件
	if a.options.Configs != "" {
		for _, f := range strings.Split(a.options.Configs, ",") {
			f = strings.TrimSpace(f)
			if f != "" {
				files = append(files, f)
			}
		}
	}

	// 配置目录
	if a.options.ConfigDir != "" {
		pattern := filepath.Join(a.options.ConfigDir, "*.yaml")
		if matches, err := filepath.Glob(pattern); err == nil {
			files = append(files, matches...)
		}
		pattern = filepath.Join(a.options.ConfigDir, "*.yml")
		if matches, err := filepath.Glob(pattern); err == nil {
			files = append(files, matches...)
		}
	}

	return files
}

// runSingle 运行单个 Flow
func (a *App) runSingle(ctx context.Context, configFile string) error {
	config, err := LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("加载配置失败 [%s]: %w", configFile, err)
	}

	flow := dataflow.NewFlow[Record](config, a.registry)

	// 设置 Prometheus 收集器（如果启用）
	if a.metricsServer != nil {
		flow.SetPrometheusCollector(a.metricsServer.Collector())
	}

	if err := flow.Build(); err != nil {
		return fmt.Errorf("[%s] 构建失败: %w", config.Name, err)
	}

	logger.Info("[%s] 开始运行", config.Name)
	if err := flow.Run(ctx); err != nil {
		// 运行失败也输出已处理的记录数
		metrics := flow.Metrics()
		logger.Info("[%s] 失败: 输入=%d, 输出=%d, 错误=%d, 耗时=%dms",
			config.Name, metrics.TotalIn, metrics.TotalOut, metrics.TotalError, metrics.Duration.Milliseconds())

		if dlq := flow.DLQ(); dlq != nil && dlq.Enabled() {
			logger.Info("[%s] DLQ: %d 条错误记录已写入", config.Name, dlq.Count())
		}

		closeErr := flow.Close()
		if closeErr != nil {
			logger.Warn("[%s] 关闭时发生错误: %v", config.Name, closeErr)
		}
		return fmt.Errorf("[%s] 运行失败: %w", config.Name, err)
	}

	// 打印统计和指标汇总
	metrics := flow.Metrics()
	logger.Info("[%s] 完成: 输入=%d, 输出=%d, 错误=%d, 耗时=%dms",
		config.Name, metrics.TotalIn, metrics.TotalOut, metrics.TotalError, metrics.Duration.Milliseconds())

	// 打印详细指标
	a.printMetricsSummary(flow, config.Name)

	// 打印 DLQ 统计
	if dlq := flow.DLQ(); dlq != nil && dlq.Enabled() {
		logger.Info("[%s] DLQ: %d 条错误记录已写入", config.Name, dlq.Count())
	}

	return flow.Close()
}

// runParallel 并行运行多个 Flow
func (a *App) runParallel(ctx context.Context, configFiles []string) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(configFiles))

	for _, file := range configFiles {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			if err := a.runSingle(ctx, f); err != nil {
				errCh <- err
			}
		}(file)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
		logger.Error("错误: %v", err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// runSequential 顺序运行多个 Flow
func (a *App) runSequential(ctx context.Context, configFiles []string) error {
	for i, file := range configFiles {
		logger.Info("========== [%d/%d] ==========", i+1, len(configFiles))
		if err := a.runSingle(ctx, file); err != nil {
			return err
		}
	}
	return nil
}

// startMetricsServer 启动 Prometheus Metrics 服务器
func (a *App) startMetricsServer() error {
	// 检查是否启用
	if a.appConfig == nil || !a.appConfig.Metrics.Enabled {
		logger.Debug("Metrics 服务未启用")
		return nil
	}

	// 设置默认值
	addr := a.appConfig.Metrics.Addr
	if addr == "" {
		addr = ":9090"
	}
	path := a.appConfig.Metrics.Path
	if path == "" {
		path = "/metrics"
	}
	namespace := a.appConfig.Metrics.Namespace
	if namespace == "" {
		namespace = "dataflow"
	}

	// 创建并启动服务器
	a.metricsServer = metrics.NewServer(metrics.ServerConfig{
		Addr:      addr,
		Path:      path,
		Namespace: namespace,
	})

	// 在后台启动服务器
	go func() {
		logger.Info("Prometheus Metrics 服务器启动: http://%s%s (namespace: %s)", addr, path, namespace)
		if err := a.metricsServer.Start(); err != nil {
			logger.Warn("Metrics 服务器错误: %v", err)
		}
	}()

	return nil
}

// stopMetricsServer 停止 Prometheus Metrics 服务器
func (a *App) stopMetricsServer() {
	if a.metricsServer != nil {
		if err := a.metricsServer.Stop(); err != nil {
			logger.Warn("停止 Metrics 服务器失败: %v", err)
		}
	}
}

// printMetricsSummary 打印指标汇总
func (a *App) printMetricsSummary(flow *dataflow.Flow[Record], name string) {
	summary := flow.Metrics()

	logger.Info("========== [%s] 指标汇总 ==========", name)
	logger.Info("运行时间: %v", summary.Duration)
	logger.Info("总输入: %d", summary.TotalIn)
	logger.Info("总输出: %d", summary.TotalOut)
	logger.Info("总错误: %d", summary.TotalError)

	if len(summary.Components) > 0 {
		logger.Info("--- 组件指标 ---")
		for _, comp := range summary.Components {
			logger.Info("  [%s] 输入=%d, 输出=%d, 错误=%d, 平均耗时=%.4fs",
				comp.ComponentName,
				comp.RecordsIn,
				comp.RecordsOut,
				comp.RecordsError,
				comp.AvgDurationSec)
		}
	}
}

// ListComponents 列出所有已注册的组件
func (a *App) ListComponents() {
	fmt.Println("=== Source 组件 ===")
	for _, name := range a.registry.ListSources() {
		fmt.Printf("  %s\n", name)
	}

	fmt.Println("\n=== Processor 组件 ===")
	for _, name := range a.registry.ListProcessors() {
		fmt.Printf("  %s\n", name)
	}

	fmt.Println("\n=== Sink 组件 ===")
	for _, name := range a.registry.ListSinks() {
		fmt.Printf("  %s\n", name)
	}
}

// LoadConfig 加载 Flow 配置文件
func LoadConfig(path string) (*dataflow.FlowConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	data = expandEnvVars(data)
	logger.Debug("加载 Flow 配置: %s\n%s", path, string(data))

	var config dataflow.FlowConfig

	switch filepath.Ext(path) {
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, &config)
	case ".json":
		err = json.Unmarshal(data, &config)
	default:
		if yamlErr := yaml.Unmarshal(data, &config); yamlErr != nil {
			err = json.Unmarshal(data, &config)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}

	return &config, nil
}

// LoadAppConfig 加载应用配置文件
func LoadAppConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	data = expandEnvVars(data)

	var config Config

	switch filepath.Ext(path) {
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, &config)
	case ".json":
		err = json.Unmarshal(data, &config)
	default:
		if yamlErr := yaml.Unmarshal(data, &config); yamlErr != nil {
			err = json.Unmarshal(data, &config)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("解析应用配置失败: %w", err)
	}

	// 设置默认值
	if config.Name == "" {
		config.Name = "dataflow"
	}
	if config.Log.Level == "" {
		config.Log.Level = "info"
	}
	if config.Log.Format == "" {
		config.Log.Format = "text"
	}
	if config.Log.Output == "" {
		config.Log.Output = "stdout"
	}
	if config.Log.Prefix == "" {
		config.Log.Prefix = "[dataflow]"
	}

	return &config, nil
}

// CheckEnvVars 检查配置文件中引用的环境变量是否都已设置
// 返回缺失的环境变量列表，空列表表示全部满足
func CheckEnvVars(configFiles []string) []string {
	var missing []string
	checked := make(map[string]bool)
	for _, path := range configFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// 从原始内容中提取 ${VAR} 变量名（在展开前检查）
		s := string(data)
		for {
			start := strings.Index(s, "${")
			if start == -1 {
				break
			}
			s = s[start+2:]
			end := strings.Index(s, "}")
			if end == -1 {
				break
			}
			varName := s[:end]
			s = s[end+1:]
			if varName == "" || checked[varName] {
				continue
			}
			checked[varName] = true
			if _, ok := os.LookupEnv(varName); !ok {
				missing = append(missing, varName)
			}
		}
	}
	return missing
}

// ValidateConfigs 解析并验证配置文件，不执行 Flow
func ValidateConfigs(configFiles []string) error {
	for _, path := range configFiles {
		config, err := LoadConfig(path)
		if err != nil {
			return fmt.Errorf("[%s] 配置解析失败: %w", path, err)
		}
		if err := config.ValidateBuild(); err != nil {
			return fmt.Errorf("[%s] 配置验证失败: %w", config.Name, err)
		}
		logger.Info("[%s] 配置验证通过", config.Name)
	}
	return nil
}

// PrintVersion 打印版本信息
func PrintVersion() {
	fmt.Printf("go-data-flow %s (built at %s, commit %s)\n", Version, BuildTime, GitCommit)
}

// PrintUsage 打印使用说明
func PrintUsage() {
	fmt.Println("用法: dataflow [选项]")
	fmt.Println()
	fmt.Println("选项:")
	fmt.Println("  -a <file>   应用配置文件路径 (config/app.yaml)")
	fmt.Println("  -c <file>   Flow 配置文件路径")
	fmt.Println("  -d <dir>    配置文件目录")
	fmt.Println("  -C <files>  多个配置文件，逗号分隔")
	fmt.Println("  -e <file>   .env 文件路径 (默认: .env)")
	fmt.Println("  -l          列出所有组件")
	fmt.Println("  -v          显示版本")
	fmt.Println("  --validate  只验证配置，不执行 Flow")
	fmt.Println("  --env-check  检查环境变量是否已设置")
	fmt.Println("  --list-flows 列出发现的 Flow 配置文件")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  dataflow -a app.yaml -c config.yaml")
	fmt.Println("  dataflow -a app.yaml -d examples/config/")
	fmt.Println("  dataflow -C config1.yaml,config2.yaml")
	fmt.Println("  dataflow -a app.yaml -c config.yaml --validate")
	fmt.Println("  dataflow -a app.yaml -d flows/ --env-check")
}
