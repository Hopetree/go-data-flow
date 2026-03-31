// Package app 提供应用程序框架
package app

// AppConfig 应用级全局配置
type AppConfig struct {
	// Name 应用名称
	Name string `json:"name" yaml:"name"`
	// Log 日志配置
	Log LogConfig `json:"log" yaml:"log"`
	// Metrics 指标配置
	Metrics MetricsConfig `json:"metrics" yaml:"metrics"`
	// Parallel 是否并行运行多 Flow（可被 Flow 配置覆盖）
	// nil 表示未设置，使用默认值 true
	Parallel *bool `json:"parallel" yaml:"parallel"`
	// BufferSize 默认缓冲区大小（可被 Flow 配置覆盖）
	BufferSize int `json:"buffer_size" yaml:"buffer_size"`
}

// LogConfig 日志配置
type LogConfig struct {
	// Level 日志级别: debug, info, warn, error
	Level string `json:"level" yaml:"level"`
	// Format 日志格式: text, json
	Format string `json:"format" yaml:"format"`
	// Output 输出目标: stdout, stderr, 或文件路径
	Output string `json:"output" yaml:"output"`
	// Prefix 日志前缀
	Prefix string `json:"prefix" yaml:"prefix"`
	// MaxSize 日志文件最大大小（MB），超过此大小会切割，默认 100
	MaxSize int `json:"max_size" yaml:"max_size"`
	// MaxBackups 保留的旧日志文件最大数量，默认 5
	MaxBackups int `json:"max_backups" yaml:"max_backups"`
	// MaxAge 保留旧日志文件的最大天数，默认 30
	MaxAge int `json:"max_age" yaml:"max_age"`
	// Compress 是否压缩旧日志文件，默认 false
	Compress bool `json:"compress" yaml:"compress"`
}

// MetricsConfig 指标配置
type MetricsConfig struct {
	// Enabled 是否启用 Prometheus 指标服务
	Enabled bool `json:"enabled" yaml:"enabled"`
	// Addr 监听地址，默认 :9090
	Addr string `json:"addr" yaml:"addr"`
	// Path metrics 路径，默认 /metrics
	Path string `json:"path" yaml:"path"`
	// Namespace 指标命名空间，默认 dataflow
	Namespace string `json:"namespace" yaml:"namespace"`
}
