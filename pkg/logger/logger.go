// Package logger 提供基于 zap 的全局日志功能
package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Config 日志配置
type Config struct {
	// Level 日志级别: debug, info, warn, error
	Level string `json:"level" yaml:"level"`
	// Format 日志格式: text, json
	Format string `json:"format" yaml:"format"`
	// Output 输出目标: stdout, stderr, 或文件路径
	Output string `json:"output" yaml:"output"`
	// Prefix 日志前缀
	Prefix string `json:"prefix" yaml:"prefix"`
	// MaxSize 日志文件最大大小（MB），超过此大小会切割
	MaxSize int `json:"max_size" yaml:"max_size"`
	// MaxBackups 保留的旧日志文件最大数量
	MaxBackups int `json:"max_backups" yaml:"max_backups"`
	// MaxAge 保留旧日志文件的最大天数
	MaxAge int `json:"max_age" yaml:"max_age"`
	// Compress 是否压缩旧日志文件
	Compress bool `json:"compress" yaml:"compress"`
}

// Logger 日志记录器
type Logger struct {
	*zap.SugaredLogger
	config Config
}

var (
	mu sync.RWMutex
	defaultLogger *Logger
	once          sync.Once
)

// Init 初始化全局日志器
func Init(cfg Config) error {
	var err error
	once.Do(func() {
		defaultLogger, err = NewLogger(cfg)
	})
	return err
}

// NewLogger 创建新的日志记录器
func NewLogger(cfg Config) (*Logger, error) {
	// 设置默认值
	if cfg.Level == "" {
		cfg.Level = "info"
	}
	if cfg.Format == "" {
		cfg.Format = "text"
	}
	if cfg.Output == "" {
		cfg.Output = "stdout"
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = 100 // 默认 100MB
	}
	if cfg.MaxBackups == 0 {
		cfg.MaxBackups = 5 // 默认保留 5 个
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 30 // 默认保留 30 天
	}

	// 解析日志级别
	level := parseLevel(cfg.Level)

	// 创建编码器配置
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}

	// 创建编码器
	var encoder zapcore.Encoder
	if cfg.Format == "json" || cfg.Format == "JSON" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		// 判断输出目标：如果是输出到文件，不使用颜色编码器
		isFile := len(cfg.Output) > 0 && cfg.Output != "stdout" && cfg.Output != "stderr"
		if isFile {
			encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		} else {
			encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		}
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 创建写入器
	var writer zapcore.WriteSyncer
	switch cfg.Output {
	case "stdout", "":
		writer = zapcore.AddSync(os.Stdout)
	case "stderr":
		writer = zapcore.AddSync(os.Stderr)
	default:
		// 使用 lumberjack 实现日志切割
		writer = zapcore.AddSync(&lumberjack.Logger{
			Filename:   cfg.Output,
			MaxSize:    cfg.MaxSize,    // MB
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,     // 天
			Compress:   cfg.Compress,
		})
	}

	// 创建核心
	core := zapcore.NewCore(encoder, writer, level)

	// 创建日志器
	// TODO(go1.25/zap): AddCaller 在 JSON 格式下与 Go 1.25 的 runtime.CallersFrames 不兼容，
	// 会导致 panic（nil pointer dereference），待 zap 上游适配后移除此限制。
	// 相关 issue: https://github.com/golang/go/issues/73747
	opts := []zap.Option{}
	if cfg.Format != "json" && cfg.Format != "JSON" {
		opts = append(opts, zap.AddCaller(), zap.AddCallerSkip(1))
	}
	zapLogger := zap.New(core, opts...)

	return &Logger{
		SugaredLogger: zapLogger.Sugar(),
		config:         cfg,
	}, nil
}

// parseLevel 解析日志级别
func parseLevel(l string) zapcore.Level {
	switch l {
	case "debug", "DEBUG":
		return zapcore.DebugLevel
	case "info", "INFO":
		return zapcore.InfoLevel
	case "warn", "WARN", "warning", "WARNING":
		return zapcore.WarnLevel
	case "error", "ERROR":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// Global 返回全局日志器
func Global() *Logger {
	mu.RLock()
	l := defaultLogger
	mu.RUnlock()
	if l == nil {
		// 如果未初始化，返回默认配置的日志器
		Init(Config{})
		mu.RLock()
		l = defaultLogger
		mu.RUnlock()
	}
	return l
}

// Sync 返回底层的 zap.Logger（用于需要结构化日志的场景）
func Sync() *zap.Logger {
	return Global().Desugar()
}

// Debug 记录调试日志
func Debug(format string, args ...interface{}) {
	Global().Debugf(format, args...)
}

// Info 记录信息日志
func Info(format string, args ...interface{}) {
	Global().Infof(format, args...)
}

// Warn 记录警告日志
func Warn(format string, args ...interface{}) {
	Global().Warnf(format, args...)
}

// Error 记录错误日志
func Error(format string, args ...interface{}) {
	Global().Errorf(format, args...)
}

// Fatal 记录致命错误并退出
func Fatal(format string, args ...interface{}) {
	Global().Fatalf(format, args...)
}

// Panic 记录 panic 日志
func Panic(format string, args ...interface{}) {
	Global().Panicf(format, args...)
}

// With 返回带有字段上下文的日志器
func With(fields ...interface{}) *Logger {
	return &Logger{
		SugaredLogger: Global().With(fields...),
		config:         Global().config,
	}
}

// SetLevel 设置日志级别
func SetLevel(level string) {
	cfg := Global().config
	cfg.Level = level
	newLogger, err := NewLogger(cfg)
	if err == nil {
		mu.Lock()
		defaultLogger = newLogger
		mu.Unlock()
	}
}

// Close 关闭日志器（刷新缓冲区）
func Close() error {
	return Global().Sync()
}

// Printf 兼容标准 log（记录 Info 级别）
func Printf(format string, args ...interface{}) {
	Info(format, args...)
}

// Print 兼容标准 log
func Print(args ...interface{}) {
	Info("%v", args...)
}

// Println 兼容标准 log
func Println(args ...interface{}) {
	Info("%v", args...)
}
