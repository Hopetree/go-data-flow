// Package sink 提供 Python 脚本 Sink 实现。
// 通过子进程启动 Python 脚本，将数据通过 stdin 以 JSON lines 格式发送给 Python 处理。
package sink

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/python/runner"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// Sink 通过 Python 脚本写入数据的 Sink。
// 从 Go channel 读取 Record，序列化为 JSON lines 后写入 Python 的 stdin。
// 当 channel 关闭后关闭 Python 的 stdin，等待 Python 处理完毕并退出。
type Sink struct {
	runner *runner.Runner
}

// Config Sink 的配置。
type Config struct {
	// Script Python 脚本路径（必填）
	Script string `json:"script"`
	// PythonExec Python 可执行文件路径，默认 "python3"
	PythonExec string `json:"python_exec"`
	// Args 传递给脚本的额外命令行参数
	Args []string `json:"args"`
	// Env 额外的环境变量（值支持 string/number/bool，自动转为字符串）
	Env map[string]interface{} `json:"env"`
}

// New 创建 Sink 实例。
func New() *Sink {
	return &Sink{}
}

// Init 解析配置。
func (s *Sink) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	r, err := runner.New(runner.Config{
		Script:     cfg.Script,
		PythonExec: cfg.PythonExec,
		Args:       cfg.Args,
		Env:        runner.ConvertEnv(cfg.Env),
	})
	if err != nil {
		return err
	}

	r.SetStderrCallback(runner.DefaultStderrHandler("[python-sink]"))

	s.runner = r
	return nil
}

// Consume 从输入通道读取数据，通过 stdin 发送给 Python 脚本处理。
func (s *Sink) Consume(ctx context.Context, in <-chan types.Record) error {
	if err := s.runner.Start(ctx); err != nil {
		return fmt.Errorf("启动 Python 进程失败: %w", err)
	}

	// 读取输入通道并发送给 Python
	for item := range in {
		data, err := json.Marshal(item)
		if err != nil {
			logger.Warn("[python-sink] 序列化记录失败: %v", err)
			continue
		}

		if err := s.runner.WriteLine(ctx, data); err != nil {
			// stdin 写入失败通常意味着 Python 进程已退出
			if closeErr := s.runner.Close(); closeErr != nil {
				logger.Warn("[python-sink] 关闭 Python 进程失败: %v", closeErr)
			}
			return fmt.Errorf("写入 Python stdin 失败: %w", err)
		}
	}

	// 所有数据发送完毕，关闭 stdin 通知 Python
	if err := s.runner.CloseStdin(); err != nil {
		logger.Warn("[python-sink] 关闭 stdin 失败: %v", err)
	}

	// 等待 Python 进程处理完毕并退出
	if err := s.runner.Close(); err != nil {
		return fmt.Errorf("Python 进程退出异常: %w", err)
	}

	return nil
}

// Close 关闭 Python 进程。
func (s *Sink) Close() error {
	if s.runner != nil {
		return s.runner.Close()
	}
	return nil
}

// 编译时接口检查
var (
	_ dataflow.Sink[types.Record] = (*Sink)(nil)
	_ dataflow.Closer             = (*Sink)(nil)
)
