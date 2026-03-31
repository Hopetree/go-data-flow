// Package source 提供 Python 脚本 Source 实现。
// 通过子进程启动 Python 脚本，从 stdout 读取 JSON lines 作为数据源。
package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/python/runner"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// Source 通过 Python 脚本读取数据的 Source。
// Python 脚本向 stdout 输出 JSON lines，每行一个 Record。
// 脚本退出即表示数据结束。
type Source struct {
	runner *runner.Runner
}

// Config Source 的配置。
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

// New 创建 Source 实例。
func New() *Source {
	return &Source{}
}

// Init 解析配置。
func (s *Source) Init(config []byte) error {
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

	r.SetStderrCallback(runner.DefaultStderrHandler("[python-source]"))

	s.runner = r
	return nil
}

// Read 从 Python 脚本 stdout 读取数据并发送到输出通道。
func (s *Source) Read(ctx context.Context, out chan<- types.Record) (int64, error) {
	if err := s.runner.Start(ctx); err != nil {
		return 0, fmt.Errorf("启动 Python 进程失败: %w", err)
	}
	defer func() {
		if closeErr := s.runner.Close(); closeErr != nil {
			logger.Warn("[python-source] 关闭 Python 进程失败: %v", closeErr)
		}
	}()

	var count int64
	for {
		line, err := s.runner.ReadLine(ctx)
		if err != nil {
			if err == io.EOF {
				return count, nil
			}
			return count, fmt.Errorf("读取 Python 输出失败: %w", err)
		}

		var record types.Record
		if err := json.Unmarshal(line, &record); err != nil {
			logger.Warn("[python-source] 解析 JSON 失败，跳过: %s", string(line))
			continue
		}

		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- record:
			count++
		}
	}
}

// Close 关闭 Python 进程。
func (s *Source) Close() error {
	if s.runner != nil {
		return s.runner.Close()
	}
	return nil
}

// 编译时接口检查
var (
	_ dataflow.Source[types.Record] = (*Source)(nil)
	_ dataflow.Closer               = (*Source)(nil)
)
