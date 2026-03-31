// Package processor 提供 Python 脚本 Processor 实现。
// 通过子进程启动 Python 脚本，从 Go channel 读取数据通过 stdin 发送给 Python，
// Python 处理后从 stdout 读取结果发送到下游 Go channel。
package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/python/runner"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/yourorg/go-data-flow/pkg/logger"
)

// Processor 通过 Python 脚本处理数据的 Processor。
// 使用双向通信：Go 写 stdin → Python 处理 → Go 读 stdout。
// 采用流式模式：stdin 写入和 stdout 读取并发进行，利用管道自然背压保证不丢数据。
type Processor struct {
	runner *runner.Runner
}

// Config Processor 的配置。
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

// New 创建 Processor 实例。
func New() *Processor {
	return &Processor{}
}

// Init 解析配置。
func (p *Processor) Init(config []byte) error {
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

	r.SetStderrCallback(runner.DefaultStderrHandler("[python-processor]"))

	p.runner = r
	return nil
}

// Process 从输入通道读取数据，发送给 Python 处理，将结果发送到输出通道。
// 采用流式模式：stdin 写入和 stdout 读取并发进行，利用管道自然背压保证不丢数据。
func (p *Processor) Process(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	if err := p.runner.Start(ctx); err != nil {
		return fmt.Errorf("启动 Python 进程失败: %w", err)
	}

	var (
		writeErr error
		readErr  error
		wg       sync.WaitGroup
	)

	// stdout 读取协程：直接写入 out 通道，由下游消费
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			line, err := p.runner.ReadLine(ctx)
			if err != nil {
				if err != io.EOF && err != context.Canceled {
					readErr = fmt.Errorf("读取 Python 输出失败: %w", err)
				}
				return
			}

			var record types.Record
			if err := json.Unmarshal(line, &record); err != nil {
				logger.Warn("[python-processor] 解析 JSON 失败，跳过: %s", string(line))
				continue
			}

			select {
			case <-ctx.Done():
				return
			case out <- record:
			}
		}
	}()

	// 写入所有输入到 Python stdin
	for item := range in {
		data, err := json.Marshal(item)
		if err != nil {
			logger.Warn("[python-processor] 序列化记录失败: %v", err)
			continue
		}

		if err := p.runner.WriteLine(ctx, data); err != nil {
			writeErr = fmt.Errorf("写入 Python stdin 失败: %w", err)
			break
		}
	}

	// 关闭 stdin，通知 Python 输入结束
	if closeErr := p.runner.CloseStdin(); closeErr != nil {
		logger.Warn("[python-processor] 关闭 stdin 失败: %v", closeErr)
	}

	// 等待 stdout 读取协程完成（Python 输出全部读完）
	wg.Wait()

	// 等待 Python 进程退出
	if closeErr := p.runner.Close(); closeErr != nil {
		return fmt.Errorf("Python 进程退出异常: %w", closeErr)
	}

	// 优先返回读取错误
	if readErr != nil {
		return readErr
	}
	return writeErr
}

// ConcurrencyCap 返回并发能力（不支持并发，因为有状态的子进程通信）。
func (p *Processor) ConcurrencyCap() dataflow.ConcurrencyCap {
	return dataflow.ConcurrencyCap{
		Supported:  false,
		IsStateful: true,
	}
}

// Close 关闭 Python 进程。
func (p *Processor) Close() error {
	if p.runner != nil {
		return p.runner.Close()
	}
	return nil
}

// 编译时接口检查
var (
	_ dataflow.Processor[types.Record] = (*Processor)(nil)
	_ dataflow.Closer                  = (*Processor)(nil)
)
