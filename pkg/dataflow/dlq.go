package dataflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// DLQRecord 死信队列错误记录
type DLQRecord = map[string]interface{}

// DLQ 管理死信队列，将组件错误路由到指定的 sink
type DLQ struct {
	sink      Sink[types.Record]
	ch        chan DLQRecord
	wg        sync.WaitGroup
	mu        sync.Mutex
	count     int
	enabled   bool
	closeOnce sync.Once
}

// NewDLQ 创建 DLQ 实例。
// 从 registry 获取 sink 组件并初始化，启动后台 goroutine 消费错误记录。
func NewDLQ(cfg DLQConfig, registry *Registry[types.Record]) (*DLQ, error) {
	if !cfg.Enabled {
		return &DLQ{enabled: false}, nil
	}

	if cfg.Sink.Name == "" {
		return nil, fmt.Errorf("DLQ 启用但未配置 sink")
	}

	sink, ok := registry.GetSink(cfg.Sink.Name)
	if !ok {
		return nil, fmt.Errorf("DLQ sink '%s' 未注册", cfg.Sink.Name)
	}

	if err := initComponent(sink, cfg.Sink.Config); err != nil {
		return nil, fmt.Errorf("DLQ sink 初始化失败: %w", err)
	}

	dlq := &DLQ{
		sink:    sink,
		ch:      make(chan DLQRecord, 100),
		enabled: true,
	}

	// 启动后台消费 goroutine
	dlq.wg.Add(1)
	go func() {
		defer dlq.wg.Done()
		// 创建带超时的 context，防止 sink 阻塞导致 Close 永久等待
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// 创建适配通道：将 DLQRecord 转换为 types.Record
		recordCh := make(chan types.Record, 100)
		var innerWg sync.WaitGroup
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			for record := range dlq.ch {
				select {
				case recordCh <- record:
				case <-ctx.Done():
					return
				}
			}
			close(recordCh)
		}()

		// 消费 DLQ 记录
		// DLQ sink 出错只记录日志不中断（避免影响主流程）
		if err := sink.Consume(ctx, recordCh); err != nil {
			logger.Warn("DLQ sink 写入错误: %v", err)
		}
		innerWg.Wait()
	}()

	return dlq, nil
}

// Enabled 返回 DLQ 是否启用
func (d *DLQ) Enabled() bool {
	return d.enabled
}

// Send 发送错误记录到 DLQ。
// 非阻塞：如果 channel 已满则丢弃记录（DLQ 不应阻塞数据管道）。
func (d *DLQ) Send(flowName, componentName, componentType, errMsg string) {
	if !d.enabled {
		return
	}

	record := DLQRecord{
		"flow_name":      flowName,
		"component_name": componentName,
		"component_type": componentType,
		"error_message":  errMsg,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}

	select {
	case d.ch <- record:
		d.mu.Lock()
		d.count++
		d.mu.Unlock()
	default:
		// channel 已满，丢弃记录避免阻塞数据管道
	}
}

// Count 返回已发送的 DLQ 记录数
func (d *DLQ) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

// Close 关闭 DLQ：停止接收新记录，等待已有记录写入完毕，关闭 sink
func (d *DLQ) Close() error {
	if !d.enabled {
		return nil
	}

	var err error
	d.closeOnce.Do(func() {
		// 关闭 channel，触发消费 goroutine 结束
		close(d.ch)
		// 等待消费完毕（内部有 30s 超时保护）
		d.wg.Wait()
		// 关闭 sink
		if closer, ok := d.sink.(Closer); ok {
			err = closer.Close()
		}
	})
	return err
}
