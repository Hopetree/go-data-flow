// Package generator 提供序列数据生成器
package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Source 生成序列数据的 Source，用于测试和性能测试
type Source struct {
	count      int
	intervalMs int // 生成间隔（毫秒）
}

// Config Source 的配置
type Config struct {
	// Count 要生成的记录数，0 表示无限
	Count int `json:"count"`
	// IntervalMs 生成间隔（毫秒），0 表示无间隔，默认 0
	IntervalMs int `json:"interval_ms"`
}

// New 创建新的 Source
func New() *Source {
	return &Source{}
}

// Init 初始化 Source
func (s *Source) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}
	s.count = cfg.Count
	s.intervalMs = cfg.IntervalMs
	return nil
}

// Read 生成序列数据
func (s *Source) Read(ctx context.Context, out chan<- types.Record) (int64, error) {
	var count int64

	// 创建定时器（如果配置了间隔）
	var ticker *time.Ticker
	if s.intervalMs > 0 {
		ticker = time.NewTicker(time.Duration(s.intervalMs) * time.Millisecond)
		defer ticker.Stop()
	}

	for i := 0; s.count == 0 || i < s.count; i++ {
		// 如果配置了间隔，等待定时器
		if ticker != nil {
			select {
			case <-ctx.Done():
				return count, ctx.Err()
			case <-ticker.C:
			}
		}

		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- types.Record{"id": i, "value": fmt.Sprintf("record-%d", i), "timestamp": time.Now().UnixMilli()}:
			count++
		}
	}
	return count, nil
}
