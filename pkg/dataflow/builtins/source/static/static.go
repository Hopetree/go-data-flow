// Package static 提供静态内存数据源
package static

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Source 从静态内存数据读取，主要用于测试和演示
type Source struct {
	data []types.Record
}

// Config Source 的配置
type Config struct {
	// Data 要发送的数据记录
	Data []types.Record `json:"data"`
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
	s.data = cfg.Data
	return nil
}

// Read 将数据写入输出通道
func (s *Source) Read(ctx context.Context, out chan<- types.Record) (int64, error) {
	var count int64
	for _, item := range s.data {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- item:
			count++
		}
	}
	return count, nil
}
