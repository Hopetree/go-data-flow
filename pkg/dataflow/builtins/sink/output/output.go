// Package output 提供控制台输出的 Sink
package output

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Sink 将数据输出到控制台
type Sink struct {
	format string // json, line, table
	limit  int    // 输出条数限制
	count  int
}

// Config Sink 的配置
type Config struct {
	// Format 输出格式: json, line, table
	Format string `json:"format"`
	// Limit 输出条数限制，0 表示无限制
	Limit int `json:"limit"`
}

// New 创建新的 Sink
func New() *Sink {
	return &Sink{
		format: "json",
	}
}

// Init 初始化 Sink
func (s *Sink) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	if cfg.Format != "" {
		s.format = cfg.Format
	}
	s.limit = cfg.Limit

	return nil
}

// Consume 消费数据并输出到控制台
func (s *Sink) Consume(ctx context.Context, in <-chan types.Record) error {
	for item := range in {
		if s.limit > 0 && s.count >= s.limit {
			return nil
		}

		switch s.format {
		case "json":
			data, _ := json.Marshal(item)
			fmt.Println(string(data))
		default:
			fmt.Printf("%+v\n", item)
		}
		s.count++
	}
	return nil
}

// Count 返回已输出的记录数
func (s *Sink) Count() int {
	return s.count
}

var _ dataflow.Sink[types.Record] = (*Sink)(nil)
