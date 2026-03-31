// Package null 提供丢弃数据的 Sink，用于性能测试
package null

import (
	"context"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Sink 丢弃所有数据
type Sink struct{}

// New 创建新的 Sink
func New() *Sink {
	return &Sink{}
}

// Init 初始化 Sink (无配置)
func (s *Sink) Init(config []byte) error {
	return nil
}

// Consume 丢弃所有数据
func (s *Sink) Consume(ctx context.Context, in <-chan types.Record) error {
	for range in {
		// 丢弃数据
	}
	return nil
}

var _ dataflow.Sink[types.Record] = (*Sink)(nil)
