// Package collect 提供内存收集的 Sink，主要用于测试
package collect

import (
	"context"
	"sync"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

// Sink 将数据收集到内存中
type Sink struct {
	data []types.Record
	mu   sync.Mutex
}

// New 创建新的 Sink
func New() *Sink {
	return &Sink{
		data: make([]types.Record, 0),
	}
}

// Init 初始化 Sink (无配置)
func (s *Sink) Init(config []byte) error {
	return nil
}

// Consume 收集数据到内存
func (s *Sink) Consume(ctx context.Context, in <-chan types.Record) error {
	for item := range in {
		s.mu.Lock()
		s.data = append(s.data, item)
		s.mu.Unlock()
	}
	return nil
}

// Data 返回收集的数据
func (s *Sink) Data() []types.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

// Count 返回收集的数据数量
func (s *Sink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// Reset 清空收集的数据
func (s *Sink) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make([]types.Record, 0)
}

var _ dataflow.Sink[types.Record] = (*Sink)(nil)
