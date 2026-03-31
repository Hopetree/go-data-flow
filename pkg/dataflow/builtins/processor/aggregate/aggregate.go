// Package aggregate 提供分组聚合处理器
package aggregate

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Processor 对数据进行分组聚合
type Processor struct {
	groupBy    []string
	aggregates map[string]func(aggregateState, types.Record) aggregateState
	state      map[string]aggregateState
	mu         sync.Mutex
}

// aggregateState 聚合状态
type aggregateState struct {
	count int
	sum   map[string]float64
	value types.Record
}

// Config Processor 的配置
type Config struct {
	// GroupBy 分组字段
	GroupBy []string `json:"group_by"`
	// Aggregates 聚合配置: field -> op (count, sum)
	Aggregates map[string]string `json:"aggregates"`
}

// New 创建新的 Processor
func New() *Processor {
	return &Processor{
		state: make(map[string]aggregateState),
	}
}

// Init 初始化 Processor
func (p *Processor) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	p.groupBy = cfg.GroupBy
	p.aggregates = make(map[string]func(aggregateState, types.Record) aggregateState)

	// 构建聚合函数
	for field, op := range cfg.Aggregates {
		f := field // 捕获变量
		switch op {
		case "count":
			p.aggregates[field] = func(s aggregateState, r types.Record) aggregateState {
				s.count++
				return s
			}
		case "sum":
			p.aggregates[field] = func(s aggregateState, r types.Record) aggregateState {
				if s.sum == nil {
					s.sum = make(map[string]float64)
				}
				if val, ok := r[f]; ok {
					if n, ok := toFloat64(val); ok {
						s.sum[f] += n
					}
				}
				return s
			}
		}
	}

	return nil
}

// Process 处理数据，进行聚合
func (p *Processor) Process(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	for item := range in {
		p.mu.Lock()
		key := p.buildKey(item)
		state := p.state[key]
		state.value = item

		for _, agg := range p.aggregates {
			state = agg(state, item)
		}

		p.state[key] = state
		p.mu.Unlock()
	}

	// 输出聚合结果
	p.mu.Lock()
	for key, state := range p.state {
		result := make(types.Record)
		result["_key"] = key
		result["_count"] = state.count
		for f, v := range state.sum {
			result[f+"_sum"] = v
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- result:
		}
	}
	p.mu.Unlock()

	return nil
}

// buildKey 构建分组键
func (p *Processor) buildKey(record types.Record) string {
	key := ""
	for _, field := range p.groupBy {
		if val, exists := record[field]; exists {
			key += fmt.Sprintf("%v|", val)
		}
	}
	return key
}

// ConcurrencyCap 声明不支持并发（有状态）
func (p *Processor) ConcurrencyCap() dataflow.ConcurrencyCap {
	return dataflow.ConcurrencyCap{
		Supported:    false,
		SuggestedMax: 1,
		IsStateful:   true,
	}
}

// toFloat64 将值转换为 float64
func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// 确保 Processor 实现了 Processor 接口
var _ dataflow.Processor[types.Record] = (*Processor)(nil)
