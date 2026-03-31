// Package jqtransform 提供基于 jq 语法的超级转换处理器
package jqtransform

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/itchyny/gojq"
	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

// Processor 使用 jq 语法进行数据转换
type Processor struct {
	query *gojq.Query
}

// Config Processor 的配置
type Config struct {
	// Query jq 查询表达式
	// 示例: "{user_id: .id, name: .name, total: (.items | map(.price) | add)}"
	Query string `json:"query"`
}

// New 创建新的 Processor
func New() *Processor {
	return &Processor{}
}

// Init 初始化 Processor
func (p *Processor) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	if cfg.Query == "" {
		return fmt.Errorf("query 不能为空")
	}

	// 解析 jq 查询
	query, err := gojq.Parse(cfg.Query)
	if err != nil {
		return fmt.Errorf("解析 jq 查询失败: %w", err)
	}

	p.query = query
	return nil
}

// Process 处理数据，使用 jq 表达式转换记录
func (p *Processor) Process(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	for item := range in {
		// 将 types.Record 转换为 map[string]interface{}
		// gojq 需要标准的 map 类型
		var input map[string]interface{} = item

		// 执行 jq 查询
		iter := p.query.Run(input)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			v, ok := iter.Next()
			if !ok {
				break
			}

			if err, isErr := v.(error); isErr {
				// jq 执行错误，跳过该记录
				_ = err
				continue
			}

			// 将结果转换为 Record
			switch result := v.(type) {
			case map[string]interface{}:
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- types.Record(result):
				}

			case types.Record:
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- result:
				}

			case []interface{}:
				// 数组结果，展开为多条记录
				for _, elem := range result {
					if elemMap, ok := elem.(map[string]interface{}); ok {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case out <- types.Record(elemMap):
						}
					}
				}

			case nil:
				// null 结果，跳过（用于过滤）
				continue

			default:
				// 其他类型（基本类型），包装为记录
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- types.Record{"_value": result}:
				}
			}
		}
	}
	return nil
}

// ConcurrencyCap 声明支持并发
func (p *Processor) ConcurrencyCap() dataflow.ConcurrencyCap {
	return dataflow.ConcurrencyCap{
		Supported:    true,
		SuggestedMax: 4,
		IsStateful:   false,
	}
}

// 确保实现了接口
var _ dataflow.Processor[types.Record] = (*Processor)(nil)
