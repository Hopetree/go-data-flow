// Package transform 提供字段转换处理器
package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

// Processor 转换记录的字段
type Processor struct {
	transform func(types.Record) ([]types.Record, error)
}

// Config Processor 的配置
type Config struct {
	// Mapping 字段映射: 原字段名 -> 新字段名
	Mapping map[string]string `json:"mapping"`
	// Add 要添加的字段
	Add map[string]interface{} `json:"add"`
	// Remove 要删除的字段
	Remove []string `json:"remove"`

	// Extract 提取指定字段的值作为新记录（支持点号分隔的嵌套路径）
	Extract string `json:"extract"`
	// ExtractKeep 提取时保留的原始字段列表
	ExtractKeep []string `json:"extract_keep"`
	// ExtractFlatten 当提取的字段是数组时，展开为多条记录
	ExtractFlatten bool `json:"extract_flatten"`
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

	p.transform = p.buildTransform(cfg)
	return nil
}

// buildTransform 根据配置构建转换函数
func (p *Processor) buildTransform(cfg Config) func(types.Record) ([]types.Record, error) {
	return func(record types.Record) ([]types.Record, error) {
		// 1. 处理 extract
		if cfg.Extract != "" {
			return p.extractAndTransform(record, cfg)
		}

		// 无 extract，执行常规转换
		result := make(types.Record)

		// 复制原始记录
		for k, v := range record {
			result[k] = v
		}

		// 应用字段映射
		for oldName, newName := range cfg.Mapping {
			if val, exists := result[oldName]; exists {
				delete(result, oldName)
				result[newName] = val
			}
		}

		// 添加字段
		for k, v := range cfg.Add {
			result[k] = v
		}

		// 删除字段
		for _, k := range cfg.Remove {
			delete(result, k)
		}

		return []types.Record{result}, nil
	}
}

// extractAndTransform 提取字段并转换
func (p *Processor) extractAndTransform(record types.Record, cfg Config) ([]types.Record, error) {
	// 获取要保留的原始字段
	keepValues := make(types.Record)
	for _, field := range cfg.ExtractKeep {
		if val, exists := record[field]; exists {
			keepValues[field] = val
		}
	}

	// 提取指定字段
	extracted := p.getNestedValue(record, cfg.Extract)
	if extracted == nil {
		// 字段不存在，返回空记录
		return []types.Record{{}}, nil
	}

	// 检查提取的值类型
	switch v := extracted.(type) {
	case map[string]interface{}:
		// 对象类型，合并保留字段后进行后续转换
		result := p.mergeAndTransform(v, keepValues, cfg)
		return []types.Record{result}, nil

	case types.Record:
		// types.Record 类型（与 map[string]interface{} 相同处理）
		result := p.mergeAndTransform(v, keepValues, cfg)
		return []types.Record{result}, nil

	case []interface{}:
		// 数组类型
		if cfg.ExtractFlatten {
			// 展开数组
			results := make([]types.Record, 0, len(v))
			for _, item := range v {
				switch itemMap := item.(type) {
				case map[string]interface{}:
					result := p.mergeAndTransform(itemMap, keepValues, cfg)
					results = append(results, result)
				case types.Record:
					result := p.mergeAndTransform(itemMap, keepValues, cfg)
					results = append(results, result)
				default:
					// 数组元素不是对象，创建包含 _value 的记录
					result := p.mergeAndTransform(map[string]interface{}{"_value": item}, keepValues, cfg)
					results = append(results, result)
				}
			}
			if len(results) == 0 {
				return []types.Record{{}}, nil
			}
			return results, nil
		}
		// 不展开，将数组放入 _value 字段
		result := p.mergeAndTransform(map[string]interface{}{"_value": v}, keepValues, cfg)
		return []types.Record{result}, nil

	default:
		// 其他类型（基本类型），返回空记录
		return []types.Record{{}}, nil
	}
}

// getNestedValue 获取嵌套字段的值
func (p *Processor) getNestedValue(record types.Record, path string) interface{} {
	parts := strings.Split(path, ".")
	var current interface{} = record

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			var exists bool
			current, exists = v[part]
			if !exists {
				return nil
			}
		case types.Record:
			var exists bool
			current, exists = v[part]
			if !exists {
				return nil
			}
		default:
			return nil
		}
	}

	return current
}

// mergeAndTransform 合并提取的值和保留字段，然后应用后续转换
func (p *Processor) mergeAndTransform(extracted, keepValues types.Record, cfg Config) types.Record {
	result := make(types.Record)

	// 复制提取的数据
	for k, v := range extracted {
		result[k] = v
	}

	// 合并保留的原始字段
	for k, v := range keepValues {
		result[k] = v
	}

	// 应用字段映射
	for oldName, newName := range cfg.Mapping {
		if val, exists := result[oldName]; exists {
			delete(result, oldName)
			result[newName] = val
		}
	}

	// 添加字段
	for k, v := range cfg.Add {
		result[k] = v
	}

	// 删除字段
	for _, k := range cfg.Remove {
		delete(result, k)
	}

	return result
}

// Process 处理数据，转换记录
func (p *Processor) Process(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	for item := range in {
		results, err := p.transform(item)
		if err != nil {
			return err
		}
		for _, result := range results {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- result:
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

// 确保 Processor 实现了 Processor 接口
var _ dataflow.Processor[types.Record] = (*Processor)(nil)
