// Package condition 提供条件过滤处理器
package condition

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

// Processor 根据条件过滤记录
type Processor struct {
	condition func(types.Record) bool
}

// Config Processor 的配置
type Config struct {
	// Field 要过滤的字段名
	Field string `json:"field"`
	// Op 操作符
	// 比较操作符: eq, ne, gt, gte, lt, lte
	// 数组操作符: in, nin
	// 存在性操作符: exists
	// 字符串操作符: contains, prefix, suffix, regex
	Op string `json:"op"`
	// Value 比较值
	Value interface{} `json:"value"`
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

	// 构建过滤条件
	condition, err := p.buildCondition(cfg)
	if err != nil {
		return fmt.Errorf("构建条件失败: %w", err)
	}
	p.condition = condition
	return nil
}

// buildCondition 根据配置构建过滤条件
func (p *Processor) buildCondition(cfg Config) (func(types.Record) bool, error) {
	// 预处理：为 in/nin 操作符构建查找集合（O(1) 查找）
	var valueSet map[interface{}]bool
	if cfg.Op == "in" || cfg.Op == "nin" {
		valueSet = buildValueSet(cfg.Value)
	}

	// 预处理：编译正则表达式
	var regex *regexp.Regexp
	if cfg.Op == "regex" {
		pattern, ok := cfg.Value.(string)
		if !ok {
			return nil, fmt.Errorf("regex 操作符的 value 必须是字符串")
		}
		var err error
		regex, err = regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("编译正则表达式失败: %w", err)
		}
	}

	return func(record types.Record) bool {
		val, exists := record[cfg.Field]

		switch cfg.Op {
		// 存在性操作符
		case "exists":
			if !exists {
				return cfg.Value == false
			}
			return cfg.Value != false

		// 比较操作符
		case "eq":
			if !exists {
				return false
			}
			return compareEqual(val, cfg.Value)
		case "ne":
			if !exists {
				return true
			}
			return !compareEqual(val, cfg.Value)
		case "gt":
			if !exists {
				return false
			}
			return compareGreater(val, cfg.Value)
		case "gte":
			if !exists {
				return false
			}
			return compareGreaterOrEqual(val, cfg.Value)
		case "lt":
			if !exists {
				return false
			}
			return compareLess(val, cfg.Value)
		case "lte":
			if !exists {
				return false
			}
			return compareLessOrEqual(val, cfg.Value)

		// 数组操作符
		case "in":
			if !exists {
				return false
			}
			return valueInSet(val, valueSet)
		case "nin":
			if !exists {
				return true
			}
			return !valueInSet(val, valueSet)

		// 字符串操作符
		case "contains":
			if !exists {
				return false
			}
			return compareContains(val, cfg.Value)
		case "prefix":
			if !exists {
				return false
			}
			return comparePrefix(val, cfg.Value)
		case "suffix":
			if !exists {
				return false
			}
			return compareSuffix(val, cfg.Value)
		case "regex":
			if !exists {
				return false
			}
			return compareRegex(val, regex)

		default:
			return true
		}
	}, nil
}

// buildValueSet 为 in/nin 操作符构建值查找集合（O(1) 查找）
func buildValueSet(value interface{}) map[interface{}]bool {
	set := make(map[interface{}]bool)

	// value 应该是一个数组
	arr, ok := value.([]interface{})
	if !ok {
		// 如果不是数组，将单个值放入集合
		set[value] = true
		return set
	}

	for _, v := range arr {
		set[v] = true
	}
	return set
}

// valueInSet 检查值是否在集合中
func valueInSet(val interface{}, set map[interface{}]bool) bool {
	// 直接查找
	if set[val] {
		return true
	}

	// 尝试类型转换后查找（处理 int vs float64 的情况）
	valFloat, valOk := toFloat64(val)
	if valOk {
		for k := range set {
			if kFloat, kOk := toFloat64(k); kOk && kFloat == valFloat {
				return true
			}
		}
	}

	return false
}

// compareEqual 比较两个值是否相等，处理类型转换
func compareEqual(a, b interface{}) bool {
	// 直接比较
	if a == b {
		return true
	}

	// 尝试数值类型转换
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		return aFloat == bFloat
	}

	// 字符串比较
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr == bStr
	}

	return false
}

// compareGreater 比较是否大于
func compareGreater(a, b interface{}) bool {
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		return aFloat > bFloat
	}

	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr > bStr
	}

	return false
}

// compareGreaterOrEqual 比较是否大于等于
func compareGreaterOrEqual(a, b interface{}) bool {
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		return aFloat >= bFloat
	}

	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr >= bStr
	}

	return false
}

// compareLess 比较是否小于
func compareLess(a, b interface{}) bool {
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		return aFloat < bFloat
	}

	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr < bStr
	}

	return false
}

// compareLessOrEqual 比较是否小于等于
func compareLessOrEqual(a, b interface{}) bool {
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		return aFloat <= bFloat
	}

	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		return aStr <= bStr
	}

	return false
}

// compareContains 检查字符串是否包含子串
func compareContains(a, b interface{}) bool {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if !aOk || !bOk {
		return false
	}
	return strings.Contains(aStr, bStr)
}

// comparePrefix 检查字符串是否有指定前缀
func comparePrefix(a, b interface{}) bool {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if !aOk || !bOk {
		return false
	}
	return strings.HasPrefix(aStr, bStr)
}

// compareSuffix 检查字符串是否有指定后缀
func compareSuffix(a, b interface{}) bool {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if !aOk || !bOk {
		return false
	}
	return strings.HasSuffix(aStr, bStr)
}

// compareRegex 正则匹配
func compareRegex(a interface{}, regex *regexp.Regexp) bool {
	aStr, ok := a.(string)
	if !ok {
		return false
	}
	return regex.MatchString(aStr)
}

// toFloat64 将值转换为 float64
func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// Process 处理数据，只输出满足条件的记录
func (p *Processor) Process(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	for item := range in {
		if p.condition(item) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- item:
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
