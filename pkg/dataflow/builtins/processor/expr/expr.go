// Package expr 提供表达式过滤处理器
package expr

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Processor 根据表达式过滤记录
type Processor struct {
	program *vm.Program
	env     map[string]interface{}
}

// Config Processor 的配置
type Config struct {
	// Expression 过滤表达式，返回 true 保留记录，false 过滤记录
	// 支持类 Python 语法，可访问记录的所有字段
	// 示例: "status == 'active' && score > 60"
	// 内置函数: str_contains, str_hasPrefix, str_hasSuffix, str_matches
	Expression string `json:"expression"`
}

// New 创建新的 Processor
func New() *Processor {
	return &Processor{}
}

// 内置辅助函数
var helperFuncs = map[string]interface{}{
	// 字符串包含检查
	"str_contains": func(s, substr string) bool {
		return strings.Contains(s, substr)
	},
	// 字符串前缀检查
	"str_hasPrefix": func(s, prefix string) bool {
		return strings.HasPrefix(s, prefix)
	},
	// 字符串后缀检查
	"str_hasSuffix": func(s, suffix string) bool {
		return strings.HasSuffix(s, suffix)
	},
	// 正则匹配
	"str_matches": func(s, pattern string) bool {
		matched, err := regexp.MatchString(pattern, s)
		if err != nil {
			return false
		}
		return matched
	},
}

// Init 初始化 Processor
func (p *Processor) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	if cfg.Expression == "" {
		return fmt.Errorf("expression 不能为空")
	}

	// 创建环境，包含辅助函数
	// 运行时会合并记录数据
	p.env = make(map[string]interface{})
	for k, v := range helperFuncs {
		p.env[k] = v
	}

	// 编译表达式
	program, err := expr.Compile(cfg.Expression,
		expr.Env(p.env),
		expr.AllowUndefinedVariables(),
		expr.AsBool(),
	)
	if err != nil {
		return fmt.Errorf("编译表达式失败: %w", err)
	}

	p.program = program
	return nil
}

// Process 处理数据，只输出满足表达式的记录
func (p *Processor) Process(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	for item := range in {
		// 合并记录数据和辅助函数到环境
		env := make(map[string]interface{})
		// 先添加辅助函数
		for k, v := range helperFuncs {
			env[k] = v
		}
		// 再添加记录数据
		for k, v := range item {
			env[k] = v
		}

		// 执行表达式
		result, err := expr.Run(p.program, env)
		if err != nil {
			// 表达式执行错误，跳过该记录
			continue
		}

		// 结果已经是 bool 类型（AsBool() 保证）
		if result.(bool) {
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
