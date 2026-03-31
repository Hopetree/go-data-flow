// Package app 提供应用程序框架
package app

import (
	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/processor/aggregate"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/processor/condition"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/processor/expr"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/processor/jqtransform"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/processor/transform"
	pythonprocessor "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/python/processor"
	pythonsink "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/python/sink"
	pythonsource "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/python/source"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/sink/collect"
	cksink "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/sink/clickhouse"
	csvsink "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/sink/csv"
	jsonsink "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/sink/json"
	nullsink "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/sink/null"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/sink/output"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/source/csv"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/source/generator"
	jsonsource "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/source/json"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/source/kafka"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/source/static"
)

// RegisterBuiltinSources 注册所有内置 Source 组件
func RegisterBuiltinSources(r *dataflow.Registry[Record]) {
	// static 组件 - 静态内存数据
	r.RegisterSource("source-static-data", func() dataflow.Source[Record] {
		return static.New()
	})

	// generator 组件 - 序列生成器
	r.RegisterSource("source-generator-sequence", func() dataflow.Source[Record] {
		return generator.New()
	})

	// csv 组件 - CSV 文件读取
	r.RegisterSource("source-csv-file", func() dataflow.Source[Record] {
		return csv.New()
	})

	// json 组件 - JSON 文件读取
	r.RegisterSource("source-json-file", func() dataflow.Source[Record] {
		return jsonsource.New()
	})

	// kafka 组件 - Kafka 消费者
	r.RegisterSource("source-kafka-consumer", func() dataflow.Source[Record] {
		return kafka.New()
	})

	// python 组件 - Python 脚本数据源
	r.RegisterSource("source-python-script", func() dataflow.Source[Record] {
		return pythonsource.New()
	})
}

// RegisterBuiltinProcessors 注册所有内置 Processor 组件
func RegisterBuiltinProcessors(r *dataflow.Registry[Record]) {
	// condition 组件 - 条件过滤
	r.RegisterProcessor("processor-condition-filter", func() dataflow.Processor[Record] {
		return condition.New()
	})

	// expr 组件 - 表达式过滤
	r.RegisterProcessor("processor-expr-filter", func() dataflow.Processor[Record] {
		return expr.New()
	})

	// transform 组件 - 字段转换
	r.RegisterProcessor("processor-transform-field", func() dataflow.Processor[Record] {
		return transform.New()
	})

	// jqtransform 组件 - jq 表达式转换
	r.RegisterProcessor("processor-jq-transform", func() dataflow.Processor[Record] {
		return jqtransform.New()
	})

	// aggregate 组件 - 分组聚合
	r.RegisterProcessor("processor-aggregate-group", func() dataflow.Processor[Record] {
		return aggregate.New()
	})

	// python 组件 - Python 脚本处理器
	r.RegisterProcessor("processor-python-script", func() dataflow.Processor[Record] {
		return pythonprocessor.New()
	})
}

// RegisterBuiltinSinks 注册所有内置 Sink 组件
func RegisterBuiltinSinks(r *dataflow.Registry[Record]) {
	// output 组件 - 控制台输出
	r.RegisterSink("sink-output-console", func() dataflow.Sink[Record] {
		return output.New()
	})

	// collect 组件 - 内存收集
	r.RegisterSink("sink-collect-memory", func() dataflow.Sink[Record] {
		return collect.New()
	})

	// null 组件 - 丢弃数据
	r.RegisterSink("sink-null-discard", func() dataflow.Sink[Record] {
		return nullsink.New()
	})

	// csv 组件 - CSV 文件输出
	r.RegisterSink("sink-csv-file", func() dataflow.Sink[Record] {
		return csvsink.New()
	})

	// json 组件 - JSON 文件输出
	r.RegisterSink("sink-json-file", func() dataflow.Sink[Record] {
		return jsonsink.New()
	})

	// python 组件 - Python 脚本数据输出
	r.RegisterSink("sink-python-script", func() dataflow.Sink[Record] {
		return pythonsink.New()
	})

	// clickhouse 组件 - ClickHouse 批量写入
	r.RegisterSink("sink-clickhouse", func() dataflow.Sink[Record] {
		return cksink.New()
	})
}

// RegisterAllBuiltins 一次性注册所有内置组件
func RegisterAllBuiltins(r *dataflow.Registry[Record]) {
	RegisterBuiltinSources(r)
	RegisterBuiltinProcessors(r)
	RegisterBuiltinSinks(r)
}
