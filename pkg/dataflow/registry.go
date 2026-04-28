package dataflow

import (
	"sort"
	"sync"
)

// Registry manages component registrations.
type Registry[T any] struct {
	mu         sync.RWMutex
	sources    map[string]func() Source[T]
	processors map[string]func() Processor[T]
	sinks      map[string]func() Sink[T]
}

// NewRegistry creates a new component registry.
func NewRegistry[T any]() *Registry[T] {
	return &Registry[T]{
		sources:    make(map[string]func() Source[T]),
		processors: make(map[string]func() Processor[T]),
		sinks:      make(map[string]func() Sink[T]),
	}
}

// RegisterSource registers a source factory function.
func (r *Registry[T]) RegisterSource(name string, builder func() Source[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources[name] = builder
}

// RegisterProcessor registers a processor factory function.
func (r *Registry[T]) RegisterProcessor(name string, builder func() Processor[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processors[name] = builder
}

// RegisterSink registers a sink factory function.
func (r *Registry[T]) RegisterSink(name string, builder func() Sink[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[name] = builder
}

// GetSource returns a new source instance by name.
func (r *Registry[T]) GetSource(name string) (Source[T], bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if builder, ok := r.sources[name]; ok {
		return builder(), true
	}
	return nil, false
}

// GetProcessor returns a new processor instance by name.
func (r *Registry[T]) GetProcessor(name string) (Processor[T], bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if builder, ok := r.processors[name]; ok {
		return builder(), true
	}
	return nil, false
}

// GetSink returns a new sink instance by name.
func (r *Registry[T]) GetSink(name string) (Sink[T], bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if builder, ok := r.sinks[name]; ok {
		return builder(), true
	}
	return nil, false
}

// ListSources returns all registered source names.
func (r *Registry[T]) ListSources() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.sources))
	for name := range r.sources {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ListProcessors returns all registered processor names.
func (r *Registry[T]) ListProcessors() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.processors))
	for name := range r.processors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ListSinks returns all registered sink names.
func (r *Registry[T]) ListSinks() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.sinks))
	for name := range r.sinks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// defaultRegistry 是框架级别的全局默认注册表。
// 使用 map[string]interface{}] 作为泛型参数，与内置组件和 App 框架保持一致。
// 未导出变量通过 GetDefaultRegistry() 访问，遵循 Go 惯例。
var defaultRegistry = NewRegistry[map[string]interface{}]()

// GetDefaultRegistry 返回全局默认注册表。
// 组件可通过 init() 函数调用此方法注册自身，实现自动注册。
// 典型用法：
//
//	func init() {
//	    dataflow.GetDefaultRegistry().RegisterSource("source-my", func() dataflow.Source[map[string]interface{}] {
//	        return &MySource{}
//	    })
//	}
//
// 然后在 main.go 中通过 blank import 触发：
//
//	import _ "myproject/internal/components/mysource"
func GetDefaultRegistry() *Registry[map[string]interface{}] {
	return defaultRegistry
}
