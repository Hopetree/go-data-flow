package dataflow

import "github.com/yourorg/go-data-flow/pkg/dataflow/metrics"

// Closer is an optional interface for components that need resource cleanup.
// If a component implements Closer, Flow will call Close() when the flow ends.
type Closer interface {
	Close() error
}

// ConcurrencyCap describes the concurrency capability of a component.
type ConcurrencyCap struct {
	// Supported indicates whether the component supports concurrent processing
	Supported bool
	// SuggestedMax is the suggested maximum concurrency (0 = no limit)
	SuggestedMax int
	// IsStateful indicates whether the component has internal state
	IsStateful bool
}

// ConcurrencyCapable is an interface for components to declare their concurrency capability.
// The framework uses this to decide whether to run the component concurrently.
//
// Design principle: Concurrency capability is decided by the component itself,
// not by the framework or configuration.
type ConcurrencyCapable interface {
	ConcurrencyCap() ConcurrencyCap
}

// ComponentInfo 包含传递给组件的上下文信息
type ComponentInfo struct {
	// FlowName is the name of the flow
	FlowName string
	// ComponentName is the component name (e.g., "source", "processor[0]", "sink")
	ComponentName string
	// ComponentType is the component type: "source", "processor", or "sink"
	ComponentType string
}

// MetricsRecorderAware is an optional interface for components that need to record custom metrics.
// If a component implements this interface, Flow will inject the metrics recorder after Init.
type MetricsRecorderAware interface {
	SetMetricsRecorder(recorder metrics.MetricsRecorder, info ComponentInfo)
}

// BaseProcessor provides a default implementation of ConcurrencyCapable.
// It declares that the component does NOT support concurrency (safe by default).
type BaseProcessor[T any] struct{}

// ConcurrencyCap returns the default concurrency capability (not supported).
func (p *BaseProcessor[T]) ConcurrencyCap() ConcurrencyCap {
	return ConcurrencyCap{Supported: false}
}

// StatelessProcessor provides a default implementation for stateless processors.
// It declares that the component supports concurrency.
type StatelessProcessor[T any] struct{}

// ConcurrencyCap returns concurrency capability (supported, no limit).
func (p *StatelessProcessor[T]) ConcurrencyCap() ConcurrencyCap {
	return ConcurrencyCap{Supported: true}
}
