package dataflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestData is a simple test struct.
type TestData struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// BlockingSource is a source that blocks until released.
type BlockingSource[T any] struct {
	items   []T
	started chan struct{}
	release chan struct{}
}

func NewBlockingSource[T any](items []T) *BlockingSource[T] {
	return &BlockingSource[T]{
		items:   items,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *BlockingSource[T]) Init(config []byte) error {
	return nil
}

func (s *BlockingSource[T]) Read(ctx context.Context, out chan<- T) (int64, error) {
	close(s.started)
	<-s.release

	var count int64
	for _, item := range s.items {
		select {
		case out <- item:
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}
	return count, nil
}

func (s *BlockingSource[T]) WaitForStart() {
	<-s.started
}

func (s *BlockingSource[T]) Release() {
	close(s.release)
}

// MockSource implements Source for testing.
type MockSource[T any] struct {
	data  []T
	err   error
	count int64
}

func (s *MockSource[T]) Init(config []byte) error {
	// If data is already set (via SetData or struct literal), don't override
	if len(s.data) > 0 {
		return nil
	}
	var cfg struct {
		Data []T `json:"data"`
	}
	if len(config) > 0 {
		if err := json.Unmarshal(config, &cfg); err != nil {
			return err
		}
		s.data = cfg.Data
	}
	return nil
}

func (s *MockSource[T]) Read(ctx context.Context, out chan<- T) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}
	for _, item := range s.data {
		select {
		case out <- item:
			s.count++
		case <-ctx.Done():
			return s.count, ctx.Err()
		}
	}
	return s.count, nil
}

func (s *MockSource[T]) SetData(data []T) {
	s.data = data
}

func (s *MockSource[T]) SetError(err error) {
	s.err = err
}

// MockProcessor implements Processor for testing.
type MockProcessor[T any] struct {
	processFunc func(T) (T, bool)
	err         error
	count       int64
	filtered    int64
	mu          sync.Mutex
}

func (p *MockProcessor[T]) Init(config []byte) error {
	return nil
}

func (p *MockProcessor[T]) Process(ctx context.Context, in <-chan T, out chan<- T) error {
	if p.err != nil {
		return p.err
	}
	for item := range in {
		p.mu.Lock()
		p.count++
		p.mu.Unlock()

		processed, pass := item, true
		if p.processFunc != nil {
			processed, pass = p.processFunc(item)
			if !pass {
				p.mu.Lock()
				p.filtered++
				p.mu.Unlock()
				continue
			}
		}

		select {
		case out <- processed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *MockProcessor[T]) SetProcessFunc(f func(T) (T, bool)) {
	p.processFunc = f
}

func (p *MockProcessor[T]) SetError(err error) {
	p.err = err
}

func (p *MockProcessor[T]) Count() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

// MockSink implements Sink for testing.
type MockSink[T any] struct {
	data  []T
	err   error
	count int64
	mu    sync.Mutex
}

func (s *MockSink[T]) Init(config []byte) error {
	s.data = make([]T, 0)
	return nil
}

func (s *MockSink[T]) Consume(ctx context.Context, in <-chan T) error {
	if s.err != nil {
		return s.err
	}
	for item := range in {
		s.mu.Lock()
		s.data = append(s.data, item)
		s.count++
		s.mu.Unlock()
	}
	return nil
}

func (s *MockSink[T]) SetError(err error) {
	s.err = err
}

func (s *MockSink[T]) Data() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

func (s *MockSink[T]) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// ========== Tests ==========

func TestNewFlow(t *testing.T) {
	registry := NewRegistry[TestData]()
	config := &FlowConfig{
		Name: "test_flow",
		Source: ComponentSpec{
			Name: "mock",
		},
		Sink: ComponentSpec{
			Name: "mock",
		},
	}

	flow := NewFlow[TestData](config, registry)
	if flow == nil {
		t.Fatal("expected non-nil flow")
	}
	if flow.config.Name != "test_flow" {
		t.Errorf("expected name 'test_flow', got '%s'", flow.config.Name)
	}
}

func TestFlowConfigDefaults(t *testing.T) {
	config := &FlowConfig{
		Name: "test",
		Source: ComponentSpec{
			Name: "mock",
		},
		Sink: ComponentSpec{
			Name: "mock",
		},
	}
	config.SetDefaults()

	if config.BufferSize != 100 {
		t.Errorf("expected default buffer size 100, got %d", config.BufferSize)
	}
}

func TestFlowConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    *FlowConfig
		expectErr error
	}{
		{
			name: "valid config",
			config: &FlowConfig{
				Name:   "test",
				Source: ComponentSpec{Name: "mock"},
				Sink:   ComponentSpec{Name: "mock"},
			},
			expectErr: nil,
		},
		{
			name: "missing name",
			config: &FlowConfig{
				Source: ComponentSpec{Name: "mock"},
				Sink:   ComponentSpec{Name: "mock"},
			},
			expectErr: ErrFlowNameRequired,
		},
		{
			name: "missing source",
			config: &FlowConfig{
				Name: "test",
				Sink:  ComponentSpec{Name: "mock"},
			},
			expectErr: ErrSourceRequired,
		},
		{
			name: "missing sink",
			config: &FlowConfig{
				Name:   "test",
				Source: ComponentSpec{Name: "mock"},
			},
			expectErr: ErrSinkRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr != nil {
				if err != tt.expectErr {
					t.Errorf("expected error %v, got %v", tt.expectErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestFlowBuild(t *testing.T) {
	t.Run("successful build", func(t *testing.T) {
		registry := NewRegistry[TestData]()
		registry.RegisterSource("mock", func() Source[TestData] {
			return &MockSource[TestData]{}
		})
		registry.RegisterSink("mock", func() Sink[TestData] {
			return &MockSink[TestData]{}
		})

		config := &FlowConfig{
			Name:       "test",
			BufferSize: 10,
			Source:     ComponentSpec{Name: "mock"},
			Sink:       ComponentSpec{Name: "mock"},
		}

		flow := NewFlow[TestData](config, registry)
		if err := flow.Build(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("unknown source", func(t *testing.T) {
		registry := NewRegistry[TestData]()
		registry.RegisterSink("mock", func() Sink[TestData] {
			return &MockSink[TestData]{}
		})

		config := &FlowConfig{
			Name:   "test",
			Source: ComponentSpec{Name: "unknown"},
			Sink:   ComponentSpec{Name: "mock"},
		}

		flow := NewFlow[TestData](config, registry)
		err := flow.Build()
		if err == nil {
			t.Error("expected error for unknown source")
		}
	})

	t.Run("unknown sink", func(t *testing.T) {
		registry := NewRegistry[TestData]()
		registry.RegisterSource("mock", func() Source[TestData] {
			return &MockSource[TestData]{}
		})

		config := &FlowConfig{
			Name:   "test",
			Source: ComponentSpec{Name: "mock"},
			Sink:   ComponentSpec{Name: "unknown"},
		}

		flow := NewFlow[TestData](config, registry)
		err := flow.Build()
		if err == nil {
			t.Error("expected error for unknown sink")
		}
	})
}

func TestFlowRun(t *testing.T) {
	t.Run("basic flow", func(t *testing.T) {
		data := []TestData{
			{ID: 1, Name: "Alice", Status: "active"},
			{ID: 2, Name: "Bob", Status: "inactive"},
			{ID: 3, Name: "Charlie", Status: "active"},
		}

		// Create instances and capture in closure
		source := &MockSource[TestData]{data: data}
		sink := &MockSink[TestData]{}

		registry := NewRegistry[TestData]()
		registry.RegisterSource("test_source", func() Source[TestData] {
			return source
		})
		registry.RegisterSink("test_sink", func() Sink[TestData] {
			return sink
		})

		config := &FlowConfig{
			Name:       "test",
			BufferSize: 10,
			Source:     ComponentSpec{Name: "test_source"},
			Sink:       ComponentSpec{Name: "test_sink"},
		}

		flow := NewFlow[TestData](config, registry)
		if err := flow.Build(); err != nil {
			t.Fatalf("build failed: %v", err)
		}

		ctx := context.Background()
		if err := flow.Run(ctx); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		stats := flow.Stats()
		if stats.TotalIn != 3 {
			t.Errorf("expected TotalIn 3, got %d", stats.TotalIn)
		}

		if sink.Count() != 3 {
			t.Errorf("expected sink count 3, got %d", sink.Count())
		}
	})

	t.Run("flow with processor", func(t *testing.T) {
		data := []TestData{
			{ID: 1, Name: "Alice", Status: "active"},
			{ID: 2, Name: "Bob", Status: "inactive"},
			{ID: 3, Name: "Charlie", Status: "active"},
		}

		source := &MockSource[TestData]{data: data}
		processor := &MockProcessor[TestData]{
			processFunc: func(d TestData) (TestData, bool) {
				return d, d.Status == "active"
			},
		}
		sink := &MockSink[TestData]{}

		registry := NewRegistry[TestData]()
		registry.RegisterSource("filter_source", func() Source[TestData] {
			return source
		})
		registry.RegisterProcessor("filter", func() Processor[TestData] {
			return processor
		})
		registry.RegisterSink("filter_sink", func() Sink[TestData] {
			return sink
		})

		config := &FlowConfig{
			Name:       "filter_test",
			BufferSize: 10,
			Source:     ComponentSpec{Name: "filter_source"},
			Processors: []ComponentSpec{{Name: "filter"}},
			Sink:       ComponentSpec{Name: "filter_sink"},
		}

		flow := NewFlow[TestData](config, registry)
		if err := flow.Build(); err != nil {
			t.Fatalf("build failed: %v", err)
		}

		ctx := context.Background()
		if err := flow.Run(ctx); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		if sink.Count() != 2 {
			t.Errorf("expected sink count 2 (only active), got %d", sink.Count())
		}
	})

	t.Run("run without build", func(t *testing.T) {
		registry := NewRegistry[TestData]()
		config := &FlowConfig{
			Name:   "test",
			Source: ComponentSpec{Name: "mock"},
			Sink:   ComponentSpec{Name: "mock"},
		}
		flow := NewFlow[TestData](config, registry)
		err := flow.Run(context.Background())
		if err != ErrFlowNotBuilt {
			t.Errorf("expected ErrFlowNotBuilt, got %v", err)
		}
	})

	t.Run("double run", func(t *testing.T) {
		// Use a slow source that blocks until we release it
		source := NewBlockingSource([]TestData{{ID: 1}, {ID: 2}, {ID: 3}})
		sink := &MockSink[TestData]{}

		registry := NewRegistry[TestData]()
		registry.RegisterSource("double_source", func() Source[TestData] {
			return source
		})
		registry.RegisterSink("double_sink", func() Sink[TestData] {
			return sink
		})

		config := &FlowConfig{
			Name:       "double_test",
			BufferSize: 10,
			Source:     ComponentSpec{Name: "double_source"},
			Sink:       ComponentSpec{Name: "double_sink"},
		}

		flow := NewFlow[TestData](config, registry)
		if err := flow.Build(); err != nil {
			t.Fatalf("build failed: %v", err)
		}

		ctx := context.Background()
		done := make(chan struct{})
		go func() {
			flow.Run(ctx)
			close(done)
		}()

		// Wait for the flow to actually start running
		source.WaitForStart()

		// Try to run again - should fail with ErrFlowAlreadyRunning
		err := flow.Run(ctx)
		if err != ErrFlowAlreadyRunning {
			t.Errorf("expected ErrFlowAlreadyRunning, got %v", err)
		}

		// Release the source and wait for completion
		source.Release()
		<-done
	})
}

func TestFlowContextCancellation(t *testing.T) {
	// Create a slow source that blocks
	source := &MockSource[TestData]{
		data: make([]TestData, 1000),
	}
	sink := &MockSink[TestData]{}

	registry := NewRegistry[TestData]()
	registry.RegisterSource("cancel_source", func() Source[TestData] {
		return source
	})
	registry.RegisterSink("cancel_sink", func() Sink[TestData] {
		return sink
	})

	config := &FlowConfig{
		Name:       "cancel_test",
		BufferSize: 10,
		Source:     ComponentSpec{Name: "cancel_source"},
		Sink:       ComponentSpec{Name: "cancel_sink"},
	}

	flow := NewFlow[TestData](config, registry)
	if err := flow.Build(); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := flow.Run(ctx)
	// The flow should complete or be cancelled
	t.Logf("run returned: %v", err)
}

func TestRegistry(t *testing.T) {
	registry := NewRegistry[TestData]()

	// Test registration
	registry.RegisterSource("test_source", func() Source[TestData] {
		return &MockSource[TestData]{}
	})
	registry.RegisterProcessor("test_processor", func() Processor[TestData] {
		return &MockProcessor[TestData]{}
	})
	registry.RegisterSink("test_sink", func() Sink[TestData] {
		return &MockSink[TestData]{}
	})

	// Test retrieval
	src, ok := registry.GetSource("test_source")
	if !ok || src == nil {
		t.Error("expected to find test_source")
	}

	proc, ok := registry.GetProcessor("test_processor")
	if !ok || proc == nil {
		t.Error("expected to find test_processor")
	}

	sink, ok := registry.GetSink("test_sink")
	if !ok || sink == nil {
		t.Error("expected to find test_sink")
	}

	// Test non-existent
	_, ok = registry.GetSource("nonexistent")
	if ok {
		t.Error("expected not to find nonexistent source")
	}

	// Test listing
	sources := registry.ListSources()
	if len(sources) != 1 || sources[0] != "test_source" {
		t.Errorf("unexpected sources list: %v", sources)
	}
}

func TestStatsCounter(t *testing.T) {
	var counter StatsCounter

	counter.AddIn(10)
	counter.AddOut(8)
	counter.AddError(2)
	counter.SetDuration(1000)

	stats := counter.Get()
	if stats.TotalIn != 10 {
		t.Errorf("expected TotalIn 10, got %d", stats.TotalIn)
	}
	if stats.TotalOut != 8 {
		t.Errorf("expected TotalOut 8, got %d", stats.TotalOut)
	}
	if stats.TotalError != 2 {
		t.Errorf("expected TotalError 2, got %d", stats.TotalError)
	}
	if stats.Duration != 1000 {
		t.Errorf("expected Duration 1000, got %d", stats.Duration)
	}

	// Test reset
	counter.Reset()
	stats = counter.Get()
	if stats.TotalIn != 0 {
		t.Errorf("expected TotalIn 0 after reset, got %d", stats.TotalIn)
	}
}

func TestConcurrencyCap(t *testing.T) {
	// Test BaseProcessor (not concurrency capable)
	base := &BaseProcessor[TestData]{}
	cap := base.ConcurrencyCap()
	if cap.Supported {
		t.Error("BaseProcessor should not support concurrency")
	}

	// Test StatelessProcessor (concurrency capable)
	stateless := &StatelessProcessor[TestData]{}
	cap = stateless.ConcurrencyCap()
	if !cap.Supported {
		t.Error("StatelessProcessor should support concurrency")
	}
}

func TestMultipleProcessors(t *testing.T) {
	source := &MockSource[int]{data: []int{1, 2, 3, 4, 5}}
	// First processor: double the value
	proc1 := &MockProcessor[int]{
		processFunc: func(v int) (int, bool) {
			return v * 2, true
		},
	}
	// Second processor: filter values > 5
	proc2 := &MockProcessor[int]{
		processFunc: func(v int) (int, bool) {
			return v, v > 5
		},
	}
	sink := &MockSink[int]{}

	registry := NewRegistry[int]()
	registry.RegisterSource("multi_source", func() Source[int] {
		return source
	})
	registry.RegisterProcessor("double", func() Processor[int] {
		return proc1
	})
	registry.RegisterProcessor("filter_gt5", func() Processor[int] {
		return proc2
	})
	registry.RegisterSink("multi_sink", func() Sink[int] {
		return sink
	})

	config := &FlowConfig{
		Name:       "multi_processor_test",
		BufferSize: 10,
		Source:     ComponentSpec{Name: "multi_source"},
		Processors: []ComponentSpec{
			{Name: "double"},
			{Name: "filter_gt5"},
		},
		Sink: ComponentSpec{Name: "multi_sink"},
	}

	flow := NewFlow[int](config, registry)
	if err := flow.Build(); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	ctx := context.Background()
	if err := flow.Run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Input: 1, 2, 3, 4, 5
	// After double: 2, 4, 6, 8, 10
	// After filter > 5: 6, 8, 10
	expected := []int{6, 8, 10}
	result := sink.Data()
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(result), result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("expected result[%d] = %d, got %d", i, v, result[i])
		}
	}
}

func TestFlowClose(t *testing.T) {
	source := &MockSource[TestData]{data: []TestData{{ID: 1}}}
	sink := &MockSink[TestData]{}

	registry := NewRegistry[TestData]()
	registry.RegisterSource("close_source", func() Source[TestData] {
		return source
	})
	registry.RegisterSink("close_sink", func() Sink[TestData] {
		return sink
	})

	config := &FlowConfig{
		Name:   "close_test",
		Source: ComponentSpec{Name: "close_source"},
		Sink:   ComponentSpec{Name: "close_sink"},
	}

	flow := NewFlow[TestData](config, registry)
	if err := flow.Build(); err != nil {
		t.Fatalf("build failed: %v", err)
	}

	ctx := context.Background()
	if err := flow.Run(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// Test close (should not error)
	if err := flow.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}

	// Double close should be safe
	if err := flow.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}
}

func BenchmarkFlowRun(b *testing.B) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}

	source := &MockSource[int]{data: data}
	sink := &MockSink[int]{}

	registry := NewRegistry[int]()
	registry.RegisterSource("bench_source", func() Source[int] {
		return source
	})
	registry.RegisterSink("bench_sink", func() Sink[int] {
		return sink
	})

	config := &FlowConfig{
		Name:       "bench",
		BufferSize: 1000,
		Source:     ComponentSpec{Name: "bench_source"},
		Sink:       ComponentSpec{Name: "bench_sink"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		flow := NewFlow[int](config, registry)
		flow.Build()
		flow.Run(context.Background())
		// Reset sink data
		sink.mu.Lock()
		sink.data = make([]int, 0)
		sink.count = 0
		sink.mu.Unlock()
	}
}

// Example demonstrates basic flow usage.
func ExampleFlow() {
	// Define data type
	type Order struct {
		ID     string  `json:"id"`
		Amount float64 `json:"amount"`
		Status string  `json:"status"`
	}

	// Create registry
	registry := NewRegistry[Order]()

	// Create and register source with data
	source := &MockSource[Order]{
		data: []Order{
			{ID: "001", Amount: 100.0, Status: "completed"},
			{ID: "002", Amount: 200.0, Status: "pending"},
			{ID: "003", Amount: 150.0, Status: "completed"},
		},
	}
	sink := &MockSink[Order]{}

	registry.RegisterSource("orders", func() Source[Order] {
		return source
	})
	registry.RegisterSink("console", func() Sink[Order] {
		return sink
	})

	// Create flow config
	config := &FlowConfig{
		Name:       "order_processing",
		BufferSize: 100,
		Source:     ComponentSpec{Name: "orders"},
		Sink:       ComponentSpec{Name: "console"},
	}

	// Build and run
	flow := NewFlow[Order](config, registry)
	if err := flow.Build(); err != nil {
		fmt.Printf("Build failed: %v\n", err)
		return
	}

	if err := flow.Run(context.Background()); err != nil {
		fmt.Printf("Run failed: %v\n", err)
		return
	}

	stats := flow.Stats()
	fmt.Printf("Processed %d records\n", stats.TotalIn)
}
