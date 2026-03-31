package condition

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestProcessor_New(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("New() returned nil")
	}
}

func TestProcessor_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "eq operator",
			config: Config{
				Field: "status",
				Op:    "eq",
				Value: "active",
			},
			wantErr: false,
		},
		{
			name: "ne operator",
			config: Config{
				Field: "count",
				Op:    "ne",
				Value: 0,
			},
			wantErr: false,
		},
		{
			name: "gt operator",
			config: Config{
				Field: "age",
				Op:    "gt",
				Value: 18,
			},
			wantErr: false,
		},
		{
			name: "in operator",
			config: Config{
				Field: "id",
				Op:    "in",
				Value: []interface{}{1, 2, 3},
			},
			wantErr: false,
		},
		{
			name: "contains operator",
			config: Config{
				Field: "message",
				Op:    "contains",
				Value: "error",
			},
			wantErr: false,
		},
		{
			name: "regex operator valid",
			config: Config{
				Field: "email",
				Op:    "regex",
				Value: `^[a-z]+@[a-z]+\.[a-z]+$`,
			},
			wantErr: false,
		},
		{
			name: "regex operator invalid pattern",
			config: Config{
				Field: "email",
				Op:    "regex",
				Value: `[invalid(`,
			},
			wantErr: true,
		},
		{
			name: "exists operator true",
			config: Config{
				Field: "name",
				Op:    "exists",
				Value: true,
			},
			wantErr: false,
		},
		{
			name: "exists operator false",
			config: Config{
				Field: "missing",
				Op:    "exists",
				Value: false,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			err := p.Init(configBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessor_Process_Eq(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "filter by eq string",
			config: Config{
				Field: "status",
				Op:    "eq",
				Value: "active",
			},
			input: []types.Record{
				{"id": 1, "status": "active"},
				{"id": 2, "status": "inactive"},
				{"id": 3, "status": "active"},
			},
			expected: 2,
		},
		{
			name: "filter by eq int",
			config: Config{
				Field: "id",
				Op:    "eq",
				Value: 1,
			},
			input: []types.Record{
				{"id": 1, "name": "one"},
				{"id": 2, "name": "two"},
				{"id": 3, "name": "three"},
			},
			expected: 1,
		},
		{
			name: "filter by eq with mixed types",
			config: Config{
				Field: "id",
				Op:    "eq",
				Value: 1.0, // float64
			},
			input: []types.Record{
				{"id": 1, "name": "one"}, // int, should match
				{"id": 2, "name": "two"},
			},
			expected: 1,
		},
		{
			name: "field not exists",
			config: Config{
				Field: "missing",
				Op:    "eq",
				Value: "value",
			},
			input: []types.Record{
				{"id": 1},
				{"id": 2},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, len(tt.input))
			out := make(chan types.Record, len(tt.input))

			for _, record := range tt.input {
				in <- record
			}
			close(in)

			ctx := context.Background()
			if err := p.Process(ctx, in, out); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			close(out)

			var count int
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Ne(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "filter by ne string",
			config: Config{
				Field: "status",
				Op:    "ne",
				Value: "inactive",
			},
			input: []types.Record{
				{"id": 1, "status": "active"},
				{"id": 2, "status": "inactive"},
				{"id": 3, "status": "pending"},
			},
			expected: 2,
		},
		{
			name: "filter by ne with mixed types",
			config: Config{
				Field: "count",
				Op:    "ne",
				Value: 5.0,
			},
			input: []types.Record{
				{"count": 5, "name": "five"},     // int(5) == float64(5.0), filtered
				{"count": 10, "name": "ten"},     // pass
				{"count": 5, "name": "another"},  // filtered
				{"count": 15, "name": "fifteen"}, // pass
			},
			expected: 2,
		},
		{
			name: "ne field not exists - should pass",
			config: Config{
				Field: "missing",
				Op:    "ne",
				Value: "value",
			},
			input: []types.Record{
				{"id": 1},
				{"id": 2},
			},
			expected: 2, // 字段不存在时，ne 返回 true
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, len(tt.input))
			out := make(chan types.Record, len(tt.input))

			for _, record := range tt.input {
				in <- record
			}
			close(in)

			ctx := context.Background()
			if err := p.Process(ctx, in, out); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			close(out)

			var count int
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Exists(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "exists true",
			config: Config{
				Field: "email",
				Op:    "exists",
				Value: true,
			},
			input: []types.Record{
				{"id": 1, "email": "test@example.com"},
				{"id": 2},
				{"id": 3, "email": "other@example.com"},
			},
			expected: 2,
		},
		{
			name: "exists false",
			config: Config{
				Field: "deleted",
				Op:    "exists",
				Value: false,
			},
			input: []types.Record{
				{"id": 1, "deleted": true},
				{"id": 2},
				{"id": 3},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, len(tt.input))
			out := make(chan types.Record, len(tt.input))

			for _, record := range tt.input {
				in <- record
			}
			close(in)

			ctx := context.Background()
			if err := p.Process(ctx, in, out); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			close(out)

			var count int
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_ContextCancellation(t *testing.T) {
	p := New()
	configBytes, _ := json.Marshal(Config{
		Field: "status",
		Op:    "eq",
		Value: "active",
	})
	if err := p.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan types.Record)
	out := make(chan types.Record, 1)

	// 在 goroutine 中运行 Process
	done := make(chan error, 1)
	go func() {
		done <- p.Process(ctx, in, out)
	}()

	// 发送一条记录
	in <- types.Record{"status": "active"}

	// 取消上下文
	cancel()

	// 关闭输入通道
	close(in)

	// 等待完成
	err := <-done
	if err != nil && err != context.Canceled {
		t.Errorf("Process() error = %v, want nil or context.Canceled", err)
	}
}

func TestProcessor_ConcurrencyCap(t *testing.T) {
	p := New()
	cap := p.ConcurrencyCap()

	if !cap.Supported {
		t.Error("ConcurrencyCap().Supported should be true")
	}
	if cap.SuggestedMax < 1 {
		t.Error("ConcurrencyCap().SuggestedMax should be at least 1")
	}
	if cap.IsStateful {
		t.Error("ConcurrencyCap().IsStateful should be false")
	}
}

// 确保实现了接口
var _ dataflow.Processor[types.Record] = (*Processor)(nil)
