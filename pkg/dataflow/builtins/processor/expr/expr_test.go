package expr

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestProcessor_New(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("New() returned nil")
	}
}

func TestProcessor_Init(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{
			name:       "simple comparison",
			expression: "status == 'active'",
			wantErr:    false,
		},
		{
			name:       "numeric comparison",
			expression: "score > 60",
			wantErr:    false,
		},
		{
			name:       "combined conditions",
			expression: "status == 'active' && score > 60",
			wantErr:    false,
		},
		{
			name:       "in operator",
			expression: "status in ['active', 'pending']",
			wantErr:    false,
		},
		{
			name:       "string contains",
			expression: "str_contains(message, 'error')",
			wantErr:    false,
		},
		{
			name:       "string hasPrefix",
			expression: "str_hasPrefix(email, 'admin')",
			wantErr:    false,
		},
		{
			name:       "string hasSuffix",
			expression: "str_hasSuffix(email, '@company.com')",
			wantErr:    false,
		},
		{
			name:       "regex match",
			expression: "str_matches(email, '^[a-z]+@')",
			wantErr:    false,
		},
		{
			name:       "field exists",
			expression: "email != nil",
			wantErr:    false,
		},
		{
			name:       "complex expression",
			expression: "(status == 'active' || status == 'pending') && score >= 60 && str_contains(email, '@')",
			wantErr:    false,
		},
		{
			name:       "empty expression",
			expression: "",
			wantErr:    true,
		},
		{
			name:       "invalid syntax",
			expression: "status ==", // 语法错误
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			config := Config{Expression: tt.expression}
			configBytes, _ := json.Marshal(config)
			err := p.Init(configBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessor_Process(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		input      []types.Record
		expected   int
	}{
		{
			name:       "eq string",
			expression: "status == 'active'",
			input: []types.Record{
				{"status": "active", "id": 1},
				{"status": "inactive", "id": 2},
				{"status": "active", "id": 3},
			},
			expected: 2,
		},
		{
			name:       "gt number",
			expression: "score > 60",
			input: []types.Record{
				{"score": 50, "name": "low"},
				{"score": 70, "name": "pass"},
				{"score": 90, "name": "high"},
			},
			expected: 2,
		},
		{
			name:       "combined conditions",
			expression: "status == 'active' && score >= 60",
			input: []types.Record{
				{"status": "active", "score": 50},
				{"status": "active", "score": 70},
				{"status": "inactive", "score": 80},
			},
			expected: 1,
		},
		{
			name:       "in operator",
			expression: "status in ['active', 'pending']",
			input: []types.Record{
				{"status": "active"},
				{"status": "pending"},
				{"status": "deleted"},
			},
			expected: 2,
		},
		{
			name:       "not in operator",
			expression: "!(status in ['deleted', 'archived'])",
			input: []types.Record{
				{"status": "active"},
				{"status": "deleted"},
				{"status": "pending"},
			},
			expected: 2,
		},
		{
			name:       "string contains",
			expression: "str_contains(message, 'error')",
			input: []types.Record{
				{"message": "error: connection failed"},
				{"message": "success"},
				{"message": "warning: timeout error"},
			},
			expected: 2,
		},
		{
			name:       "string hasPrefix",
			expression: "str_hasPrefix(path, '/api/')",
			input: []types.Record{
				{"path": "/api/users"},
				{"path": "/web/index"},
				{"path": "/api/products"},
			},
			expected: 2,
		},
		{
			name:       "string hasSuffix",
			expression: "str_hasSuffix(email, '@company.com')",
			input: []types.Record{
				{"email": "user@company.com"},
				{"email": "admin@other.org"},
				{"email": "guest@company.com"},
			},
			expected: 2,
		},
		{
			name:       "regex match",
			expression: "str_matches(user_id, '^user-\\\\d+$')",
			input: []types.Record{
				{"user_id": "user-123"},
				{"user_id": "admin-1"},
				{"user_id": "user-456"},
			},
			expected: 2,
		},
		{
			name:       "field exists",
			expression: "email != nil",
			input: []types.Record{
				{"email": "test@example.com", "name": "a"},
				{"name": "b"}, // no email
				{"email": "other@example.com", "name": "c"},
			},
			expected: 2,
		},
		{
			name:       "field not exists",
			expression: "email == nil",
			input: []types.Record{
				{"email": "test@example.com", "name": "a"},
				{"name": "b"}, // no email
				{"email": "other@example.com", "name": "c"},
			},
			expected: 1,
		},
		{
			name:       "complex expression",
			expression: "(status == 'active' || role == 'admin') && score > 50",
			input: []types.Record{
				{"status": "active", "role": "user", "score": 60},
				{"status": "inactive", "role": "admin", "score": 30},
				{"status": "active", "role": "user", "score": 40},
				{"status": "inactive", "role": "admin", "score": 60},
			},
			expected: 2,
		},
		{
			name:       "range check",
			expression: "score >= 60 && score <= 100",
			input: []types.Record{
				{"score": 50},
				{"score": 60},
				{"score": 75},
				{"score": 100},
				{"score": 110},
			},
			expected: 3,
		},
		{
			name:       "nested field access",
			expression: "user.name == 'alice'",
			input: []types.Record{
				{"user": map[string]interface{}{"name": "alice", "age": 30}},
				{"user": map[string]interface{}{"name": "bob", "age": 25}},
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			config := Config{Expression: tt.expression}
			configBytes, _ := json.Marshal(config)
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
	config := Config{Expression: "status == 'active'"}
	configBytes, _ := json.Marshal(config)
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
