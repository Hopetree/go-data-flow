package condition

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestCompareRegex(t *testing.T) {
	// 预编译正则表达式用于测试
	patterns := map[string]*regexp.Regexp{
		"simple":    regexp.MustCompile(`^user-\d+$`),
		"email":     regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`),
		"phone":     regexp.MustCompile(`^\d{3}-\d{3}-\d{4}$`),
		"ip":        regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`),
		"empty":     regexp.MustCompile(`^$`),
		"any":       regexp.MustCompile(`.*`),
	}

	tests := []struct {
		name     string
		a        interface{}
		pattern  string
		expected bool
	}{
		// simple pattern: ^user-\d+$
		{"match user-123", "user-123", "simple", true},
		{"match user-1", "user-1", "simple", true},
		{"no match user-abc", "user-abc", "simple", false},
		{"no match admin-123", "admin-123", "simple", false},
		{"no match partial", "user-123-extra", "simple", false},

		// email pattern
		{"valid email", "test@example.com", "email", true},
		{"valid email with dots", "user.name@sub.domain.com", "email", true},
		{"invalid email no @", "testexample.com", "email", false},
		{"invalid email no domain", "test@", "email", false},

		// phone pattern
		{"valid phone", "123-456-7890", "phone", true},
		{"invalid phone", "123-456-789", "phone", false},
		{"invalid phone letters", "abc-def-ghij", "phone", false},

		// ip pattern
		{"valid ip", "192.168.1.1", "ip", true},
		{"invalid ip", "192.168.1", "ip", false},

		// edge cases
		{"empty match empty", "", "empty", true},
		{"empty no match", "hello", "empty", false},
		{"any matches empty", "", "any", true},
		{"any matches text", "hello world", "any", true},

		// non-string input
		{"int input", 123, "any", false},
		{"nil input", nil, "any", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex := patterns[tt.pattern]
			if regex == nil {
				t.Fatalf("Unknown pattern: %s", tt.pattern)
			}
			result := compareRegex(tt.a, regex)
			if result != tt.expected {
				t.Errorf("compareRegex(%v, %s) = %v, want %v", tt.a, tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Regex(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		input       []types.Record
		expected    int
		expectError bool
	}{
		{
			name: "regex user pattern",
			config: Config{
				Field: "id",
				Op:    "regex",
				Value: `^user-\d+$`,
			},
			input: []types.Record{
				{"id": "user-123"},
				{"id": "user-456"},
				{"id": "admin-1"},
				{"id": "guest"},
			},
			expected: 2,
		},
		{
			name: "regex email pattern",
			config: Config{
				Field: "email",
				Op:    "regex",
				Value: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
			},
			input: []types.Record{
				{"email": "valid@example.com"},
				{"email": "invalid-email"},
				{"email": "also.valid@domain.org"},
			},
			expected: 2,
		},
		{
			name: "regex simple prefix",
			config: Config{
				Field: "path",
				Op:    "regex",
				Value: `^/api/`,
			},
			input: []types.Record{
				{"path": "/api/users"},
				{"path": "/api/products"},
				{"path": "/web/index"},
			},
			expected: 2,
		},
		{
			name: "regex case insensitive",
			config: Config{
				Field: "status",
				Op:    "regex",
				Value: `(?i)^(active|pending)$`,
			},
			input: []types.Record{
				{"status": "active"},
				{"status": "ACTIVE"},
				{"status": "Pending"},
				{"status": "inactive"},
			},
			expected: 3,
		},
		{
			name: "regex field not exists",
			config: Config{
				Field: "missing",
				Op:    "regex",
				Value: `.*`,
			},
			input: []types.Record{
				{"other": "value"},
			},
			expected: 0,
		},
		{
			name: "regex non-string field",
			config: Config{
				Field: "count",
				Op:    "regex",
				Value: `\d+`,
			},
			input: []types.Record{
				{"count": 123},
				{"count": 456},
			},
			expected: 0, // int 不是 string，不匹配
		},
		{
			name: "regex invalid pattern",
			config: Config{
				Field: "id",
				Op:    "regex",
				Value: `[invalid(`,
			},
			input:       []types.Record{},
			expected:    0,
			expectError: true, // 初始化应该失败
		},
		{
			name: "regex value not string",
			config: Config{
				Field: "id",
				Op:    "regex",
				Value: 123, // 不是字符串
			},
			input:       []types.Record{},
			expected:    0,
			expectError: true, // 初始化应该失败
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			err := p.Init(configBytes)

			if tt.expectError {
				if err == nil {
					t.Error("Init() expected error, got nil")
				}
				return
			}

			if err != nil {
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
