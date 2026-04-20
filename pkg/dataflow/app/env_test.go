package app

import (
	"testing"
)

func TestExpandEnvVars(t *testing.T) {
	t.Setenv("TEST_VAR", "hello")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "无占位符原样返回",
			input:    "name: my-flow",
			expected: "name: my-flow",
		},
		{
			name:     "单个变量替换",
			input:    "host: ${TEST_VAR}",
			expected: "host: hello",
		},
		{
			name:     "无花括号变量替换",
			input:    "host: $TEST_VAR",
			expected: "host: hello",
		},
		{
			name:     "变量嵌入在值中间",
			input:    "dsn: tcp://${TEST_VAR}:9000",
			expected: "dsn: tcp://hello:9000",
		},
		{
			name:     "空内容",
			input:    "",
			expected: "",
		},
		{
			name:     "不存在的变量保留原样",
			input:    "key: ${NONEXISTENT_VAR}",
			expected: "key: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(expandEnvVars([]byte(tt.input)))
			if result != tt.expected {
				t.Errorf("expandEnvVars() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestExpandEnvVarsMultipleVars(t *testing.T) {
	t.Setenv("HOST", "localhost")
	t.Setenv("PORT", "9090")
	t.Setenv("DB_NAME", "mydb")

	input := `source:
  host: ${HOST}
  port: ${PORT}
sink:
  dsn: "tcp://${HOST}:${PORT}/${DB_NAME}"`

	expected := `source:
  host: localhost
  port: 9090
sink:
  dsn: "tcp://localhost:9090/mydb"`

	result := string(expandEnvVars([]byte(input)))
	if result != expected {
		t.Errorf("expandEnvVars() mismatch:\ngot:\n%s\nwant:\n%s", result, expected)
	}
}
