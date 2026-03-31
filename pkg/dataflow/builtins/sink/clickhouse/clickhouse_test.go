package clickhouse

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestSink_New(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() 返回 nil")
	}
}

func TestSink_Init_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		errContains string
	}{
		{
			name:        "缺少 ck_address",
			config:      Config{Table: "test_table"},
			wantErr:     true,
			errContains: "ck_address",
		},
		{
			name:        "缺少 table",
			config:      Config{CKAddress: []string{"localhost:9000"}},
			wantErr:     true,
			errContains: "table",
		},
		{
			name:        "连接失败（无服务）",
			config:      Config{CKAddress: []string{"localhost:9000"}, Table: "test_table"},
			wantErr:     true,
			errContains: "连接",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			configBytes, _ := json.Marshal(tt.config)
			err := s.Init(configBytes)
			if tt.wantErr {
				if err == nil {
					t.Fatal("期望返回错误，但得到 nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("错误 = %v, 应包含 %q", err, tt.errContains)
				}
			} else if err != nil {
				t.Fatalf("不期望返回错误: %v", err)
			}
		})
	}
}

func TestSink_Init_Defaults(t *testing.T) {
	cfg := Config{
		CKAddress:  []string{"localhost:9000"},
		CKUser:     "",
		CKDatabase: "",
		Table:      "test_table",
	}
	// 连接会失败，但可以验证默认值已应用
	s := New()
	configBytes, _ := json.Marshal(cfg)
	err := s.Init(configBytes)
	if err == nil {
		t.Fatal("期望连接失败")
	}

	// 验证默认值（Init 中连接失败前已赋值）
	if s.ckUser != "default" {
		t.Errorf("ckUser = %q, 期望 %q", s.ckUser, "default")
	}
	if s.ckDatabase != "default" {
		t.Errorf("ckDatabase = %q, 期望 %q", s.ckDatabase, "default")
	}
	if s.batchSize != defaultBatchSize {
		t.Errorf("batchSize = %d, 期望 %d", s.batchSize, defaultBatchSize)
	}
	if s.flushInterval.Milliseconds() != defaultFlushIntervalMs {
		t.Errorf("flushInterval = %v, 期望 %dms", s.flushInterval, defaultFlushIntervalMs)
	}
	if s.maxOpenConns != defaultMaxOpenConns {
		t.Errorf("maxOpenConns = %d, 期望 %d", s.maxOpenConns, defaultMaxOpenConns)
	}
	if s.maxIdleConns != defaultMaxIdleConns {
		t.Errorf("maxIdleConns = %d, 期望 %d", s.maxIdleConns, defaultMaxIdleConns)
	}
}

func TestSink_Init_FieldMapping(t *testing.T) {
	cfg := Config{
		CKAddress:    []string{"localhost:9000"},
		Table:        "test_table",
		FieldMapping: map[string]string{"id": "user_id", "name": "user_name"},
	}
	s := New()
	configBytes, _ := json.Marshal(cfg)
	_ = s.Init(configBytes) // 连接会失败，但字段映射已初始化

	if s.columnToSource == nil {
		t.Fatal("columnToSource 未初始化")
	}
	if s.columnToSource["user_id"] != "id" {
		t.Errorf("columnToSource[\"user_id\"] = %q, 期望 %q", s.columnToSource["user_id"], "id")
	}
	if s.columnToSource["user_name"] != "name" {
		t.Errorf("columnToSource[\"user_name\"] = %q, 期望 %q", s.columnToSource["user_name"], "name")
	}
	if len(s.columns) != 2 {
		t.Errorf("columns 长度 = %d, 期望 2", len(s.columns))
	}
}

func TestSink_ExtractColumns(t *testing.T) {
	s := New()
	record := types.Record{
		"z_field": "3",
		"a_field": "1",
		"m_field": "2",
	}
	cols := s.extractColumns(record)
	expected := []string{"a_field", "m_field", "z_field"}
	if len(cols) != len(expected) {
		t.Fatalf("extractColumns() 长度 = %d, 期望 %d", len(cols), len(expected))
	}
	for i, v := range expected {
		if cols[i] != v {
			t.Errorf("extractColumns()[%d] = %q, 期望 %q", i, cols[i], v)
		}
	}
}

func TestSink_BuildInsertQuery(t *testing.T) {
	tests := []struct {
		name     string
		sink     *Sink
		expected string
	}{
		{
			name:     "无列名",
			sink:     &Sink{ckDatabase: "mydb", table: "mytable"},
			expected: "INSERT INTO mydb.mytable",
		},
		{
			name:     "有列名",
			sink:     &Sink{ckDatabase: "mydb", table: "mytable", columns: []string{"id", "name"}},
			expected: "INSERT INTO mydb.mytable (id, name)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.sink.buildInsertQuery()
			if got != tt.expected {
				t.Errorf("buildInsertQuery() = %q, 期望 %q", got, tt.expected)
			}
		})
	}
}

func TestSink_MapRecord_DirectMapping(t *testing.T) {
	s := &Sink{
		columns:      []string{"id", "name", "value"},
		fieldMapping: nil,
	}
	record := types.Record{
		"id":    1,
		"name":  "test",
		"value": 42.5,
	}
	values := s.mapRecord(record)
	if len(values) != 3 {
		t.Fatalf("期望 3 个值，得到 %d", len(values))
	}
	if values[0] != 1 {
		t.Errorf("values[0] = %v, 期望 1", values[0])
	}
	if values[1] != "test" {
		t.Errorf("values[1] = %v, 期望 test", values[1])
	}
	if values[2] != 42.5 {
		t.Errorf("values[2] = %v, 期望 42.5", values[2])
	}
}

func TestSink_MapRecord_FieldMapping(t *testing.T) {
	s := &Sink{
		columns:        []string{"user_id", "user_name", "score"},
		fieldMapping:   map[string]string{"id": "user_id", "name": "user_name", "val": "score"},
		columnToSource: map[string]string{"user_id": "id", "user_name": "name", "score": "val"},
	}
	record := types.Record{
		"id":   1,
		"name": "test",
		"val":  100,
	}
	values := s.mapRecord(record)
	if values[0] != 1 {
		t.Errorf("values[0] = %v, 期望 1", values[0])
	}
	if values[1] != "test" {
		t.Errorf("values[1] = %v, 期望 test", values[1])
	}
	if values[2] != 100 {
		t.Errorf("values[2] = %v, 期望 100", values[2])
	}
}

func TestSink_Close_NilConn(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Errorf("Close() nil 连接不应报错，得到: %v", err)
	}
}

func TestSink_Count(t *testing.T) {
	s := New()
	if s.Count() != 0 {
		t.Errorf("新建 Sink 的 Count() = %d, 期望 0", s.Count())
	}
}
