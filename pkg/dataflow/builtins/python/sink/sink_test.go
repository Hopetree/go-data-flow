package sink

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestInit_ValidConfig(t *testing.T) {
	s := New()

	scriptPath := "../testdata/file_sink.py"
	outputFile := filepath.Join(os.TempDir(), "python_sink_test.jsonl")
	config, _ := json.Marshal(map[string]interface{}{
		"script": scriptPath,
		"env":    map[string]string{"SINK_OUTPUT_FILE": outputFile},
	})

	if err := s.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}
	defer os.Remove(outputFile)
}

func TestConsume_Data(t *testing.T) {
	s := New()

	outputFile := filepath.Join(os.TempDir(), "python_sink_consume_test.jsonl")
	config, _ := json.Marshal(map[string]interface{}{
		"script": "../testdata/file_sink.py",
		"env":    map[string]string{"SINK_OUTPUT_FILE": outputFile},
	})

	if err := s.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}
	defer os.Remove(outputFile)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	in := make(chan types.Record, 3)
	in <- types.Record{"id": 1, "name": "alice"}
	in <- types.Record{"id": 2, "name": "bob"}
	close(in)

	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() 失败: %v", err)
	}

	// 验证输出文件
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("输出文件有 %d 行，期望 2", len(lines))
	}

	var record1 map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &record1); err != nil {
		t.Fatalf("解析第 1 行失败: %v", err)
	}
	if record1["name"] != "alice" {
		t.Errorf("第 1 行 name = %v, 期望 alice", record1["name"])
	}
}
