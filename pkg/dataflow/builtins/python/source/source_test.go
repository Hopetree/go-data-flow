package source

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestInit_ValidConfig(t *testing.T) {
	s := New()

	scriptPath := "../testdata/echo_source.py"
	config, _ := json.Marshal(map[string]string{"script": scriptPath})

	if err := s.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}
}

func TestInit_EmptyScript(t *testing.T) {
	s := New()

	config, _ := json.Marshal(map[string]string{"script": ""})

	if err := s.Init(config); err == nil {
		t.Fatal("Init() 应该在脚本路径为空时返回错误")
	}
}

func TestRead_Data(t *testing.T) {
	s := New()

	scriptPath := "../testdata/echo_source.py"
	config, _ := json.Marshal(map[string]string{"script": scriptPath})

	if err := s.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out := make(chan types.Record, 10)
	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() 失败: %v", err)
	}

	if count != 3 {
		t.Errorf("count = %d, 期望 3", count)
	}

	close(out)
	var records []types.Record
	for r := range out {
		records = append(records, r)
	}

	if len(records) != 3 {
		t.Fatalf("收到 %d 条记录，期望 3", len(records))
	}

	expected := []string{"alice", "bob", "charlie"}
	for i, name := range expected {
		if records[i]["name"] != name {
			t.Errorf("records[%d].name = %v, 期望 %s", i, records[i]["name"], name)
		}
	}
}

func TestRead_ContextCancel(t *testing.T) {
	s := New()

	scriptPath := "../testdata/echo_source.py"
	config, _ := json.Marshal(map[string]string{"script": scriptPath})

	if err := s.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// 立即取消
	cancel()

	out := make(chan types.Record, 10)
	_, err := s.Read(ctx, out)
	if err == nil {
		t.Fatal("Read() 应该在 context 取消时返回错误")
	}
}
