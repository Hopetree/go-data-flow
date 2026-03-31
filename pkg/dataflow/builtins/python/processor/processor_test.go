package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestInit_ValidConfig(t *testing.T) {
	p := New()

	config, _ := json.Marshal(map[string]string{
		"script": "../testdata/echo_processor.py",
	})

	if err := p.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}
}

func TestProcess_Data(t *testing.T) {
	p := New()

	config, _ := json.Marshal(map[string]string{
		"script": "../testdata/echo_processor.py",
	})

	if err := p.Init(config); err != nil {
		t.Fatalf("Init() 失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	in := make(chan types.Record, 3)
	out := make(chan types.Record, 3)

	// 用协程写入输入并关闭
	go func() {
		in <- types.Record{"name": "alice"}
		in <- types.Record{"name": "bob"}
		close(in)
	}()

	// 用协程运行 Process，以便同时消费 out
	done := make(chan error, 1)
	go func() {
		done <- p.Process(ctx, in, out)
	}()

	// 先等待 Process 完成
	var results []types.Record
	err := <-done
	if err != nil {
		t.Fatalf("Process() 失败: %v", err)
	}

	// Process 完成后关闭 out，再读取结果
	close(out)
	for result := range out {
		results = append(results, result)
	}

	if len(results) != 2 {
		t.Fatalf("收到 %d 条记录，期望 2", len(results))
	}

	if results[0]["name"] != "alice" {
		t.Errorf("results[0].name = %v, 期望 alice", results[0]["name"])
	}
	if results[0]["processed"] != true {
		t.Errorf("results[0].processed = %v, 期望 true", results[0]["processed"])
	}
}

func TestConcurrencyCap(t *testing.T) {
	p := New()
	cap := p.ConcurrencyCap()
	if cap.Supported {
		t.Error("Python Processor 不应支持并发")
	}
	if !cap.IsStateful {
		t.Error("Python Processor 应该标记为有状态")
	}
}
