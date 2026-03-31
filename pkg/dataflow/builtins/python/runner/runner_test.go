package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// getTestScriptPath 获取测试脚本绝对路径
func getTestScriptPath(name string) string {
	return filepath.Join("..", "testdata", name)
}

func TestNew_ValidConfig(t *testing.T) {
	r, err := New(Config{
		Script:     getTestScriptPath("echo_source.py"),
		PythonExec: "python3",
	})
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	if r.scriptPath == "" {
		t.Error("scriptPath 不应为空")
	}
	if r.pythonExec != "python3" {
		t.Errorf("pythonExec = %q, 期望 %q", r.pythonExec, "python3")
	}
}

func TestNew_EmptyScript(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("New() 应该在脚本路径为空时返回错误")
	}
	if !strings.Contains(err.Error(), "未指定") {
		t.Errorf("错误信息不正确: %v", err)
	}
}

func TestNew_ScriptNotFound(t *testing.T) {
	_, err := New(Config{Script: "/nonexistent/script.py"})
	if err == nil {
		t.Fatal("New() 应该在脚本不存在时返回错误")
	}
}

func TestStartAndReadLine(t *testing.T) {
	r, err := New(Config{
		Script:     getTestScriptPath("echo_source.py"),
		PythonExec: "python3",
	})
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}

	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() 失败: %v", err)
	}
	defer r.Close()

	// 读取 3 条记录
	expectedIDs := []string{"1", "2", "3"}
	for i, id := range expectedIDs {
		line, err := r.ReadLine(ctx)
		if err != nil {
			t.Fatalf("第 %d 次 ReadLine() 失败: %v", i+1, err)
		}

		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("解析 JSON 失败: %v (line: %s)", err, string(line))
		}

		if record["id"] != float64(i+1) {
			t.Errorf("第 %d 条记录 id = %v, 期望 %d", i+1, record["id"], i+1)
		}
		_ = id
	}

	// 第 4 次读取应该返回 EOF
	_, err = r.ReadLine(ctx)
	if err == nil {
		t.Fatal("期望 EOF，但没有返回错误")
	}
}

func TestClose_Idempotent(t *testing.T) {
	r, err := New(Config{
		Script:     getTestScriptPath("echo_source.py"),
		PythonExec: "python3",
	})
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}

	ctx := context.Background()
	r.Start(ctx)

	// 多次 Close 不应 panic
	if err := r.Close(); err != nil {
		t.Fatalf("第一次 Close() 失败: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("第二次 Close() 失败: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("第三次 Close() 失败: %v", err)
	}
}

func TestContextCancel_KillsProcess(t *testing.T) {
	r, err := New(Config{
		Script:     getTestScriptPath("echo_source.py"),
		PythonExec: "python3",
	})
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() 失败: %v", err)
	}

	// 取消 context
	cancel()

	// 等待一小段时间让进程被杀死
	time.Sleep(100 * time.Millisecond)

	// Close 不应阻塞
	if err := r.Close(); err != nil {
		// context 取消可能导致非零退出码，这是正常的
		t.Logf("Close() 返回: %v (可能是 context 取消导致的)", err)
	}
}

func TestStderrCapture(t *testing.T) {
	r, err := New(Config{
		Script:     getTestScriptPath("file_sink.py"),
		PythonExec: "python3",
		Env:        map[string]string{"SINK_OUTPUT_FILE": "/tmp/test_stderr_capture.jsonl"},
	})
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}

	var stderrLines []string
	r.SetStderrCallback(func(line string) {
		stderrLines = append(stderrLines, line)
	})

	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() 失败: %v", err)
	}

	// 写入一些数据
	record := map[string]interface{}{"test": true}
	data, _ := json.Marshal(record)
	r.WriteLine(ctx, data)
	r.CloseStdin()

	// 等待退出
	r.Close()

	// 检查 stderr 输出
	if len(stderrLines) == 0 {
		t.Error("未捕获到 stderr 输出")
	}
	// 清理
	os.Remove("/tmp/test_stderr_capture.jsonl")
}

func TestWriteLineAndReadLine(t *testing.T) {
	r, err := New(Config{
		Script:     getTestScriptPath("echo_processor.py"),
		PythonExec: "python3",
	})
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}

	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() 失败: %v", err)
	}
	defer r.Close()

	// 写入一条数据
	input := map[string]interface{}{"name": "alice"}
	data, _ := json.Marshal(input)
	if err := r.WriteLine(ctx, data); err != nil {
		t.Fatalf("WriteLine() 失败: %v", err)
	}

	// 关闭 stdin 通知 Python 输入结束
	r.CloseStdin()

	// 读取输出
	line, err := r.ReadLine(ctx)
	if err != nil {
		t.Fatalf("ReadLine() 失败: %v", err)
	}

	var record map[string]interface{}
	if err := json.Unmarshal(line, &record); err != nil {
		t.Fatalf("解析 JSON 失败: %v", err)
	}

	if record["name"] != "alice" {
		t.Errorf("name = %v, 期望 alice", record["name"])
	}
	if record["processed"] != true {
		t.Errorf("processed = %v, 期望 true", record["processed"])
	}

	// 第二次读取应该 EOF
	_, err = r.ReadLine(ctx)
	if err == nil {
		t.Fatal("期望 EOF")
	}
}
