package dataflow

import (
	"sync"
	"testing"
)

// TestGetDefaultRegistry 验证全局默认注册表是单例
func TestGetDefaultRegistry(t *testing.T) {
	r1 := GetDefaultRegistry()
	r2 := GetDefaultRegistry()
	if r1 != r2 {
		t.Error("GetDefaultRegistry() 应返回同一个实例")
	}
	if r1 == nil {
		t.Fatal("GetDefaultRegistry() 不应返回 nil")
	}
}

// TestDefaultRegistryRegistration 验证通过 DefaultRegistry 注册的组件可以被获取
func TestDefaultRegistryRegistration(t *testing.T) {
	r := GetDefaultRegistry()

	srcName := "test-default-source-" + t.Name()
	procName := "test-default-proc-" + t.Name()
	sinkName := "test-default-sink-" + t.Name()

	r.RegisterSource(srcName, func() Source[map[string]interface{}] {
		return nil
	})
	r.RegisterProcessor(procName, func() Processor[map[string]interface{}] {
		return nil
	})
	r.RegisterSink(sinkName, func() Sink[map[string]interface{}] {
		return nil
	})

	if _, ok := r.GetSource(srcName); !ok {
		t.Errorf("应找到注册的 source '%s'", srcName)
	}
	if _, ok := r.GetProcessor(procName); !ok {
		t.Errorf("应找到注册的 processor '%s'", procName)
	}
	if _, ok := r.GetSink(sinkName); !ok {
		t.Errorf("应找到注册的 sink '%s'", sinkName)
	}
}

// TestDefaultRegistryConcurrency 验证 DefaultRegistry 的并发安全性
func TestDefaultRegistryConcurrency(t *testing.T) {
	r := GetDefaultRegistry()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := t.Name() + "-" + string(rune('A'+idx))
			r.RegisterSource(name, func() Source[map[string]interface{}] {
				return nil
			})
			r.GetSource(name)
		}(i)
	}

	wg.Wait()
}
