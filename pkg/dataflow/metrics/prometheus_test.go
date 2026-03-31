package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewCollector(t *testing.T) {
	collector := NewCollector("test")
	if collector == nil {
		t.Fatal("期望创建 Collector")
	}
	if collector.namespace != "test" {
		t.Errorf("期望 namespace=test, 实际 %s", collector.namespace)
	}
}

func TestCollectorRegister(t *testing.T) {
	collector := NewCollector("test")

	tests := []struct {
		name   string
		desc   MetricDesc
	}{
		{
			name: "Counter",
			desc: MetricDesc{
				Name:   "requests_total",
				Type:   MetricTypeCounter,
				Help:   "Total requests",
				Labels: []string{"method"},
			},
		},
		{
			name: "Gauge",
			desc: MetricDesc{
				Name:   "active_connections",
				Type:   MetricTypeGauge,
				Help:   "Active connections",
				Labels: []string{"service"},
			},
		},
		{
			name: "Histogram",
			desc: MetricDesc{
				Name:   "request_duration",
				Type:   MetricTypeHistogram,
				Help:   "Request duration",
				Labels: []string{"endpoint"},
			},
		},
		{
			name: "Summary",
			desc: MetricDesc{
				Name:   "response_size",
				Type:   MetricTypeSummary,
				Help:   "Response size",
				Labels: []string{"path"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := collector.Register(tt.desc); err != nil {
				t.Fatalf("注册指标失败: %v", err)
			}
		})
	}
}

func TestCollectorCounter(t *testing.T) {
	collector := NewCollector("test")
	collector.Register(MetricDesc{
		Name:   "requests_total",
		Type:   MetricTypeCounter,
		Help:   "Total requests",
		Labels: []string{"method"},
	})

	// 测试 IncCounter
	collector.IncCounter("requests_total", prometheus.Labels{"method": "GET"})

	// 测试 AddCounter
	collector.AddCounter("requests_total", 5, prometheus.Labels{"method": "POST"})

	// 验证指标存在（不验证具体值，因为需要从 registry 读取）
	if len(collector.counters) != 1 {
		t.Errorf("期望 1 个 counter, 实际 %d", len(collector.counters))
	}
}

func TestCollectorGauge(t *testing.T) {
	collector := NewCollector("test")
	collector.Register(MetricDesc{
		Name:   "connections",
		Type:   MetricTypeGauge,
		Help:   "Active connections",
		Labels: []string{"service"},
	})

	collector.SetGauge("connections", 100, prometheus.Labels{"service": "api"})
	collector.IncGauge("connections", prometheus.Labels{"service": "api"})
	collector.DecGauge("connections", prometheus.Labels{"service": "api"})

	if len(collector.gauges) != 1 {
		t.Errorf("期望 1 个 gauge, 实际 %d", len(collector.gauges))
	}
}

func TestCollectorHistogram(t *testing.T) {
	collector := NewCollector("test")
	collector.Register(MetricDesc{
		Name:   "duration",
		Type:   MetricTypeHistogram,
		Help:   "Request duration",
		Labels: []string{"path"},
	})

	for i := 0; i < 100; i++ {
		collector.ObserveHistogram("duration", float64(i)/10, prometheus.Labels{"path": "/api"})
	}

	if len(collector.histograms) != 1 {
		t.Errorf("期望 1 个 histogram, 实际 %d", len(collector.histograms))
	}
}

func TestCollectorSummary(t *testing.T) {
	collector := NewCollector("test")
	collector.Register(MetricDesc{
		Name:   "size",
		Type:   MetricTypeSummary,
		Help:   "Response size",
		Labels: []string{"type"},
	})

	for i := 0; i < 100; i++ {
		collector.ObserveSummary("size", float64(i)*100, prometheus.Labels{"type": "json"})
	}

	if len(collector.summaries) != 1 {
		t.Errorf("期望 1 个 summary, 实际 %d", len(collector.summaries))
	}
}

func TestCollectorHandler(t *testing.T) {
	collector := NewCollector("test")
	collector.Register(MetricDesc{
		Name:   "test_metric",
		Type:   MetricTypeCounter,
		Help:   "Test metric",
		Labels: []string{"label"},
	})

	collector.IncCounter("test_metric", prometheus.Labels{"label": "value"})

	handler := collector.Handler()
	if handler == nil {
		t.Fatal("期望非 nil handler")
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("期望状态码 200, 实际 %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "test_test_metric") {
		t.Errorf("期望包含指标名, 实际: %s", body)
	}
}

func TestMetricsServer(t *testing.T) {
	config := MetricsServerConfig{
		Addr:      ":0", // 使用随机端口
		Path:      "/metrics",
		Namespace: "test",
	}

	server := NewMetricsServer(config)
	if server == nil {
		t.Fatal("期望创建 MetricsServer")
	}

	// 验证配置
	if server.addr == "" {
		t.Error("期望 addr 非空")
	}
	if server.path != "/metrics" {
		t.Errorf("期望 path=/metrics, 实际 %s", server.path)
	}
	if server.collector == nil {
		t.Error("期望 collector 非空")
	}
}

func TestMetricsServerCollector(t *testing.T) {
	server := NewMetricsServer(MetricsServerConfig{
		Namespace: "test",
	})

	collector := server.Collector()
	if collector == nil {
		t.Fatal("期望非 nil collector")
	}

	// 注册并使用指标
	collector.Register(MetricDesc{
		Name:   "test_counter",
		Type:   MetricTypeCounter,
		Help:   "Test counter",
		Labels: []string{},
	})
	collector.IncCounter("test_counter", nil)
}

func TestDefaultCollector(t *testing.T) {
	c1 := Default()
	c2 := Default()

	// 应该返回相同的实例
	if c1 != c2 {
		t.Error("期望 Default() 返回单例")
	}
}

func TestCollectorNonExistentMetric(t *testing.T) {
	collector := NewCollector("test")

	// 操作不存在的指标应该不会 panic
	collector.IncCounter("nonexistent", nil)
	collector.SetGauge("nonexistent", 1, nil)
	collector.ObserveHistogram("nonexistent", 1, nil)
	collector.ObserveSummary("nonexistent", 1, nil)
}

func TestCollectorRegistry(t *testing.T) {
	collector := NewCollector("test")
	registry := collector.Registry()

	if registry == nil {
		t.Fatal("期望非 nil registry")
	}

	// 验证可以注册到 registry
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_simple_counter",
		Help: "Simple counter",
	})
	registry.MustRegister(counter)
}
