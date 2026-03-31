// Package metrics 提供 Prometheus 指标收集和暴露功能
package metrics

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricType 指标类型
type MetricType int

const (
// MetricTypeCounter 计数器指标类型
	MetricTypeCounter MetricType = iota
	// MetricTypeGauge Gauge 指标类型
	MetricTypeGauge
	// MetricTypeHistogram Histogram 指标类型
	MetricTypeHistogram
	// MetricTypeSummary Summary 指标类型
	MetricTypeSummary
)

// MetricDesc 指标描述
type MetricDesc struct {
	Name   string
	Type   MetricType
	Help   string
	Labels []string
}

// Collector 指标收集器
type Collector struct {
	mu         sync.RWMutex
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	summaries  map[string]*prometheus.SummaryVec
	registry   *prometheus.Registry
	namespace  string
}

// NewCollector 创建新的指标收集器
func NewCollector(namespace string) *Collector {
	return &Collector{
		counters:   make(map[string]*prometheus.CounterVec),
		gauges:     make(map[string]*prometheus.GaugeVec),
		histograms: make(map[string]*prometheus.HistogramVec),
		summaries:  make(map[string]*prometheus.SummaryVec),
		registry:   prometheus.NewRegistry(),
		namespace:  namespace,
	}
}

// Register 注册指标
func (c *Collector) Register(desc MetricDesc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fullName := c.namespace + "_" + desc.Name

	switch desc.Type {
	case MetricTypeCounter:
		counter := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fullName,
				Help: desc.Help,
			},
			desc.Labels,
		)
		c.counters[desc.Name] = counter
		c.registry.MustRegister(counter)

	case MetricTypeGauge:
		gauge := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: fullName,
				Help: desc.Help,
			},
			desc.Labels,
		)
		c.gauges[desc.Name] = gauge
		c.registry.MustRegister(gauge)

	case MetricTypeHistogram:
		histogram := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    fullName,
				Help:    desc.Help,
				Buckets: prometheus.DefBuckets,
			},
			desc.Labels,
		)
		c.histograms[desc.Name] = histogram
		c.registry.MustRegister(histogram)

	case MetricTypeSummary:
		summary := prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: fullName,
				Help: desc.Help,
			},
			desc.Labels,
		)
		c.summaries[desc.Name] = summary
		c.registry.MustRegister(summary)
	}

	return nil
}

// IncCounter 增加计数器
func (c *Collector) IncCounter(name string, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if counter, ok := c.counters[name]; ok {
		counter.With(labels).Inc()
	}
}

// AddCounter 增加计数器值
func (c *Collector) AddCounter(name string, value float64, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if counter, ok := c.counters[name]; ok {
		counter.With(labels).Add(value)
	}
}

// SetGauge 设置 Gauge 值
func (c *Collector) SetGauge(name string, value float64, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if gauge, ok := c.gauges[name]; ok {
		gauge.With(labels).Set(value)
	}
}

// IncGauge 增加 Gauge 值
func (c *Collector) IncGauge(name string, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if gauge, ok := c.gauges[name]; ok {
		gauge.With(labels).Inc()
	}
}

// DecGauge 减少 Gauge 值
func (c *Collector) DecGauge(name string, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if gauge, ok := c.gauges[name]; ok {
		gauge.With(labels).Dec()
	}
}

// ObserveHistogram 观察 Histogram 值
func (c *Collector) ObserveHistogram(name string, value float64, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if histogram, ok := c.histograms[name]; ok {
		histogram.With(labels).Observe(value)
	}
}

// ObserveSummary 观察 Summary 值
func (c *Collector) ObserveSummary(name string, value float64, labels prometheus.Labels) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if summary, ok := c.summaries[name]; ok {
		summary.With(labels).Observe(value)
	}
}

// Registry 返回底层的 Prometheus Registry
func (c *Collector) Registry() *prometheus.Registry {
	return c.registry
}

// MustRegister 注册 Prometheus Collector 到 registry
func (c *Collector) MustRegister(collector prometheus.Collector) {
	c.registry.MustRegister(collector)
}

// Handler 返回 HTTP Handler
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

// Server Prometheus 指标服务器
type Server struct {
	addr     string
	path     string
	server   *http.Server
	collector *Collector
}

// ServerConfig 服务器配置
type ServerConfig struct {
	// Addr 监听地址，默认 :9090
	Addr string `json:"addr"`
	// Path metrics 路径，默认 /metrics
	Path string `json:"path"`
	// Namespace 指标命名空间
	Namespace string `json:"namespace"`
}

// NewServer 创建新的 metrics 服务器
func NewServer(config ServerConfig) *Server {
	if config.Addr == "" {
		config.Addr = ":9090"
	}
	if config.Path == "" {
		config.Path = "/metrics"
	}
	if config.Namespace == "" {
		config.Namespace = "dataflow"
	}

	collector := NewCollector(config.Namespace)

	// 注册默认的 Go 运行时指标收集器
	collector.registry.MustRegister(collectors.NewGoCollector())
	collector.registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// 注册默认的 dataflow 指标
	if err := collector.Register(MetricDesc{
		Name:   "records_in_total",
		Type:   MetricTypeCounter,
		Help:   "Total number of records read from source",
		Labels: []string{"flow", "component"},
	}); err != nil {
		return nil
	}
	if err := collector.Register(MetricDesc{
		Name:   "records_out_total",
		Type:   MetricTypeCounter,
		Help:   "Total number of records written to sink",
		Labels: []string{"flow", "component"},
	}); err != nil {
		return nil
	}
	if err := collector.Register(MetricDesc{
		Name:   "records_error_total",
		Type:   MetricTypeCounter,
		Help:   "Total number of errors encountered",
		Labels: []string{"flow", "component"},
	}); err != nil {
		return nil
	}
	if err := collector.Register(MetricDesc{
		Name:   "duration_seconds",
		Type:   MetricTypeHistogram,
		Help:   "Duration of flow execution in seconds",
		Labels: []string{"flow", "component"},
	}); err != nil {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle(config.Path, collector.Handler())

	return &Server{
		addr:      config.Addr,
		path:      config.Path,
		server:    &http.Server{Addr: config.Addr, Handler: mux},
		collector: collector,
	}
}

// Collector 返回指标收集器
func (s *Server) Collector() *Collector {
	return s.collector
}

// Start 启动服务器
func (s *Server) Start() error {
	fmt.Printf("Prometheus metrics 服务器启动: http://%s%s\n", s.addr, s.path)
	return s.server.ListenAndServe()
}

// Stop 停止服务器
func (s *Server) Stop() error {
	return s.server.Close()
}

// DefaultCollector 默认指标收集器
var defaultCollector *Collector
var once sync.Once

// Default 返回默认指标收集器
func Default() *Collector {
	once.Do(func() {
		defaultCollector = NewCollector("procflow")
		prometheus.MustRegister(defaultCollector.registry)
	})
	return defaultCollector
}
