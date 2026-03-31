package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsRecorder 记录处理过程中的指标
type MetricsRecorder interface {
	// RecordIn 记录进入组件的记录数
	RecordIn(count int64)
	// RecordOut 记录离开组件的记录数
	RecordOut(count int64)
	// RecordError 记录错误数
	RecordError(count int64)
	// RecordDuration 记录处理耗时
	RecordDuration(seconds float64)

	// 自定义指标记录方法

	// AddCounter 增加自定义计数器指标
	AddCounter(name string, value float64, labels prometheus.Labels)
	// SetGauge 设置自定义 Gauge 指标
	SetGauge(name string, value float64, labels prometheus.Labels)
	// ObserveHistogram 观察自定义 Histogram 指标
	ObserveHistogram(name string, value float64, labels prometheus.Labels)
	// ObserveSummary 观察自定义 Summary 指标
	ObserveSummary(name string, value float64, labels prometheus.Labels)
}

// noopMetricsRecorder 空实现的指标记录器
type noopMetricsRecorder struct{}

func (n *noopMetricsRecorder) RecordIn(count int64)                     {}
func (n *noopMetricsRecorder) RecordOut(count int64)                    {}
func (n *noopMetricsRecorder) RecordError(count int64)                  {}
func (n *noopMetricsRecorder) RecordDuration(seconds float64)           {}
func (n *noopMetricsRecorder) AddCounter(name string, value float64, labels prometheus.Labels) {
}
func (n *noopMetricsRecorder) SetGauge(name string, value float64, labels prometheus.Labels) {
}
func (n *noopMetricsRecorder) ObserveHistogram(name string, value float64, labels prometheus.Labels) {
}
func (n *noopMetricsRecorder) ObserveSummary(name string, value float64, labels prometheus.Labels) {
}

// PrometheusCollector Prometheus 指标收集器接口
type PrometheusCollector interface {
	// AddCounter 增加计数器值
	AddCounter(name string, value float64, labels prometheus.Labels)
	// ObserveHistogram 观察 Histogram 值
	ObserveHistogram(name string, value float64, labels prometheus.Labels)
}

// componentMetrics 组件级别的指标
type componentMetrics struct {
	flowName      string
	componentName string
	promCollector PrometheusCollector

	recordsIn     atomic.Int64
	recordsOut    atomic.Int64
	recordsError  atomic.Int64
	processingSec atomic.Int64 // 存储纳秒，避免浮点精度问题
}

func (m *componentMetrics) RecordIn(count int64) {
	m.recordsIn.Add(count)

	if m.promCollector != nil {
		m.promCollector.AddCounter("records_in_total", float64(count), prometheus.Labels{
			"flow":      m.flowName,
			"component": m.componentName,
		})
	}
}

func (m *componentMetrics) RecordOut(count int64) {
	m.recordsOut.Add(count)

	if m.promCollector != nil {
		m.promCollector.AddCounter("records_out_total", float64(count), prometheus.Labels{
			"flow":      m.flowName,
			"component": m.componentName,
		})
	}
}

func (m *componentMetrics) RecordError(count int64) {
	m.recordsError.Add(count)

	if m.promCollector != nil {
		m.promCollector.AddCounter("records_error_total", float64(count), prometheus.Labels{
			"flow":      m.flowName,
			"component": m.componentName,
		})
	}
}

func (m *componentMetrics) RecordDuration(seconds float64) {
	m.processingSec.Add(int64(seconds * 1e9))

	if m.promCollector != nil {
		m.promCollector.ObserveHistogram("duration_seconds", seconds, prometheus.Labels{
			"flow":      m.flowName,
			"component": m.componentName,
		})
	}
}

// AddCounter 增加自定义计数器指标
func (m *componentMetrics) AddCounter(name string, value float64, labels prometheus.Labels) {
	if m.promCollector == nil {
		return
	}
	mergedLabels := prometheus.Labels{
		"flow":      m.flowName,
		"component": m.componentName,
	}
	for k, v := range labels {
		mergedLabels[k] = v
	}
	m.promCollector.AddCounter(name, value, mergedLabels)
}

// SetGauge 设置自定义 Gauge 指标
func (m *componentMetrics) SetGauge(name string, value float64, labels prometheus.Labels) {
	if m.promCollector == nil {
		return
	}
	mergedLabels := prometheus.Labels{
		"flow":      m.flowName,
		"component": m.componentName,
	}
	for k, v := range labels {
		mergedLabels[k] = v
	}
	m.promCollector.AddCounter(name, value, mergedLabels)
}

// ObserveHistogram 观察自定义 Histogram 指标
func (m *componentMetrics) ObserveHistogram(name string, value float64, labels prometheus.Labels) {
	if m.promCollector == nil {
		return
	}
	mergedLabels := prometheus.Labels{
		"flow":      m.flowName,
		"component": m.componentName,
	}
	for k, v := range labels {
		mergedLabels[k] = v
	}
	m.promCollector.ObserveHistogram(name, value, mergedLabels)
}

// ObserveSummary 观察自定义 Summary 指标
func (m *componentMetrics) ObserveSummary(name string, value float64, labels prometheus.Labels) {
	if m.promCollector == nil {
		return
	}
	mergedLabels := prometheus.Labels{
		"flow":      m.flowName,
		"component": m.componentName,
	}
	for k, v := range labels {
		mergedLabels[k] = v
	}
	m.promCollector.ObserveHistogram(name, value, mergedLabels)
}

func (m *componentMetrics) Snapshot() (in, out, errs int64, avgSec float64) {
	in = m.recordsIn.Load()
	out = m.recordsOut.Load()
	errs = m.recordsError.Load()
	ns := m.processingSec.Load()
	if in > 0 {
		avgSec = float64(ns) / 1e9 / float64(in)
	}
	return
}

// FlowMetrics 流程级别的指标聚合
type FlowMetrics struct {
	mu            sync.RWMutex
	components    map[string]*componentMetrics
	flowName      string
	promCollector PrometheusCollector
}

// NewFlowMetrics 创建流程级别的指标聚合器
func NewFlowMetrics(flowName string) *FlowMetrics {
	return &FlowMetrics{
		components: make(map[string]*componentMetrics),
		flowName:   flowName,
	}
}

// SetPrometheusCollector 设置 Prometheus 收集器
func (fm *FlowMetrics) SetPrometheusCollector(collector PrometheusCollector) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.promCollector = collector
}

// GetOrCreate 获取或创建组件指标
func (fm *FlowMetrics) GetOrCreate(name string) *componentMetrics {
	fm.mu.RLock()
	if m, ok := fm.components[name]; ok {
		fm.mu.RUnlock()
		return m
	}
	fm.mu.RUnlock()

	fm.mu.Lock()
	defer fm.mu.Unlock()
	if m, ok := fm.components[name]; ok {
		return m
	}
	m := &componentMetrics{
		flowName:      fm.flowName,
		componentName: name,
		promCollector: fm.promCollector,
	}
	fm.components[name] = m
	return m
}

// Snapshot 返回所有组件的指标快照
func (fm *FlowMetrics) Snapshot() []MetricSnapshot {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	snapshots := make([]MetricSnapshot, 0, len(fm.components))
	for name, m := range fm.components {
		in, out, errs, avgSec := m.Snapshot()
		snapshots = append(snapshots, MetricSnapshot{
			ComponentName:  name,
			RecordsIn:      in,
			RecordsOut:     out,
			RecordsError:   errs,
			AvgDurationSec: avgSec,
		})
	}
	return snapshots
}

// MetricSnapshot 指标快照
type MetricSnapshot struct {
	ComponentName  string
	RecordsIn      int64
	RecordsOut     int64
	RecordsError   int64
	AvgDurationSec float64
	ProcessingTime time.Duration
}

// MetricsSummary 指标汇总
type MetricsSummary struct {
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
	TotalIn    int64
	TotalOut   int64
	TotalError int64
	Components []MetricSnapshot
}
