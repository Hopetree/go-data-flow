package dataflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/metrics"
)

// Flow 是数据流管道编排器。
// 管理 Source → Processors → Sink 的执行。
type Flow[T any] struct {
	config   *FlowConfig
	registry *Registry[T]

	source     Source[T]
	processors []Processor[T]
	sink       Sink[T]

	// 并发配置
	processorConcurrency []int // 每个 processor 的并发数

	// 统计信息
	stats     StatsCounter
	running   atomic.Bool
	closeOnce sync.Once
	startTime time.Time
	endTime   time.Time

	// 指标收集
	flowMetrics *metrics.FlowMetrics
}

// NewFlow 创建一个新的 Flow 实例。
func NewFlow[T any](config *FlowConfig, registry *Registry[T]) *Flow[T] {
	config.SetDefaults()
	return &Flow[T]{
		config:              config,
		registry:            registry,
		processors:          make([]Processor[T], 0),
		processorConcurrency: make([]int, 0),
		flowMetrics:         metrics.NewFlowMetrics(config.Name),
	}
}

// SetPrometheusCollector 设置 Prometheus 指标收集器。
// 设置后，Flow 运行时会实时将指标上报到 Prometheus。
func (f *Flow[T]) SetPrometheusCollector(collector metrics.PrometheusCollector) {
	f.flowMetrics.SetPrometheusCollector(collector)
}

// Build 初始化所有组件。
// 从注册表创建组件实例并调用每个组件的 Init 方法。
// 如果找不到任何组件或初始化失败则返回错误。
func (f *Flow[T]) Build() error {
	// 验证配置
	if err := f.config.Validate(); err != nil {
		return fmt.Errorf("配置验证失败: %w", err)
	}

	// 构建 Source
	src, ok := f.registry.GetSource(f.config.Source.Name)
	if !ok {
		return fmt.Errorf("%w: source '%s'", ErrComponentNotFound, f.config.Source.Name)
	}
	if err := initComponent(src, f.config.Source.Config); err != nil {
		return fmt.Errorf("source 初始化失败: %w", err)
	}
	f.source = src

	// 注入 Source 的 MetricsRecorder
	f.injectMetricsRecorder(src, "source", "source")

	// 构建 Processors
	for i, spec := range f.config.Processors {
		proc, ok := f.registry.GetProcessor(spec.Name)
		if !ok {
			return fmt.Errorf("%w: processor[%d] '%s'", ErrComponentNotFound, i, spec.Name)
		}
		if err := initComponent(proc, spec.Config); err != nil {
			return fmt.Errorf("processor[%d] 初始化失败: %w", i, err)
		}
		f.processors = append(f.processors, proc)

		// 注入 Processor 的 MetricsRecorder
		componentName := fmt.Sprintf("processor[%d]", i)
		f.injectMetricsRecorder(proc, componentName, "processor")

		// 检测并发能力
		concurrency := 1 // 默认单 goroutine
		if cc, ok := proc.(ConcurrencyCapable); ok {
			cap := cc.ConcurrencyCap()
			if cap.Supported {
				// 有状态的组件强制单线程
				if cap.IsStateful {
					concurrency = 1
				} else if spec.Concurrency != nil && *spec.Concurrency >= 1 {
					// 用户配置优先
					concurrency = *spec.Concurrency
				} else {
					// 使用组件默认值
					concurrency = cap.SuggestedMax
					if concurrency <= 0 {
						concurrency = 4 // 默认并发数
					}
				}
			}
		}
		f.processorConcurrency = append(f.processorConcurrency, concurrency)
	}

	// 构建 Sink
	sink, ok := f.registry.GetSink(f.config.Sink.Name)
	if !ok {
		return fmt.Errorf("%w: sink '%s'", ErrComponentNotFound, f.config.Sink.Name)
	}
	if err := initComponent(sink, f.config.Sink.Config); err != nil {
		return fmt.Errorf("sink 初始化失败: %w", err)
	}
	f.sink = sink

	// 注入 Sink 的 MetricsRecorder
	f.injectMetricsRecorder(sink, "sink", "sink")

	return nil
}

// Run 执行数据管道。
// 如果 flow 未构建、已在运行或任何组件失败则返回错误。
// 阻塞直到所有数据处理完成或 context 被取消。
func (f *Flow[T]) Run(ctx context.Context) error {
	// 检查是否已构建
	if f.source == nil || f.sink == nil {
		return ErrFlowNotBuilt
	}

	// 确保单次运行
	if !f.running.CompareAndSwap(false, true) {
		return ErrFlowAlreadyRunning
	}
	defer f.running.Store(false)

	// 重置状态
	f.startTime = time.Now()
	f.endTime = time.Time{}
	f.stats.Reset()

	// 创建通道链
	// 通道数 = processor 数 + 1 (source -> processor0, processorN -> sink)
	numChannels := len(f.processors) + 1
	channels := make([]chan T, numChannels)
	for i := range channels {
		channels[i] = make(chan T, f.config.BufferSize)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numChannels+1)

	// 启动 Source（单 goroutine）
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(channels[0])
		m := f.flowMetrics.GetOrCreate("source")
		n, err := f.source.Read(ctx, channels[0])
		m.RecordOut(n) // source 输出记录
		f.stats.AddIn(n)
		if err != nil {
			m.RecordError(1)
			f.stats.AddError(1)
			errCh <- fmt.Errorf("source: %w", err)
		}
	}()

	// 启动 Processors（支持并发）
	for i, proc := range f.processors {
		concurrency := f.processorConcurrency[i]
		inCh := channels[i]
		outCh := channels[i+1]

		if concurrency <= 1 {
			// 单 goroutine 模式
			wg.Add(1)
			go func(idx int, p Processor[T], in <-chan T, out chan<- T) {
				defer wg.Done()
				m := f.flowMetrics.GetOrCreate(fmt.Sprintf("processor[%d]", idx))

				// 包装输入通道，计数 RecordIn
				wrappedIn := make(chan T)
				go func() {
					defer close(wrappedIn)
					for item := range in {
						m.RecordIn(1)
						wrappedIn <- item
					}
				}()

				// 包装输出通道，计数 RecordOut
				wrappedOut := make(chan T)
				go func() {
					defer close(out)
					for item := range wrappedOut {
						m.RecordOut(1)
						out <- item
					}
				}()

				start := time.Now()
				if err := p.Process(ctx, wrappedIn, wrappedOut); err != nil {
					m.RecordError(1)
					f.stats.AddError(1)
					errCh <- fmt.Errorf("processor[%d]: %w", idx, err)
				}
				m.RecordDuration(time.Since(start).Seconds())
				close(wrappedOut) // Process 返回后关闭，触发输出包装协程结束
			}(i, proc, inCh, outCh)
		} else {
			// 并发模式：启动多个 worker，使用 fan-out/fan-in
			wg.Add(1)
			go func(idx int, p Processor[T], in <-chan T, out chan<- T, workers int) {
				defer wg.Done()
				defer close(out)

				m := f.flowMetrics.GetOrCreate(fmt.Sprintf("processor[%d]", idx))
				var workerWg sync.WaitGroup

				// 创建分发和合并通道
				fanOutCh := make(chan T, f.config.BufferSize)
				fanInCh := make(chan T, f.config.BufferSize)

				// Fan-out: 将输入分发到多个 worker
				go func() {
					defer close(fanOutCh)
					for item := range in {
						fanOutCh <- item
						m.RecordIn(1)
					}
				}()

				// Fan-in: 收集 worker 输出
				go func() {
					workerWg.Wait()
					close(fanInCh)
				}()

				// 启动多个 worker
				for w := 0; w < workers; w++ {
					workerWg.Add(1)
					go func() {
						defer workerWg.Done()
						start := time.Now()
						if err := p.Process(ctx, fanOutCh, fanInCh); err != nil {
							m.RecordError(1)
							f.stats.AddError(1)
							select {
							case errCh <- fmt.Errorf("processor[%d]: %w", idx, err):
							default:
							}
						}
						m.RecordDuration(time.Since(start).Seconds())
					}()
				}

				// 将合并后的输出写入下一阶段
				for item := range fanInCh {
					out <- item
					m.RecordOut(1)
				}
			}(i, proc, inCh, outCh, concurrency)
		}
	}

	// 启动 Sink（单 goroutine）
	// 使用包装通道来记录 sink 接收的记录数（input）
	sinkInCh := channels[numChannels-1]
	wg.Add(1)
	go func() {
		defer wg.Done()
		m := f.flowMetrics.GetOrCreate("sink")

		// 创建包装通道：计数 sink 从通道读取的记录数
		countedCh := make(chan T)
		// sinkDone: 当 sink 完成后关闭，通知 wrapper 切换为排空模式
		// 防止 sink 提前退出（如 limit 限制）时上游生产者因通道满而阻塞导致死锁
		sinkDone := make(chan struct{})
		go func() {
			defer close(countedCh)
			for item := range sinkInCh {
				select {
				case <-sinkDone:
					// sink 已完成，只排空上游通道并计数，不再转发
					m.RecordIn(1)
				case countedCh <- item:
					m.RecordIn(1)
				}
			}
		}()

		start := time.Now()
		if err := f.sink.Consume(ctx, countedCh); err != nil {
			m.RecordError(1)
			f.stats.AddError(1)
			errCh <- fmt.Errorf("sink: %w", err)
		}
		close(sinkDone) // 通知 wrapper 停止转发，切换为排空模式
		m.RecordDuration(time.Since(start).Seconds())
	}()

	// 等待完成
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
		close(errCh)
	}()

	// 等待结果
	select {
	case <-ctx.Done():
		f.recordDuration()
		return ctx.Err()
	case err := <-errCh:
		// 排空剩余错误
		go func() {
			for range errCh {
			}
		}()
		f.recordDuration()
		return err
	case <-done:
		f.recordDuration()
		return nil
	}
}

// Stats 返回当前统计信息。
func (f *Flow[T]) Stats() Stats {
	return f.stats.Get()
}

// IsRunning 返回 flow 是否正在运行。
func (f *Flow[T]) IsRunning() bool {
	return f.running.Load()
}

// Metrics 返回指标汇总。
// TotalIn = source 的 output（Read() 返回值）
// TotalOut = 管道最后一个有输出的组件的 output
//   - 有 processor 时：最后一个 processor 的 RecordOut
//   - 无 processor 时：source 的 RecordOut
// TotalError = StatsCounter 中所有组件错误之和
// Components 为各组件独立明细。
func (f *Flow[T]) Metrics() metrics.MetricsSummary {
	stats := f.stats.Get()

	summary := metrics.MetricsSummary{
		StartTime:  f.startTime,
		EndTime:    f.endTime,
		Duration:   f.endTime.Sub(f.startTime),
		TotalIn:    stats.TotalIn,
		TotalError: stats.TotalError,
		Components: f.flowMetrics.Snapshot(),
	}

	// 计算 TotalOut（最后一个有输出的组件的 RecordOut）
	for _, comp := range summary.Components {
		if comp.RecordsOut > summary.TotalOut {
			summary.TotalOut = comp.RecordsOut
		}
	}

	return summary
}

// Close 释放组件持有的资源。
// 实现了 Closer 接口的组件会被调用 Close() 方法。
func (f *Flow[T]) Close() error {
	var err error
	f.closeOnce.Do(func() {
		// 关闭 source
		if closer, ok := f.source.(Closer); ok {
			if e := closer.Close(); e != nil && err == nil {
				err = e
			}
		}
		// 关闭 processors
		for _, p := range f.processors {
			if closer, ok := p.(Closer); ok {
				if e := closer.Close(); e != nil && err == nil {
					err = e
				}
			}
		}
		// 关闭 sink
		if closer, ok := f.sink.(Closer); ok {
			if e := closer.Close(); e != nil && err == nil {
				err = e
			}
		}
	})
	return err
}

// recordDuration 记录运行时长。
func (f *Flow[T]) recordDuration() {
	f.endTime = time.Now()
	if !f.startTime.IsZero() {
		f.stats.SetDuration(f.endTime.Sub(f.startTime).Milliseconds())
	}
}

// initComponent 使用配置初始化组件。
func initComponent(component interface{}, config map[string]interface{}) error {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	switch c := component.(type) {
	case interface{ Init([]byte) error }:
		return c.Init(configBytes)
	default:
		return fmt.Errorf("组件未实现 Init 方法")
	}
}

// injectMetricsRecorder 为组件注入 MetricsRecorder（如果组件实现了 MetricsRecorderAware 接口)
func (f *Flow[T]) injectMetricsRecorder(component interface{}, componentName string, componentType string) {
	if aware, ok := component.(MetricsRecorderAware); ok {
		recorder := f.flowMetrics.GetOrCreate(componentName)
		info := ComponentInfo{
			FlowName:      f.config.Name,
			ComponentName: componentName,
			ComponentType: componentType,
		}
		aware.SetMetricsRecorder(recorder, info)
	}
}
