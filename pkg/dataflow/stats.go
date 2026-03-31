package dataflow

import "sync/atomic"

// Stats holds the runtime statistics for a Flow.
type Stats struct {
	// TotalIn is the total number of records read from source.
	TotalIn int64
	// TotalOut is the total number of records written to sink.
	TotalOut int64
	// TotalError is the total number of errors encountered.
	TotalError int64
	// Duration is the total run duration in milliseconds.
	Duration int64
}

// StatsCounter provides thread-safe statistics counting.
type StatsCounter struct {
	totalIn    atomic.Int64
	totalOut   atomic.Int64
	totalError atomic.Int64
	duration   atomic.Int64
}

// AddIn increments the input counter by n.
func (s *StatsCounter) AddIn(n int64) {
	s.totalIn.Add(n)
}

// AddOut increments the output counter by n.
func (s *StatsCounter) AddOut(n int64) {
	s.totalOut.Add(n)
}

// AddError increments the error counter by n.
func (s *StatsCounter) AddError(n int64) {
	s.totalError.Add(n)
}

// SetDuration sets the run duration in milliseconds.
func (s *StatsCounter) SetDuration(ms int64) {
	s.duration.Store(ms)
}

// Get returns a snapshot of current statistics.
func (s *StatsCounter) Get() Stats {
	return Stats{
		TotalIn:    s.totalIn.Load(),
		TotalOut:   s.totalOut.Load(),
		TotalError: s.totalError.Load(),
		Duration:   s.duration.Load(),
	}
}

// Reset resets all counters to zero.
func (s *StatsCounter) Reset() {
	s.totalIn.Store(0)
	s.totalOut.Store(0)
	s.totalError.Store(0)
	s.duration.Store(0)
}
