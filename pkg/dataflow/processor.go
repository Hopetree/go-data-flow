package dataflow

import "context"

// Processor is the data processor interface.
// Processors read data from input channel, process it, and send to output channel.
type Processor[T any] interface {
	// Init initializes the component with JSON configuration.
	// Returns an error if config parsing or validation fails.
	Init(config []byte) error

	// Process reads data from input channel, processes it, and sends to output channel.
	// ctx: context for cancellation and timeout
	// in: input channel to read from (upstream writes to this)
	// out: output channel to write to (downstream reads from this)
	// Returns: error if any
	//
	// Implementation guidelines:
	// - Use for data := range in { ... } to iterate input channel
	// - Can filter data (not send to out)
	// - Can transform 1 record to N records (1:N)
	// - Can merge N records to 1 record (N:1)
	// - DO NOT close out channel (managed by framework)
	// - Return when in channel is closed and all data is processed
	Process(ctx context.Context, in <-chan T, out chan<- T) error
}
