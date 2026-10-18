package protocol

import (
	"context"
	"time"
)

// Result captures the outcome of a single synthetic request execution.
type Result struct {
	Timestamp  time.Time
	Duration   time.Duration
	StatusCode int
	BytesSent  int64
	BytesRecv  int64
	Err        error
}

// Client defines the common high-throughput load client interface.
type Client interface {
	// Execute dispatches a single request across the protocol connection pool.
	Execute(ctx context.Context) Result
	// Close releases associated network resources, sockets, and memory buffers.
	Close() error
}

// TargetConfig specifies the load target endpoint and protocol parameters.
type TargetConfig struct {
	Protocol    string            `json:"protocol"` // "http1", "http2", "websocket", "grpc"
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers"`
	Body        []byte            `json:"body"`
	Timeout     time.Duration     `json:"timeout"`
	KeepAlive   time.Duration     `json:"keep_alive"`
	MaxConns    int               `json:"max_conns"`
	TLSInsecure bool              `json:"tls_insecure"`
}
