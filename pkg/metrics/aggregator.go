package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Aggregator accumulates metrics across workers and generator nodes.
type Aggregator struct {
	mu           sync.RWMutex
	hist         *Histogram
	startTime    time.Time
	successCount atomic.Int64
	errorCount   atomic.Int64
	bytesSent    atomic.Int64
	bytesRecv    atomic.Int64
	statusCodes  sync.Map // int -> *atomic.Int64
	errorReasons sync.Map // string -> *atomic.Int64
}

// NewAggregator creates a new metrics Aggregator.
func NewAggregator() *Aggregator {
	// Tracks latencies from 1 us to 1 hour (3.6e9 us) with 3 significant figures
	h, err := NewHistogram(1, 3600000000, 3)
	if err != nil {
		panic(err)
	}

	return &Aggregator{
		hist:      h,
		startTime: time.Now(),
	}
}

// RecordOutcome records a single request outcome.
func (a *Aggregator) RecordOutcome(duration time.Duration, statusCode int, sentBytes, recvBytes int64, err error) {
	_ = a.hist.RecordDuration(duration)
	a.bytesSent.Add(sentBytes)
	a.bytesRecv.Add(recvBytes)

	if statusCode > 0 {
		val, _ := a.statusCodes.LoadOrStore(statusCode, new(atomic.Int64))
		val.(*atomic.Int64).Add(1)
	}

	if err != nil || (statusCode >= 400 && statusCode < 600) {
		a.errorCount.Add(1)
		errStr := "status_code_error"
		if err != nil {
			errStr = err.Error()
		}
		val, _ := a.errorReasons.LoadOrStore(errStr, new(atomic.Int64))
		val.(*atomic.Int64).Add(1)
	} else {
		a.successCount.Add(1)
	}
}

// MetricsSummary is the consolidated metrics report.
type MetricsSummary struct {
	DurationSeconds float64          `json:"duration_sec"`
	TotalRequests   int64            `json:"total_requests"`
	SuccessRequests int64            `json:"success_requests"`
	ErrorRequests   int64            `json:"error_requests"`
	CurrentRPS      float64          `json:"current_rps"`
	BytesSent       int64            `json:"bytes_sent"`
	BytesReceived   int64            `json:"bytes_received"`
	ThroughputMBps  float64          `json:"throughput_mb_sec"`
	StatusCodes     map[int]int64    `json:"status_codes"`
	ErrorReasons    map[string]int64 `json:"error_reasons,omitempty"`
	LatencySnapshot Snapshot         `json:"latencies_us"`
}

// Summary generates an instantaneous or cumulative summary.
func (a *Aggregator) Summary() MetricsSummary {
	elapsed := time.Since(a.startTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	succ := a.successCount.Load()
	errs := a.errorCount.Load()
	tot := succ + errs
	sent := a.bytesSent.Load()
	recv := a.bytesRecv.Load()

	statusMap := make(map[int]int64)
	a.statusCodes.Range(func(key, value any) bool {
		statusMap[key.(int)] = value.(*atomic.Int64).Load()
		return true
	})

	errMap := make(map[string]int64)
	a.errorReasons.Range(func(key, value any) bool {
		errMap[key.(string)] = value.(*atomic.Int64).Load()
		return true
	})

	throughput := float64(sent+recv) / (1024 * 1024) / elapsed

	return MetricsSummary{
		DurationSeconds: elapsed,
		TotalRequests:   tot,
		SuccessRequests: succ,
		ErrorRequests:   errs,
		CurrentRPS:      float64(tot) / elapsed,
		BytesSent:       sent,
		BytesReceived:   recv,
		ThroughputMBps:  throughput,
		StatusCodes:     statusMap,
		ErrorReasons:    errMap,
		LatencySnapshot: a.hist.GetSnapshot(),
	}
}

// FormatTable formats the summary as an ASCII benchmark report table.
func (s *MetricsSummary) FormatTable() string {
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("========================================================================================\n")
	sb.WriteString("                     TURBOSTORM-LOAD PERFORMANCE BENCHMARK REPORT                      \n")
	sb.WriteString("========================================================================================\n")
	sb.WriteString(fmt.Sprintf(" Duration:          %.2fs\n", s.DurationSeconds))
	sb.WriteString(fmt.Sprintf(" Total Requests:    %d\n", s.TotalRequests))
	sb.WriteString(fmt.Sprintf(" Successful:        %d\n", s.SuccessRequests))
	sb.WriteString(fmt.Sprintf(" Errors:            %d (%.2f%%)\n", s.ErrorRequests, float64(s.ErrorRequests)/float64(s.TotalRequests+1)*100))
	sb.WriteString(fmt.Sprintf(" Throughput:        %.2f req/s (%.2f MB/s)\n", s.CurrentRPS, s.ThroughputMBps))
	sb.WriteString("----------------------------------------------------------------------------------------\n")
	sb.WriteString(" LATENCY PERCENTILES (Microseconds & Milliseconds):\n")
	sb.WriteString(fmt.Sprintf("   Min:             %8d us  (%8.2f ms)\n", s.LatencySnapshot.MinMicros, float64(s.LatencySnapshot.MinMicros)/1000.0))
	sb.WriteString(fmt.Sprintf("   Mean:            %8.0f us  (%8.2f ms)\n", s.LatencySnapshot.MeanMicros, s.LatencySnapshot.MeanMicros/1000.0))
	sb.WriteString(fmt.Sprintf("   p50 (Median):    %8d us  (%8.2f ms)\n", s.LatencySnapshot.P50Micros, float64(s.LatencySnapshot.P50Micros)/1000.0))
	sb.WriteString(fmt.Sprintf("   p90:             %8d us  (%8.2f ms)\n", s.LatencySnapshot.P90Micros, float64(s.LatencySnapshot.P90Micros)/1000.0))
	sb.WriteString(fmt.Sprintf("   p95:             %8d us  (%8.2f ms)\n", s.LatencySnapshot.P95Micros, float64(s.LatencySnapshot.P95Micros)/1000.0))
	sb.WriteString(fmt.Sprintf("   p99:             %8d us  (%8.2f ms)\n", s.LatencySnapshot.P99Micros, float64(s.LatencySnapshot.P99Micros)/1000.0))
	sb.WriteString(fmt.Sprintf("   p99.9:           %8d us  (%8.2f ms)\n", s.LatencySnapshot.P999Micros, float64(s.LatencySnapshot.P999Micros)/1000.0))
	sb.WriteString(fmt.Sprintf("   p99.99:          %8d us  (%8.2f ms)\n", s.LatencySnapshot.P9999Micros, float64(s.LatencySnapshot.P9999Micros)/1000.0))
	sb.WriteString(fmt.Sprintf("   Max:             %8d us  (%8.2f ms)\n", s.LatencySnapshot.MaxMicros, float64(s.LatencySnapshot.MaxMicros)/1000.0))
	sb.WriteString("----------------------------------------------------------------------------------------\n")
	sb.WriteString(" HTTP STATUS DISTRIBUTION:\n")
	for code, count := range s.StatusCodes {
		sb.WriteString(fmt.Sprintf("   [%d]: %d requests\n", code, count))
	}
	sb.WriteString("========================================================================================\n")
	return sb.String()
}
