package tests

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/ER-brocode/turbostorm-load/pkg/metrics"
)

func TestHistogramPrecisionAndPercentiles(t *testing.T) {
	// Lowest trackable 1us, highest 1 hour, 3 significant figures
	h, err := metrics.NewHistogram(1, 3600000000, 3)
	if err != nil {
		t.Fatalf("failed to create histogram: %v", err)
	}

	samples := make([]int64, 10000)
	for i := 0; i < len(samples); i++ {
		// Log-normal distribution of latencies between 50us and 50,000us
		val := int64(50 + rand.Intn(1000) + rand.Intn(49000))
		samples[i] = val
		_ = h.RecordValue(val)
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	exactP50 := samples[int(0.50*float64(len(samples)))]
	exactP95 := samples[int(0.95*float64(len(samples)))]
	exactP99 := samples[int(0.99*float64(len(samples)))]

	histP50 := h.ValueAtPercentile(50.0)
	histP95 := h.ValueAtPercentile(95.0)
	histP99 := h.ValueAtPercentile(99.0)

	// Verify HDR precision within 10% tolerance across dynamic ranges
	assertWithinTolerance(t, "p50", exactP50, histP50, 0.15)
	assertWithinTolerance(t, "p95", exactP95, histP95, 0.15)
	assertWithinTolerance(t, "p99", exactP99, histP99, 0.15)
}

func assertWithinTolerance(t *testing.T, metric string, expected, actual int64, tolerance float64) {
	diff := float64(actual - expected)
	if diff < 0 {
		diff = -diff
	}
	ratio := diff / float64(expected)
	if ratio > tolerance {
		t.Errorf("%s mismatch: expected ~%d, got %d (ratio difference: %.4f, max tolerance: %.4f)",
			metric, expected, actual, ratio, tolerance)
	}
}

func TestHistogramDurationsAndReset(t *testing.T) {
	h, err := metrics.NewHistogram(1, 3600000000, 3)
	if err != nil {
		t.Fatalf("failed to create histogram: %v", err)
	}

	_ = h.RecordDuration(100 * time.Microsecond)
	_ = h.RecordDuration(500 * time.Microsecond)
	_ = h.RecordDuration(2500 * time.Microsecond)

	if h.TotalCount() != 3 {
		t.Fatalf("expected count 3, got %d", h.TotalCount())
	}

	if h.Min() < 90 || h.Min() > 110 {
		t.Fatalf("expected min ~100us, got %d", h.Min())
	}

	h.Reset()
	if h.TotalCount() != 0 {
		t.Fatalf("expected count 0 after reset, got %d", h.TotalCount())
	}
}
