package metrics

import (
	"fmt"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram implements a High Dynamic Range (HDR) Histogram with fixed memory footprint
// and configurable precision. Latency recordings are in microseconds.
type Histogram struct {
	mu                   sync.RWMutex
	lowestTrackableValue int64
	highestTrackableValue int64
	significantFigures   int
	unitMagnitude        int32
	subBucketCount       int32
	subBucketHalfCount   int32
	subBucketMask        int64
	subBucketHalfCountMag int32
	leadingZeroCountBase int32
	countsLen            int
	counts               []int64
	totalCount           atomic.Int64
	minNonZeroValue      atomic.Int64
	maxValue             atomic.Int64
}

// NewHistogram creates a new HDR Histogram configured for tracking latencies in microseconds.
// lowestTrackableValue: minimum positive value (e.g. 1 microsecond)
// highestTrackableValue: maximum trackable value (e.g. 3600000000 = 1 hour in microseconds)
// significantFigures: precision in digits (1 to 5, typically 3)
func NewHistogram(lowestTrackableValue, highestTrackableValue int64, significantFigures int) (*Histogram, error) {
	if lowestTrackableValue < 1 {
		return nil, fmt.Errorf("lowestTrackableValue must be >= 1")
	}
	if highestTrackableValue < 2*lowestTrackableValue {
		return nil, fmt.Errorf("highestTrackableValue must be >= 2 * lowestTrackableValue")
	}
	if significantFigures < 1 || significantFigures > 5 {
		return nil, fmt.Errorf("significantFigures must be between 1 and 5")
	}

	h := &Histogram{
		lowestTrackableValue:  lowestTrackableValue,
		highestTrackableValue: highestTrackableValue,
		significantFigures:    significantFigures,
	}

	largestValueWithSignificance := 2 * int64(math.Pow10(significantFigures))
	subBucketCountMagnitude := int32(math.Ceil(math.Log2(float64(largestValueWithSignificance))))
	h.subBucketHalfCountMag = int32(math.Max(0, float64(subBucketCountMagnitude-1)))
	h.subBucketCount = int32(1) << (h.subBucketHalfCountMag + 1)
	h.subBucketHalfCount = h.subBucketCount / 2
	h.subBucketMask = int64(h.subBucketCount-1) << 0

	unitMag := int32(math.Floor(math.Log2(float64(lowestTrackableValue))))
	h.unitMagnitude = unitMag

	bucketCount := h.getBucketIndex(highestTrackableValue) + 1
	h.countsLen = int(bucketCount+1) * int(h.subBucketHalfCount)
	h.counts = make([]int64, h.countsLen)

	h.leadingZeroCountBase = 64 - unitMag - h.subBucketHalfCountMag - 1
	h.minNonZeroValue.Store(math.MaxInt64)
	h.maxValue.Store(0)

	return h, nil
}

// RecordValue records a single observation in microseconds.
func (h *Histogram) RecordValue(val int64) error {
	return h.RecordValues(val, 1)
}

// RecordDuration records a time.Duration converted to microseconds.
func (h *Histogram) RecordDuration(d time.Duration) error {
	return h.RecordValue(d.Microseconds())
}

// RecordValues records multiple occurrences of an observation.
func (h *Histogram) RecordValues(val int64, count int64) error {
	if count <= 0 {
		return nil
	}
	if val < 0 {
		return fmt.Errorf("value %d cannot be negative", val)
	}
	if val > h.highestTrackableValue {
		val = h.highestTrackableValue
	}

	idx := h.countsIndexFor(val)
	if idx < 0 || idx >= h.countsLen {
		idx = h.countsLen - 1
	}

	h.mu.Lock()
	h.counts[idx] += count
	h.mu.Unlock()

	h.totalCount.Add(count)

	if val > 0 {
		for {
			curMin := h.minNonZeroValue.Load()
			if val >= curMin || h.minNonZeroValue.CompareAndSwap(curMin, val) {
				break
			}
		}
	}
	for {
		curMax := h.maxValue.Load()
		if val <= curMax || h.maxValue.CompareAndSwap(curMax, val) {
			break
		}
	}

	return nil
}

func (h *Histogram) countsIndexFor(val int64) int {
	bucketIndex := h.getBucketIndex(val)
	subBucketIndex := h.getSubBucketIndex(val, bucketIndex)
	return h.countsIndex(bucketIndex, subBucketIndex)
}

func (h *Histogram) countsIndex(bucketIndex, subBucketIndex int32) int {
	baseIndex := int(bucketIndex+1) * int(h.subBucketHalfCount)
	offset := int(subBucketIndex) - int(h.subBucketHalfCount)
	return baseIndex + offset
}

func (h *Histogram) getBucketIndex(val int64) int32 {
	pow2Ceiling := 64 - bits.LeadingZeros64(uint64(val|int64(h.subBucketMask)))
	return int32(pow2Ceiling) - h.unitMagnitude - (h.subBucketHalfCountMag + 1)
}

func (h *Histogram) getSubBucketIndex(val int64, bucketIndex int32) int32 {
	return int32(val >> (bucketIndex + h.unitMagnitude))
}

func (h *Histogram) valueFromIndex(bucketIndex, subBucketIndex int32) int64 {
	return int64(subBucketIndex) << (bucketIndex + h.unitMagnitude)
}

// ValueAtPercentile computes the value at given percentile (e.g. 50.0, 95.0, 99.0, 99.9).
func (h *Histogram) ValueAtPercentile(percentile float64) int64 {
	if percentile <= 0.0 {
		return h.Min()
	}
	if percentile >= 100.0 {
		return h.Max()
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	total := h.totalCount.Load()
	if total == 0 {
		return 0
	}

	threshold := int64(math.Ceil(percentile / 100.0 * float64(total)))
	var countSoFar int64

	for i := 0; i < h.countsLen; i++ {
		countSoFar += h.counts[i]
		if countSoFar >= threshold {
			bucketIndex := int32(i/int(h.subBucketHalfCount)) - 1
			subBucketIndex := int32(i%int(h.subBucketHalfCount)) + h.subBucketHalfCount
			return h.valueFromIndex(bucketIndex, subBucketIndex)
		}
	}
	return h.Max()
}

// Mean calculates the arithmetic mean in microseconds.
func (h *Histogram) Mean() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := h.totalCount.Load()
	if total == 0 {
		return 0.0
	}

	var sum int64
	for i := 0; i < h.countsLen; i++ {
		cnt := h.counts[i]
		if cnt > 0 {
			bucketIndex := int32(i/int(h.subBucketHalfCount)) - 1
			subBucketIndex := int32(i%int(h.subBucketHalfCount)) + h.subBucketHalfCount
			val := h.valueFromIndex(bucketIndex, subBucketIndex)
			sum += val * cnt
		}
	}
	return float64(sum) / float64(total)
}

// Min returns the minimum non-zero value recorded.
func (h *Histogram) Min() int64 {
	v := h.minNonZeroValue.Load()
	if v == math.MaxInt64 {
		return 0
	}
	return v
}

// Max returns the maximum value recorded.
func (h *Histogram) Max() int64 {
	return h.maxValue.Load()
}

// TotalCount returns total observations recorded.
func (h *Histogram) TotalCount() int64 {
	return h.totalCount.Load()
}

// Reset clears all counters and statistics.
func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.counts {
		h.counts[i] = 0
	}
	h.totalCount.Store(0)
	h.minNonZeroValue.Store(math.MaxInt64)
	h.maxValue.Store(0)
}

// Snapshot represents a point-in-time percentile summary of latencies.
type Snapshot struct {
	TotalRequests int64   `json:"total_requests"`
	MinMicros     int64   `json:"min_us"`
	MeanMicros    float64 `json:"mean_us"`
	MaxMicros     int64   `json:"max_us"`
	P50Micros     int64   `json:"p50_us"`
	P90Micros     int64   `json:"p90_us"`
	P95Micros     int64   `json:"p95_us"`
	P99Micros     int64   `json:"p99_us"`
	P999Micros    int64   `json:"p999_us"`
	P9999Micros   int64   `json:"p9999_us"`
}

// GetSnapshot returns a Snapshot with calculated percentiles.
func (h *Histogram) GetSnapshot() Snapshot {
	return Snapshot{
		TotalRequests: h.TotalCount(),
		MinMicros:     h.Min(),
		MeanMicros:    h.Mean(),
		MaxMicros:     h.Max(),
		P50Micros:     h.ValueAtPercentile(50.0),
		P90Micros:     h.ValueAtPercentile(90.0),
		P95Micros:     h.ValueAtPercentile(95.0),
		P99Micros:     h.ValueAtPercentile(99.0),
		P999Micros:    h.ValueAtPercentile(99.9),
		P9999Micros:   h.ValueAtPercentile(99.99),
	}
}
