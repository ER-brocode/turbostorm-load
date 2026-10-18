package generator

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ER-brocode/turbostorm-load/pkg/metrics"
	"github.com/ER-brocode/turbostorm-load/pkg/protocol"
)

// TrafficPattern represents the load curve shape.
type TrafficPattern string

const (
	PatternConstant TrafficPattern = "constant"
	PatternRamp     TrafficPattern = "ramp"
	PatternStep     TrafficPattern = "step"
	PatternSinusoid TrafficPattern = "sinusoid"
	PatternSpike    TrafficPattern = "spike"
)

// GeneratorConfig defines workload parameters for a load test run.
type GeneratorConfig struct {
	Target            protocol.TargetConfig `json:"target"`
	Pattern           TrafficPattern        `json:"pattern"`
	InitialRPS        float64               `json:"initial_rps"`
	TargetRPS         float64               `json:"target_rps"`
	Duration          time.Duration         `json:"duration"`
	RampDuration      time.Duration         `json:"ramp_duration"`
	Concurrency       int                   `json:"concurrency"`
	SinusoidPeriod    time.Duration         `json:"sinusoid_period"`
	SpikeInterval     time.Duration         `json:"spike_interval"`
	SpikeMultiplier   float64               `json:"spike_multiplier"`
	PoissonArrival    bool                  `json:"poisson_arrival"`
}

// Generator is a high-throughput synthetic traffic generator engine.
type Generator struct {
	config     GeneratorConfig
	aggregator *metrics.Aggregator
	running    atomic.Bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	startTime  time.Time
	curTargetRPS atomic.Uint64 // float64 bits stored as uint64
}

// NewGenerator creates a synthetic traffic generator instance.
func NewGenerator(cfg GeneratorConfig) *Generator {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 50
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.InitialRPS <= 0 && cfg.TargetRPS <= 0 {
		cfg.TargetRPS = 100
	}
	if cfg.InitialRPS <= 0 {
		cfg.InitialRPS = cfg.TargetRPS
	}
	if cfg.Pattern == "" {
		cfg.Pattern = PatternConstant
	}
	if cfg.SinusoidPeriod <= 0 {
		cfg.SinusoidPeriod = 30 * time.Second
	}
	if cfg.SpikeInterval <= 0 {
		cfg.SpikeInterval = 10 * time.Second
	}
	if cfg.SpikeMultiplier <= 0 {
		cfg.SpikeMultiplier = 3.0
	}

	g := &Generator{
		config:     cfg,
		aggregator: metrics.NewAggregator(),
		stopCh:     make(chan struct{}),
	}
	g.setRPS(cfg.InitialRPS)
	return g
}

func (g *Generator) setRPS(rps float64) {
	g.curTargetRPS.Store(math.Float64bits(rps))
}

// GetCurrentTargetRPS returns the instantaneous calculated RPS target.
func (g *Generator) GetCurrentTargetRPS() float64 {
	return math.Float64frombits(g.curTargetRPS.Load())
}

// CalculateRPS calculates the expected RPS at elapsed duration based on configured pattern.
func (g *Generator) CalculateRPS(elapsed time.Duration) float64 {
	switch g.config.Pattern {
	case PatternConstant:
		return g.config.TargetRPS

	case PatternRamp:
		if g.config.RampDuration <= 0 {
			return g.config.TargetRPS
		}
		if elapsed >= g.config.RampDuration {
			return g.config.TargetRPS
		}
		ratio := float64(elapsed) / float64(g.config.RampDuration)
		return g.config.InitialRPS + ratio*(g.config.TargetRPS-g.config.InitialRPS)

	case PatternStep:
		// Step up every 5 seconds or ramp interval
		stepDuration := 5 * time.Second
		if g.config.RampDuration > 0 {
			stepDuration = g.config.RampDuration / 4
		}
		steps := int(elapsed / stepDuration)
		if steps > 4 {
			steps = 4
		}
		stepDelta := (g.config.TargetRPS - g.config.InitialRPS) / 4.0
		return g.config.InitialRPS + float64(steps)*stepDelta

	case PatternSinusoid:
		// Oscillation between InitialRPS and TargetRPS
		mid := (g.config.TargetRPS + g.config.InitialRPS) / 2.0
		amplitude := math.Abs(g.config.TargetRPS-g.config.InitialRPS) / 2.0
		radians := 2 * math.Pi * (float64(elapsed%g.config.SinusoidPeriod) / float64(g.config.SinusoidPeriod))
		return mid + amplitude*math.Sin(radians)

	case PatternSpike:
		base := g.config.InitialRPS
		mod := elapsed % g.config.SpikeInterval
		// Spike lasts for 10% of spike interval
		if mod < g.config.SpikeInterval/10 {
			return base * g.config.SpikeMultiplier
		}
		return base

	default:
		return g.config.TargetRPS
	}
}

// Start launches worker goroutines and dispatcher according to the traffic ramp curve.
func (g *Generator) Start(ctx context.Context) error {
	if !g.running.CompareAndSwap(false, true) {
		return nil
	}

	g.startTime = time.Now()
	tokenBucket := make(chan struct{}, 1000000)

	// Dispatcher goroutine calculates rate and fills token bucket
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		lastEmit := time.Now()
		for {
			select {
			case <-g.stopCh:
				return
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				elapsed := now.Sub(g.startTime)
				if g.config.Duration > 0 && elapsed >= g.config.Duration {
					g.Stop()
					return
				}

				targetRPS := g.CalculateRPS(elapsed)
				if targetRPS < 1 {
					targetRPS = 1
				}
				g.setRPS(targetRPS)

				dt := now.Sub(lastEmit).Seconds()
				lastEmit = now

				tokensToGenerate := int(targetRPS * dt)
				if g.config.PoissonArrival && tokensToGenerate > 0 {
					// Poisson distribution variation via Exponential inter-arrivals
					tokensToGenerate = int(float64(tokensToGenerate) * (0.8 + 0.4*rand.Float64()))
				}

				for i := 0; i < tokensToGenerate; i++ {
					select {
					case tokenBucket <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	// Spawn concurrency workers
	for w := 0; w < g.config.Concurrency; w++ {
		g.wg.Add(1)
		go func(workerID int) {
			defer g.wg.Done()
			client, err := protocol.CreateClient(g.config.Target)
			if err != nil {
				return
			}
			defer client.Close()

			for {
				select {
				case <-g.stopCh:
					return
				case <-ctx.Done():
					return
				case <-tokenBucket:
					reqCtx, cancel := context.WithTimeout(ctx, g.config.Target.Timeout)
					res := client.Execute(reqCtx)
					cancel()

					g.aggregator.RecordOutcome(res.Duration, res.StatusCode, res.BytesSent, res.BytesRecv, res.Err)
				}
			}
		}(w)
	}

	return nil
}

// Stop gracefully signals all workers to stop and awaits in-flight operations.
func (g *Generator) Stop() {
	if g.running.CompareAndSwap(true, false) {
		close(g.stopCh)
	}
}

// Wait blocks until all generator workers terminate.
func (g *Generator) Wait() {
	g.wg.Wait()
}

// Aggregator returns the active metrics aggregator.
func (g *Generator) Aggregator() *metrics.Aggregator {
	return g.aggregator
}

// Report gets current metrics summary.
func (g *Generator) Report() metrics.MetricsSummary {
	return g.aggregator.Summary()
}
