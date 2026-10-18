package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ER-brocode/turbostorm-load/pkg/generator"
	"github.com/ER-brocode/turbostorm-load/pkg/protocol"
)

func TestTrafficRampCalculation(t *testing.T) {
	cfg := generator.GeneratorConfig{
		Pattern:      generator.PatternRamp,
		InitialRPS:   100,
		TargetRPS:    1000,
		Duration:     10 * time.Second,
		RampDuration: 10 * time.Second,
	}

	gen := generator.NewGenerator(cfg)

	// At t = 0s, expected RPS = 100
	rps0 := gen.CalculateRPS(0)
	if rps0 != 100 {
		t.Fatalf("expected 100 RPS at t=0, got %f", rps0)
	}

	// At t = 5s (50% ramp), expected RPS = 550
	rps5 := gen.CalculateRPS(5 * time.Second)
	if rps5 < 549 || rps5 > 551 {
		t.Fatalf("expected ~550 RPS at t=5s, got %f", rps5)
	}

	// At t = 10s (100% ramp), expected RPS = 1000
	rps10 := gen.CalculateRPS(10 * time.Second)
	if rps10 != 1000 {
		t.Fatalf("expected 1000 RPS at t=10s, got %f", rps10)
	}
}

func TestSinusoidAndSpikePatterns(t *testing.T) {
	cfg := generator.GeneratorConfig{
		Pattern:         generator.PatternSinusoid,
		InitialRPS:      200,
		TargetRPS:       800,
		SinusoidPeriod:  20 * time.Second,
		SpikeInterval:   10 * time.Second,
		SpikeMultiplier: 4.0,
	}
	gen := generator.NewGenerator(cfg)

	// Midpoint test: at t=0, sin(0) = 0 -> (800+200)/2 = 500
	mid := gen.CalculateRPS(0)
	if mid < 499 || mid > 501 {
		t.Fatalf("expected sinusoid midpoint 500, got %f", mid)
	}

	// Test Spike
	cfg.Pattern = generator.PatternSpike
	genSpike := generator.NewGenerator(cfg)
	// During spike window (0s - 1s in a 10s period)
	spikeRps := genSpike.CalculateRPS(100 * time.Millisecond)
	if spikeRps != 800 { // 200 * 4 = 800
		t.Fatalf("expected spike RPS 800, got %f", spikeRps)
	}
}

func TestTrafficGenerationRampExecution(t *testing.T) {
	reqCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("synthetic response payload"))
	}))
	defer server.Close()

	cfg := generator.GeneratorConfig{
		Target: protocol.TargetConfig{
			Protocol: "http1",
			URL:      server.URL,
			Timeout:  2 * time.Second,
		},
		Pattern:      generator.PatternRamp,
		InitialRPS:   50,
		TargetRPS:    200,
		Duration:     1500 * time.Millisecond,
		RampDuration: 1 * time.Second,
		Concurrency:  10,
	}

	gen := generator.NewGenerator(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := gen.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start generator: %v", err)
	}

	gen.Wait()
	summary := gen.Report()

	if summary.TotalRequests == 0 {
		t.Fatalf("expected requests to be generated, got 0")
	}
	if summary.SuccessRequests == 0 {
		t.Fatalf("expected successful requests, got 0")
	}

	fmt.Printf("[Test] Total: %d, Success: %d, p50: %d us, p99: %d us\n",
		summary.TotalRequests, summary.SuccessRequests,
		summary.LatencySnapshot.P50Micros, summary.LatencySnapshot.P99Micros)
}
