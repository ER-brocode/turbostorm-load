package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ER-brocode/turbostorm-load/pkg/coordinator"
	"github.com/ER-brocode/turbostorm-load/pkg/generator"
	"github.com/ER-brocode/turbostorm-load/pkg/protocol"
)

func main() {
	mode := flag.String("mode", "standalone", "Execution mode: standalone, master, or follower")
	url := flag.String("url", "http://127.0.0.1:8080/", "Target target URL")
	proto := flag.String("proto", "http1", "Protocol: http1, http2, websocket, or grpc")
	rps := flag.Float64("rps", 1000, "Target requests per second")
	initialRPS := flag.Float64("initial-rps", 100, "Initial ramp requests per second")
	pattern := flag.String("pattern", "ramp", "Traffic pattern: constant, ramp, step, sinusoid, spike")
	duration := flag.Duration("duration", 30*time.Second, "Test run duration")
	rampDuration := flag.Duration("ramp", 10*time.Second, "Ramp-up duration")
	concurrency := flag.Int("c", 100, "Number of concurrent worker goroutines")
	listenAddr := flag.String("listen", ":9090", "Listen address for coordinator or follower")
	masterAddr := flag.String("master", "127.0.0.1:9090", "Master coordinator address (for follower mode)")
	nodeID := flag.String("id", "worker-1", "Worker node ID")

	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("========================================================================\n")
	fmt.Printf(" TURBOSTORM-LOAD: Synthetic Traffic Generator & Distributed Orchestrator\n")
	fmt.Printf("========================================================================\n")
	fmt.Printf(" Mode:            %s\n", *mode)
	fmt.Printf(" Target URL:      %s\n", *url)
	fmt.Printf(" Protocol:        %s\n", *proto)
	fmt.Printf(" Pattern:         %s\n", *pattern)
	fmt.Printf(" Target RPS:      %.0f (Initial: %.0f)\n", *rps, *initialRPS)
	fmt.Printf(" Concurrency:     %d workers\n", *concurrency)
	fmt.Printf(" Duration:        %v (Ramp: %v)\n", *duration, *rampDuration)
	fmt.Printf("------------------------------------------------------------------------\n")

	switch *mode {
	case "master":
		coord := coordinator.NewCoordinator("master-0", coordinator.RoleMaster, *listenAddr, "")
		if err := coord.StartHTTP(); err != nil {
			fmt.Printf("Error starting coordinator: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Master coordinator listening on %s. Waiting for followers or load plan...\n", *listenAddr)
		<-ctx.Done()
		_ = coord.Stop()

	case "follower":
		coord := coordinator.NewCoordinator(*nodeID, coordinator.RoleFollower, *listenAddr, *masterAddr)
		_ = coord.StartHTTP()
		fc := coordinator.NewFollowerClient(*nodeID, *masterAddr)
		go fc.StartHeartbeat(ctx, *listenAddr, func() metricsSummary {
			return metricsSummary{}
		})
		fmt.Printf("Follower node %s listening on %s, reporting to %s\n", *nodeID, *listenAddr, *masterAddr)
		<-ctx.Done()
		_ = coord.Stop()

	case "standalone":
		cfg := generator.GeneratorConfig{
			Target: protocol.TargetConfig{
				Protocol: *proto,
				URL:      *url,
				Timeout:  5 * time.Second,
				MaxConns: *concurrency * 2,
			},
			Pattern:      generator.TrafficPattern(*pattern),
			InitialRPS:   *initialRPS,
			TargetRPS:    *rps,
			Duration:     *duration,
			RampDuration: *rampDuration,
			Concurrency:  *concurrency,
		}

		gen := generator.NewGenerator(cfg)
		if err := gen.Start(ctx); err != nil {
			fmt.Printf("Failed to start generator: %v\n", err)
			os.Exit(1)
		}

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		done := make(chan struct{})
		go func() {
			gen.Wait()
			close(done)
		}()

		running := true
		for running {
			select {
			case <-ctx.Done():
				fmt.Println("\nAborting load test...")
				gen.Stop()
				running = false
			case <-done:
				running = false
			case <-ticker.C:
				snap := gen.Report()
				fmt.Printf("[%5.1fs] Active RPS: %8.1f | Req: %6d | Err: %4d | p50: %5d us | p99: %5d us\n",
					snap.DurationSeconds, snap.CurrentRPS, snap.TotalRequests, snap.ErrorRequests,
					snap.LatencySnapshot.P50Micros, snap.LatencySnapshot.P99Micros)
			}
		}

		finalReport := gen.Report()
		fmt.Println(finalReport.FormatTable())
	}
}

type metricsSummary struct{}
