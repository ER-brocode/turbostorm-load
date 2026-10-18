package protocol

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// GRPCClient provides low-latency gRPC unary and streaming load generator
// with wire framing over HTTP/2.
type GRPCClient struct {
	config TargetConfig
	mu     sync.Mutex
	client *http.Client
}

// NewGRPCClient instantiates a synthetic gRPC client.
func NewGRPCClient(cfg TargetConfig) *GRPCClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 1000
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        cfg.MaxConns,
		MaxIdleConnsPerHost: cfg.MaxConns,
		MaxConnsPerHost:     cfg.MaxConns,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.TLSInsecure,
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
	}

	return &GRPCClient{
		config: cfg,
		client: client,
	}
}

// EncodeGRPCFrame prepends the 5-byte gRPC prefix (1-byte compressed flag + 4-byte big-endian message length)
func EncodeGRPCFrame(payload []byte) []byte {
	buf := make([]byte, 5+len(payload))
	buf[0] = 0 // uncompressed
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	return buf
}

// DecodeGRPCFrame extracts the gRPC message payload from framed bytes.
func DecodeGRPCFrame(framed []byte) ([]byte, error) {
	if len(framed) < 5 {
		return nil, fmt.Errorf("invalid grpc frame length %d", len(framed))
	}
	length := binary.BigEndian.Uint32(framed[1:5])
	if int(5+length) > len(framed) {
		return nil, fmt.Errorf("truncated grpc frame: expected %d bytes, got %d", 5+length, len(framed))
	}
	return framed[5 : 5+length], nil
}

// Execute performs a synthetic gRPC unary invocation.
func (c *GRPCClient) Execute(ctx context.Context) Result {
	framedPayload := EncodeGRPCFrame(c.config.Body)
	sentBytes := int64(len(framedPayload))

	reqURL := c.config.URL
	if !strings.HasPrefix(reqURL, "http://") && !strings.HasPrefix(reqURL, "https://") {
		reqURL = "http://" + reqURL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(framedPayload))
	if err != nil {
		return Result{
			Timestamp: time.Now(),
			Err:       err,
		}
	}

	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	req.Header.Set("User-Agent", "turbostorm-grpc/1.0")

	for k, v := range c.config.Headers {
		req.Header.Set(k, v)
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{
			Timestamp: start,
			Duration:  duration,
			BytesSent: sentBytes,
			Err:       err,
		}
	}

	var recvBytes int64
	if resp.Body != nil {
		recvBytes, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	grpcStatus := 0
	if statusHeader := resp.Trailer.Get("grpc-status"); statusHeader != "" {
		if statusHeader != "0" {
			grpcStatus = 1
		}
	}

	var resErr error
	if grpcStatus != 0 || resp.StatusCode >= 400 {
		resErr = fmt.Errorf("grpc status code: %d, http status: %d", grpcStatus, resp.StatusCode)
	}

	return Result{
		Timestamp:  start,
		Duration:   duration,
		StatusCode: resp.StatusCode,
		BytesSent:  sentBytes,
		BytesRecv:  recvBytes,
		Err:        resErr,
	}
}

// Close releases connection pool.
func (c *GRPCClient) Close() error {
	if tr, ok := c.client.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	return nil
}

// CreateClient factory selects appropriate protocol client.
func CreateClient(cfg TargetConfig) (Client, error) {
	u, err := url.Parse(cfg.URL)
	if err == nil {
		if u.Scheme == "ws" || u.Scheme == "wss" || cfg.Protocol == "websocket" || cfg.Protocol == "ws" {
			return NewWSClient(cfg), nil
		}
		if cfg.Protocol == "grpc" {
			return NewGRPCClient(cfg), nil
		}
	}

	if cfg.Protocol == "grpc" {
		return NewGRPCClient(cfg), nil
	}
	if cfg.Protocol == "websocket" || cfg.Protocol == "ws" {
		return NewWSClient(cfg), nil
	}

	return NewHTTPClient(cfg), nil
}
