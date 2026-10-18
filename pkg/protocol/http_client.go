package protocol

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"time"
)

// HTTPClient is a pooled, reuse-optimized HTTP/1.1 and HTTP/2 synthetic client.
type HTTPClient struct {
	client *http.Client
	config TargetConfig
}

// NewHTTPClient creates an optimized HTTP synthetic client with fine-tuned socket buffers.
func NewHTTPClient(cfg TargetConfig) *HTTPClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 1000
	}
	if cfg.Method == "" {
		cfg.Method = http.MethodGet
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxConns,
		MaxIdleConnsPerHost:   cfg.MaxConns,
		MaxConnsPerHost:       cfg.MaxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:   false,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.TLSInsecure,
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
	}

	return &HTTPClient{
		client: client,
		config: cfg,
	}
}

// Execute performs one HTTP request measuring accurate transit latency.
func (c *HTTPClient) Execute(ctx context.Context) Result {
	var bodyReader io.Reader
	sentBytes := int64(len(c.config.Body))
	if sentBytes > 0 {
		bodyReader = bytes.NewReader(c.config.Body)
	}

	req, err := http.NewRequestWithContext(ctx, c.config.Method, c.config.URL, bodyReader)
	if err != nil {
		return Result{
			Timestamp: time.Now(),
			Err:       err,
		}
	}

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

	// Drain response body to enable HTTP keep-alive connection reuse
	var recvBytes int64
	if resp.Body != nil {
		recvBytes, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	return Result{
		Timestamp:  start,
		Duration:   duration,
		StatusCode: resp.StatusCode,
		BytesSent:  sentBytes,
		BytesRecv:  recvBytes,
	}
}

// Close releases connection pool.
func (c *HTTPClient) Close() error {
	if tr, ok := c.client.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	return nil
}
