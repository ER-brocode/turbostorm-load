package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ER-brocode/turbostorm-load/pkg/metrics"
)

// FollowerClient allows worker nodes to report heartbeats and stream metrics back to master.
type FollowerClient struct {
	nodeID     string
	masterAddr string
	client     *http.Client
}

// NewFollowerClient instantiates a follower agent client.
func NewFollowerClient(nodeID, masterAddr string) *FollowerClient {
	return &FollowerClient{
		nodeID:     nodeID,
		masterAddr: masterAddr,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// StartHeartbeat begins periodic heartbeat registration with the master.
func (fc *FollowerClient) StartHeartbeat(ctx context.Context, listenAddr string, getMetrics func() metrics.MetricsSummary) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nodeStatus := NodeStatus{
				ID:       fc.nodeID,
				Address:  listenAddr,
				Role:     RoleFollower,
				State:    "active",
				Metrics:  getMetrics(),
				LastSeen: time.Now(),
			}

			payload, _ := json.Marshal(nodeStatus)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				fmt.Sprintf("http://%s/api/v1/nodes/heartbeat", fc.masterAddr),
				bytes.NewReader(payload))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				resp, postErr := fc.client.Do(req)
				if postErr == nil && resp != nil {
					_ = resp.Body.Close()
				}
			}
		}
	}
}
