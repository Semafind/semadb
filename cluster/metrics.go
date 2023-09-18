package cluster

import "github.com/prometheus/client_golang/prometheus"

type clusterNodeMetrics struct {
	rpcRequestCount *prometheus.CounterVec
	rpcDuration     *prometheus.HistogramVec
}

func newClusterNodeMetrics() *clusterNodeMetrics {
	return &clusterNodeMetrics{
		rpcRequestCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cluster_node_rpc_request_count",
				Help: "Total number of RPC requests made.",
			},
			[]string{"handler"},
		),
		rpcDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "cluster_node_rpc_duration_seconds",
				Help:    "RPC request latencies in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"handler"},
		),
	}
}

func (c *ClusterNode) RegisterMetrics(reg *prometheus.Registry) {
	reg.MustRegister(c.metrics.rpcRequestCount)
	reg.MustRegister(c.metrics.rpcDuration)
}
