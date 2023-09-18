package cluster

import "github.com/prometheus/client_golang/prometheus"

type clusterNodeMetrics struct {
	// ---------------------------
	rpcRequestCount *prometheus.CounterVec
	rpcDuration     *prometheus.HistogramVec
	// ---------------------------
	pointInsertCount prometheus.Counter
	pointUpdateCount prometheus.Counter
	pointDeleteCount prometheus.Counter
	pointSearchCount prometheus.Counter
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
		pointInsertCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cluster_node_point_insert_count",
				Help: "Total number of points inserted.",
			},
		),
		pointUpdateCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cluster_node_point_update_count",
				Help: "Total number of points updated.",
			},
		),
		pointDeleteCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cluster_node_point_delete_count",
				Help: "Total number of points deleted.",
			},
		),
		pointSearchCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "cluster_node_point_search_count",
				Help: "Total number of points searched.",
			},
		),
	}
}

func (c *ClusterNode) RegisterMetrics(reg *prometheus.Registry) {
	reg.MustRegister(c.metrics.rpcRequestCount)
	reg.MustRegister(c.metrics.rpcDuration)
	reg.MustRegister(c.metrics.pointInsertCount)
	reg.MustRegister(c.metrics.pointUpdateCount)
	reg.MustRegister(c.metrics.pointDeleteCount)
	reg.MustRegister(c.metrics.pointSearchCount)
}
