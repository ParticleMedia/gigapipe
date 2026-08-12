package metric

import (
	"os"
	"strconv"
	"strings"

	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var defaultRequestLatencyBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100,
}

func RequestLatencyBuckets() []float64 {
	raw := strings.TrimSpace(os.Getenv("READER_REQUEST_LATENCY_BUCKETS"))
	if raw == "" {
		return defaultRequestLatencyBuckets
	}

	parts := strings.Split(raw, ",")
	buckets := make([]float64, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			logger.Error("invalid READER_REQUEST_LATENCY_BUCKETS value ", raw,
				": unparseable bound; falling back to defaults")
			return defaultRequestLatencyBuckets
		}
		buckets = append(buckets, v)
	}

	// Prometheus rejects non-increasing bucket bounds.
	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= buckets[i-1] {
			logger.Error("invalid READER_REQUEST_LATENCY_BUCKETS value ", raw,
				": bounds not strictly increasing; falling back to defaults")
			return defaultRequestLatencyBuckets
		}
	}

	return buckets
}

var RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "gigapipe_request_duration_seconds",
	Help:    "Reader HTTP request latency in seconds",
	Buckets: RequestLatencyBuckets(),
}, []string{"route", "status", "downstream"})
