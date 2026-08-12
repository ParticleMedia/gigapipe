package stat

import (
	"net/http"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/metric"
)

func ObserveLatency(route, status string, seconds float64) {
	metric.RequestDuration.WithLabelValues(route, status).Observe(seconds)
}

func InstrumentRoute(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		ObserveLatency(route, strconv.Itoa(sw.statusCode), time.Since(start).Seconds())
	}
}

type statusWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.statusCode = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}
