package ruler

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"google.golang.org/protobuf/proto"
)

// capture holds what the fake remote-write endpoint received.
type capture struct {
	hits    int
	headers http.Header
	body    *prompb.WriteRequest
}

// newFakeMimir returns a test server that snappy-decodes and proto-unmarshals
// each remote-write request into c, replying with status.
func newFakeMimir(t *testing.T, c *capture, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.hits++
		c.headers = r.Header.Clone()
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		decoded, err := snappy.Decode(nil, raw)
		if err != nil {
			t.Errorf("snappy decode: %v", err)
		}
		var wr prompb.WriteRequest
		if err := proto.Unmarshal(decoded, &wr); err != nil {
			t.Errorf("proto unmarshal: %v", err)
		}
		c.body = &wr
		w.WriteHeader(status)
	}))
}

func sampleVector() promql.Vector {
	return promql.Vector{
		{
			T:      1700000000000,
			F:      42,
			Metric: labels.FromStrings("__name__", "http_requests_total", "instance", "a"),
		},
	}
}

func TestRemoteWrite_SendsSnappyProtoWithHeaders(t *testing.T) {
	var c capture
	srv := newFakeMimir(t, &c, http.StatusOK)
	defer srv.Close()

	w := NewRemoteWriteWriter(srv.URL, 5*time.Second, "")
	if err := w.Write("", "job:http:rate5m", map[string]string{"team": "infra"}, sampleVector()); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if c.hits != 1 {
		t.Fatalf("expected 1 request, got %d", c.hits)
	}
	if got := c.headers.Get("Content-Encoding"); got != "snappy" {
		t.Errorf("Content-Encoding = %q, want snappy", got)
	}
	if got := c.headers.Get("Content-Type"); got != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", got)
	}
	if got := c.headers.Get("X-Prometheus-Remote-Write-Version"); got != "0.1.0" {
		t.Errorf("remote-write version = %q, want 0.1.0", got)
	}

	if len(c.body.GetTimeseries()) != 1 {
		t.Fatalf("expected 1 series, got %d", len(c.body.GetTimeseries()))
	}
	got := labelMap(c.body.GetTimeseries()[0].GetLabels())
	if got["__name__"] != "job:http:rate5m" {
		t.Errorf("__name__ = %q, want job:http:rate5m", got["__name__"])
	}
	if got["team"] != "infra" {
		t.Errorf("team = %q, want infra", got["team"])
	}
	if got["instance"] != "a" {
		t.Errorf("instance = %q, want a", got["instance"])
	}
	s := c.body.GetTimeseries()[0].GetSamples()[0]
	if s.GetValue() != 42 || s.GetTimestamp() != 1700000000000 {
		t.Errorf("sample = (%v, %d), want (42, 1700000000000)", s.GetValue(), s.GetTimestamp())
	}
}

func TestRemoteWrite_EmptyVectorSkipsRequest(t *testing.T) {
	var c capture
	srv := newFakeMimir(t, &c, http.StatusOK)
	defer srv.Close()

	w := NewRemoteWriteWriter(srv.URL, 5*time.Second, "")
	if err := w.Write("", "r", nil, promql.Vector{}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if c.hits != 0 {
		t.Fatalf("expected no request for empty vector, got %d", c.hits)
	}
}

func TestRemoteWrite_Non2xxReturnsError(t *testing.T) {
	var c capture
	srv := newFakeMimir(t, &c, http.StatusBadRequest)
	defer srv.Close()

	w := NewRemoteWriteWriter(srv.URL, 5*time.Second, "")
	err := w.Write("", "r", nil, sampleVector())
	if err == nil {
		t.Fatal("expected an error on non-2xx response, got nil")
	}
}

func TestRemoteWrite_StaticTenantHeader(t *testing.T) {
	// Federation is off in tests, so the static tenant supplies X-Scope-OrgID.
	var c capture
	srv := newFakeMimir(t, &c, http.StatusOK)
	defer srv.Close()

	w := NewRemoteWriteWriter(srv.URL, 5*time.Second, "team-a")
	if err := w.Write("ignored-oid", "r", nil, sampleVector()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := c.headers.Get("X-Scope-OrgID"); got != "team-a" {
		t.Errorf("X-Scope-OrgID = %q, want team-a", got)
	}
}

func TestRemoteWrite_NoTenantOmitsHeader(t *testing.T) {
	var c capture
	srv := newFakeMimir(t, &c, http.StatusOK)
	defer srv.Close()

	w := NewRemoteWriteWriter(srv.URL, 5*time.Second, "")
	if err := w.Write("", "r", nil, sampleVector()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, ok := c.headers["X-Scope-OrgID"]; ok {
		t.Errorf("X-Scope-OrgID should be absent when no tenant is configured")
	}
}
