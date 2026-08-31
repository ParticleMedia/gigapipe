package ruler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/metrico/qryn/v5/shared/federation"
	"github.com/prometheus/prometheus/promql"
	"google.golang.org/protobuf/proto"
)

const maxErrorBodyBytes = 1024

type remoteWriteWriter struct {
	url          string
	client       *http.Client
	staticTenant string
}

func NewRemoteWriteWriter(url string, timeout time.Duration, staticTenant string) RecordingRuleWriter {
	return &remoteWriteWriter{
		url:          url,
		client:       &http.Client{Timeout: timeout},
		staticTenant: staticTenant,
	}
}

func (w *remoteWriteWriter) Write(oid, record string, ruleLabels map[string]string, v promql.Vector) error {
	if len(v) == 0 {
		return nil
	}

	data, err := proto.Marshal(vectorToWriteRequest(record, ruleLabels, v))
	if err != nil {
		return fmt.Errorf("ruler: marshal remote-write request: %w", err)
	}
	compressed := snappy.Encode(nil, data)

	ctx, cancel := context.WithTimeout(context.Background(), w.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(compressed))
	if err != nil {
		return fmt.Errorf("ruler: build remote-write request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("User-Agent", "gigapipe-ruler")
	if tenant := w.tenant(oid); tenant != "" {
		req.Header.Set("X-Scope-OrgID", tenant)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("ruler: send remote-write request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return fmt.Errorf("ruler: remote-write to %s failed: status %d: %s",
			w.url, resp.StatusCode, bytes.TrimSpace(snippet))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (w *remoteWriteWriter) tenant(oid string) string {
	if federation.Enabled() && oid != "" {
		return oid
	}
	return w.staticTenant
}
