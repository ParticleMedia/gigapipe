package controller

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/promise"
)

func ShardIndex(fingerprint uint64, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	return int(fingerprint % uint64(shardCount))
}

func PartitionTimeSeries(in *model.TimeSeriesData, shardCount int) []*model.TimeSeriesData {
	if in == nil {
		return nil
	}
	if shardCount < 1 {
		shardCount = 1
	}
	out := make([]*model.TimeSeriesData, shardCount)
	for s := range out {
		out[s] = &model.TimeSeriesData{MMeta: in.MMeta, MOid: in.MOid}
	}
	for i, fp := range in.MFingerprint {
		b := out[ShardIndex(fp, shardCount)]
		b.MFingerprint = append(b.MFingerprint, fp)
		if i < len(in.MDate) {
			b.MDate = append(b.MDate, in.MDate[i])
		}
		var labels string
		if i < len(in.MLabels) {
			labels = in.MLabels[i]
			b.MLabels = append(b.MLabels, labels)
		}
		if i < len(in.MTTLDays) {
			b.MTTLDays = append(b.MTTLDays, in.MTTLDays[i])
		}
		if i < len(in.MType) {
			b.MType = append(b.MType, in.MType[i])
		}
		var meta string
		if i < len(in.MMetadata) {
			meta = in.MMetadata[i]
			b.MMetadata = append(b.MMetadata, meta)
		}
		b.Size += 14 + len(labels) + len(meta)
	}
	return out
}

func PartitionTimeSamples(in *model.TimeSamplesData, shardCount int) []*model.TimeSamplesData {
	if in == nil {
		return nil
	}
	if shardCount < 1 {
		shardCount = 1
	}
	out := make([]*model.TimeSamplesData, shardCount)
	for s := range out {
		out[s] = &model.TimeSamplesData{MOid: in.MOid}
	}
	for i, fp := range in.MFingerprint {
		b := out[ShardIndex(fp, shardCount)]
		b.MFingerprint = append(b.MFingerprint, fp)
		if i < len(in.MTimestampNS) {
			b.MTimestampNS = append(b.MTimestampNS, in.MTimestampNS[i])
		}
		var msg string
		if i < len(in.MMessage) {
			msg = in.MMessage[i]
			b.MMessage = append(b.MMessage, msg)
		}
		if i < len(in.MValue) {
			b.MValue = append(b.MValue, in.MValue[i])
		}
		if i < len(in.MTTLDays) {
			b.MTTLDays = append(b.MTTLDays, in.MTTLDays[i])
		}
		if i < len(in.MType) {
			b.MType = append(b.MType, in.MType[i])
		}
		b.Size += len(msg) + 26
	}
	return out
}

type LokiShardServices struct {
	TS  [][]service.IInsertServiceV2
	Spl [][]service.IInsertServiceV2

	tsRR  []atomic.Uint32
	splRR []atomic.Uint32
}

func NewLokiShardServices(ts, spl [][]service.IInsertServiceV2) *LokiShardServices {
	return &LokiShardServices{
		TS:    ts,
		Spl:   spl,
		tsRR:  make([]atomic.Uint32, len(ts)),
		splRR: make([]atomic.Uint32, len(spl)),
	}
}

var lokiShard atomic.Pointer[LokiShardServices]

func SetLokiShardServices(s *LokiShardServices) { lokiShard.Store(s) }

func CurrentLokiShardServices() *LokiShardServices { return lokiShard.Load() }

func (s *LokiShardServices) pickTS(shard int) service.IInsertServiceV2 {
	reps := s.TS[shard]
	if len(reps) == 1 {
		return reps[0]
	}
	n := s.tsRR[shard].Add(1) - 1
	return reps[int(n%uint32(len(reps)))]
}

func (s *LokiShardServices) pickSpl(shard int) service.IInsertServiceV2 {
	reps := s.Spl[shard]
	if len(reps) == 1 {
		return reps[0]
	}
	n := s.splRR[shard].Add(1) - 1
	return reps[int(n%uint32(len(reps)))]
}

func lokiShardServices() *LokiShardServices {
	s := lokiShard.Load()
	if s == nil || len(s.TS) == 0 {
		return nil
	}
	return s
}

type lokiShardCtxKey struct{}

var withLokiShardMarker = WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {
	*r = *r.WithContext(context.WithValue(r.Context(), lokiShardCtxKey{}, true))
	return nil
})

func shardRoutingActive(ctx context.Context) bool {
	if lokiShardServices() == nil {
		return false
	}
	v, _ := ctx.Value(lokiShardCtxKey{}).(bool)
	return v
}

func pushShardedTimeSeries(req helpers.SizeGetter) []*promise.Promise[uint32] {
	svcs := lokiShardServices()
	if req == nil || svcs == nil {
		return nil
	}
	ts, ok := req.(*model.TimeSeriesData)
	if !ok {
		return []*promise.Promise[uint32]{doPush(req, service.INSERT_MODE_SYNC, svcs.pickTS(0))}
	}
	parts := PartitionTimeSeries(ts, len(svcs.TS))
	var ps []*promise.Promise[uint32]
	for i, part := range parts {
		if len(part.MFingerprint) == 0 {
			continue
		}
		ps = append(ps, doPush(part, service.INSERT_MODE_SYNC, svcs.pickTS(i)))
	}
	return ps
}

func pushShardedSamples(req helpers.SizeGetter) []*promise.Promise[uint32] {
	svcs := lokiShardServices()
	if req == nil || svcs == nil {
		return nil
	}
	spl, ok := req.(*model.TimeSamplesData)
	if !ok {
		return []*promise.Promise[uint32]{doPush(req, service.INSERT_MODE_SYNC, svcs.pickSpl(0))}
	}
	parts := PartitionTimeSamples(spl, len(svcs.Spl))
	var ps []*promise.Promise[uint32]
	for i, part := range parts {
		if len(part.MFingerprint) == 0 {
			continue
		}
		ps = append(ps, doPush(part, service.INSERT_MODE_SYNC, svcs.pickSpl(i)))
	}
	return ps
}
