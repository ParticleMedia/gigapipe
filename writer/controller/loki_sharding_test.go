package controller

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/promise"
)

// stubInsertSvc is a no-op IInsertServiceV2 used to assert replica selection.
type stubInsertSvc struct{ name string }

func (s *stubInsertSvc) Run()                                                     {}
func (s *stubInsertSvc) Stop()                                                    {}
func (s *stubInsertSvc) Request(helpers.SizeGetter, int) *promise.Promise[uint32] { return nil }
func (s *stubInsertSvc) Ping() (time.Time, error)                                 { return time.Time{}, nil }
func (s *stubInsertSvc) GetState(int) int                                         { return 0 }
func (s *stubInsertSvc) GetNodeName() string                                      { return s.name }
func (s *stubInsertSvc) Init()                                                    {}
func (s *stubInsertSvc) PlanFlush()                                               {}

func TestLokiShardServicesRoundRobin(t *testing.T) {
	// shard 0 has two write replicas; shard 1 has a single replica.
	s0r0, s0r1 := &stubInsertSvc{"s0r0"}, &stubInsertSvc{"s0r1"}
	s1r0 := &stubInsertSvc{"s1r0"}
	svcs := NewLokiShardServices(
		[][]service.IInsertServiceV2{{s0r0, s0r1}, {s1r0}},
		[][]service.IInsertServiceV2{{s0r0, s0r1}, {s1r0}},
	)

	// shard 0 round-robins across its two replicas across successive picks.
	var got []string
	for i := 0; i < 4; i++ {
		got = append(got, svcs.pickTS(0).GetNodeName())
	}
	want := []string{"s0r0", "s0r1", "s0r0", "s0r1"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pickTS(0) sequence = %v, want %v", got, want)
		}
	}

	// A single-replica shard always returns that replica (no rotation).
	for i := 0; i < 3; i++ {
		if n := svcs.pickSpl(1).GetNodeName(); n != "s1r0" {
			t.Errorf("pickSpl(1) = %s want s1r0", n)
		}
	}
}

func TestShardIndex(t *testing.T) {
	cases := []struct {
		fp   uint64
		n    int
		want int
	}{
		{0, 3, 0},
		{1, 3, 1},
		{2, 3, 2},
		{3, 3, 0},
		{7, 3, 1},
		{42, 1, 0}, // single shard always 0
		{42, 0, 0}, // defensive: <1 treated as single shard
	}
	for _, c := range cases {
		if got := ShardIndex(c.fp, c.n); got != c.want {
			t.Errorf("ShardIndex(%d, %d) = %d, want %d", c.fp, c.n, got, c.want)
		}
	}
}

func TestPartitionTimeSeries(t *testing.T) {
	const n = 3
	now := time.Now()
	in := &model.TimeSeriesData{
		MDate:        []time.Time{now, now, now, now},
		MLabels:      []string{"{a}", "{bb}", "{ccc}", "{dddd}"},
		MFingerprint: []uint64{3, 1, 2, 4}, // remainders mod 3: 0,1,2,1
		MTTLDays:     []uint16{1, 2, 3, 4},
		MType:        []uint8{1, 1, 1, 1},
		MMetadata:    []string{"m0", "m1", "m2", "m3"},
		MMeta:        "meta",
		MOid:         "tenant-1",
	}

	out := PartitionTimeSeries(in, n)
	if len(out) != n {
		t.Fatalf("got %d buckets, want %d", len(out), n)
	}

	// Expected placement by fingerprint % 3.
	wantFP := [][]uint64{{3}, {1, 4}, {2}}
	wantLabels := [][]string{{"{a}"}, {"{bb}", "{dddd}"}, {"{ccc}"}}
	var totalRows int
	for s := 0; s < n; s++ {
		b := out[s]
		if b == nil {
			t.Fatalf("bucket %d is nil", s)
		}
		if b.MMeta != "meta" || b.MOid != "tenant-1" {
			t.Errorf("bucket %d lost scalars: MMeta=%q MOid=%q", s, b.MMeta, b.MOid)
		}
		if len(b.MFingerprint) != len(wantFP[s]) {
			t.Fatalf("bucket %d: got %v fingerprints, want %v", s, b.MFingerprint, wantFP[s])
		}
		for i := range wantFP[s] {
			if b.MFingerprint[i] != wantFP[s][i] {
				t.Errorf("bucket %d row %d: fp=%d want %d", s, i, b.MFingerprint[i], wantFP[s][i])
			}
			if b.MLabels[i] != wantLabels[s][i] {
				t.Errorf("bucket %d row %d: label=%q want %q", s, i, b.MLabels[i], wantLabels[s][i])
			}
			if b.MFingerprint[i]%uint64(n) != uint64(s) {
				t.Errorf("bucket %d row %d fp=%d does not belong in shard %d", s, i, b.MFingerprint[i], s)
			}
		}
		// All per-row slices stay aligned.
		rows := len(b.MFingerprint)
		if len(b.MDate) != rows || len(b.MLabels) != rows || len(b.MTTLDays) != rows ||
			len(b.MType) != rows || len(b.MMetadata) != rows {
			t.Errorf("bucket %d: misaligned slices", s)
		}
		// Size recomputed per the builder formula.
		var wantSize int
		for i := 0; i < rows; i++ {
			wantSize += 14 + len(b.MLabels[i]) + len(b.MMetadata[i])
		}
		if b.Size != wantSize {
			t.Errorf("bucket %d: Size=%d want %d", s, b.Size, wantSize)
		}
		totalRows += rows
	}
	if totalRows != len(in.MFingerprint) {
		t.Errorf("row count not preserved: got %d want %d", totalRows, len(in.MFingerprint))
	}
}

func TestPartitionTimeSamples(t *testing.T) {
	const n = 2
	in := &model.TimeSamplesData{
		MFingerprint: []uint64{10, 11, 12, 13}, // mod 2: 0,1,0,1
		MTimestampNS: []int64{100, 101, 102, 103},
		MMessage:     []string{"aa", "bbb", "c", "dddd"},
		MValue:       []float64{0, 0, 0, 0},
		MTTLDays:     []uint16{0, 0, 0, 0},
		MType:        []uint8{1, 1, 1, 1},
		MOid:         "t2",
	}
	out := PartitionTimeSamples(in, n)
	if len(out) != n {
		t.Fatalf("got %d buckets want %d", len(out), n)
	}
	wantFP := [][]uint64{{10, 12}, {11, 13}}
	var totalRows int
	for s := 0; s < n; s++ {
		b := out[s]
		if b.MOid != "t2" {
			t.Errorf("bucket %d lost MOid: %q", s, b.MOid)
		}
		rows := len(b.MFingerprint)
		if rows != len(wantFP[s]) {
			t.Fatalf("bucket %d: got %v want %v", s, b.MFingerprint, wantFP[s])
		}
		if len(b.MTimestampNS) != rows || len(b.MMessage) != rows ||
			len(b.MValue) != rows || len(b.MTTLDays) != rows || len(b.MType) != rows {
			t.Errorf("bucket %d: misaligned slices", s)
		}
		var wantSize int
		for i := 0; i < rows; i++ {
			if b.MFingerprint[i]%uint64(n) != uint64(s) {
				t.Errorf("bucket %d row %d fp=%d misrouted", s, i, b.MFingerprint[i])
			}
			wantSize += len(b.MMessage[i]) + 26
		}
		if b.Size != wantSize {
			t.Errorf("bucket %d: Size=%d want %d", s, b.Size, wantSize)
		}
		totalRows += rows
	}
	if totalRows != len(in.MFingerprint) {
		t.Errorf("row count not preserved: got %d want %d", totalRows, len(in.MFingerprint))
	}
}

func TestPartitionSingleShardAndNil(t *testing.T) {
	// shardCount <= 1 -> single bucket holding every row.
	ts := &model.TimeSeriesData{
		MFingerprint: []uint64{5, 9, 13},
		MLabels:      []string{"a", "b", "c"},
		MDate:        []time.Time{{}, {}, {}},
		MTTLDays:     []uint16{0, 0, 0},
		MType:        []uint8{1, 1, 1},
		MMetadata:    []string{"", "", ""},
	}
	out := PartitionTimeSeries(ts, 1)
	if len(out) != 1 || len(out[0].MFingerprint) != 3 {
		t.Fatalf("single-shard TS partition wrong: %+v", out)
	}

	if PartitionTimeSeries(nil, 3) != nil {
		t.Errorf("nil TS input should return nil")
	}
	if PartitionTimeSamples(nil, 3) != nil {
		t.Errorf("nil samples input should return nil")
	}

	// empty-but-non-nil input -> shardCount empty buckets.
	empty := PartitionTimeSamples(&model.TimeSamplesData{}, 3)
	if len(empty) != 3 {
		t.Fatalf("empty samples: got %d buckets want 3", len(empty))
	}
	for s, b := range empty {
		if b == nil || len(b.MFingerprint) != 0 || b.Size != 0 {
			t.Errorf("bucket %d not empty: %+v", s, b)
		}
	}
}
