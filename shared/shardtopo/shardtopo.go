package shardtopo

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Querier interface {
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
}

type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})
}

type Replica struct {
	ShardNum   int
	ReplicaNum int
	HostName   string
	HostAddr   string
	Port       uint16
}

type Shard struct {
	ShardNum int
	Replicas []Replica
}

type Snapshot struct {
	ClusterName string
	ShardCount  int
	Shards      []Shard
}

const discoveryQuery = `SELECT shard_num, replica_num, host_name, host_address, port ` +
	`FROM system.clusters WHERE cluster = $1 ORDER BY shard_num, replica_num`

func Discover(ctx context.Context, q Querier, clusterName string, allowIPs map[string]bool) (*Snapshot, error) {
	if clusterName == "" {
		return nil, fmt.Errorf("shardtopo: empty cluster name")
	}
	if len(allowIPs) == 0 {
		return nil, fmt.Errorf("shardtopo: CLICKHOUSE_SERVER resolved to no write IPs; refusing to route writes")
	}
	rows, err := q.Query(ctx, discoveryQuery, clusterName)
	if err != nil {
		return nil, fmt.Errorf("shardtopo: system.clusters query failed: %w", err)
	}
	defer rows.Close()

	var replicas []Replica
	for rows.Next() {
		var r Replica
		var shardNum, replicaNum uint32
		if err := rows.Scan(&shardNum, &replicaNum, &r.HostName, &r.HostAddr, &r.Port); err != nil {
			return nil, fmt.Errorf("shardtopo: scan system.clusters row: %w", err)
		}
		r.ShardNum = int(shardNum)
		r.ReplicaNum = int(replicaNum)
		replicas = append(replicas, r)
		fmt.Println("shardtopo: discovered replica: ", r.HostAddr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("shardtopo: iterate system.clusters: %w", err)
	}
	return BuildSnapshot(clusterName, replicas, allowIPs)
}

func BuildSnapshot(clusterName string, replicas []Replica, allowIPs map[string]bool) (*Snapshot, error) {
	if len(replicas) == 0 {
		return nil, fmt.Errorf("shardtopo: cluster %q has no replicas in system.clusters", clusterName)
	}
	strict := allowIPs != nil
	byShard := map[int][]Replica{}
	maxShard := 0
	for _, r := range replicas {
		if r.ShardNum < 1 {
			return nil, fmt.Errorf("shardtopo: invalid shard_num %d", r.ShardNum)
		}
		byShard[r.ShardNum] = append(byShard[r.ShardNum], r)
		if r.ShardNum > maxShard {
			maxShard = r.ShardNum
		}
	}
	if len(byShard) != maxShard {
		return nil, fmt.Errorf("shardtopo: cluster %q shard numbers are not contiguous 1..%d (saw %d distinct)",
			clusterName, maxShard, len(byShard))
	}

	shards := make([]Shard, maxShard)
	for shard := 1; shard <= maxShard; shard++ {
		candidates := byShard[shard]
		if len(candidates) == 0 {
			return nil, fmt.Errorf("shardtopo: cluster %q missing shard_num %d", clusterName, shard)
		}
		if strict {
			eligible := make([]Replica, 0, len(candidates))
			for _, c := range candidates {
				if replicaInWriteSet(c, allowIPs) {
					eligible = append(eligible, c)
				}
			}
			if len(eligible) == 0 {
				return nil, fmt.Errorf(
					"shardtopo: cluster %q shard_num %d has no replica in the CLICKHOUSE_SERVER write set; "+
						"refusing to route shard %d writes to a non-write node", clusterName, shard, shard)
			}
			candidates = eligible
		}
		sort.Slice(candidates, func(i, j int) bool {
			ci, cj := candidates[i], candidates[j]
			if ci.ReplicaNum != cj.ReplicaNum {
				return ci.ReplicaNum < cj.ReplicaNum
			}
			return ci.HostAddr < cj.HostAddr
		})
		shards[shard-1] = Shard{ShardNum: shard, Replicas: candidates}
	}
	return &Snapshot{ClusterName: clusterName, ShardCount: maxShard, Shards: shards}, nil
}

func replicaInWriteSet(r Replica, allowIPs map[string]bool) bool {
	return allowIPs[normalizeIP(r.HostAddr)] || allowIPs[normalizeIP(r.HostName)]
}

func normalizeIP(s string) string {
	s = strings.TrimSpace(s)
	if ip := net.ParseIP(s); ip != nil {
		return ip.String()
	}
	return s
}

func ResolveIPSet(host string) (map[string]bool, error) {
	ips, err := net.LookupHost(host)
	if err != nil {
		return nil, fmt.Errorf("shardtopo: resolving write host %q: %w", host, err)
	}
	set := make(map[string]bool, len(ips))
	for _, ip := range ips {
		set[normalizeIP(ip)] = true
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("shardtopo: write host %q resolved to no addresses", host)
	}
	return set, nil
}

type Topology struct {
	discover func(context.Context) (*Snapshot, error)
	interval time.Duration
	log      Logger

	onGrow func(prev, next *Snapshot) error

	snap atomic.Pointer[Snapshot]

	stop   chan struct{}
	doneMu sync.Mutex
	done   chan struct{}
}

func New(ctx context.Context, q Querier, clusterName, seedHost string, interval time.Duration, log Logger, onGrow func(prev, next *Snapshot) error) (*Topology, error) {
	t := &Topology{
		interval: interval,
		log:      log,
		onGrow:   onGrow,
		discover: func(c context.Context) (*Snapshot, error) {
			allowIPs, err := ResolveIPSet(seedHost)
			if err != nil {
				return nil, err
			}
			return Discover(c, q, clusterName, allowIPs)
		},
	}
	snap, err := t.discover(ctx)
	if err != nil {
		return nil, err
	}
	t.snap.Store(snap)
	return t, nil
}

func (t *Topology) Snapshot() *Snapshot { return t.snap.Load() }

func (t *Topology) ShardCount() int {
	s := t.snap.Load()
	if s == nil {
		return 0
	}
	return s.ShardCount
}

func (t *Topology) applyRefresh(next *Snapshot) (live *Snapshot, swapped bool) {
	cur := t.snap.Load()
	if cur == nil || next.ShardCount == cur.ShardCount {
		t.snap.Store(next)
		return next, true
	}

	if next.ShardCount < cur.ShardCount {
		if t.log != nil {
			t.log.Error(fmt.Sprintf(
				"shardtopo: shard count DECREASE held (%d -> %d) for cluster %q; "+
					"operator-coordinated cutover required, keeping current layout",
				cur.ShardCount, next.ShardCount, next.ClusterName))
		}
		return cur, false
	}

	if t.onGrow != nil {
		if err := t.onGrow(cur, next); err != nil {
			if t.log != nil {
				t.log.Error(fmt.Sprintf(
					"shardtopo: shard count increase (%d -> %d) for cluster %q NOT applied: "+
						"growing the write fan-out failed: %v; keeping current layout",
					cur.ShardCount, next.ShardCount, next.ClusterName, err))
			}
			return cur, false
		}
	}
	t.snap.Store(next)
	if t.log != nil {
		t.log.Info(fmt.Sprintf(
			"shardtopo: shard count increase applied (%d -> %d) for cluster %q; "+
				"routing new writes across %d shards",
			cur.ShardCount, next.ShardCount, next.ClusterName, next.ShardCount))
	}
	return next, true
}

// Start launches the bounded refresh loop. interval <= 0 disables refresh.
func (t *Topology) Start() {
	if t.interval <= 0 {
		return
	}
	t.doneMu.Lock()
	if t.done != nil {
		t.doneMu.Unlock()
		return
	}
	t.stop = make(chan struct{})
	t.done = make(chan struct{})
	t.doneMu.Unlock()

	go func() {
		defer close(t.done)
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			select {
			case <-t.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				next, err := t.discover(ctx)
				cancel()
				if err != nil {
					if t.log != nil {
						t.log.Error("shardtopo: refresh failed, keeping current layout:", err)
					}
					continue
				}
				if _, swapped := t.applyRefresh(next); swapped && t.log != nil {
					t.log.Info(fmt.Sprintf("shardtopo: layout refreshed (%d shards)", next.ShardCount))
				}
			}
		}
	}()
}

func (t *Topology) Stop() {
	t.doneMu.Lock()
	stop, done := t.stop, t.done
	t.stop, t.done = nil, nil
	t.doneMu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}
