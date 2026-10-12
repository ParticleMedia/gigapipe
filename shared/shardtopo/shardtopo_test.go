package shardtopo

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func rep(shard, replica int, addr string) Replica {
	return Replica{ShardNum: shard, ReplicaNum: replica, HostName: addr, HostAddr: addr, Port: 9000}
}

func TestBuildSnapshotHappyPath(t *testing.T) {
	replicas := []Replica{
		rep(1, 1, "10.0.0.1"), rep(1, 2, "10.0.0.2"),
		rep(2, 1, "10.0.0.3"), rep(2, 2, "10.0.0.4"),
	}
	snap, err := BuildSnapshot("c", replicas, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.ShardCount != 2 || len(snap.Shards) != 2 {
		t.Fatalf("ShardCount=%d len=%d want 2/2", snap.ShardCount, len(snap.Shards))
	}
	// With no write filter, ALL replicas of each shard are kept, ordered by
	// replica_num so round-robin is deterministic.
	if len(snap.Shards[0].Replicas) != 2 || len(snap.Shards[1].Replicas) != 2 {
		t.Fatalf("want all replicas kept per shard: %+v", snap.Shards)
	}
	if snap.Shards[0].Replicas[0].HostAddr != "10.0.0.1" || snap.Shards[0].Replicas[1].HostAddr != "10.0.0.2" {
		t.Errorf("shard 1 replica order wrong: %+v", snap.Shards[0].Replicas)
	}
	if snap.Shards[1].Replicas[0].HostAddr != "10.0.0.3" {
		t.Errorf("shard 2 first replica wrong: %+v", snap.Shards[1].Replicas)
	}
}

func TestBuildSnapshotKeepsAllEligibleReplicas(t *testing.T) {
	// Shard 1 has three replicas but only two are in the write set; both eligible
	// ones must be retained (for load-balancing) and the non-eligible one dropped.
	replicas := []Replica{
		rep(1, 1, "10.0.0.1"), rep(1, 2, "10.0.0.2"), rep(1, 3, "10.0.0.3"),
		rep(2, 1, "10.0.0.4"),
	}
	allow := map[string]bool{"10.0.0.1": true, "10.0.0.3": true, "10.0.0.4": true}
	snap, err := BuildSnapshot("c", replicas, allow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Shards[0].Replicas) != 2 {
		t.Fatalf("shard 1 should keep 2 eligible replicas, got %+v", snap.Shards[0].Replicas)
	}
	if snap.Shards[0].Replicas[0].HostAddr != "10.0.0.1" || snap.Shards[0].Replicas[1].HostAddr != "10.0.0.3" {
		t.Errorf("shard 1 eligible replicas wrong/unordered: %+v", snap.Shards[0].Replicas)
	}
	for _, rep := range snap.Shards[0].Replicas {
		if rep.HostAddr == "10.0.0.2" {
			t.Errorf("kept a non-write-set replica: %s", rep.HostAddr)
		}
	}
	if len(snap.Shards[1].Replicas) != 1 {
		t.Errorf("shard 2 should keep its single eligible replica, got %+v", snap.Shards[1].Replicas)
	}
}

func TestBuildSnapshotFiltersToWriteSet(t *testing.T) {
	// Each shard has two replicas; only one per shard is a designated write node
	// (in the allow-list). Selection must pick the write node even when it has a
	// higher replica_num, and must never pick the non-write replica.
	replicas := []Replica{
		rep(1, 1, "10.0.0.1"), rep(1, 2, "10.0.0.2"), // shard 1: write node is .2
		rep(2, 1, "10.0.0.3"), rep(2, 2, "10.0.0.4"), // shard 2: write node is .3
	}
	allow := map[string]bool{"10.0.0.2": true, "10.0.0.3": true}
	snap, err := BuildSnapshot("c", replicas, allow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.Shards[0].Replicas[0].HostAddr != "10.0.0.2" {
		t.Errorf("shard 1 picked non-write node: got %s want 10.0.0.2", snap.Shards[0].Replicas[0].HostAddr)
	}
	if snap.Shards[1].Replicas[0].HostAddr != "10.0.0.3" {
		t.Errorf("shard 2 picked non-write node: got %s want 10.0.0.3", snap.Shards[1].Replicas[0].HostAddr)
	}
	// Every retained replica must be in the write set.
	for i, s := range snap.Shards {
		for _, rep := range s.Replicas {
			if !allow[rep.HostAddr] {
				t.Errorf("shard %d retained %s which is NOT in the write set", i+1, rep.HostAddr)
			}
		}
	}
}

func TestBuildSnapshotRejectsShardWithNoWriteNode(t *testing.T) {
	// Shard 2's replicas are all absent from the write set -> must error, never
	// route shard 2 writes to a non-write node.
	replicas := []Replica{
		rep(1, 1, "10.0.0.1"),
		rep(2, 1, "10.0.0.9"), // not in allow set
	}
	allow := map[string]bool{"10.0.0.1": true}
	if _, err := BuildSnapshot("c", replicas, allow); err == nil {
		t.Fatalf("expected error: shard 2 has no write-eligible replica")
	}
}

func TestBuildSnapshotNormalizesIPForm(t *testing.T) {
	// host_address may be stored in a non-canonical IPv6 form; matching must be
	// form-independent.
	replicas := []Replica{{ShardNum: 1, ReplicaNum: 1, HostAddr: "::ffff:10.0.0.5", HostName: "n1", Port: 9000}}
	allow := map[string]bool{normalizeIP("::ffff:10.0.0.5"): true}
	if _, err := BuildSnapshot("c", replicas, allow); err != nil {
		t.Errorf("normalized IPv6 write node rejected: %v", err)
	}
}

func TestBuildSnapshotDeterministic(t *testing.T) {
	// Same input (different order) must yield the same selection.
	a := []Replica{rep(1, 2, "b"), rep(1, 1, "a"), rep(2, 1, "c")}
	b := []Replica{rep(1, 1, "a"), rep(2, 1, "c"), rep(1, 2, "b")}
	sa, _ := BuildSnapshot("c", a, nil)
	sb, _ := BuildSnapshot("c", b, nil)
	if sa.Shards[0].Replicas[0].HostAddr != sb.Shards[0].Replicas[0].HostAddr {
		t.Errorf("non-deterministic selection: %s vs %s", sa.Shards[0].Replicas[0].HostAddr, sb.Shards[0].Replicas[0].HostAddr)
	}
}

func TestBuildSnapshotNonContiguous(t *testing.T) {
	// shard 1 and 3 present, 2 missing -> error (would silently drop rows).
	replicas := []Replica{rep(1, 1, "a"), rep(3, 1, "c")}
	if _, err := BuildSnapshot("c", replicas, nil); err == nil {
		t.Fatalf("expected error for non-contiguous shards")
	}
}

func TestBuildSnapshotEmpty(t *testing.T) {
	if _, err := BuildSnapshot("c", nil, nil); err == nil {
		t.Fatalf("expected error for empty cluster")
	}
}

func TestApplyRefreshAppliesIncreaseHoldsDecrease(t *testing.T) {
	cur := &Snapshot{ClusterName: "c", ShardCount: 3, Shards: make([]Shard, 3)}
	var grew int32
	top := &Topology{onGrow: func(prev, next *Snapshot) error {
		atomic.AddInt32(&grew, 1)
		if prev.ShardCount != 3 || next.ShardCount != 4 {
			t.Errorf("onGrow got %d -> %d, want 3 -> 4", prev.ShardCount, next.ShardCount)
		}
		return nil
	}}
	top.snap.Store(cur)

	// Replica-only change (same N) -> swap, no grow hook.
	sameN := &Snapshot{ClusterName: "c", ShardCount: 3, Shards: make([]Shard, 3)}
	if live, swapped := top.applyRefresh(sameN); !swapped || live != sameN {
		t.Errorf("replica-only refresh should swap; swapped=%v", swapped)
	}
	if atomic.LoadInt32(&grew) != 0 {
		t.Errorf("onGrow must not fire on a same-count refresh; calls=%d", grew)
	}

	// Shard-count INCREASE -> applied immediately after onGrow succeeds.
	grow := &Snapshot{ClusterName: "c", ShardCount: 4, Shards: make([]Shard, 4)}
	live, swapped := top.applyRefresh(grow)
	if !swapped || live.ShardCount != 4 || top.Snapshot().ShardCount != 4 {
		t.Errorf("increase should apply immediately; swapped=%v live=%d snap=%d",
			swapped, live.ShardCount, top.Snapshot().ShardCount)
	}
	if atomic.LoadInt32(&grew) != 1 {
		t.Errorf("onGrow should fire exactly once on increase; calls=%d", grew)
	}

	// Shard-count DECREASE -> held, keep current (the now-live 4).
	shrink := &Snapshot{ClusterName: "c", ShardCount: 2, Shards: make([]Shard, 2)}
	live, swapped = top.applyRefresh(shrink)
	if swapped || live.ShardCount != 4 || top.Snapshot().ShardCount != 4 {
		t.Errorf("decrease must be held; swapped=%v live=%d snap=%d",
			swapped, live.ShardCount, top.Snapshot().ShardCount)
	}
}

func TestApplyRefreshHoldsIncreaseWhenGrowFails(t *testing.T) {
	// If the write path cannot be made ready for the new shards, the larger layout
	// must NOT be published — otherwise the snapshot would advertise a shard with
	// no write service behind it.
	cur := &Snapshot{ClusterName: "c", ShardCount: 3, Shards: make([]Shard, 3)}
	top := &Topology{onGrow: func(prev, next *Snapshot) error {
		return fmt.Errorf("boom")
	}}
	top.snap.Store(cur)

	grow := &Snapshot{ClusterName: "c", ShardCount: 4, Shards: make([]Shard, 4)}
	live, swapped := top.applyRefresh(grow)
	if swapped || live.ShardCount != 3 || top.Snapshot().ShardCount != 3 {
		t.Errorf("increase must be held when onGrow fails; swapped=%v live=%d snap=%d",
			swapped, live.ShardCount, top.Snapshot().ShardCount)
	}
}

func TestTopologyNewUsesInjectedDiscover(t *testing.T) {
	// Exercise the atomic snapshot accessors without a real ClickHouse.
	var calls int32
	snap := &Snapshot{ClusterName: "c", ShardCount: 3, Shards: make([]Shard, 3)}
	top := &Topology{discover: func(_ context.Context) (*Snapshot, error) {
		atomic.AddInt32(&calls, 1)
		return snap, nil
	}}
	next, _ := top.discover(nil)
	top.snap.Store(next)
	if top.ShardCount() != 3 {
		t.Errorf("ShardCount=%d want 3", top.ShardCount())
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("discover calls=%d want 1", calls)
	}
}
