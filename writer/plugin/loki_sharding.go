package plugin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	chconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/shared/shardtopo"
	"github.com/metrico/qryn/v5/writer/chwrapper"
	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/service/insert"
	"github.com/metrico/qryn/v5/writer/utils/logger"
)

var lokiShardTopo *shardtopo.Topology

type shardLogger struct{}

func (shardLogger) Info(a ...interface{})  { logger.Info(a...) }
func (shardLogger) Error(a ...interface{}) { logger.Error(a...) }

func lokiLocalShardWritesEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CLICKHOUSE_LOCAL_SHARD_WRITES"))) {
	case "1", "true", "on", "yes":
		return true
	}
	return false
}

func shardRefreshInterval() time.Duration {
	v := strings.TrimSpace(os.Getenv("CLICKHOUSE_SHARD_REFRESH_INTERVAL"))
	if v == "" {
		return time.Minute
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		logger.Error("Invalid CLICKHOUSE_SHARD_REFRESH_INTERVAL, defaulting to 1m:", v)
		return time.Minute
	}
	return d
}

func (p *QrynWriterPlugin) buildLokiShardServices(cfg chconfig.ClokiBaseSettingServer) (service.InsertSvcMap, service.InsertSvcMap) {
	tsMap := make(service.InsertSvcMap)
	splMap := make(service.InsertSvcMap)
	if !lokiLocalShardWritesEnabled() {
		return tsMap, splMap
	}
	if len(cfg.DATABASE_DATA) == 0 {
		logger.Error("CLICKHOUSE_LOCAL_SHARD_WRITES enabled but DATABASE_DATA is empty")
		os.Exit(1)
	}
	base := cfg.DATABASE_DATA[0]
	if base.ClusterName == "" {
		logger.Error("CLICKHOUSE_LOCAL_SHARD_WRITES requires a cluster (set CLUSTER_NAME); refusing to start")
		os.Exit(1)
	}
	if len(p.ServicesObject.Dbv2Map) == 0 {
		logger.Error("CLICKHOUSE_LOCAL_SHARD_WRITES: no seed ClickHouse connection available")
		os.Exit(1)
	}

	interval := time.Millisecond * time.Duration(cfg.SYSTEM_SETTINGS.DBTimer*1000)

	buildReplica := func(shardIdx, repIdx int, rep shardtopo.Replica) (service.IInsertServiceV2, service.IInsertServiceV2, string) {
		node := base
		node.Host = rep.HostAddr
		node.Node = fmt.Sprintf("qryn_shard_%d_r%d", shardIdx+1, repIdx+1)
		node.ClusterName = ""
		dnm := model.DataDatabasesMap{ClokiBaseDataBase: node}

		nodeCopy := node
		session := chwrapper.IChClientFactory(func() (chwrapper.IChClient, error) {
			return chwrapper.NewSmartDatabaseAdapter(&nodeCopy, true)
		})

		tsSvc := insert.NewTimeSeriesInsertService(model.InsertServiceOpts{
			Session:     session,
			Node:        &dnm,
			Interval:    interval,
			ParallelNum: cfg.SYSTEM_SETTINGS.ChannelsTimeSeries,
			AsyncInsert: base.AsyncInsert,
		})
		splSvc := insert.NewSamplesInsertService(model.InsertServiceOpts{
			Session:        session,
			Node:           &dnm,
			Interval:       interval,
			ParallelNum:    cfg.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert:    base.AsyncInsert,
			MaxQueueSize:   int64(cfg.SYSTEM_SETTINGS.DBBulk),
			OnBeforeInsert: func() { tsSvc.PlanFlush() },
		})

		tsSvc.Init()
		go tsSvc.Run()
		splSvc.Init()
		go splSvc.Run()
		return tsSvc, splSvc, node.Node
	}

	buildShard := func(shardIdx int, sh shardtopo.Shard) (tsReps, splReps []service.IInsertServiceV2, names []string) {
		for r, rep := range sh.Replicas {
			ts, spl, name := buildReplica(shardIdx, r, rep)
			tsReps = append(tsReps, ts)
			splReps = append(splReps, spl)
			names = append(names, name)
		}
		return
	}

	onGrow := func(prev, next *shardtopo.Snapshot) error {
		cur := controllerv1.CurrentLokiShardServices()
		var curTS, curSpl [][]service.IInsertServiceV2
		if cur != nil {
			curTS, curSpl = cur.TS, cur.Spl
		}
		newTS := append([][]service.IInsertServiceV2(nil), curTS...)
		newSpl := append([][]service.IInsertServiceV2(nil), curSpl...)
		for i := len(newTS); i < next.ShardCount; i++ {
			tsReps, splReps, _ := buildShard(i, next.Shards[i])
			newTS = append(newTS, tsReps)
			newSpl = append(newSpl, splReps)
		}
		controllerv1.SetLokiShardServices(controllerv1.NewLokiShardServices(newTS, newSpl))
		logger.Info(fmt.Sprintf("Loki local-shard writes: write fan-out grown %d -> %d shards", prev.ShardCount, next.ShardCount))
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topo, err := shardtopo.New(ctx, p.ServicesObject.Dbv2Map[0], base.ClusterName, base.Host,
		shardRefreshInterval(), shardLogger{}, onGrow)
	if err != nil {
		logger.Error("CLICKHOUSE_LOCAL_SHARD_WRITES: topology discovery failed:", err)
		os.Exit(1)
	}
	snap := topo.Snapshot()

	tsSvcs := make([][]service.IInsertServiceV2, snap.ShardCount)
	splSvcs := make([][]service.IInsertServiceV2, snap.ShardCount)
	totalNodes := 0
	for i := 0; i < snap.ShardCount; i++ {
		tsReps, splReps, names := buildShard(i, snap.Shards[i])
		tsSvcs[i] = tsReps
		splSvcs[i] = splReps
		for r, name := range names {
			tsMap[name] = tsReps[r]
			splMap[name] = splReps[r]
		}
		totalNodes += len(tsReps)
	}

	controllerv1.SetLokiShardServices(controllerv1.NewLokiShardServices(tsSvcs, splSvcs))
	lokiShardTopo = topo
	topo.Start()
	logger.Info(fmt.Sprintf("Loki local-shard writes enabled: routing to %d shards by fingerprint %% N, load-balancing writes across %d replica nodes", snap.ShardCount, totalNodes))
	return tsMap, splMap
}

// StopLokiShardWrites halts the topology refresh loop. Safe to call when sharding
// was never enabled.
func StopLokiShardWrites() {
	if lokiShardTopo != nil {
		lokiShardTopo.Stop()
		lokiShardTopo = nil
	}
}
