package store

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/groupcache"
	"github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common/stats"
)

// Called periodically in a goroutine. Must include the current instance among the fetched nodes.
type PeerFetcher interface {
	Fetch() ([]cluster.Node, error)
}

// TODO: we should consider extending contexts in groupcache lib further to:
// 1) control hot cache population rate deterministically (currently hardcoded at 10%)
// 2) add more advanced caching behaviors like propagation to peer caches,
//	  or populate cache and skip underlying store write, etc.

// Note: Endpoint is concatenated with Name in groupcache internals, and AddrSelf is expected as HOST:PORT.
type GroupcacheConfig struct {
	Name         string
	Memory_bytes int64
	AddrSelf     string
	Endpoint     string
	NodeReqCh    cluster.NodeReqChType
}

// Add in-memory caching to the given store.
func MakeGroupcacheStore(underlying Store, cfg *GroupcacheConfig, ttlc *TTLConfig, stat stats.StatsReceiver) (Store, http.Handler, error) {
	if ttlc == nil {
		return nil, nil, fmt.Errorf("MakeGroupcacheStore requires a non-nil TTLConfig")
	}
	stat = stat.Scope("bundlestoreCache")
	go stats.StartUptimeReporting(stat, stats.BundlestoreUptime_ms, "", stats.DefaultStartupGaugeSpikeLen)

	// Create the cache which knows how to retrieve the underlying bundle data.
	var cache = groupcache.NewGroup(
		cfg.Name,
		cfg.Memory_bytes,
		groupcache.GetterFunc(func(ctx groupcache.Context, bundleName string, dest groupcache.Sink) (*time.Time, error) {
			log.Info("Not cached, try to fetch bundle and populate cache: ", bundleName)
			stat.Counter(stats.GroupcacheReadUnderlyingCounter).Inc(1)
			defer stat.Latency(stats.GroupcacheReadUnderlyingLatency_ms).Time().Stop()

			resource, err := underlying.OpenForRead(bundleName)
			if err != nil {
				return nil, err
			}
			defer resource.Close()
			data, err := ioutil.ReadAll(resource)
			if err != nil {
				return nil, err
			}
			var ttl *time.Time
			if resource.TTLValue != nil {
				ttl = &resource.TTLValue.TTL
			}
			return ttl, dest.SetBytes(data)
		}),
		groupcache.ContainerFunc(func(ctx groupcache.Context, bundleName string) (bool, error) {
			log.Info("Not cached, try to check bundle existence: ", bundleName)
			stat.Counter(stats.GroupcacheExistUnderlyingCounter).Inc(1)
			defer stat.Latency(stats.GroupcacheExistUnderlyingLatency_ms).Time().Stop()

			ok, err := underlying.Exists(bundleName)
			if err != nil {
				return false, err
			}
			return ok, nil
		}),
		groupcache.PutterFunc(func(ctx groupcache.Context, bundleName string, data []byte, ttl *time.Time) error {
			log.Info("New bundle, write and populate cache: ", bundleName)
			stat.Counter(stats.GroupcacheWriteUnderlyingCounter).Inc(1)
			defer stat.Latency(stats.GroupcacheWriteUnderlyingLatency_ms).Time().Stop()

			ttlv := &TTLValue{TTL: *ttl, TTLKey: ttlc.TTLKey}
			buf := ioutil.NopCloser(bytes.NewReader(data))
			r := NewResource(buf, int64(len(data)), ttlv)
			err := underlying.Write(bundleName, r)
			if err != nil {
				return err
			}
			return nil
		}),
	)

	// Create and initialize peer group.
	// The HTTPPool constructor will register as a global PeerPicker on our behalf.
	poolOpts := &groupcache.HTTPPoolOptions{BasePath: cfg.Endpoint}
	pool := groupcache.NewHTTPPoolOpts("http://"+cfg.AddrSelf, poolOpts)
	go loop(cfg.NodeReqCh, pool, cache, stat)

	return &groupcacheStore{underlying: underlying, cache: cache, stat: stat, ttlConfig: ttlc}, pool, nil
}

// Convert 'host:port' node ids to the format expected by groupcache peering, http URLs.
func toPeers(nodes []cluster.Node, stat stats.StatsReceiver) []string {
	peers := []string{}
	for _, node := range nodes {
		peers = append(peers, "http://"+string(node.Id()))
	}
	log.Info("New groupcacheStore peers: ", peers)
	stat.Counter(stats.GroupcachePeerDiscoveryCounter).Inc(1)
	stat.Gauge(stats.GroupcachePeerCountGauge).Update(int64(len(peers)))
	return peers
}

// Loop will listen for cluster updates and create a list of peer addresses to update groupcache.
// Cluster is expected to include the current node.
// Also updates cache stats, every 1s for now to account for arbitrary stat latch time.
func loop(nodesReqCh cluster.NodeReqChType, pool *groupcache.HTTPPool, cache *groupcache.Group, stat stats.StatsReceiver) {
	nodesCh := make(chan []cluster.Node)
	statsTicker := time.NewTicker(1 * time.Second)
	for {
		nodesReqCh <- nodesCh // send a request for the nodes
		pool.Set(toPeers(<-nodesCh, stat)...)

		// record stats every 1s
		select {
		case <-statsTicker.C:
			updateCacheStats(cache, stat)
		default:
		}
	}
}

// Implements snapshot/store.Store interface
type groupcacheStore struct {
	underlying Store
	cache      *groupcache.Group
	stat       stats.StatsReceiver
	ttlConfig  *TTLConfig
}

func (s *groupcacheStore) OpenForRead(name string) (*Resource, error) {
	log.Info("Read() checking for cached bundle: ", name)
	defer s.stat.Latency(stats.GroupcacheReadLatency_ms).Time().Stop()
	s.stat.Counter(stats.GroupcacheReadCounter).Inc(1)
	var data []byte
	ttl, err := s.cache.Get(nil, name, groupcache.AllocatingByteSliceSink(&data))
	if err != nil {
		return nil, err
	}
	var ttlv *TTLValue = nil
	if ttl != nil {
		ttlv = &TTLValue{TTL: *ttl, TTLKey: s.ttlConfig.TTLKey}
	}
	rc := ioutil.NopCloser(bytes.NewReader(data))
	s.stat.Counter(stats.GroupcacheReadOkCounter).Inc(1)
	return NewResource(rc, int64(len(data)), ttlv), nil
}

func (s *groupcacheStore) Exists(name string) (bool, error) {
	log.Info("Exists() checking for cached bundle: ", name)
	defer s.stat.Latency(stats.GroupcachExistsLatency_ms).Time().Stop()
	s.stat.Counter(stats.GroupcacheExistsCounter).Inc(1)
	ok, err := s.cache.Contain(nil, name)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	s.stat.Counter(stats.GroupcacheExistsOkCounter).Inc(1)
	return true, nil
}

func (s *groupcacheStore) Write(name string, resource *Resource) error {
	defer s.stat.Latency(stats.GroupcacheWriteLatency_ms).Time().Stop()
	s.stat.Counter(stats.GroupcacheWriteCounter).Inc(1)
	if resource == nil {
		log.Info("Writing nil resource is a no op.")
		return nil
	}
	// Read data into a []byte and make a right-sized copy, as ReadAll will reserve at 2x capacity
	b, err := ioutil.ReadAll(resource)
	if err != nil {
		return err
	}
	c := make([]byte, len(b))
	copy(c, b)

	var ttl *time.Time
	if resource.TTLValue != nil {
		ttl = &resource.TTLValue.TTL
	}
	log.Infof("Write() bundle %s: length: %d ttl: %s", name, len(b), ttl)
	if err := s.cache.Put(nil, name, c, ttl); err != nil {
		return err
	}
	s.stat.Counter(stats.GroupcacheWriteOkCounter).Inc(1)
	return nil
}

func (s *groupcacheStore) Root() string {
	return s.underlying.Root()
}

// The groupcache lib updates its stats in the background - we need to convert those to our own stat representation.
// Gauges are expected to fluctuate, counters are expected to only ever increase.
func updateCacheStats(cache *groupcache.Group, stat stats.StatsReceiver) {
	stat.Gauge(stats.GroupcacheMainBytesGauge).Update(cache.CacheStats(groupcache.MainCache).Bytes)
	stat.Gauge(stats.GroupcacheMainItemsGauge).Update(cache.CacheStats(groupcache.MainCache).Items)
	stat.Counter(stats.GroupcacheMainGetsCounter).Update(cache.CacheStats(groupcache.MainCache).Gets)
	stat.Counter(stats.GroupcacheMainHitsCounter).Update(cache.CacheStats(groupcache.MainCache).Hits)
	stat.Counter(stats.GroupcacheMainEvictionsCounter).Update(cache.CacheStats(groupcache.MainCache).Evictions)

	stat.Gauge(stats.GroupcacheHotBytesGauge).Update(cache.CacheStats(groupcache.HotCache).Bytes)
	stat.Gauge(stats.GroupcacheHotItemsGauge).Update(cache.CacheStats(groupcache.HotCache).Items)
	stat.Counter(stats.GroupcacheHotGetsCounter).Update(cache.CacheStats(groupcache.HotCache).Gets)
	stat.Counter(stats.GroupcacheHotHitsCounter).Update(cache.CacheStats(groupcache.HotCache).Hits)
	stat.Counter(stats.GroupcacheHotEvictionsCounter).Update(cache.CacheStats(groupcache.HotCache).Evictions)

	stat.Counter(stats.GroupcacheGetCounter).Update(cache.Stats.Gets.Get())
	stat.Counter(stats.GroupcacheContainCounter).Update(cache.Stats.Contains.Get())
	stat.Counter(stats.GroupcachePutCounter).Update(cache.Stats.Puts.Get())
	stat.Counter(stats.GroupcacheHitCounter).Update(cache.Stats.CacheHits.Get())
	stat.Counter(stats.GroupcacheLoadCounter).Update(cache.Stats.Loads.Get())
	stat.Counter(stats.GroupcacheCheckCounter).Update(cache.Stats.Checks.Get())
	stat.Counter(stats.GroupcacheStoreCounter).Update(cache.Stats.Stores.Get())
	stat.Counter(stats.GroupcachePeerGetsCounter).Update(cache.Stats.PeerLoads.Get())
	stat.Counter(stats.GroupcachePeerChecksCounter).Update(cache.Stats.PeerChecks.Get())
	stat.Counter(stats.GroupcachePeerPutsCounter).Update(cache.Stats.PeerStores.Get())
	stat.Counter(stats.GroupcachPeerErrCounter).Update(cache.Stats.PeerErrors.Get())
	stat.Counter(stats.GroupcacheLocalLoadCounter).Update(cache.Stats.LocalLoads.Get())
	stat.Counter(stats.GroupcacheLocalLoadErrCounter).Update(cache.Stats.LocalLoadErrs.Get())
	stat.Counter(stats.GroupcacheLocalCheckCounter).Update(cache.Stats.LocalChecks.Get())
	stat.Counter(stats.GroupcacheLocalCheckErrCounter).Update(cache.Stats.LocalCheckErrs.Get())
	stat.Counter(stats.GroupcacheLocalStoreCounter).Update(cache.Stats.LocalStores.Get())
	stat.Counter(stats.GroupcacheLocalStoreErrCounter).Update(cache.Stats.LocalStoreErrs.Get())
	stat.Counter(stats.GroupcacheIncomingRequestsCounter).Update(cache.Stats.ServerRequests.Get())
}
