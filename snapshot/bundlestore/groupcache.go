package bundlestore

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/groupcache"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
)

//TODO: we should consider modifying google groupcache lib further to:
// 1) It makes more sense given our use-case to cache bundles loaded via peer 100% of the time (currently 10%).
// 2) Modify peer proto to support setting bundle data on the peer that owns the bundlename. (via PopulateCache()).
// 3) For populate cache requests from user, fill the right cache, main or hot.
//
//TODO: Add a doneCh/Done() to stop the created goroutine.

// Called periodically in a goroutine. Must include the current instance among the fetched nodes.
type PeerFetcher interface {
	Fetch() ([]cluster.Node, error)
}

// Note: Endpoint is concatenated with Name in groupcache internals, and AddrSelf is expected as HOST:PORT.
type GroupcacheConfig struct {
	Name         string
	Memory_bytes int64
	AddrSelf     string
	Endpoint     string
	Cluster      *cluster.Cluster
}

// Add in-memory caching to the given store.
func MakeGroupcacheStore(underlying Store, cfg *GroupcacheConfig, stat stats.StatsReceiver) (Store, http.Handler, error) {
	stat = stat.Scope("bundlestoreCache")

	// Create the cache which knows how to retrieve the underlying bundle data.
	var cache = groupcache.NewGroup(cfg.Name, cfg.Memory_bytes, groupcache.GetterFunc(
		func(ctx groupcache.Context, bundleName string, dest groupcache.Sink) error {
			log.Info("Not cached, try to fetch bundle and populate cache: ", bundleName)
			stat.Counter("readUnderlyingCounter").Inc(1) // TODO errata metric - remove if unused
			reader, err := underlying.OpenForRead(bundleName)
			if err != nil {
				return err
			}
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				return err
			}
			dest.SetBytes(data)
			return nil
		},
	))

	// Create and initialize peer group.
	// The HTTPPool constructor will register as a global PeerPicker on our behalf.
	poolOpts := &groupcache.HTTPPoolOptions{BasePath: cfg.Endpoint}
	pool := groupcache.NewHTTPPoolOpts("http://"+cfg.AddrSelf, poolOpts)
	go loop(cfg.Cluster, pool, cache, stat)

	return &groupcacheStore{underlying: underlying, cache: cache, stat: stat}, pool, nil
}

// Convert 'host:port' node ids to the format expected by groupcache peering, http URLs.
func toPeers(nodes []cluster.Node, stat stats.StatsReceiver) []string {
	peers := []string{}
	for _, node := range nodes {
		peers = append(peers, "http://"+string(node.Id()))
	}
	log.Info("New groupcacheStore peers: ", peers)
	stat.Counter("peerDiscoveryCounter").Inc(1)
	stat.Gauge("peerCountGauge").Update(int64(len(peers)))
	return peers
}

// Loop will listen for cluster updates and create a list of peer addresses to update groupcache.
// Cluster is expected to include the current node.
// Also updates cache stats, every 1s for now to account for arbitrary stat latch time.
func loop(c *cluster.Cluster, pool *groupcache.HTTPPool, cache *groupcache.Group, stat stats.StatsReceiver) {
	sub := c.Subscribe()
	pool.Set(toPeers(c.Members(), stat)...)
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-sub.Updates:
			pool.Set(toPeers(c.Members(), stat)...)
		case <-ticker.C:
			updateCacheStats(cache, stat)
		}
	}
}

// The groupcache lib updates its stats in the background - we need to convert those to our own stat representation.
// Gauges are expected to fluctuate, counters are expected to only ever increase.
func updateCacheStats(cache *groupcache.Group, stat stats.StatsReceiver) {
	stat.Gauge("mainBytesGauge").Update(cache.CacheStats(groupcache.MainCache).Bytes)   // TODO errata metric - remove if unused
	stat.Gauge("mainItemsGauge").Update(cache.CacheStats(groupcache.MainCache).Items)   // TODO errata metric - remove if unused
	stat.Counter("mainGetsCounter").Update(cache.CacheStats(groupcache.MainCache).Gets) // TODO errata metric - remove if unused
	stat.Counter("mainHitsCounter").Update(cache.CacheStats(groupcache.MainCache).Hits) // TODO errata metric - remove if unused

	stat.Gauge("hotBytesGauge").Update(cache.CacheStats(groupcache.HotCache).Bytes)   // TODO errata metric - remove if unused
	stat.Gauge("hotItemsGauge").Update(cache.CacheStats(groupcache.HotCache).Items)   // TODO errata metric - remove if unused
	stat.Counter("hotGetsCounter").Update(cache.CacheStats(groupcache.HotCache).Gets) // TODO errata metric - remove if unused
	stat.Counter("hotHitsCounter").Update(cache.CacheStats(groupcache.HotCache).Hits) // TODO errata metric - remove if unused

	stat.Counter("cacheGetCounter").Update(cache.Stats.Gets.Get())                        // TODO errata metric - remove if unused
	stat.Counter("cacheHitCounter").Update(cache.Stats.CacheHits.Get())                   // TODO errata metric - remove if unused
	stat.Counter("cachePeerGetsCounter").Update(cache.Stats.PeerLoads.Get())              // TODO errata metric - remove if unused
	stat.Counter("cachePeerErrCounter").Update(cache.Stats.PeerErrors.Get())              // TODO errata metric - remove if unused
	stat.Counter("cacheLocalLoadCounter").Update(cache.Stats.LocalLoads.Get())            // TODO errata metric - remove if unused
	stat.Counter("cacheLocalLoadErrCounter").Update(cache.Stats.LocalLoadErrs.Get())      // TODO errata metric - remove if unused
	stat.Counter("cacheIncomingRequestsCounter").Update(cache.Stats.ServerRequests.Get()) // TODO errata metric - remove if unused
}

type groupcacheStore struct {
	underlying Store
	cache      *groupcache.Group
	stat       stats.StatsReceiver
}

func (s *groupcacheStore) OpenForRead(name string) (io.ReadCloser, error) {
	log.Info("Read() checking for cached bundle: ", name)
	defer s.stat.Latency("readLatency_ms").Time().Stop()
	s.stat.Counter("readCounter").Inc(1)
	var data []byte
	if err := s.cache.Get(nil, name, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return nil, err
	}
	s.stat.Counter("readOkCounter").Inc(1) // TODO errata metric - remove if unused
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (s *groupcacheStore) Exists(name string) (bool, error) {
	log.Info("Exists() checking for cached bundle: ", name)
	defer s.stat.Latency("existsLatency_ms").Time().Stop()
	s.stat.Counter("existsCounter").Inc(1)
	if err := s.cache.Get(nil, name, groupcache.TruncatingByteSliceSink(&[]byte{})); err != nil {
		return false, nil
	}
	s.stat.Counter("existsOkCounter").Inc(1) // TODO errata metric - remove if unused
	return true, nil
}

func (s *groupcacheStore) Write(name string, data io.Reader, ttl *TTLValue) error {
	log.Info("Write() populating cache: ", name)
	defer s.stat.Latency("writeLatency_ms").Time().Stop()
	s.stat.Counter("writeCounter").Inc(1)
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	err = s.underlying.Write(name, bytes.NewBuffer(b), ttl)
	if err != nil {
		return err
	}

	s.cache.PopulateCache(name, b)
	s.stat.Counter("writeOkCounter").Inc(1) // TODO errata metric - remove if unused
	return nil
}

func (s *groupcacheStore) Root() string {
	return s.underlying.Root()
}
