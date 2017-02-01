package bundlestore

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"

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

	// Create and initialize peer group.
	// The HTTPPool constructor will register as a global PeerPicker on our behalf.
	poolOpts := &groupcache.HTTPPoolOptions{BasePath: cfg.Endpoint}
	pool := groupcache.NewHTTPPoolOpts("http://"+cfg.AddrSelf, poolOpts)
	go loop(cfg.Cluster, pool, stat)

	// Create the cache which knows how to retrieve the underlying bundle data.
	var cache = groupcache.NewGroup(cfg.Name, cfg.Memory_bytes, groupcache.GetterFunc(
		func(ctx groupcache.Context, bundleName string, dest groupcache.Sink) error {
			log.Print("Not cached, try to fetch bundle and populate cache: ", bundleName)
			stat.Counter("readUnderlyingCounter").Inc(1)
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

	return &groupcacheStore{underlying: underlying, cache: cache, stat: stat}, pool, nil
}

// Convert 'host:port' node ids to the format expected by groupcache peering, http URLs.
func toPeers(nodes []cluster.Node, stat stats.StatsReceiver) []string {
	peers := []string{}
	for _, node := range nodes {
		peers = append(peers, "http://"+string(node.Id()))
	}
	log.Print("New groupcacheStore peers: ", peers)
	stat.Counter("peerDiscoveryCounter").Inc(1)
	stat.Gauge("peerCountGauge").Update(int64(len(peers)))
	return peers
}

// Loop will listen for cluster updates and create a list of peer addresses to update groupcache.
// Cluster is expected to include the current node.
func loop(c *cluster.Cluster, pool *groupcache.HTTPPool, stat stats.StatsReceiver) {
	sub := c.Subscribe()
	pool.Set(toPeers(c.Members(), stat)...)
	for {
		select {
		case <-sub.Updates:
			pool.Set(toPeers(c.Members(), stat)...)
		}
	}
}

func updateCacheStats(cache *groupcache.Group, stat stats.StatsReceiver) {
	stat.Gauge("mainBytesGauge").Update(cache.CacheStats(groupcache.MainCache).Bytes)
	stat.Gauge("mainItemsGauge").Update(cache.CacheStats(groupcache.MainCache).Items)
	stat.Gauge("mainGetsGauge").Update(cache.CacheStats(groupcache.MainCache).Gets)
	stat.Gauge("mainHitsGauge").Update(cache.CacheStats(groupcache.MainCache).Hits)

	stat.Gauge("hotBytesGauge").Update(cache.CacheStats(groupcache.HotCache).Bytes)
	stat.Gauge("hotItemsGauge").Update(cache.CacheStats(groupcache.HotCache).Items)
	stat.Gauge("hotGetsGauge").Update(cache.CacheStats(groupcache.HotCache).Gets)
	stat.Gauge("hotHitsGauge").Update(cache.CacheStats(groupcache.HotCache).Hits)

	stat.Gauge("cacheGetGauge").Update(cache.Stats.Gets.Get())
	stat.Gauge("cacheHitGauge").Update(cache.Stats.CacheHits.Get())
	stat.Gauge("cachePeerGetsGauge").Update(cache.Stats.PeerLoads.Get())
	stat.Gauge("cachePeerErrGauge").Update(cache.Stats.PeerErrors.Get())
	stat.Gauge("cacheLocalLoadGauge").Update(cache.Stats.LocalLoads.Get())
	stat.Gauge("cacheLocalLoadErrGauge").Update(cache.Stats.LocalLoadErrs.Get())
	stat.Gauge("cacheIncomingRequestsGauge").Update(cache.Stats.ServerRequests.Get())
}

type groupcacheStore struct {
	underlying Store
	cache      *groupcache.Group
	stat       stats.StatsReceiver
}

func (s *groupcacheStore) OpenForRead(name string) (io.ReadCloser, error) {
	log.Print("Read() checking for cached bundle: ", name)
	defer s.stat.Latency("readLatency_ms").Time().Stop()
	defer updateCacheStats(s.cache, s.stat)
	s.stat.Counter("readCounter").Inc(1)
	var data []byte
	if err := s.cache.Get(nil, name, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return nil, err
	}
	s.stat.Counter("readOkCounter").Inc(1)
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (s *groupcacheStore) Exists(name string) (bool, error) {
	log.Print("Exists() checking for cached bundle: ", name)
	defer s.stat.Latency("existsLatency_ms").Time().Stop()
	defer updateCacheStats(s.cache, s.stat)
	s.stat.Counter("existsCounter").Inc(1)
	if err := s.cache.Get(nil, name, groupcache.TruncatingByteSliceSink(&[]byte{})); err != nil {
		return false, nil
	}
	s.stat.Counter("existsOkCounter").Inc(1)
	return true, nil
}

func (s *groupcacheStore) Write(name string, data io.Reader) error {
	log.Print("Write() populating cache: ", name)
	defer s.stat.Latency("writeLatency_ms").Time().Stop()
	defer updateCacheStats(s.cache, s.stat)
	s.stat.Counter("writeCounter").Inc(1)
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	err = s.underlying.Write(name, bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	s.cache.PopulateCache(name, b)
	s.stat.Counter("writeOkCounter").Inc(1)
	return nil
}
