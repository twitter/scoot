package bundlestore

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"time"

	"github.com/scootdev/groupcache"
	"github.com/scootdev/scoot/cloud/cluster"
)

//TODO: we should consider modifying google groupcache lib further to:
// 1) It makes more sense given our use-case to cache bundles loaded via peer 100% of the time (currently 10%).
// 2) Modify peer proto to support setting bundle data on the peer that owns the bundlename. (via PopulateCache()).
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
	Fetcher      PeerFetcher
}

// Add in-memory caching to the given store.
func MakeGroupcacheStore(underlying Store, cfg *GroupcacheConfig) (Store, http.Handler, error) {
	// Create and initialize peer group.
	// The HTTPPool constructor will register as a global PeerPicker on our behalf.
	poolOpts := &groupcache.HTTPPoolOptions{BasePath: cfg.Endpoint}
	pool := groupcache.NewHTTPPoolOpts("http://"+cfg.AddrSelf, poolOpts)
	go loop(cfg.Fetcher, pool)

	// Create the cache which knows how to retrieve the underlying bundle data.
	var cache = groupcache.NewGroup(cfg.Name, cfg.Memory_bytes, groupcache.GetterFunc(
		func(ctx groupcache.Context, bundleName string, dest groupcache.Sink) error {
			log.Print("Not cached, try to fetch bundle and populate cache: ", bundleName)
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

	return &groupcacheStore{underlying: underlying, cache: cache}, pool, nil
}

// Loop will fetch peers and create a list of addresses, including self, to update groupcache.
func loop(fetcher PeerFetcher, pool *groupcache.HTTPPool) {
	prevPeers := []string{}
	for {
		peerNodes, err := fetcher.Fetch()
		if err != nil {
			log.Print("Unable to fetch groupcache peers: ", err)
			continue
		}
		peers := []string{}
		for _, node := range peerNodes {
			peers = append(peers, "http://"+string(node.Id()))
		}
		if reflect.DeepEqual(prevPeers, peers) {
			continue
		}
		log.Print("Setting groupcacheStore peers: ", peers)
		prevPeers = peers
		pool.Set(peers...)
		time.Sleep(1 * time.Second)
	}
}

type groupcacheStore struct {
	underlying Store
	cache      *groupcache.Group
	writeCache map[string][]byte
}

func (s *groupcacheStore) OpenForRead(name string) (io.ReadCloser, error) {
	log.Print("Read() checking for cached bundle: ", name)
	var data []byte
	if err := s.cache.Get(nil, name, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		log.Print("############ read.fail.Cache: ", s.cache.CacheStats(groupcache.MainCache), s.cache.Stats)
		return nil, err
	}
	log.Print("############ read.ok.Cache: ", s.cache.CacheStats(groupcache.MainCache), s.cache.Stats)
	return ioutil.NopCloser(bytes.NewReader(data)), nil

}

func (s *groupcacheStore) Exists(name string) (bool, error) {
	log.Print("Exists() checking for cached bundle: ", name)
	//TODO: what if it exists but we get an err? Can we get existence without also getting all data?
	if err := s.cache.Get(nil, name, groupcache.TruncatingByteSliceSink(&[]byte{})); err != nil {
		log.Print("############ exists.fail.Cache: ", s.cache.CacheStats(groupcache.MainCache), s.cache.Stats)
		return false, nil
	}
	log.Print("############ exists.ok.Cache: ", s.cache.CacheStats(groupcache.MainCache), s.cache.Stats)
	return true, nil
}

func (s *groupcacheStore) Write(name string, data io.Reader) error {
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	err = s.underlying.Write(name, bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	log.Print("Populating cache with store.Write() data: ", name)
	s.cache.PopulateCache(name, b)
	return nil
}
