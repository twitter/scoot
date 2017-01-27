package bundlestore

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/golang/groupcache"
	"github.com/scootdev/scoot/cloud/cluster"
)

type PeerFetcher interface {
	Fetch() ([]cluster.Node, error)
}

type GroupcacheConfig struct {
	Name         string
	Memory_bytes int64
	AddrSelf     string
	Endpoint     string
	Fetcher      PeerFetcher
}

// Add in-memory caching to the given store.
func MakeGroupcacheStore(underlying Store, cfg *GroupcacheConfig) (Store, http.Handler, error) {
	// Fetch peers and create a list of addresses including self.
	peerNodes, err := cfg.Fetcher.Fetch()
	if err != nil {
		return nil, nil, err
	}
	peers := []string{}
	for _, node := range peerNodes {
		peers = append(peers, string(node.Id()))
	}
	log.Print("Adding groupcacheStore peers: ", peers)

	// Create and initialize peer group.
	// The HTTPPool constructor will register as a global PeerPicker on our behalf.
	poolOpts := &groupcache.HTTPPoolOptions{BasePath: cfg.Endpoint}
	pool := groupcache.NewHTTPPoolOpts(cfg.AddrSelf, poolOpts)
	pool.Set(peers...)

	// Create the cache which knows how to retreive the underlying bundle data.
	var cache = groupcache.NewGroup(cfg.Name, cfg.Memory_bytes, groupcache.GetterFunc(
		func(ctx groupcache.Context, bundleName string, dest groupcache.Sink) error {
			log.Print("Not cached, fetching bundle: ", bundleName)
			if reader, err := underlying.OpenForRead(bundleName); err != nil {
				return err
			} else if data, err := ioutil.ReadAll(reader); err != nil {
				return err
			} else {
				dest.SetBytes(data)
				return nil
			}
		},
	))

	//TODO: start loop to periodically update the list of peers.
	return &groupcacheStore{underlying: underlying, cache: cache}, pool, nil
}

type groupcacheStore struct {
	underlying Store
	cache      *groupcache.Group
}

func (s *groupcacheStore) OpenForRead(name string) (io.ReadCloser, error) {
	log.Print("Read() checking for cached bundle: ", name)
	var data []byte
	if err := s.cache.Get(nil, name, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return nil, err
	}
	log.Print("cache: ", s.cache.CacheStats(groupcache.MainCache), s.cache.Stats)
	return ioutil.NopCloser(bytes.NewReader(data)), nil

}

func (s *groupcacheStore) Exists(name string) (bool, error) {
	log.Print("Exists() checking for cached bundle: ", name)
	//TODO: what if it exists but we get an err? Can we get existence without also getting all data?
	var data []byte
	if err := s.cache.Get(nil, name, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return false, nil
	}
	log.Print("cache: ", s.cache.CacheStats(groupcache.MainCache), s.cache.Stats)
	return true, nil
}

func (s *groupcacheStore) Write(name string, data io.Reader) error {
	return s.underlying.Write(name, data)
}
