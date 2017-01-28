package bundlestore

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/scootdev/groupcache"
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
		peers = append(peers, "http://"+string(node.Id()))
	}
	log.Print("Adding groupcacheStore peers: ", peers)

	// Create and initialize peer group.
	// The HTTPPool constructor will register as a global PeerPicker on our behalf.
	poolOpts := &groupcache.HTTPPoolOptions{BasePath: cfg.Endpoint}
	pool := groupcache.NewHTTPPoolOpts(cfg.AddrSelf, poolOpts)
	pool.Set(peers...)
	// Create the cache which knows how to retrieve the underlying bundle data.
	var cache = groupcache.NewGroup(cfg.Name, cfg.Memory_bytes, groupcache.GetterFunc(
		func(ctx groupcache.Context, bundleName string, dest groupcache.Sink) error {
			log.Print("Not cached, fetching bundle and populating cache: ", bundleName)
			var reader io.Reader
			var data []byte
			if reader, err = underlying.OpenForRead(bundleName); err != nil {
				return err
			}
			if data, err = ioutil.ReadAll(reader); err != nil {
				return err
			}
			dest.SetBytes(data)
			return nil
		},
	))

	//TODO: start loop to periodically update the list of peers.
	return &groupcacheStore{underlying: underlying, cache: cache}, pool, nil
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
	var b []byte
	var err error
	if b, err = ioutil.ReadAll(data); err != nil {
		return err
	}
	if err := s.underlying.Write(name, bytes.NewBuffer(b)); err != nil {
		return err
	}

	log.Print("Populating cache with store.Write() data: ", name)
	s.cache.PopulateCache(name, b)
	return nil
}
