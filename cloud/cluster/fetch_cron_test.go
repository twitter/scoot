package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFetchCron(t *testing.T) {
	f := &fakeFetcher{}
	tickerCh := make(chan time.Time)
	h := makeCronHelper(t, f, tickerCh)
	h.f.setFakeNodes("node1", "node2", "node3")
	tickerCh <- time.Now()
	h.assertFetch(t, "node1", "node2", "node3")
}

type cronHelper struct {
	t      *testing.T
	tickCh chan time.Time
	f      *fakeFetcher
	c      *fakeCluster
}

func makeCronHelper(t *testing.T, f *fakeFetcher, tickerCh chan time.Time) *cronHelper {
	h := &cronHelper{
		t:      t,
		tickCh: tickerCh,
		f:      f,
		c:      &fakeCluster{},
	}
	StartFetchCron(h.f, h.tickCh, h.c)
	return h
}

func (h *cronHelper) assertFetch(t *testing.T, expectedNodes ...string) {
	got := h.c.GetNodes()
	for i, n := range expectedNodes {
		assert.Equal(t, n, got[i].String())
	}
}

// fakeFetcher for testing fetch cron
type fakeFetcher struct {
	mutex sync.Mutex
	nodes []Node
}

func (f *fakeFetcher) Fetch() ([]Node, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.nodes, nil
}

func (f *fakeFetcher) setFakeNodes(nodes ...string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, n := range nodes {
		f.nodes = append(f.nodes, NewIdNode(n))
	}
}

type fakeCluster struct {
	latestNodeList   []Node
	latestNodeListMu sync.RWMutex
}

func (fc *fakeCluster) RetrieveCurrentNodeUpdates() []NodeUpdate {
	return nil
}

func (fc *fakeCluster) SetLatestNodesList(nodes []Node) {
	fc.latestNodeListMu.Lock()
	defer fc.latestNodeListMu.Unlock()
	fc.latestNodeList = nodes
}

func (fc *fakeCluster) GetNodes() []Node {
	fc.latestNodeListMu.RLock()
	defer fc.latestNodeListMu.RUnlock()
	ret := fc.latestNodeList[:]
	return ret
}
