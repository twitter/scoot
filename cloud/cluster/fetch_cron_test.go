package cluster

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wisechengyi/scoot/common"
)

// TestFetchCron verify that cron fetcher can put multiple node lists on the channel
// we don't want cluster to block repeated (cron) fetches.
func TestFetchCron(t *testing.T) {
	fetchDoneCh := make(chan bool)
	f := &fakeFetcher{fetchDoneCh: fetchDoneCh}
	f.setFakeNodes("node1", "node2", "node3")
	fetchedNodesCh := StartFetchCron(f, 10*time.Millisecond, common.DefaultClusterChanSize, nil)
	<-fetchDoneCh

	f.setFakeNodes("node1", "node2", "node4")
	<-fetchDoneCh

	assertFetch(t, fetchedNodesCh, makeNodeList("node1", "node2", "node3"), makeNodeList("node1", "node2", "node4"))
}

func assertFetch(t *testing.T, fetchedNodesCh chan []Node, expected1, expected2 []Node) {
	nodes := <-fetchedNodesCh
	// verify we get expected1 at least once from the channel
	assert.True(t, reflect.DeepEqual(expected1, nodes))
	for ; reflect.DeepEqual(expected1, nodes); nodes = <-fetchedNodesCh {
	}
	// verify we get expected2 is seen
	assert.True(t, reflect.DeepEqual(expected2, nodes), fmt.Sprintf("expected %v, got %v", expected2, nodes))
}

func makeNodeList(nodeIds ...string) []Node {
	ret := []Node{}
	for _, nId := range nodeIds {
		node := NewIdNode(nId)
		ret = append(ret, node)
	}
	return ret
}

// fakeFetcher for testing fetch cron
type fakeFetcher struct {
	nodes         []Node
	nodesMu       sync.RWMutex
	fetchUpdateCh chan []Node
	fetchDoneCh   chan bool
}

func (f *fakeFetcher) Fetch() ([]Node, error) {
	f.nodesMu.RLock()
	defer func() {
		f.nodesMu.RUnlock()
		if f.fetchDoneCh != nil {
			f.fetchDoneCh <- true
		}
	}()

	return f.nodes, nil
}

func (f *fakeFetcher) setFakeNodes(nodes ...string) {
	f.nodesMu.Lock()
	defer f.nodesMu.Unlock()
	f.nodes = []Node{}
	for _, n := range nodes {
		f.nodes = append(f.nodes, NewIdNode(n))
	}
}
