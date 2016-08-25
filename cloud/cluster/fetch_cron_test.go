package cluster_test

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestFetchCron(t *testing.T) {
	h := makeCronHelper(t)
	h.assertFetch(t, h.setupTest())
	h.assertFetch(t, h.setupTest("host1:1234"))
	h.assertFetch(t, h.setupTest("host1:1234", "host2:8888"))
	h.assertFetch(t, h.setupTest("host1:1234"))
	h.assertFetch(t, h.setupTest())
}

func (h *cronHelper) setupTest(nodeNames ...string) []cluster.Node {
	nodes := nodes(nodeNames)
	sort.Sort(cluster.NodeSorter(nodes))
	h.f.setResult(nodes)
	return nodes
}

type cronHelper struct {
	t        *testing.T
	time     time.Duration
	f        *fakeFetcher
	ch       chan interface{}
	updateCh chan []cluster.NodeUpdate
	// c        *cluster.FetchCron
	cl *cluster.Cluster
}

func makeCronHelper(t *testing.T) *cronHelper {
	h := &cronHelper{t: t, time: time.Nanosecond}
	h.f = &fakeFetcher{}
	h.ch = make(chan interface{})
	h.cl = cluster.NewCluster([]cluster.Node{}, h.updateCh, h.ch, h.f)
	// h.c = cluster.NewFetchCron(h.f, h.time, h.ch)
	return h
}

func (h *cronHelper) assertFetch(t *testing.T, expected []cluster.Node) {
	timeout := time.After(time.Millisecond * 100)
	for {
		select {
		case actual := <-h.ch:
			if _, ok := actual.([]cluster.Node); ok {
				if reflect.DeepEqual(expected, actual) {
					return
				}
			}
		case <-timeout:
			t.Fatalf("Unequal, expected %v", expected)
		}
	}
}

func nodes(ids []string) []cluster.Node {
	n := []cluster.Node{}
	for _, name := range ids {
		n = append(n, cluster.NewIdNode(name))
	}
	return n
}

// fakeFetcher for testing fetch cron
type fakeFetcher struct {
	mutex sync.Mutex
	nodes []cluster.Node
}

func (f *fakeFetcher) Fetch() ([]cluster.Node, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.nodes, nil
}

func (f *fakeFetcher) setResult(nodes []cluster.Node) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.nodes = nodes
}
