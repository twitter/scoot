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
	h.assertFetch(t, h.setupTest("host1:1234"))
	h.assertFetch(t, h.setupTest("host1:1234", "host2:8888"))
	h.assertFetch(t, h.setupTest("host1:1234", "host1:1234"))
	h.assertFetch(t, h.setupTest(""))
}

func (h *cronHelper) setupTest(nodeNames ...string) []cluster.Node {
	nodes := nodes(nodeNames)
	h.f.setResult(nodes)
	sort.Sort(cluster.NodeSorter(nodes))
	return nodes
}

type cronHelper struct {
	t    *testing.T
	c    *cluster.FetchCron
	cl   *cluster.Cluster
	f    *fakeFetcher
	time time.Duration
}

func makeCronHelper(t *testing.T) *cronHelper {
	h := &cronHelper{t: t}
	h.cl = cluster.NewCluster([]cluster.Node{}, make(chan []cluster.NodeUpdate), make(chan []cluster.Node))
	h.time = time.Nanosecond
	h.f = &fakeFetcher{}
	h.c = cluster.NewFetchCron(h.f, h.time, h.cl.StateCh)
	return h
}

func (h *cronHelper) assertFetch(t *testing.T, expected []cluster.Node) {
	actual := <-h.c.Ch
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Unequal, expected %v, received %v", expected, actual)
	}
}

func nodes(ids []string) []cluster.Node {
	nodeNames := make(map[string]bool)
	for _, str := range ids {
		nodeNames[str] = true
	}
	n := []cluster.Node{}
	for name, _ := range nodeNames {
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
