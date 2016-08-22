package cluster_test

import (
	"testing"
	"github.com/scootdev/scoot/cloud/cluster"
	"time"
	"reflect"
	"sync"
)

func TestFetchCron(t *testing.T) {
	c, exp := setUpTest(time.Second, "host1:1234")
	assertEqual(t, exp, c.Cl.Current())
	c, exp = setUpTest(time.Second, "host1:1234", "host2:8888")
	assertEqual(t, exp, c.Cl.Current())
	c, exp = setUpTest(time.Second, "host1:1234", "host1:1234")
	assertEqual(t, exp, c.Cl.Current())
	c, exp = setUpTest(time.Second, "")
	assertEqual(t, exp, c.Cl.Current())
}

func setUpTest(t time.Duration, nodeNames ...string) (*cluster.FetchCron, []cluster.Node) {
	f := fakeFetcher{}
	f.setResult(nodes(nodeNames), nil)
	c := cluster.NewFetchCron(f, t)
	expected := nodes(nodeNames)
	// give fetcher time to fetch and pass nodes to cluster
	time.Sleep(t * 3)
	c.Ticker.Stop()
	return c, expected
}

func assertEqual(t *testing.T, expected, actual []cluster.Node) {
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
	mutex	sync.Mutex
	nodes	[]cluster.Node
	err		error
}

func (f fakeFetcher) Fetch() ([]cluster.Node, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.nodes, f.err
}

func (f *fakeFetcher) setResult(nodes []cluster.Node, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.nodes = nodes
	f.err = err
}