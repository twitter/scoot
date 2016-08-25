package local

import (
	"os/exec"
	"regexp"
	"strings"
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
)

// Poor man's dynamic localhost cluster nodes.
// Note: lsof is slow to return on osx so we just use 'ps' and regex match the port.
func MakeFetcher() cluster.Fetcher {
	return &localFetcher{}
}

type localFetcher struct{}

func Subscribe() (cluster.Subscription, cluster.Fetcher) {
	return cluster.Subscribe(), MakeFetcher()
}

func (f *localFetcher) Fetch() (nodes []cluster.Node, err error) {
	var data []byte
	if data, err = f.fetchData(); err != nil {
		return nil, err
	} else if nodes, err = f.parseData(data); err != nil {
		return nil, err
	} else {
		return nodes, nil
	}
}

func (f *localFetcher) fetchData() ([]byte, error) {
	cmd := exec.Command("ps", "x")
	data, err := cmd.Output()
	return data, err
}

func (f *localFetcher) parseData(data []byte) ([]cluster.Node, error) {
	nodes := []cluster.Node{}
	lines := string(data)
	// This is ugly but it works for now.
	re := regexp.MustCompile("workerserver.*thrift_port(?: *|=)(\\d*)")
	for _, line := range strings.Split(lines, "\n") {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 2 {
			nodes = append(nodes, cluster.NewIdNode("localhost:"+matches[1]))
		}
	}
	return nodes, nil
}

type fakeFetcher struct {
	mutex sync.Mutex
	nodes []cluster.Node
	err   error
}

func (f *fakeFetcher) Fetch() ([]cluster.Node, error) {
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

func nodes(ids ...string) []cluster.Node {
	n := make([]cluster.Node, len(ids))
	for idx, str := range ids {
		n[idx] = cluster.NewIdNode(str)
	}
	return n
}
