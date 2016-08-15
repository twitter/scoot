package local

import (
	"os/exec"
	"regexp"
	"strings"
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
)

// Poor man's dynamic localhost cluster nodes.
// Note: lsof is slow to return on osx so we just use 'ps' and regex match the port.
func MakeFetcher(regexCapturePort string) cluster.Fetcher {
	return &localFetcher{regexCapturePort: regexCapturePort}
}

type localFetcher struct {
	regexCapturePort string
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
	re := regexp.MustCompile(f.regexCapturePort)
	for _, line := range strings.Split(lines, "\n") {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 2 {
			nodes = append(nodes, memory.NewIdNode("localhost:"+matches[1]))
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
		n[idx] = memory.NewIdNode(str)
	}
	return n
}
