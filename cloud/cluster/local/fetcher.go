// Package local provides a cluster Fetcher implementation for
// obtaining state of nodes running on the local machine.
package local

import (
	"errors"
	"os/exec"
	"regexp"
	"strings"

	"github.com/scootdev/scoot/cloud/cluster"
)

// Poor man's dynamic localhost cluster nodes.
// Note: lsof is slow to return on osx so we just use 'ps' and regex match the port.
func MakeFetcher() cluster.Fetcher {
	return &localFetcher{}
}

type localFetcher struct{}

// Implements cluster.Fetcher interface for local Nodes via ps
func (f *localFetcher) Fetch() (nodes []cluster.Node, err error) {
	var data []byte
	if data, err = f.fetchData(); err != nil {
		return nil, err
	} else if nodes, err = parseData(data); err != nil {
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

func parseData(data []byte) ([]cluster.Node, error) {
	nodes := []cluster.Node{}
	lines := string(data)
	for _, line := range strings.Split(lines, "\n") {
		thrift, err := parseFlag("thrift_addr", line)
		if err == nil {
			nodes = append(nodes, cluster.NewIdNode(thrift))
		}
	}
	return nodes, nil
}

func parseFlag(flag, line string) (string, error) {
	// This is ugly but it works for now.
	re := regexp.MustCompile("workerserver.*" + flag + "(?: +|=)([^ ]*)")
	matches := re.FindStringSubmatch(line)
	if len(matches) == 2 {
		return matches[1], nil
	}
	return "", errors.New("Could not parse flag:'" + flag + "', from line:'" + line + "'")

}
