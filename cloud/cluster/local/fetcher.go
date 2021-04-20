// Package local provides a cluster Fetcher implementation for
// obtaining state of nodes running on the local machine.
package local

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/twitter/scoot/cloud/cluster"
)

// Proletariat person's dynamic localhost cluster nodes.
// Note: lsof is slow to return on osx so we just use 'ps' and regex match the port.
func MakeFetcher(procName, portFlag string) cluster.Fetcher {
	return &localFetcher{
		procName: procName,
		addrFlag: portFlag,
		re:       regexp.MustCompile(fmt.Sprintf("%s.*%s(?: +|=)([^ ]*)", procName, portFlag)),
	}
}

type localFetcher struct {
	procName string
	addrFlag string
	re       *regexp.Regexp
}

// Implements cluster.Fetcher interface for local Nodes via ps
func (f *localFetcher) Fetch() (nodes []cluster.Node, err error) {
	var data []byte
	if data, err = f.fetchData(); err != nil {
		return nil, err
	} else if nodes, err = parseData(data, f.procName, f.addrFlag, f.re); err != nil {
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

func parseData(data []byte, procName, addrFlag string, re *regexp.Regexp) ([]cluster.Node, error) {
	nodes := []cluster.Node{}
	lines := string(data)
	for _, line := range strings.Split(lines, "\n") {
		thrift, err := parseFlag(line, procName, addrFlag, re)
		if err == nil {
			nodes = append(nodes, cluster.NewIdNode(thrift))
		}
	}
	return nodes, nil
}

func parseFlag(line, procName, addrFlag string, re *regexp.Regexp) (string, error) {
	matches := re.FindStringSubmatch(line)
	if len(matches) == 2 {
		return matches[1], nil
	}
	return "", errors.New("Could not parse flag:'" + addrFlag + "', from line:'" + line + "'")
}
