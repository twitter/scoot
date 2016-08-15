package local

import (
	"reflect"
	"testing"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
)

func TestFetcher(t *testing.T) {
	psOutput := `
77595   ??  S      0:00.38 /usr/libexec/USBAgent
73170 s004  T      0:01.54 emacs -nw scoot.rb
79003 s004  S+     0:00.02 ./workerserver -thrift_port 9876
79004 s004  S+     0:00.02 ./workerserver -thrift_port 9877
 8440 s005  Ss     0:01.58 /bin/bash
`
	expected := []cluster.Node{
		memory.NewIdNode("localhost:9876"),
		memory.NewIdNode("localhost:9877"),
	}

	f := &localFetcher{regexCapturePort: "workerserver.*thrift_port(?: *|=)(\\d*)"}
	nodes, err := f.parseData([]byte(psOutput))
	if err != nil {
		t.Fatalf("error parsing: %v", err)
	}
	if !reflect.DeepEqual(expected, nodes) {
		t.Fatalf("Parsed wrong: %v %v %v", psOutput, expected, nodes)
	}
}
