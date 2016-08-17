package memory_test

import (
	"testing"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
)

func TestDiffer(t *testing.T) {
	d := memory.MakeDiffer()
	// no nodes removed or added
	assertUpdates(t, d, []string{}, cluster.NodeUpdates{})
	// 1 node added
	assertUpdates(t, d, []string{"host1:1234"}, cluster.NodeUpdates{add("host1:1234")})
	// 1 node removed
	assertUpdates(t, d, []string{}, cluster.NodeUpdates{remove("host1:1234")})
	// 2 nodes added
	assertUpdates(t, d, []string{"host1:1234", "host1:4321"}, cluster.NodeUpdates{add("host1:1234"), add("host1:4321")})
	// 1 node added, same node removed
	// TODO: (rcouto) Differ should know when a node is removed and then re-added in between diffs
	assertUpdates(t, d, []string{"host1:1234", "host1:4321"}, cluster.NodeUpdates{})
	// 1 node added, different node removed
	assertUpdates(t, d, []string{"host1:1234", "host1:6789"}, cluster.NodeUpdates{remove("host1:4321"), add("host1:6789")})
	// 2 nodes removed
	assertUpdates(t, d, []string{}, cluster.NodeUpdates{remove("host1:1234"), remove("host1:6789")})
}

func assertUpdates(t *testing.T, d *memory.Differ, nodeNames []string, expected cluster.NodeUpdates) {
	nodes := []cluster.Node{}
	// create nodes to make diff against
	for _, n := range nodeNames {
		node := memory.NewIdNode(n)
		nodes = append(nodes, node)
	}
	actual := d.MakeDiff(nodes)
	
	if len(actual) != len(expected) {
		t.Fatalf("Unequal updates %v %v", actual, expected)
	}
	for i, ex := range expected {
		act := actual[i]
		if ex != act {
		}
	}
}