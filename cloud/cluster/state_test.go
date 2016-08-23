package cluster_test

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"sort"
	"testing"
)

func TestState(t *testing.T) {
	s := cluster.MakeState([]cluster.Node{})
	// no nodes removed or added
	assertUpdates(t, s, []string{}, []cluster.NodeUpdate{})
	// 1 node added
	assertUpdates(t, s, []string{"host1:1234"}, []cluster.NodeUpdate{cluster.NewAdd(cluster.NewIdNode("host1:1234"))})
	// 1 node removed
	assertUpdates(t, s, []string{}, []cluster.NodeUpdate{cluster.NewRemove(cluster.NodeId("host1:1234"))})
	// 2 nodes added
	assertUpdates(t, s, []string{"host1:1234", "host1:4321"}, []cluster.NodeUpdate{cluster.NewAdd(cluster.NewIdNode("host1:1234")), cluster.NewAdd(cluster.NewIdNode("host1:4321"))})
	// 1 node added, same node removed
	// TODO: (rcouto) Differ should know when a node is removed and then re-added in between diffs
	assertUpdates(t, s, []string{"host1:1234", "host1:4321"}, []cluster.NodeUpdate{})
	// 1 node added, different node removed
	assertUpdates(t, s, []string{"host1:1234", "host1:6789"}, []cluster.NodeUpdate{cluster.NewRemove(cluster.NodeId("host1:4321")), cluster.NewAdd(cluster.NewIdNode("host1:6789"))})
	// 2 nodes removed
	assertUpdates(t, s, []string{}, []cluster.NodeUpdate{cluster.NewRemove(cluster.NodeId("host1:1234")), cluster.NewRemove(cluster.NodeId("host1:6789"))})
}

func TestUpdateState(t *testing.T) {
	s := cluster.MakeState([]cluster.Node{})
	// empty cluster
	assertMembers(t, s, []cluster.NodeUpdate{}, []cluster.Node{})
	// add one node
	assertMembers(t, s,
		[]cluster.NodeUpdate{cluster.NewAdd(cluster.NewIdNode("host1:1234"))},
		[]cluster.Node{cluster.NewIdNode("host1:1234")})
	// remove one node
	assertMembers(t, s,
		[]cluster.NodeUpdate{cluster.NewRemove(cluster.NewIdNode("host1:1234").Id())},
		[]cluster.Node{})
	// add two nodes
	assertMembers(t, s,
		[]cluster.NodeUpdate{cluster.NewAdd(cluster.NewIdNode("host1:8888")), cluster.NewAdd(cluster.NewIdNode("host1:9999"))},
		[]cluster.Node{cluster.NewIdNode("host1:8888"), cluster.NewIdNode("host1:9999")})
	// remove one node, leave one
	assertMembers(t, s,
		[]cluster.NodeUpdate{cluster.NewRemove(cluster.NewIdNode("host1:8888").Id())},
		[]cluster.Node{cluster.NewIdNode("host1:9999")})
	// two adds followed by remove should resolve to one add
	assertMembers(t, s,
		[]cluster.NodeUpdate{cluster.NewAdd(cluster.NewIdNode("host1:9999")), cluster.NewAdd(cluster.NewIdNode("host1:9999")), cluster.NewRemove(cluster.NodeId("host1:9999"))},
		[]cluster.Node{cluster.NewIdNode("host1:9999")})
	// two removes followed by add should resolve to one remove
	assertMembers(t, s,
		[]cluster.NodeUpdate{cluster.NewRemove(cluster.NodeId("host1:9999")), cluster.NewRemove(cluster.NodeId("host1:9999")), cluster.NewAdd(cluster.NewIdNode("host1:9999"))},
		[]cluster.Node{})
}

func assertUpdates(t *testing.T, s *cluster.State, nodeNames []string, expected []cluster.NodeUpdate) {
	nodes := []cluster.Node{}
	// create nodes to make diff against
	for _, n := range nodeNames {
		node := cluster.NewIdNode(n)
		nodes = append(nodes, node)
	}
	actual := s.SetAndDiff(nodes)
	sort.Sort(cluster.NodeUpdateSorter(actual))
	if len(actual) != len(expected) {
		t.Fatalf("Unequal updates %v %v", actual, expected)
	}
	for i, ex := range expected {
		act := actual[i]
		if ex.Id != act.Id {
			t.Fatalf("Unequal updates %v %v", actual, expected)
		}
	}
}

func assertMembers(t *testing.T, s *cluster.State, updates []cluster.NodeUpdate, expected []cluster.Node) {
	s.FilterAndUpdate(updates)
	actual := s.Nodes

	if len(actual) != len(expected) {
		t.Fatalf("Unequal members %v %v", actual, expected)
	}
	for _, ex := range expected {
		act := actual[ex.Id()]
		if ex.Id() != act.Id() {
			t.Fatalf("Unequal members %v %v", act, ex)
		}
	}
}
