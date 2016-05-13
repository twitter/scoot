package cluster_membership

import "fmt"
import "testing"

type testNode struct {
	id           string
	msgsReceived []string
}

func (n *testNode) Id() string {
	return n.id
}

func (n *testNode) SendMessage(msg string) error {
	n.msgsReceived = append(n.msgsReceived, msg)
	return nil
}

func generateTestNodes(size int) []Node {
	nodes := make([]Node, size)

	for s := 0; s < size; s++ {
		nodes[s] = &testNode{
			id: fmt.Sprintf("test_node_%d", s),
		}
	}

	return nodes
}

/*
 * Verify Creating an Empty Static Cluster
 */
func TestCreateEmptyStaticCluster(t *testing.T) {
	var emptySlice []Node
	sc := StaticClusterFactory(emptySlice)

	members := sc.Members()
	if len(members) != 0 {
		t.Error(fmt.Sprintf("Empty Static Cluster should have 0 nodes"))
	}
}

/*
 * Verify Static Clusters are Created Correctly
 */
func TestCreateStaticCluster(t *testing.T) {

	testNodes := generateTestNodes(10)
	sc := StaticClusterFactory(testNodes)
	members := sc.Members()

	if len(members) != len(testNodes) {
		t.Error("number of nodes supplied at creation differs from number of nodes in created staticCluster")
	}

	testNodeMap := make(map[string]Node)
	for _, node := range testNodes {
		testNodeMap[node.Id()] = node
	}

	//Ensure all nodes passed into factory are present in the cluster
	for _, node := range members {

		_, exists := testNodeMap[node.Id()]
		if !exists {
			t.Error(fmt.Sprintf("node %s is not in created cluster", node.Id()))
		}
	}
}
