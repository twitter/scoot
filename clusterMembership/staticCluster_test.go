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
	for _, nodeId := range members {

		_, exists := testNodeMap[nodeId]
		if !exists {
			t.Error(fmt.Sprintf("node %s is not in created cluster", nodeId))
		}
	}
}

/*
 * Static Cluster SendMessage to node not in cluster
 */
func TestStaticClusterSendMessageToNodeNotInCluster(t *testing.T) {
	var emptySlice []Node
	sc := StaticClusterFactory(emptySlice)

	err := sc.SendMessage("Hello Test", "Node_X")
	if err == nil {
		t.Error("Static Cluster should return error if message sent to node not in the cluster")
	}
}

/*
 * Static Cluster SendMessage to node in cluster
 */
func TestStaticClusterSendMessageToNodeInCluster(t *testing.T) {

	tNode := testNode{
		id: "testNode1",
	}

	nodes := make([]Node, 1)
	nodes[0] = &tNode
	sc := StaticClusterFactory(nodes)

	testMsg := "Hello Test"
	err := sc.SendMessage(testMsg, tNode.Id())

	if err != nil {
		t.Error("Static Cluster Should SendMessage to node in it successfully")
	}

	if len(tNode.msgsReceived) != 1 {
		t.Error("Sending Message to testNode failed")
	}

	if tNode.msgsReceived[0] != testMsg {
		t.Error(fmt.Sprintf("Message was Not Transmitted Successfully.  Expected %s, Actual: %s",
			testMsg, tNode.msgsReceived[0]))
	}
}
