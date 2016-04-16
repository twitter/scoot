package cluster_membership

import "fmt"
import "testing"

type testNode struct {
	id           string
	msgsReceived []string
}

func (n *testNode) GetId() string {
	return n.id
}

func (n *testNode) SendMessage(msg string) error {
	n.msgsReceived = append(n.msgsReceived, msg)
	return nil
}

func generateTestNodes(size int) []Node {
	var nodes = make([]Node, size)

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
	var sc = StaticClusterFactory(emptySlice)

	var members = sc.GetMembers()
	if len(members) != 0 {
		t.Error(fmt.Sprintf("Empty Static Cluster should have 0 nodes"))
	}
}

/*
 * Verify Static Clusters are Created Correctly
 */
func TestCreateStaticCluster(t *testing.T) {

	var testNodes = generateTestNodes(10)
	var sc = StaticClusterFactory(testNodes)
	var members = sc.GetMembers()

	if len(members) != len(testNodes) {
		t.Error("number of nodes supplied at creation differs from number of nodes in created staticCluster")
	}

	var testNodeMap = make(map[string]Node)
	for _, node := range testNodes {
		testNodeMap[node.GetId()] = node
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
	var sc = StaticClusterFactory(emptySlice)

	var err = sc.SendMessage("Hello Test", "Node_X")
	if err == nil {
		t.Error("Static Cluster should return error if message sent to node not in the cluster")
	}
}

/*
 * Static Cluster SendMessage to node in cluster
 */
func TestStaticClusterSendMessageToNodeInCluster(t *testing.T) {

	var tNode = testNode{
		id: "testNode1",
	}

	var nodes = make([]Node, 1)
	nodes[0] = &tNode
	var sc = StaticClusterFactory(nodes)

	var testMsg = "Hello Test"
	var err = sc.SendMessage(testMsg, tNode.GetId())

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
