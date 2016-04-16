package cluster_membership

import "fmt"
import "testing"

/*
 * Verify Creating an Empty Dynamic Cluster
 */
func TestCreateEmptyDynamicCluster(t *testing.T) {

	var emptyNodes []Node
	dc := DynamicClusterFactory(emptyNodes)
	members := dc.Members()

	if len(members) != 0 {
		t.Error("Empty Dynamic Cluster should have 0 nodes")
	}
}

/*
 * Verify Creating a Dynamic Cluster
 */
func TestCreateDynamicCluster(t *testing.T) {
	testNodes := generateTestNodes(10)
	dc := DynamicClusterFactory(testNodes)
	members := dc.Members()

	if len(members) != len(testNodes) {
		t.Error("number of nodes supplied at creation differs from the number of nodes in dynamic cluster")
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
 * Verify Add Node to Cluster
 */
func TestAddNodesToDynamicCluster(t *testing.T) {

	var emptyNodes []Node
	dc := DynamicClusterFactory(emptyNodes)
	members := dc.Members()

	if len(members) != 0 {
		t.Error("Empty Dynamic Cluster should have 0 nodes")
	}

	tNode := testNode{
		id: "testNode1",
	}

	dc.AddNode(&tNode)
	members = dc.Members()
	if len(members) != 1 {
		t.Error("Dynamic Cluster should have 1 node")
	}

	if members[0] != tNode.Id() {
		t.Error(fmt.Sprintf("Dynamic Cluster should have 1 node with id %s", tNode.Id()))
	}

	tNode2 := testNode{
		id: "testNode2",
	}
	dc.AddNode(&tNode2)
	members = dc.Members()

	if len(members) != 2 {
		t.Error("Dynamic Cluster should have 2 node")
	}

	if members[0] != tNode.Id() {
		t.Error(fmt.Sprintf("Dynamic Cluster should have node with id %s", tNode.Id()))
	}

	if members[1] != tNode2.Id() {
		t.Error(fmt.Sprintf("Dynamic Cluster should have node with id %s", tNode2.Id()))
	}
}

/*
 * Verify that Messages can be sent to an added nodes
 */
func TestSendMessageToAddedNode(t *testing.T) {
	var emptyNodes []Node
	dc := DynamicClusterFactory(emptyNodes)
	tNode := testNode{
		id: "testNode1",
	}

	dc.AddNode(&tNode)
	err := dc.SendMessage("Hello Test", tNode.Id())

	if err != nil {
		t.Error("Failed to Send Message to Added Node in Dynamic Cluster")
	}
}

/*
 * Verify Add Node to Cluster, Node Already in Cluster,
 * Add should be Idempotent
 */
func TestAddNodeToClusterThatAlreadyExists(t *testing.T) {
	var emptyNodes []Node
	dc := DynamicClusterFactory(emptyNodes)

	members := dc.Members()
	tNode := testNode{
		id: "testNode1",
	}

	dc.AddNode(&tNode)
	members = dc.Members()
	if len(members) != 1 {
		t.Error("Dynamic Cluster should have 1 node")
	}

	dc.AddNode(&tNode)
	members = dc.Members()
	if len(members) != 1 {
		t.Error("Dynamic Cluster should have 1 node")
	}
}

/*
 * Verify that Delete is Idempotent, a non existant node can
 * be deleted from an empty cluster
 */
func TestDeleteNodeFromEmptyCluster(t *testing.T) {
	var emptyNodes []Node
	dc := DynamicClusterFactory(emptyNodes)

	dc.RemoveNode("node_X")
	if len(dc.Members()) != 0 {
		t.Error("Dynamic Cluster should have 0 nodes after Delete of Non Existant Node")
	}
}

/*
 * Verify that nodes can successfully be removed from the cluster
 */
func TestDeleteNodeFromCluster(t *testing.T) {
	tNode := testNode{
		id: "testNode1",
	}

	nodes := make([]Node, 1)
	nodes[0] = &tNode
	dc := DynamicClusterFactory(nodes)

	if len(dc.Members()) != 1 {
		t.Error("Dynamic Cluster should have 1 node")
	}

	dc.RemoveNode(tNode.Id())

	if len(dc.Members()) != 0 {
		t.Error("Dynamic CLuster Should have 0 nodes")
	}
}

/*
 * Verify that messages cannot be sent to deleted nodes
 */
func TestCanNotSendMessageToDeletedNode(t *testing.T) {
	tNode := testNode{
		id: "testNode1",
	}

	nodes := make([]Node, 1)
	nodes[0] = &tNode
	dc := DynamicClusterFactory(nodes)

	//verify that we can send message to node in the cluster
	testMsg := "Hello Test"
	err := dc.SendMessage(testMsg, tNode.Id())

	if err != nil {
		t.Error("Failed to Send Message to Node in Dynamic Cluster")
	}

	//remove node and verify we cannot send messages to it anymore
	dc.RemoveNode(tNode.Id())
	err = dc.SendMessage(testMsg, tNode.Id())

	if err == nil {
		t.Error("Should not be able to Send Message to Deleted Node")
	}
}

/*
 * SendMessage to Node not in Cluster Returns an Error
 */
func TestDynamicClusterSendMessageToNodeNotInCluster(t *testing.T) {
	var emptyNodes []Node
	dc := DynamicClusterFactory(emptyNodes)

	err := dc.SendMessage("Hello Test", "Node_X")
	if err == nil {
		t.Error("Dynamic Cluster should return error if message sent to node not in cluster")
	}
}

/*
 * SendMessage to node in cluster
 */
func TestDynamicClusterSendMessageToNodeInCluster(t *testing.T) {

	tNode := testNode{
		id: "testNode1",
	}

	nodes := make([]Node, 1)
	nodes[0] = &tNode
	dc := DynamicClusterFactory(nodes)

	testMsg := "Hello Test"
	err := dc.SendMessage(testMsg, tNode.Id())

	if err != nil {
		t.Error("Dynamic Cluster Should SendMessage to node in it successfully")
	}

	if len(tNode.msgsReceived) != 1 {
		t.Error("Sending Message to testNode failed")
	}

	if tNode.msgsReceived[0] != testMsg {
		t.Error(fmt.Sprintf("Message was Not Transmitted Successfully.  Expected %s, Actual: %s",
			testMsg, tNode.msgsReceived[0]))
	}
}
