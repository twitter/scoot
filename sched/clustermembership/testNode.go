package cluster_membership

import "fmt"
import "math/rand"
import "sync"
import "time"

import msg "github.com/scootdev/scoot/messages"

type TestNode struct {
	id           string
	MsgsReceived []string
	mutex        sync.Mutex
}

func (n *TestNode) Id() string {
	return n.id
}

func (n *TestNode) SendMessage(task msg.Task) error {

	//delay message to mimic network call for a
	delayMS := time.Duration(rand.Intn(500)) * time.Microsecond
	time.Sleep(delayMS)

	n.mutex.Lock()
	for _, cmd := range task.Commands {
		n.MsgsReceived = append(n.MsgsReceived, cmd)
	}
	n.mutex.Unlock()

	return nil
}

func GenerateTestNodes(size int) []Node {
	nodes := make([]Node, size)

	for s := 0; s < size; s++ {
		nodes[s] = &TestNode{
			id: fmt.Sprintf("test_node_%d", s),
		}
	}

	return nodes
}

/*
 * Creates a Static LocalNode Cluster with the specified
 * Number of Nodes in it.
 */
func StaticTestNodeClusterFactory(size int) Cluster {
	nodes := make([]Node, size)

	for s := 0; s < size; s++ {
		nodes[s] = &TestNode{
			id: fmt.Sprintf("static_node_%d", s),
		}
	}

	return StaticClusterFactory(nodes)
}
