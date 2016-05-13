package cluster_implementations

import "fmt"
import "math/rand"
import "sync"
import "time"

import cm "github.com/scootdev/scoot/sched/clustermembership"

type TestNode struct {
	id           string
	MsgsReceived []string
	mutex        sync.Mutex
}

func (n *TestNode) Id() string {
	return n.id
}

func (n *TestNode) SendMessage(msg string) error {

	//delay message to mimic network call for a
	delayMS := time.Duration(rand.Intn(500)) * time.Microsecond
	time.Sleep(delayMS)

	n.mutex.Lock()
	n.MsgsReceived = append(n.MsgsReceived, msg)
	n.mutex.Unlock()

	return nil
}

func GenerateTestNodes(size int) []cm.Node {
	nodes := make([]cm.Node, size)

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
func StaticTestNodeClusterFactory(size int) cm.Cluster {
	nodes := make([]cm.Node, size)

	for s := 0; s < size; s++ {
		nodes[s] = &TestNode{
			id: fmt.Sprintf("static_node_%d", s),
		}
	}

	return cm.StaticClusterFactory(nodes)
}
