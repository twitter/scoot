package cluster_implementations

import "fmt"
import "math/rand"
import "time"

import msg "github.com/scootdev/scoot/messages"
import cm "github.com/scootdev/scoot/sched/clustermembership"

/*
 * localNode ClusterMember are for test purposes.
 * Simulates a node locally and just prints all
 * received messages
 */
type localNode struct {
	name string
}

func (n localNode) SendMessage(task msg.Task) error {

	//delay message to mimic network call for a
	delayMS := time.Duration(rand.Intn(500)) * time.Microsecond
	time.Sleep(delayMS)

	/*for _, cmd := range task.Commands {
		fmt.Println(fmt.Sprintf("%s, received command: %s: %s", n.name, task.Id, cmd))
	}*/
	return nil
}

func (n localNode) Id() string {
	return n.name
}

/*
 * Creates a Static LocalNode Cluster with the specified
 * Number of Nodes in it.
 */
func StaticLocalNodeClusterFactory(size int) cm.Cluster {
	nodes := make([]cm.Node, size)

	for s := 0; s < size; s++ {
		nodes[s] = localNode{
			name: fmt.Sprintf("static_node_%d", s),
		}
	}

	return cm.StaticClusterFactory(nodes)
}

/*
 * Creates a Dynamic LocalNode Cluster with the initial
 * number of Nodes in it.
 */
func DynamicLocalNodeClusterFactory(initialSize int) cm.Cluster {
	nodes := make([]cm.Node, initialSize)

	for s := 0; s < initialSize; s++ {
		nodes[s] = localNode{
			name: fmt.Sprintf("dynamic_node_%d", s),
		}
	}

	return cm.DynamicClusterFactory(nodes)
}
