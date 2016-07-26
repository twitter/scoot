package distributor_test

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/sched/distributor"
	"testing"
)

func TestPoolDistributor_OneNodeCluster(t *testing.T) {
	h := makeHelper(t, "node1")
	defer h.close()
	h.reserve("node1")
	h.empty()
	h.release("node1")
	h.reserve("node1")
	h.empty()
}

func TestPoolDynamicDistributor_AddNodes(t *testing.T) {
	h := makeHelper(t, "node1")
	defer h.close()

	h.reserve("node1")
	h.empty()
	h.add("node2")
	h.reserve("node2")
	h.empty()
}

func TestDynamicDistributor_RemoveNodesUnReserved(t *testing.T) {
	h := makeHelper(t, "node1", "node2")
	defer h.close()

	h.reserve("node1")
	h.remove("node2")
	h.empty()
}

func TestDynamicDistributor_RemoveNodesReserved(t *testing.T) {
	h := makeHelper(t, "node1", "node2")
	defer h.close()

	h.reserve("node1")
	h.reserve("node2")

	h.remove("node1")
	h.release("node1")
	h.release("node2")
	h.reserve("node2")
	h.empty()

}

func TestDynamicDistributor_ReAddedRemovedNodeIsAvailable(t *testing.T) {
	h := makeHelper(t, "node1", "node2")
	defer h.close()

	h.reserve("node1")
	h.reserve("node2")
	h.empty()
	h.remove("node1")
	h.add("node1")

	// TODO(dbentley): this behavior looks weird, because it
	// means that two clients have node1 reserved at the same
	// time...
	h.reserve("node1")
	h.empty()
}

type helper struct {
	t        *testing.T
	d        *distributor.PoolDistributor
	ch       chan []cluster.NodeUpdate
	reserved map[string]cluster.Node
}

func makeHelper(t *testing.T, node ...string) *helper {
	r := &helper{
		t:        t,
		ch:       make(chan []cluster.NodeUpdate),
		reserved: make(map[string]cluster.Node),
	}
	nodes := []cluster.Node{}
	for _, n := range node {
		nodes = append(nodes, memory.NewIdNode(n))
	}

	r.d = distributor.NewPoolDistributor(nodes, r.ch)
	return r
}

func (h *helper) close() {
	h.d.Close()
}

func (h *helper) reserve(expected string) {
	actual := <-h.d.Reserve
	if string(actual.Id()) != expected {
		h.t.Fatalf("reserved wrong node %v; expected %v", actual, expected)
	}
	h.reserved[string(actual.Id())] = actual
}

func (h *helper) empty() {
	select {
	case n := <-h.d.Reserve:
		h.t.Fatalf("not empty: %v", n)
	default:
	}
}

func (h *helper) release(node string) {
	n := h.reserved[node]
	h.d.Release <- n
}

func (h *helper) add(node string) {
	update := cluster.NewAdd(memory.NewIdNode(node))
	h.ch <- []cluster.NodeUpdate{update}
}

func (h *helper) remove(node string) {
	update := cluster.NewRemove(cluster.NodeId(node))
	h.ch <- []cluster.NodeUpdate{update}
}
