package memory_test

import (
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/memory"
	"testing"
)

func TestMembers(t *testing.T) {
	h := makeHelper(t)
	defer h.close()
	h.assertMembers()
	h.add("node1")
	h.assertMembers("node1")
	h.remove("node1")
	h.assertMembers()
	h.add("node1", "node2")
	h.assertMembers("node1", "node2")
	h.add("node1")
	h.assertMembers("node1", "node2")
	h.remove("node1", "node2")
	h.assertMembers()
	// Try confusing it by removing a nonexisting node
	h.remove("node3")
	h.assertMembers()
	h.add("node3", "node4")
	h.assertMembers("node3", "node4")
}

func TestSubscribe(t *testing.T) {
	h := makeHelper(t)
	defer h.close()
	h.assertMembers()
	s := h.subscribe()
	defer s.Closer.Close()
	h.assertInitialMembers(s)
	h.add("node1")
	h.assertUpdates(s, add("node1"))
	h.add("node2")
	h.add("node3")
	h.remove("node1")
	h.assertUpdates(s, add("node2"), add("node3"), remove("node1"))

	// Add a second subscription
	s2 := h.subscribe()
	defer s2.Closer.Close()
	h.assertInitialMembers(s2, "node2", "node3")

	// Now test that a subscriber that's not pulling doesn't block others
	h.add("node4")
	h.assertUpdates(s, add("node4"))
	h.remove("node2")
	h.assertUpdates(s, remove("node2"))
	h.assertMembers("node3", "node4")
	h.assertUpdates(s2, add("node4"), remove("node2"))
}

// Below here are helpers that make it easy to write more fluent tests.

type helper struct {
	t  *testing.T
	c  cluster.Cluster
	ch chan []cluster.NodeUpdate
}

func makeHelper(t *testing.T) *helper {
	h := &helper{t: t}
	h.ch = make(chan []cluster.NodeUpdate)
	h.c = memory.NewCluster(nil, h.ch)
	return h
}

func (h *helper) close() {
	h.c.Close()
}

func (h *helper) assertMembers(node ...string) {
	assertMembersEqual(makeNodes(node...), h.c.Members(), h.t)
}

func (h *helper) add(node ...string) {
	updates := []cluster.NodeUpdate{}
	for _, n := range node {
		updates = append(updates, add(n))
	}
	h.ch <- updates
}

func (h *helper) remove(node ...string) {
	updates := []cluster.NodeUpdate{}
	for _, n := range node {
		updates = append(updates, remove(n))
	}
	h.ch <- updates
}

func (h *helper) subscribe() cluster.Subscription {
	return h.c.Subscribe()
}

func (h *helper) assertInitialMembers(s cluster.Subscription, node ...string) {
	assertMembersEqual(s.InitialMembers, makeNodes(node...), h.t)
}

func (h *helper) assertUpdates(s cluster.Subscription, expected ...cluster.NodeUpdate) {
	// Calling Members makes sure that any updates sent have propagated from the cluster to the subscription
	h.c.Members()
	actual := <-s.Updates
	h.assertUpdatesEqual(expected, actual)
}

func (h *helper) assertUpdatesEqual(expected []cluster.NodeUpdate, actual []cluster.NodeUpdate) {
	if len(expected) != len(actual) {
		h.t.Fatalf("unequal updates: %v %v", expected, actual)
	}
	for i, ex := range expected {
		act := actual[i]
		if ex.UpdateType != act.UpdateType || ex.Id != act.Id {
			h.t.Fatalf("unequal updates: %v %v (from %v %v)", ex, act, expected, actual)
		}
	}
}

func makeNodes(node ...string) []cluster.Node {
	r := []cluster.Node{}
	for _, n := range node {
		r = append(r, memory.NewIdNode(n))
	}
	return r
}

func add(node string) cluster.NodeUpdate {
	return cluster.NewAdd(memory.NewIdNode(node))
}

func remove(node string) cluster.NodeUpdate {
	return cluster.NewRemove(cluster.NodeId(node))
}

func assertMembersEqual(expected []cluster.Node, actual []cluster.Node, t *testing.T) {
	if len(expected) != len(actual) {
		t.Fatalf("unequal members: %v %v", expected, actual)
	}
	for i, ex := range expected {
		act := actual[i]
		if ex.Id() != act.Id() {
			t.Fatalf("unequal members: %v %v (from %v %v)", ex.Id(), act.Id(), expected, actual)
		}
	}
}
