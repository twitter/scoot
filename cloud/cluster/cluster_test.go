package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClusterUpdates(t *testing.T) {
	wait := time.Second

	h := makeHelper(t)
	h.setFetchedNodes("node1")
	h.assertNodeUpdates(t, []NodeUpdate{h.makeUpdate("node1", NodeAdded)}, wait)
	h.setFetchedNodes()
	h.assertNodeUpdates(t, []NodeUpdate{h.makeUpdate("node1", NodeRemoved)}, wait)
	h.setFetchedNodes("node1", "node2")
	h.assertNodeUpdates(t, []NodeUpdate{h.makeUpdate("node1", NodeAdded), h.makeUpdate("node2", NodeAdded)}, wait)
	h.setFetchedNodes("node2")
	h.assertNodeUpdates(t, []NodeUpdate{h.makeUpdate("node1", NodeRemoved)}, wait)
	h.setFetchedNodes("node1", "node2", "node3", "node5")
	h.setFetchedNodes("node1", "node2", "node3", "node4")
	h.assertNodeUpdates(t, []NodeUpdate{h.makeUpdate("node1", NodeAdded), h.makeUpdate("node3", NodeAdded), h.makeUpdate("node4", NodeAdded)}, wait)
}

// Below here are helpers that make it easy to write more fluent tests.

type helper struct {
	t            *testing.T
	c            Cluster
	currentNodes []Node
}

func makeHelper(t *testing.T) *helper {
	h := &helper{t: t}
	h.c = NewCluster(nil)
	h.currentNodes = []Node{}
	return h
}

func (h *helper) assertNodeUpdates(t *testing.T, expectedUpdates []NodeUpdate, maxWait time.Duration) {
	start := time.Now()
	ticker := time.NewTicker(ClusterUpdateLoopFrequency)
	defer ticker.Stop()
	var updates []NodeUpdate
	for range ticker.C {

		if time.Since(start) > maxWait {
			assert.Fail(t, fmt.Sprintf("max time exceeded waiting for %v", expectedUpdates))
			return
		}

		updates = h.c.RetrieveCurrentNodeUpdates()

		if len(updates) > 0 {
			// validate the updates
			if len(expectedUpdates) != len(updates) {
				continue
			}
			found := true
			for _, eUpdate := range expectedUpdates {
				found = false
				for _, update := range updates {
					if eUpdate.String() == update.String() {
						found = true
						break
					}
				}
				if !found {
					break
				}
			}
			if found {
				return
			}

			assert.Fail(t, fmt.Sprintf("expected %v, got %v", expectedUpdates, updates))
			return
		}
	}
}

func (h *helper) setFetchedNodes(nodes ...string) {
	asNodes := []Node{}
	for _, n := range nodes {
		asNodes = append(asNodes, NewIdNode(n))
	}
	// h.currentNodes = asNodes
	h.c.SetLatestNodesList(asNodes)
}

func (h *helper) makeUpdate(node string, updateType NodeUpdateType) NodeUpdate {
	if updateType == NodeAdded {
		return NewAdd(NewIdNode(node))
	}
	return NewRemove((NodeId(node)))
}
