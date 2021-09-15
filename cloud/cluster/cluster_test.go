package cluster

import (
	"fmt"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/twitter/scoot/common/log/hooks"
)

func TestClusterUpdates(t *testing.T) {
	log.AddHook(hooks.NewContextHook())

	ff := &fakeFetcher{}
	wait := 3 * time.Second

	nodeUpdatesCh, _ := NewCluster(nil, ff, true, 10*time.Millisecond, 10)

	setFetchedNodes(ff, "node1")
	assertNodeUpdates(t, []NodeUpdate{makeUpdate("node1", NodeAdded)}, nodeUpdatesCh, wait)
	setFetchedNodes(ff)
	assertNodeUpdates(t, []NodeUpdate{makeUpdate("node1", NodeRemoved)}, nodeUpdatesCh, wait)
	setFetchedNodes(ff, "node1", "node2")
	assertNodeUpdates(t, []NodeUpdate{makeUpdate("node1", NodeAdded), makeUpdate("node2", NodeAdded)}, nodeUpdatesCh, wait)
	setFetchedNodes(ff, "node2")
	assertNodeUpdates(t, []NodeUpdate{makeUpdate("node1", NodeRemoved)}, nodeUpdatesCh, wait)
	setFetchedNodes(ff, "node1", "node2", "node3", "node5")
	setFetchedNodes(ff, "node1", "node2", "node3", "node4")
	assertNodeUpdates(t, []NodeUpdate{makeUpdate("node1", NodeAdded), makeUpdate("node3", NodeAdded), makeUpdate("node4", NodeAdded)}, nodeUpdatesCh, wait)
}

// Below here are utility functions that make it easy to write more fluent tests.

func assertNodeUpdates(t *testing.T, expectedUpdates []NodeUpdate, nodeUpdateCh chan []NodeUpdate, maxWait time.Duration) {
	start := time.Now()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var updates []NodeUpdate
	for range ticker.C {

		if time.Since(start) > maxWait {
			assert.Fail(t, fmt.Sprintf("max time exceeded waiting for %v", expectedUpdates))
			return
		}

		updates = <-nodeUpdateCh

		if len(expectedUpdates) == len(updates) {
			// validate the updates
			for i := range expectedUpdates {
				if expectedUpdates[i].String() != updates[i].String() {
					assert.Fail(t, fmt.Sprintf("expected %v, got %v", expectedUpdates, updates))
				}
			}
			break
		}
	}
}

func setFetchedNodes(fetcher *fakeFetcher, nodes ...string) {
	fetcher.setFakeNodes(nodes...)
}

func makeUpdate(node string, updateType NodeUpdateType) NodeUpdate {
	if updateType == NodeAdded {
		return NewAdd(NewIdNode(node))
	}
	return NewRemove((NodeId(node)))
}
