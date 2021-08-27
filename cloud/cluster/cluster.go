// Cluster provides the means for coordinating the schedulers and workers that
// make up a Scoot system. This is achieved mainly through the Cluster type,
// individual Nodes, and Subscriptions to cluster changes.
package cluster

import (
	"sort"
	"sync"
	"time"
)

var ClusterUpdateLoopFrequency time.Duration = time.Duration(250) * time.Millisecond

// Cluster represents a group of Nodes and has mechanism for
// setting current Node list and receiving correlating Node updates.
type Cluster interface {
	RetrieveCurrentNodeUpdates() []NodeUpdate // used by scheduler's cluster state to get updates
	SetLatestNodesList(nodes []Node)          // used by fetcher to give cluster the list of current nodes
	GetNodes() []Node
}

// cluster implementation of Cluster
type cluster struct {
	state *state

	// latestFetchedNodes, when not nil, contains the latest node list from fetcher.  When
	// cluster start processing the latestFetchedNodes, it grabs a copy an sets this to nil
	latestFetchedNodes   []Node
	latestFetchedNodesMu sync.RWMutex

	// currentNodeUpdates, the node updates that the scheduler needs to process.  When
	// scheduler gets the contents of currentNodeUpdates, currentNodeUpdates is reset to nil.
	currentNodeUpdates   []NodeUpdate
	currentNodeUpdatesMu sync.RWMutex
}

// Cluster's ch channel accepts []Node and []NodeUpdate types, which then
// get passed to its state to either SetAndDiff or UpdateAndFilter
func NewCluster() Cluster {
	s := makeState([]Node{})
	c := &cluster{
		state:              s,
		latestFetchedNodes: []Node{},
		currentNodeUpdates: []NodeUpdate{},
	}
	go c.loop()
	return c
}

// loop continuously get the latest list of nodes (on latestNodeList, set by fetcher) and
// create a NodeUpdateList (scheduler will get this list update it's cluster state)
func (c *cluster) loop() {
	ticker := time.NewTicker(ClusterUpdateLoopFrequency)
	for range ticker.C {
		lastestNodeList := c.getLatestNodesList()
		if lastestNodeList != nil {
			sort.Sort(NodeSorter(lastestNodeList))
			updates := c.state.setAndDiff(lastestNodeList)
			c.addToCurrentNodeUpdates(updates)
		}
	}
}

// SetLatestNodesList set the latest list of nodes seen by a fetcher
func (c *cluster) SetLatestNodesList(nodes []Node) {
	c.latestFetchedNodesMu.Lock()
	defer c.latestFetchedNodesMu.Unlock()
	c.latestFetchedNodes = nodes
}

// get the lastest list of nodes seen by a fetcher
func (c *cluster) getLatestNodesList() []Node {
	c.latestFetchedNodesMu.RLock()
	defer c.latestFetchedNodesMu.RUnlock()
	ret := c.latestFetchedNodes
	return ret
}

// addToCurrentNodeUpdates accumulate the node updates.  These will be
// the node updates that the scheduler's cluster state has not yet seen
func (c *cluster) addToCurrentNodeUpdates(updates []NodeUpdate) {
	c.currentNodeUpdatesMu.Lock()
	defer c.currentNodeUpdatesMu.Unlock()
	c.currentNodeUpdates = append(c.currentNodeUpdates, updates...)
}

// return the list of current node update (node updates that the scheduler's
// cluster state has not yet seen) and empty the current node update list
func (c *cluster) RetrieveCurrentNodeUpdates() []NodeUpdate {
	c.currentNodeUpdatesMu.Lock()
	defer c.currentNodeUpdatesMu.Unlock()
	ret := c.currentNodeUpdates
	c.currentNodeUpdates = []NodeUpdate{}
	return ret
}

func (c *cluster) GetNodes() []Node {
	c.latestFetchedNodesMu.RLock()
	defer c.latestFetchedNodesMu.RUnlock()
	ret := []Node{}
	for _, node := range c.state.nodes {
		ret = append(ret, node)
	}
	return ret
}
