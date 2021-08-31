// Cluster provides the means for coordinating the schedulers and workers that
// make up a Scoot system. This is achieved mainly through the Cluster type,
// individual Nodes, and Subscriptions to cluster changes.
package cluster

import (
	"sort"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/stats"
)

var ClusterUpdateLoopFrequency time.Duration = time.Duration(250) * time.Millisecond
var ClusterChBufferSize = 100

// cluster implementation of Cluster
type Cluster struct {
	state *state

	stat stats.StatsReceiver

	// latestFetchedNodesCh, contains the latest node list from fetcher.
	fetchedNodesCh chan []Node

	// nodesReqCh, used (by groupcache) to request the current list of nodes
	NodesReqCh chan chan []Node

	// nodesUpdatesChan, the node updates that the scheduler needs to process.
	NodesUpdatesCh chan []NodeUpdate

	useNodesUpdatesCh bool

	priorNodeUpdateTime  time.Time
	priorFetchUpdateTime time.Time
}

// Cluster's ch channel accepts []Node and []NodeUpdate types, which then
// get passed to its state to either SetAndDiff or UpdateAndFilter
func NewCluster(stat stats.StatsReceiver, initialNodes []Node, fetchedNodesCh chan []Node, useNodesUpdatesCh bool) (chan []NodeUpdate, chan chan []Node) {
	s := makeState(initialNodes)
	var nodesUpdatesCh chan []NodeUpdate = nil
	var nodesReqCh chan chan []Node = nil
	if useNodesUpdatesCh {
		nodesUpdatesCh = make(chan []NodeUpdate, common.DefaultClusterChanSize)
	} else {
		nodesReqCh = make(chan chan []Node)
	}
	c := &Cluster{
		state:                s,
		stat:                 stat,
		fetchedNodesCh:       fetchedNodesCh,
		NodesReqCh:           nodesReqCh,
		NodesUpdatesCh:       nodesUpdatesCh,
		useNodesUpdatesCh:    useNodesUpdatesCh,
		priorNodeUpdateTime:  time.Now(),
		priorFetchUpdateTime: time.Now(),
	}
	if stat == nil {
		c.stat = stats.NilStatsReceiver()
	}
	go c.loop()
	return nodesUpdatesCh, nodesReqCh
}

// loop continuously get the latest list of nodes (on latestNodeList, set by fetcher) and
// create a NodeUpdateList (scheduler will get this list update it's cluster state) and
// respond to any request for the node list (groupcache uses this mechanism)
func (c *Cluster) loop() {
	ticker := time.NewTicker(ClusterUpdateLoopFrequency)
	for range ticker.C {
		lastestNodeList := c.getLatestNodesList()
		if lastestNodeList != nil {
			sort.Sort(NodeSorter(lastestNodeList))
			updates := c.state.setAndDiff(lastestNodeList)
			if c.NodesUpdatesCh != nil {
				c.addToCurrentNodeUpdates(updates)
			}
		}

		if c.NodesReqCh != nil {
			select {
			case respCh := <-c.NodesReqCh:
				respCh <- c.getNodes()
			default:
			}
		}
	}
}

// pull up to ClusterChBufferSize node lists off the fetchedNodesCh
// keep only the latest list of nodes from the fetcher
func (c *Cluster) getLatestNodesList() []Node {
	var currentNodes []Node
	haveFetchedNodes := false
LOOP:
	for i := 0; i < ClusterChBufferSize; i++ {
		select {
		case currentNodes = <-c.fetchedNodesCh:
			haveFetchedNodes = true
		default:
			break LOOP
		}
	}
	if !haveFetchedNodes {
		return c.getNodes()
	}
	return currentNodes
}

// addToCurrentNodeUpdates accumulate the node updates.  These will be
// the node updates that the scheduler's cluster state has not yet seen
func (c *Cluster) addToCurrentNodeUpdates(updates []NodeUpdate) {
	c.NodesUpdatesCh <- updates
	if len(updates) > 0 {
		log.Infof("cluster has %d new node updates", len(updates))
		// record time since last time saw node
		c.stat.Gauge(stats.ClusterNodeUpdateFreqMs).Update(time.Since(c.priorNodeUpdateTime).Milliseconds())
		c.priorNodeUpdateTime = time.Now()
	}
}

// GetNodes get (a copy of) the list of nodes last fetched by fetcher
func (c *Cluster) getNodes() []Node {
	ret := []Node{}
	for _, node := range c.state.nodes {
		ret = append(ret, node)
	}
	return ret
}
