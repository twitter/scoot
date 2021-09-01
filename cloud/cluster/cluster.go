// Cluster provides the means for coordinating the schedulers and workers that
// make up a Scoot system. This is achieved mainly through the Cluster type,
// individual Nodes, and Subscriptions to cluster changes.
package cluster

import (
	"sort"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

var ClusterChBufferSize = 100

// cluster implementation of Cluster
type Cluster struct {
	state *state

	stat stats.StatsReceiver

	updateFreq time.Duration

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

// NewCluster creates a Cluster object and starts its processing loop and returns either a nodes updates channel
// (reporting node add/remove events) or nodes list channel (reporting the current list of nodes).
// The processing loop continuously gets the latest nodes list from the fetched nodes channel and either
// put the list of added/removed nodes on the nodes updates channel or if the nodes list is requested put the
// current list of nodes on the nodes list channel
func NewCluster(stat stats.StatsReceiver, fetcher Fetcher, useNodesUpdatesCh bool, updateFreqMs time.Duration, chBufferSize int) (chan []NodeUpdate, chan chan []Node) {
	fetchedNodesCh := StartFetchCron(fetcher, updateFreqMs, chBufferSize)

	s := makeState([]Node{})
	var updatesCh chan []NodeUpdate = nil
	var nodesReqCh chan chan []Node = nil
	if useNodesUpdatesCh {
		updatesCh = make(chan []NodeUpdate, chBufferSize)
	} else {
		nodesReqCh = make(chan chan []Node)
	}
	c := &Cluster{
		state:                s,
		updateFreq:           updateFreqMs,
		stat:                 stat,
		fetchedNodesCh:       fetchedNodesCh,
		NodesReqCh:           nodesReqCh,
		NodesUpdatesCh:       updatesCh,
		useNodesUpdatesCh:    useNodesUpdatesCh,
		priorNodeUpdateTime:  time.Now(),
		priorFetchUpdateTime: time.Now(),
	}
	if stat == nil {
		c.stat = stats.NilStatsReceiver()
	}
	go c.loop()

	return updatesCh, nodesReqCh
}

// loop continuously get the latest list of nodes (on channel, typically set by fetcher).
// If the cluster is using the nodes updates channel, process the nodes putting the list
// of nodeAdd, nodeRemove entries on the nodes updates channel.  Otherwise if a list of nodes
// has been requested, put the most latest list of nodes on the nodes list channel
func (c *Cluster) loop() {
	ticker := time.NewTicker(c.updateFreq)
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

// pull (up to ClusterChBufferSize) node lists off the fetchedNodesCh
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

// addToCurrentNodeUpdates put the node updates on the nodes updates channel.
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
