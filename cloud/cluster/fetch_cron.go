package cluster

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type fetchCron struct {
	freq           time.Duration
	f              Fetcher
	fetchedNodesCh chan []Node
	stat           stats.StatsReceiver
	priorNumNodes  int
	priorFetchTime time.Time
}

// Defines the way in which a full set of Nodes in a Cluster is retrieved
type Fetcher interface {
	Fetch() ([]Node, error)
}

// Given a Fetcher implementation and a duration, start a ticker loop that
// fetches the current nodes and updates cluster's latestNodeList with this list
func StartFetchCron(f Fetcher, freq time.Duration, fetchBufferSize int, stat stats.StatsReceiver) chan []Node {
	c := &fetchCron{
		freq:           freq,
		f:              f,
		fetchedNodesCh: make(chan []Node, fetchBufferSize),
		stat:           stat,
	}
	if stat == nil {
		c.stat = stats.NilStatsReceiver()
	}
	log.Infof("starting fetch cron with frequency %s and channel buffer size %d", freq, fetchBufferSize)
	go c.loop()
	return c.fetchedNodesCh
}

func (c *fetchCron) loop() {
	c.doFetch() // initial fetch

	tickCh := time.NewTicker(c.freq)
	for range tickCh.C {
		c.doFetch() // cron fetch
	}
}

func (c *fetchCron) doFetch() {
	start := time.Now()
	nodes, err := c.f.Fetch()
	fetchDuration := time.Since(start)

	if err != nil {
		// TODO(rcouto): Correctly handle as many errors as possible
		c.stat.Gauge(stats.ClusterFetchedError).Update(1)
		c.stat.Gauge(stats.ClusterNumFetchedNodes).Update(0)
		return
	}
	c.fetchedNodesCh <- nodes

	// logging and stats
	// note: if fetchedNodesCh is full, FetchFreqMs will include time waiting for
	// the channel to become free (typically it is cluster pulling node lists from the channel)
	if len(nodes) != c.priorNumNodes {
		log.Infof("num fetched nodes changed from %d to %d", c.priorNumNodes, len(nodes))
		c.priorNumNodes = len(nodes)
	}
	c.stat.Gauge(stats.ClusterFetchFreqMs).Update(time.Since(c.priorFetchTime).Milliseconds())
	c.stat.Gauge(stats.ClusterFetchDurationMs).Update(fetchDuration.Milliseconds())
	c.stat.Gauge(stats.ClusterNumFetchedNodes).Update(int64(len(nodes)))
	c.priorFetchTime = time.Now()
}
