package local

import (
	"testing"
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
)

func TestEndToEnd(t *testing.T) {
	ticker := make(chan time.Time)

	fetcher := &fakeFetcher{}
	ch := cluster.MakeDiffSubscription(ticker, fetcher)
	setResult := func(result []string, err error) {
		fetcher.setResult(nodes(result...), err)
	}
	cluster.CommonTestEndToEnd(t, ch, ticker, setResult)
}
