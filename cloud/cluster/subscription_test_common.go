package cluster

import (
	"fmt"
	"testing"
	"time"
)

func CommonTestEndToEnd(t *testing.T, c chan []NodeUpdate, ticker chan time.Time, setResult func([]string, error)) {
	// Handy closures that save us having to pass around t, c, etc.
	var expected []NodeUpdate
	add := func(addr string) {
		expected = append(expected, NodeUpdate{
			UpdateType: NodeAdded,
			Id:         NodeId(addr),
		})
	}
	remove := func(addr string) {
		expected = append(expected, NodeUpdate{
			UpdateType: NodeRemoved,
			Id:         NodeId(addr),
		})
	}
	done := func() {
		assertUpdates(t, c, expected)
		expected = nil
	}

	setResult([]string{"host1:1234", "host1:2222", "host2:80"}, nil)
	tick(ticker)
	add("host1:1234")
	add("host1:2222")
	add("host2:80")
	done()

	setResult([]string{"host1:1234", "host1:2222"}, nil)
	tick(ticker)
	remove("host2:80")
	done()

	tick(ticker)
	done()

	setResult(nil, fmt.Errorf("Some error"))
	tick(ticker)
	done()

	setResult([]string{"host1:1234", "host1:2222"}, nil)
	tick(ticker)
	done()

	setResult([]string{"host1:2222", "host3:1234"}, nil)
	tick(ticker)
	add("host3:1234")
	remove("host1:1234")
	done()

	setResult([]string{"host1:2222", "host3:1234", "host4:1234"}, nil)
	tick(ticker)
	close(ticker)
	add("host4:1234")
	done()
	update, ok := <-c
	if ok {
		t.Fatalf("channel should be closed but got %v", update)
	}
}

func assertUpdates(t *testing.T, c chan []NodeUpdate, expecteds []NodeUpdate) {
	if len(expecteds) == 0 {
		select {
		case updates := <-c:
			t.Fatalf("unexpected updates: %v", updates)
		default:
			return
		}
	}

	actuals := <-c
	if len(actuals) != len(expecteds) {
		t.Fatalf("lengths don't match. updates: %v %v; expected %v %v", len(actuals), actuals, len(expecteds), expecteds)
	}
	for i, actual := range actuals {
		expected := expecteds[i]
		if actual.UpdateType != expected.UpdateType ||
			string(actual.Id) != string(expected.Id) {
			t.Fatalf("bad update at index %v: %v; expected %v", i, actual, expected)
		}
	}
}

func tick(ch chan time.Time) {
	ch <- time.Now()
}
