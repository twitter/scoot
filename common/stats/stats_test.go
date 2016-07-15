package stats

import (
	"testing"
	"time"
)

func TestPrecisionChange(t *testing.T) {
	stat := DefaultStatsReceiver().(*defaultStatsReceiver)
	if stat.precision != time.Nanosecond {
		t.Fatal("Default precision should be nanos.")
	}

	statp := stat.Precision(time.Millisecond).(*defaultStatsReceiver)
	if stat.precision != time.Nanosecond {
		t.Fatal("Default precision should still nanos.")
	}
	if statp.precision != time.Millisecond {
		t.Fatal("New stat precision should be millis.")
	}
}

func TestScopeChange(t *testing.T) {
	stat := DefaultStatsReceiver().(*defaultStatsReceiver)
	if len(stat.scope) != 0 {
		t.Fatal("Default scope should be empty.")
	}

	statp := stat.Scope("a/b", "c").(*defaultStatsReceiver)
	if len(stat.scope) != 0 {
		t.Fatal("Default scope should still empty.")
	}
	if len(statp.scope) != 2 || statp.scope[0] != "a_SLASH_b" || statp.scope[1] != "c" {
		t.Fatal("Invalid scope value: ", statp.scope)
	}
	if statp.scopedName("d") != "a_SLASH_b/c/d" {
		t.Fatal("Invalid scope name: " + statp.scopedName("d"))
	}
}

func TestRegister(t *testing.T) {
	reg := NewFinagleStatsRegistry()
	if reg.GetOrRegister("counter", NewCounter()) == nil {
		t.Fatal("Registry did not save instrument")
	}
	if reg.GetOrRegister("gauge", NewGauge()) == nil {
		t.Fatal("Registry did not save instrument")
	}
	if reg.GetOrRegister("gaugeFloat", NewGaugeFloat()) == nil {
		t.Fatal("Registry did not save instrument")
	}
	if reg.GetOrRegister("histogram", NewHistogram()) == nil {
		t.Fatal("Registry did not save instrument")
	}
	if reg.GetOrRegister("latency", NewLatency()) == nil {
		t.Fatal("Registry did not save instrument")
	}
}

func TestMarshal(t *testing.T) {
	ct := make(chan time.Time, 2)
	Time = NewTestTime(time.Unix(0, 0), time.Nanosecond*5, ct)
	defer close(ct)

	reg := NewFinagleStatsRegistry()
	reg.GetOrRegister("counter", NewCounter()).(Counter).Inc(1)
	reg.GetOrRegister("gauge", NewGauge()).(Gauge).Update(2)

	reg.GetOrRegister("latency", NewLatency()).(Latency).Time().Stop()
	Time = NewTestTime(time.Unix(0, 0), time.Nanosecond*10, ct)
	reg.GetOrRegister("latency", NewLatency()).(Latency).Time().Stop()

	bytes, err := reg.(MarshalerPretty).MarshalJSONPretty()
	expected :=
		`{
  "counter": 1,
  "gauge": 2,
  "latency.avg": 7.5,
  "latency.count": 2,
  "latency.max": 10,
  "latency.min": 5,
  "latency.p50": 7.5,
  "latency.p90": 10,
  "latency.p95": 10,
  "latency.p99": 10,
  "latency.p999": 10,
  "latency.p9999": 10,
  "latency.sum": 15
}`
	if string(bytes) != expected {
		t.Fatal("Wrong json marshal output: ", string(bytes), err)
	}
}

func TestNonLatching(t *testing.T) {
	stat := DefaultStatsReceiver().(*defaultStatsReceiver)
	stat.Counter("counter").Inc(1)

	rendered := string(stat.Render(false))
	if rendered != `{"counter":{"count":1}}` {
		t.Fatal("Expected current stats in render", rendered)
	}

	rendered = string(stat.Render(false))
	if rendered != `{"counter":{"count":0}}` {
		t.Fatal("Expected clearing of stats after render", rendered)
	}
}

func TestLatching(t *testing.T) {
	ct := make(chan time.Time, 2)
	Time = NewTestTime(time.Unix(0, 0), 0, ct)
	statIface, cancelFn := NewLatchedStatsReceiver(time.Second)
	stat := statIface.(*defaultStatsReceiver)
	defer cancelFn()

	// Captured registry should initially be empty even though we added a counter.
	stat.Counter("counter")
	rendered := string(stat.Render(true))
	if rendered != "{}" {
		t.Fatal("Expected empty latch with time=0: ", rendered)
	}

	// Captured registry should still be empty.
	ensureChannelSend(cap(ct), func() { ct <- Time.Now().Add(1) })
	rendered = string(stat.Render(true))
	if rendered != "{}" {
		t.Fatal("Expected empty latch with time=1: ", rendered)
	}

	// Should be captured after enough time has passed.
	ensureChannelSend(cap(ct), func() { ct <- Time.Now().Add(time.Minute) })
	rendered = string(stat.Render(true))
	if rendered == "{}" {
		t.Fatal("Expected non-empty latch with time=0: ", rendered)
	}
}

// Three sends are required to ensure the first is handled, not just goroutine-queued...
// This function may seem hacky, but it's the simplest way to do ordered testing when we're
// select'ing on two+ channels: one channel to trigger updates, another accepting
// requests and immediately responding with a cached update.
//
// The Memory Model is light on details regarding select'ing from multiple channels,
// but testing shows that the following sequence (or something similar) will happen
// for unbuffered channels:
// - first send is received/popped but doesn't enter the case statement. The channel is ready again.
// - second send is is received and blocks further sends as the first send is still pending.
// - third send blocks until the receive case statement for the first send is fully executed.
//
// Suggestions or further rationale welcomed.
func ensureChannelSend(chanCapacity int, sendFn func()) {
	for i := 0; i < chanCapacity+3; i++ {
		sendFn()
	}
}
