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
	Time = NewTestTime(time.Unix(0, 0), time.Nanosecond, ct)
	defer close(ct)

	// Does first capture only after 5ns has passed.
	latched := time.Nanosecond * 5
	statIface, cancelFn := NewLatchedStatsReceiver(latched)
	stat := statIface.(*defaultStatsReceiver)
	defer cancelFn()

	// Registry should not be captured until we accrue measurements.
	stat.Counter("counter")
	ct <- Time.Now()
	rendered := string(stat.Render(true))
	if rendered != "{}" {
		t.Fatal("Expected empty latch with time=0: ", rendered)
	}

	// Captured registry should be updated here and render should pick that up.
	ct <- Time.Now().Add(time.Minute)
	rendered = string(stat.Render(true))
	if rendered == "{}" {
		t.Fatal("Expected non-empty latch with time=0: ", rendered)
	}
	if stat.Counter().Count() != 0 {
		t.Fatal("Expected counter to be cleared after non-latch")
	}
}
