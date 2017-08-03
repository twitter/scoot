package stats

import (
	"regexp"
	"testing"
	"time"
)

func TestPrecisionChange(t *testing.T) {
	stat := DefaultStatsReceiver().(*defaultStatsReceiver)
	if stat.precision != time.Millisecond {
		t.Fatal("Default precision should be nanos.")
	}

	statp := stat.Precision(time.Nanosecond).(*defaultStatsReceiver)
	if stat.precision != time.Millisecond {
		t.Fatal("Default precision should still nanos.")
	}
	if statp.precision != time.Nanosecond {
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
	ct := make(chan time.Time)
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
	stat.Latency("latency").Stop()

	rendered := string(stat.Render(false))
	matched, err := regexp.MatchString(`"count":1,`, rendered)
	if !matched {
		t.Fatal("Expected current stats in render", rendered, err)
	}

	rendered = string(stat.Render(false))
	matched, err = regexp.MatchString(`"count":0,`, rendered)
	if !matched {
		t.Fatal("Expected clearing of stats after render", rendered, err)
	}
}

func TestLatching(t *testing.T) {
	ct := make(chan time.Time)
	Time = NewTestTime(time.Unix(0, 0), 0, ct)
	statIface, cancelFn := NewLatchedStatsReceiver(time.Second)
	stat := statIface.(*defaultStatsReceiver)
	defer cancelFn()

	// Captured registry should initially be empty even though we added a latency metric.
	stat.Latency("latency")
	rendered := string(stat.Render(false))
	if rendered != "{}" {
		t.Fatal("Expected empty latch with time=0: ", rendered)
	}

	// Captured registry should still be empty.
	ct <- Time.Now().Add(1)
	rendered = string(stat.Render(false))
	if rendered != "{}" {
		t.Fatal("Expected empty latch with time=1: ", rendered)
	}

	// Should be captured after enough time has passed.
	ct <- Time.Now().Add(time.Minute)
	rendered = string(stat.Render(false))
	if rendered == "{}" {
		t.Fatal("Expected non-empty latch with time=1m: ", rendered)
	}
}
