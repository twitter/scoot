// This package borrows from go-metrics so we can avoid exposing dependencies,
// and to give callers more flexibility in recording and formatting scoped stats.
//
// Original license: github.com/rcrowley/go-metrics/blob/master/LICENSE
//
package stats

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/rcrowley/go-metrics"
)

// For testing.
var Time StatsTime = DefaultStatsTime()

// Stats users can either reference this global receiver or construct their own.
var CurrentStatsReceiver StatsReceiver = NilStatsReceiver()

// Overridable instrument creation.
var NewCounter func() Counter = newMetricCounter
var NewGauge func() Gauge = newMetricGauge
var NewGaugeFloat func() GaugeFloat = newMetricGaugeFloat
var NewHistogram func() Histogram = newMetricHistogram
var NewLatency func() Latency = newLatency

// To check if pretty printing is supported.
type MarshalerPretty interface {
	MarshalJSONPretty() ([]byte, error)
}

//
// Similar to the go-metrics registry but with most methods removed.
//
// Note: the default StatsRegistry does not support the Latency metric.
type StatsRegistry interface {
	// Gets an existing metric or registers the given one.
	// The interface can be the metric to register if not found in registry,
	// or a function returning the metric for lazy instantiation.
	GetOrRegister(string, interface{}) interface{}

	// Unregister the metric with the given name.
	Unregister(string)

	// Call the given function for each registered metric.
	Each(func(string, interface{}))
}

//
// A registry wrapper for metrics that will be collected about the runtime
// performance of an application.
//
// A quick note about name elements: hierarchical names are stored using a '/'
// path separator. To avoid confusion, variadic name elements passed to any
// method will have '/' characters in their names replaced by the string
// "_SLASH_" before they are used internally. This is instead of failing,
// because sometimes counters are dynamically generated (i.e. with error
// names), and it is better to strip the path elements than to, for example,
// panic.
//
type StatsReceiver interface {
	// Return a stats receiver that will automatically namespace elements with
	// the given scope args.
	//
	//   statsReceiver.Scope("foo", "bar").Stat("baz")  // is equivalent to
	//   statsReceiver.Stat("foo", "bar", "baz")
	//
	Scope(scope ...string) StatsReceiver

	// If StatsRegistry supports the latency instrument:
	//
	// Returns a copy that can in turn create a Latency instrument that will use the
	// given precision as its display precision when the stats are rendered as
	// JSON. For example:
	//
	//   statsReceiver.Precision(time.Millisecond).Stat("foo_ms")
	//
	// means that the 'foo_ms' stat will have its nanosecond data points displayed
	// as milliseconds when rendered. Note that this does _not_ affect the
	// captured data in any way, only its display.
	//
	// If the given duration is <= 1ns, we will default to ns.
	Precision(time.Duration) StatsReceiver

	// Provides an event counter
	Counter(name ...string) Counter

	// Provides a histogram of sampled stats over time. Times output in
	// nanoseconds by default, but can be adjusted by using the Precision()
	// function.
	Latency(name ...string) Latency

	// Add a gauge, which holds an int64 value that can be set arbitrarily.
	Gauge(name ...string) Gauge

	// Add a gauge, which holds a float64 value that can be set arbitrarily.
	GaugeFloat(name ...string) GaugeFloat

	// Removes the given named stats item if it exists
	Remove(name ...string)

	// Construct a JSON string by marshaling the registry.
	Render(pretty bool) []byte
}

//
// DefaultStats is a small wrapper around a go-metrics like registry.
// Uses defaultStatsRegistry and sets latched duration to zero.
// Note: a <=0 latch means that the stats are reset on every call to Render().
func DefaultStatsReceiver() StatsReceiver {
	stat, _ := NewCustomStatsReceiver(nil, 0)
	return stat
}

// Like DefaultStatsReceiver() but latched interval is made explicit.
// Starts a goroutine that periodically captures all and clears select instruments.
// Note: setting latched to <=0 will disable latching so rendering/resetting is on demand.
func NewLatchedStatsReceiver(latched time.Duration) (stat StatsReceiver, cancelFn func()) {
	return NewCustomStatsReceiver(nil, latched)
}

// Like DefaultStatsReceiver() but registry and latched are made explicit.
func NewCustomStatsReceiver(makeRegistry func() StatsRegistry, latched time.Duration) (stat StatsReceiver, cancelFn func()) {
	if makeRegistry == nil {
		makeRegistry = func() StatsRegistry { return metrics.NewRegistry() }
	}
	reg := makeRegistry()
	defaultStat := &defaultStatsReceiver{
		makeRegistry: makeRegistry,
		registry:     reg,
		latched:      latched,
		precision:    time.Nanosecond,
		captured:     &reg,
	}
	cancel := func() {}

	if latched > 0 {
		log.Println("Starting latched stat goroutine.")
		var ctx context.Context
		ticker := Time.NewTicker(latched)
		ctx, cancel = context.WithCancel(context.Background())
		firstSnapshotAt := Time.Now().Add(latched).Truncate(latched)

		go func() {
			var t time.Time
		Loop:
			for {
				select {
				case <-ctx.Done():
					ticker.Stop()
					return
				case t = <-ticker.C():
					if t.Before(firstSnapshotAt) {
						continue Loop
					}
					defaultStat.capture()
					defaultStat.clear()
				}
			}
		}()
	}

	return defaultStat, cancel
}

type defaultStatsReceiver struct {
	makeRegistry func() StatsRegistry
	registry     StatsRegistry
	latched      time.Duration
	precision    time.Duration
	scope        []string
	captured     *StatsRegistry
}

func (s *defaultStatsReceiver) Scope(scope ...string) StatsReceiver {
	return &defaultStatsReceiver{s.makeRegistry, s.registry, s.latched, s.precision, s.scoped(scope...), s.captured}
}

func (s *defaultStatsReceiver) Precision(precision time.Duration) StatsReceiver {
	if precision < 1 {
		precision = 1
	}
	return &defaultStatsReceiver{s.makeRegistry, s.registry, s.latched, precision, s.scope, s.captured}
}

func (s *defaultStatsReceiver) Counter(name ...string) Counter {
	return s.registry.GetOrRegister(s.scopedName(name...), NewCounter).(Counter)
}

func (s *defaultStatsReceiver) Gauge(name ...string) Gauge {
	return s.registry.GetOrRegister(s.scopedName(name...), NewGauge).(Gauge)
}

func (s *defaultStatsReceiver) GaugeFloat(name ...string) GaugeFloat {
	return s.registry.GetOrRegister(s.scopedName(name...), NewGaugeFloat).(GaugeFloat)
}

func (s *defaultStatsReceiver) Histogram(name ...string) Histogram {
	return s.registry.GetOrRegister(s.scopedName(name...), NewHistogram).(Histogram)
}

func (s *defaultStatsReceiver) Latency(name ...string) Latency {
	//nl := func() Latency { return NewLatency().Precision(s.precision) }
	//return s.registry.GetOrRegister(s.scopedName(name...), nl).(Latency)
	// Can't do lazy instantiation since we may use metric.Registry and it can't cast lazy derived types.
	return s.registry.GetOrRegister(s.scopedName(name...), NewLatency().Precision(s.precision)).(Latency)
}

func (s *defaultStatsReceiver) Remove(name ...string) {
	s.registry.Unregister(s.scopedName(name...))
}

func (s *defaultStatsReceiver) Render(pretty bool) []byte {
	reg := s.registry
	if s.latched != 0 {
		reg = *s.captured // render from captured state when latched.
	}

	var err error
	var bytes []byte
	if mp, ok := reg.(MarshalerPretty); ok && pretty {
		bytes, err = mp.MarshalJSONPretty()
	} else {
		bytes, err = json.Marshal(reg)
	}

	if err != nil {
		panic("StatsRegistry bug, cannot be marshaled")
	}
	if s.latched == 0 {
		s.clear() // reset on every call to render when not latched.
	}
	return bytes
}

func (s *defaultStatsReceiver) capture() {
	reg := s.makeRegistry()
	s.captured = &reg
	s.registry.Each(func(name string, i interface{}) {
		switch m := i.(type) {
		case Counter:
			(*s.captured).GetOrRegister(name, m.Capture())
		case Gauge:
			(*s.captured).GetOrRegister(name, m.Capture())
		case GaugeFloat:
			(*s.captured).GetOrRegister(name, m.Capture())
		case Histogram:
			(*s.captured).GetOrRegister(name, m.Capture())
		case Latency:
			(*s.captured).GetOrRegister(name, m.Capture())
		default:
			log.Println("Unrecognized capture instrument: ", name, i)
		}
	})
}

func (s *defaultStatsReceiver) clear() {
	//log.Println("Clearing")
	s.registry.Each(func(name string, i interface{}) {
		switch m := i.(type) {
		case metrics.Counter:
			m.Clear()
		case metrics.Histogram:
			m.Clear()
		}
	})
}

func (s *defaultStatsReceiver) scoped(scope ...string) []string {
	for i, s := range scope {
		scope[i] = strings.Replace(s, "/", "_SLASH_", -1)
	}
	return append(s.scope[:], scope...)
}

func (s *defaultStatsReceiver) scopedName(scope ...string) string {
	return strings.Join(s.scoped(scope...), "/")
}

//
// NilStats ignores all stats operations.
//
func NilStatsReceiver(scope ...string) StatsReceiver {
	return &nilStatsReceiver{}
}

type nilStatsReceiver struct{}

func (s *nilStatsReceiver) Scope(scope ...string) StatsReceiver             { return s }
func (s *nilStatsReceiver) Precision(precision time.Duration) StatsReceiver { return s }
func (s *nilStatsReceiver) Counter(name ...string) Counter {
	return &metricCounter{&metrics.NilCounter{}}
}
func (s *nilStatsReceiver) Gauge(name ...string) Gauge {
	return &metricGauge{&metrics.NilGauge{}}
}
func (s *nilStatsReceiver) GaugeFloat(name ...string) GaugeFloat {
	return &metricGaugeFloat{&metrics.NilGaugeFloat64{}}
}
func (s *nilStatsReceiver) Histogram(name ...string) Histogram {
	return &metricHistogram{&metrics.NilHistogram{}}
}
func (s *nilStatsReceiver) Latency(name ...string) Latency {
	return newNilLatency()
}
func (s *nilStatsReceiver) Remove(name ...string)     {}
func (s *nilStatsReceiver) Render(pretty bool) []byte { return []byte{} }

//
// Minimally mirror go-metrics instruments.
//
// Counter
type Counter interface {
	Capture() Counter
	Clear()
	Count() int64
	Inc(int64)
}
type metricCounter struct{ metrics.Counter }

func (m *metricCounter) Capture() Counter { return &metricCounter{m.Snapshot()} }
func newMetricCounter() Counter           { return &metricCounter{metrics.NewCounter()} }

// Gauge
type Gauge interface {
	Capture() Gauge
	Update(int64)
	Value() int64
}
type metricGauge struct{ metrics.Gauge }

func (m *metricGauge) Capture() Gauge { return &metricGauge{m.Snapshot()} }
func newMetricGauge() Gauge           { return &metricGauge{metrics.NewGauge()} }

// GaugeFloat
type GaugeFloat interface {
	Capture() GaugeFloat
	Update(float64)
	Value() float64
}
type metricGaugeFloat struct{ metrics.GaugeFloat64 }

func (m *metricGaugeFloat) Capture() GaugeFloat { return &metricGaugeFloat{m.Snapshot()} }
func newMetricGaugeFloat() GaugeFloat           { return &metricGaugeFloat{metrics.NewGaugeFloat64()} }

// Viewable histogram without updates or capture.
type HistogramView interface {
	Mean() float64
	Count() int64
	Max() int64
	Min() int64
	Sum() int64
	Percentiles(ps []float64) []float64
}

// Histogram
type Histogram interface {
	HistogramView
	Capture() Histogram
	Update(int64)
}
type metricHistogram struct{ metrics.Histogram }

func (m *metricHistogram) Capture() Histogram { return &metricHistogram{m.Snapshot()} }
func newMetricHistogram() Histogram {
	return &metricHistogram{metrics.NewHistogram(metrics.NewUniformSample(1000))}
}

// Latency. Default implementation uses Histogram as its base.
type Latency interface {
	Capture() Latency
	Time() Latency //returns self.
	Stop()
	GetPrecision() time.Duration
	Precision(time.Duration) Latency //returns self.
}
type metricLatency struct {
	metrics.Histogram
	start     time.Time
	precision time.Duration
}
type nilLatency struct{}

func (l *metricLatency) Time() Latency { l.start = Time.Now(); return l }
func (l *metricLatency) Stop()         { l.Update(Time.Since(l.start).Nanoseconds()) }
func (l *metricLatency) Capture() Latency {
	return &metricLatency{l.Histogram.Snapshot(), l.start, l.precision}
}
func (l *metricLatency) GetPrecision() time.Duration {
	return l.precision
}
func (l *metricLatency) Precision(p time.Duration) Latency {
	if p < 1 {
		p = 1
	}
	l.precision = p
	return l
}
func newLatency() Latency {
	return &metricLatency{Histogram: metrics.NewHistogram(metrics.NewUniformSample(1000)), precision: time.Nanosecond}
}

func (l *nilLatency) Time() Latency                   { return l }
func (l *nilLatency) Stop()                           {}
func (l *nilLatency) Capture() Latency                { return l }
func (l *nilLatency) GetPrecision() time.Duration     { return 0 }
func (l *nilLatency) Precision(time.Duration) Latency { return l }
func newNilLatency() Latency                          { return &nilLatency{} }

//
// Twitter/Finagle style metrics
//
type finagleStatsRegistry struct {
	metrics.Registry
}

func NewFinagleStatsRegistry() StatsRegistry {
	return &finagleStatsRegistry{metrics.NewRegistry()}
}

type jsonMap map[string]interface{}

// MarshalJSON returns a byte slice containing a JSON representation of all
// the metrics in the Registry.
func (r *finagleStatsRegistry) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.MarshalAll())
}
func (r *finagleStatsRegistry) MarshalJSONPretty() ([]byte, error) {
	return json.MarshalIndent(r.MarshalAll(), "", "  ")
}
func (r *finagleStatsRegistry) MarshalAll() jsonMap {
	data := make(map[string]interface{}, 0)
	r.Each(func(name string, i interface{}) {
		switch stat := i.(type) {
		case Counter:
			data[name] = stat.Count()
		case Gauge:
			data[name] = stat.Value()
		case GaugeFloat:
			data[name] = stat.Value()
		case Histogram:
			h := stat.Capture()
			r.marshalHistogram(data, name, h, time.Nanosecond)
		case Latency:
			l := stat.Capture()
			r.marshalHistogram(data, name, l.(HistogramView), l.GetPrecision())
		default:
			log.Println("Unrecognized marshal instrument: ", name, i)
		}
	})
	return data
}
func (r *finagleStatsRegistry) marshalHistogram(
	data jsonMap,
	name string,
	hist HistogramView,
	precision time.Duration,
) {
	f64p := float64(precision)
	i64p := int64(precision)
	data[name+".avg"] = hist.Mean() / f64p
	data[name+".count"] = hist.Count()
	data[name+".max"] = hist.Max() / i64p
	data[name+".min"] = hist.Min() / i64p
	data[name+".sum"] = hist.Sum() / i64p

	pctls := hist.Percentiles(defaultPercentiles)
	for i, pctl := range pctls {
		data[name+"."+defaultPercentileLabels[i]] = pctl / f64p
	}
}

var defaultPercentiles = []float64{0.5, 0.9, 0.95, 0.99, 0.999, 0.9999}
var defaultPercentileLabels = []string{"p50", "p90", "p95", "p99", "p999", "p9999"}
