package stats

import (
	"time"
)

// wraps the stdlib time.Ticker struct, allows for mocking in tests
type StatsTicker interface {
	C() <-chan time.Time
	Stop()
}

type statsTicker struct {
	*time.Ticker
}

func (s *statsTicker) C() <-chan time.Time { return s.Ticker.C }

func NewStatsTicker(dur time.Duration) StatsTicker {
	return &statsTicker{time.NewTicker(dur)}
}

// Defines the calls we make to the stdlib time package. Allows for overriding in tests.
type StatsTime interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTicker(d time.Duration) StatsTicker
}

type defaultStatsTime struct{}

func (defaultStatsTime) Now() time.Time                        { return time.Now() }
func (defaultStatsTime) Since(t time.Time) time.Duration       { return time.Since(t) }
func (defaultStatsTime) NewTicker(d time.Duration) StatsTicker { return NewStatsTicker(d) }

var stdlibStatsTime = defaultStatsTime{}

// Returns a StatsTime instance backed by the stdlib 'time' package
func DefaultStatsTime() StatsTime { return stdlibStatsTime }

// Testing
type testStatsTime struct {
	now   time.Time
	since time.Duration
	ch    <-chan time.Time
}
type testStatsTicker struct {
	*time.Ticker
	ch <-chan time.Time
}

func (t testStatsTime) Now() time.Time                      { return t.now }
func (t testStatsTime) Since(time.Time) time.Duration       { return t.since }
func (t testStatsTime) NewTicker(time.Duration) StatsTicker { return &testStatsTicker{ch: t.ch} }
func (t *testStatsTicker) C() <-chan time.Time              { return t.ch }
func (t *testStatsTicker) Stop()                            {}

func DefaultTestTime() StatsTime {
	return testStatsTime{time.Unix(0, 0), 0, make(chan time.Time)}
}
func NewTestTime(now time.Time, since time.Duration, ch <-chan time.Time) StatsTime {
	return testStatsTime{now, since, ch}
}
