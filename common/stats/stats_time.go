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
