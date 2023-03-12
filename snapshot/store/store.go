// This package defines the Store interfaces for reading and writing bundles
// (or any artifact data) to some underlying system, and Store implementations.
package store

import (
	"io"
	"time"

	"github.com/wisechengyi/scoot/common/stats"
)

var DefaultTTL time.Duration = time.Hour * 24 * 180 //180 days. If zero, no ttl will be applied by default.
const (
	// DefaultTTLKey is primarily used for communicating ttl(RFC1123) over http.
	DefaultTTLKey string = "x-scoot-expires"
	// DefaultTTLFormat is to specify the format for parsing ttl(RFC1123).
	DefaultTTLFormat string = time.RFC1123
)

// Stores should generally support TTL, at this time httpStore and groupcacheStore implement it.
type TTLValue struct {
	TTL    time.Time
	TTLKey string
}

// TTLConfig configures defaults for stores.
type TTLConfig struct {
	TTL       time.Duration
	TTLKey    string
	TTLFormat string
}

// Gets a TTLValue based on Now given a TTLConfig, or nil
func GetTTLValue(c *TTLConfig) *TTLValue {
	if c != nil {
		return &TTLValue{TTL: time.Now().Add(c.TTL), TTLKey: c.TTLKey}
	}
	return nil
}

// Return a time.Duration based on Now given a TTLValue specifying a fixed time.
// Returns a zero duration if TTLValue is nil or if duration would be negative.
func GetDurationTTL(t *TTLValue) time.Duration {
	var d time.Duration
	if t != nil {
		d = t.TTL.Sub(time.Now())
	}
	if d < 0 {
		d = 0
	}
	return d
}

// Resource encapsulates a Store resource and embeds an io.ReadCloser around the data
// Length: length in bytes of data to be read or written
// TTLValue: TTL value for the resource, or nil if not supporting TTL
type Resource struct {
	io.ReadCloser
	Length   int64
	TTLValue *TTLValue
}

// NewResource constructs a new resource.
func NewResource(rc io.ReadCloser, l int64, ttlv *TTLValue) *Resource {
	return &Resource{ReadCloser: rc, Length: l, TTLValue: ttlv}
}

// Read-only operations on store, limited for now to a couple essential functions.
type StoreRead interface {
	// Check if the bundle exists. Not guaranteed to be any cheaper than actually reading the bundle.
	Exists(name string) (bool, error)

	// Open the bundle for streaming read. It is the caller's responsibility to call Close().
	OpenForRead(name string) (*Resource, error)

	// Get the base location, like a directory or base URI that the Store writes to
	Root() string
}

// Write operations on store, limited to a one-shot writing operation since bundles are immutable.
// If ttl config is nil then the store will use its defaults.
type StoreWrite interface {
	// Does a streaming write of the given bundle. There is no concept of partial writes (partial=failed).
	Write(name string, resource *Resource) error
}

// Combines read and write operations on store. This is what most of the code will use.
type Store interface {
	StoreRead
	StoreWrite
}

// Encapsulating struct for instances of Stores and accompanying configurations
type StoreConfig struct {
	Store  Store
	TTLCfg *TTLConfig
	Stat   stats.StatsReceiver
}
