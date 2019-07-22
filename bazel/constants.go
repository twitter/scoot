package bazel

const (
	// Instance-related constants
	DefaultInstanceName = ""

	// Resource storage-related constants
	StorePrefix = "blob"

	// Snapshot-related constants
	SnapshotIDPrefix = "bz"
	InvalidIDMsg     = "Expected ID to be of format bz-<sha256>-<sizeBytes>, was"

	// Nil-data/Empty SHA-256 data
	EmptySha  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	EmptySize = int64(0)

	// GRPC Server connection-related setting limits (defaults)
	MaxSimultaneousConnections = 0 // limits total simultaneous connections via the Listener
	MaxRequestsPerSecond       = 0 // limits total incoming requests allowed per second
	MaxRequestsBurst           = 0 // allows this many requests in a burst faster than MaxRPS average
	MaxConcurrentStreams       = 0 // limits concurrent streams _per client_
	MaxConnIdleMins            = 1 // limits open idle connections before the server closes them
)
