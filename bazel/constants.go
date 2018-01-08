package bazel

const (
	// Instance-related constants
	DefaultInstanceName = ""

	// Resource storage-related constants
	StorePrefix = "blob"

	// Snapshot-related constants
	SnapshotIDPrefix = "bz"
	InvalidIDMsg     = "Expected ID to be of format bz-<sha256>-<sizeBytes>, was"
)
