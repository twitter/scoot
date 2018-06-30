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

	// Execution constants
	TaskIDPrefix   = "Bazel_ExecuteRequest"
	CommandDefault = "BZ_PLACEHOLDER"
)
