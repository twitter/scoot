package runner

import (
	"io"
)

// OutputCreator lets clients create new Outputs so they can save data.
// This is how Runner can save stdout and stderr.
// OutputCreator is the filesystem that creates many Outputs; Output is one file in that.
// (NB: Saver is temporary until we save output into a new Snapshot)
type OutputCreator interface {
	// Create an output for the given ID
	Create(id string) (Output, error)
}

// Output is a sink for one file's worth of output
type Output interface {
	// Write (and close) straight to the Output
	io.WriteCloser

	// A URI to this Output. Clients can read data by accessing the URI.
	// This lets us change how we save, because we can write to a local file,
	// or hdfs, or s3, or any other addressable store.
	// Returns a resource path prefixed with, for example, "file://" or "hdfs://".
	URI() string

	// Copies (if necessary) the URI target to local file and returns that absolute file path.
	AsFile() string
}
