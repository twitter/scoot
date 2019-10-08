package bundlestore

const (
	// Store-related environment variables
	BundlestoreDirEnvVar = "BUNDLESTORE_STORE_DIR"

	// Maximum concurrent connections allowed to access underlying Store resources
	MaxConnections = 100

	// Addrs
	DefaultApiBundlestore_HTTP = "localhost:9094"
	DefaultApiBundlestore_GRPC = "localhost:9098"

	ApiBundlestorePorts     = 11100
	ApiBundlestoreGRPCPorts = 12100
)
