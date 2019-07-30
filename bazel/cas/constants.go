package cas

import (
	"fmt"
	"time"
)

const (
	// Resource naming constants
	ResourceNameType   = "blobs"
	ResourceNameAction = "uploads"

	// Default buffer sizes
	DefaultReadCapacity = 1024 * 1024

	// Batch parallelism for underlying store operations
	// NOTE experimental/arbitrary setting. Consider adding a mechanism to set via StoreConfigs
	// NOTE if service implementation is changed, retest with setting <= 5
	BatchParallelism = 5

	// ActionCache constants
	ResultAddressKey = "ActionCacheResult"

	// GRPC Server connection-related setting limits recommended for CAS
	MaxSimultaneousConnections = 1000 // limits total simultaneous connections via the Listener
	MaxRequestsPerSecond       = 500  // limits total incoming requests allowed per second
	MaxRequestsBurst           = 250  // allows this many requests in a burst faster than MaxRPS average
	MaxConcurrentStreams       = 0    // limits concurrent streams _per client_
)

// Resource naming format guidelines
var ResourceReadFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<hash>/<size>[/filename]", ResourceNameType)
var ResourceWriteFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<uuid>/%s/<hash>/<size>[/filename]", ResourceNameAction, ResourceNameType)

// Default TTL for CAS-based operations
var DefaultTTL time.Duration = time.Hour * 24 * 7
