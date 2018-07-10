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

	// ActionCache constants
	ResultAddressKey = "ActionCacheResult"
)

// Resource naming format guidelines
var ResourceReadFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<hash>/<size>[/filename]", ResourceNameType)
var ResourceWriteFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<uuid>/%s/<hash>/<size>[/filename]", ResourceNameAction, ResourceNameType)

// Default TTL for CAS-based operations
var DefaultTTL time.Duration = time.Hour * 24 * 7
