package bazel

import (
	"fmt"
)

// TODO reorg / location?
const (
	// Instance-related constants
	DefaultInstanceName = ""

	// Store artifact resource name pre/post fixes
	StorePrePostFix = "blob"

	// Resource naming constants
	ResourceTypeStr   = "blobs"
	ResourceActionStr = "uploads"
)

var ResourceReadFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<hash>/<size>[/filename]", ResourceTypeStr)
var ResourceWriteFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<uuid>/%s/<hash>/<size>[/filename]", ResourceActionStr, ResourceTypeStr)
