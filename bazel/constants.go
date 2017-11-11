package bazel

import (
	"fmt"
)

const (
	// Instance-related constants
	DefaultInstanceName = ""

	// Store artifact resource name pre/post fixes
	StorePrePostFix = "blob"

	// Resource naming constants
	ResourceTypeStr = "blobs"

	// Buffer 16MiB
	MaxCASBufferSize = 16777216
)

var ResourceFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<hash>/<size>[/filename]", ResourceTypeStr)
