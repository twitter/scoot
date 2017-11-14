package cas

import (
	"fmt"
)

const (
	// Resource naming constants
	ResourceNameType   = "blobs"
	ResourceNameAction = "uploads"
)

// Resource naming format guidelines
var ResourceReadFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<hash>/<size>[/filename]", ResourceNameType)
var ResourceWriteFormatStr string = fmt.Sprintf("[<instance-name>/]%s/<uuid>/%s/<hash>/<size>[/filename]", ResourceNameAction, ResourceNameType)
