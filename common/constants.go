package common

import (
	"time"
)

const DefaultClientTimeout = time.Minute

const DefaultFetchFreqMin = 3 * time.Minute
const DefaultClusterChanSize = 100

const DefaultSagaUpdateChSize = 10000
