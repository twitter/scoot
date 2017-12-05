package bazel

import (
	"time"
)

// Default updater is a no-op
func (bf *bzFiler) Update() error {
	return bf.updater.Update()
}

func (bf *bzFiler) UpdateInterval() time.Duration {
	return bf.updater.UpdateInterval()
}
