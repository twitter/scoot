package bazel

import (
	"time"
)

// Default updater is a no-op
func (bf *BzFiler) Update() error {
	return bf.updater.Update()
}

func (bf *BzFiler) UpdateInterval() time.Duration {
	return bf.updater.UpdateInterval()
}
