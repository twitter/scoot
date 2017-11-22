package bazel

import (
	"time"
)

func (bf *bzFiler) Update() error {
	return bf.updater.Update()
}

func (bf *bzFiler) UpdateInterval() time.Duration {
	return bf.updater.UpdateInterval()
}
