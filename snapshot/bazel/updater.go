package bzfiler

import (
	"time"
)

// // Updater allows Filers to have a means to manage updates on the underlying resources
// type Updater interface {
// 	// Trigger an update on the underlying resource
// 	Update() error

// 	// Get the configured update frequency from the Updater.
// 	// This lets us use the high-level interface to control update concurrency.
// 	UpdateInterval() time.Duration
// }

func (*BzFiler) Update() error {
	return nil
}

func (*BzFiler) UpdateInterval() time.Duration {
	return time.Second * 0
}
