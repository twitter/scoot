package server

import (
	"time"

	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/snapshot"
)

// Create a new handler that implements daemon protocol and works with domain types.
//
// TODO: when Runner eventually implements Poll(), we could get rid of handler and use runner directly in server.
func NewHandler(runner runner.Service, filer snapshot.Filer, pollInterval time.Duration) *Handler {
	return &Handler{
		runner:       runner,
		filer:        filer,
		pollInterval: pollInterval,
	}
}

type Handler struct {
	runner       runner.Service
	filer        snapshot.Filer
	pollInterval time.Duration
}

func (h *Handler) CreateSnapshot(path string) (snapshotId string, err error) {
	return h.filer.Ingest(path)
}

func (h *Handler) CheckoutSnapshot(snapshotID string, dir string) error {
	_, err := h.filer.CheckoutAt(snapshotID, dir)
	return err
}

func (h *Handler) Run(cmd *runner.Command) (status runner.RunStatus, err error) {
	// Direct delegation to underlying runner.
	return h.runner.Run(cmd)
}

func (h *Handler) Poll(runIds []runner.RunID, timeout time.Duration, returnAll bool) (statuses []runner.RunStatus) {
	// Set up pollTicker to periodically query runner for status.
	// Set up callerTimer to handle user-specified timeout.
	pollTicker := time.NewTicker(h.pollInterval)
	callerTimer := &time.Timer{}
	if timeout > 0 {
		callerTimer = time.NewTimer(timeout)
		defer callerTimer.Stop()
	}
	defer pollTicker.Stop()

	// Helper that loops over the provided runIds and stores a status if it's finished or if all==true.
	// Returns true if any of the status entries are finished.
	fillStatuses := func() bool {
		completed := false
		statuses = nil
		for _, runId := range runIds {
			status, _, _ := h.runner.Status(runId)
			if status.State.IsDone() {
				completed = true
			}
			if returnAll || status.State.IsDone() {
				statuses = append(statuses, status)
			}
		}
		return completed
	}

	// For zero timeout, poll for status and immediately return.
	// Otherwise, loop until we see a finished status or hit the user-specified timeout.
	if timeout == 0 {
		fillStatuses()
		return
	}
	for {
		select {
		case <-callerTimer.C:
			fillStatuses()
			return
		case <-pollTicker.C:
			anyCompleted := fillStatuses()
			if anyCompleted {
				return
			}
		}
	}
}
