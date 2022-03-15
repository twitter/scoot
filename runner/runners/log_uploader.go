package runners

import "errors"

// LogUploader provides functionality to upload logs to permanent storage, used by invoker to upload task logs
type LogUploader interface {
	UploadLog(blobID string, filepath string, cancelCh chan struct{}) (string, error)
}

// Implementations for testing (single_test.go)

// Creates a new LogUploader that will not do anything
func NewNoopLogUploader() *NoopLogUploader {
	return &NoopLogUploader{}
}

type NoopLogUploader struct{}

func (u *NoopLogUploader) UploadLog(blobID string, filepath string, cancelCh chan struct{}) (string, error) {
	return "fake_blob_url", nil
}

// Creates a new LogUploader that will send a signal to readyCh when UploadLog is called
// and then block on cancelCh
func NewNoopWaitingLogUploader() *NoopWaitingLogUploader {
	return &NoopWaitingLogUploader{
		readyCh: make(chan struct{}),
	}
}

type NoopWaitingLogUploader struct {
	readyCh chan struct{}
}

func (u *NoopWaitingLogUploader) UploadLog(blobID string, filepath string, cancelCh chan struct{}) (string, error) {
	// this is used to signal that this function has been called
	u.readyCh <- struct{}{}
	// wait for cancel request
	<-cancelCh
	return "", errors.New("aborted")
}
