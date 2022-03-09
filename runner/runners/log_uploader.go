package runners

// LogUploader provides functionality to upload logs to permanent storage, used by invoker to upload task logs
type LogUploader interface {
	UploadLog(blobId string, filepath string, cancelCh chan struct{}) (string, error)
}

// Implementations for testing (single_test.go)

// Creates a new LogUploader that will not do anything
func NewNoopLogUploader() *NoopLogUploader {
	return &NoopLogUploader{}
}

type NoopLogUploader struct{}

func (u *NoopLogUploader) UploadLog(blobId string, filepath string, cancelCh chan struct{}) (string, error) {
	return "fake_blob_url", nil
}

// Creates a new LogUploader that will just block on cancel channel
func NewWaitingLogUploader() *NoopWaitingLogUploader {
	return &NoopWaitingLogUploader{}
}

type NoopWaitingLogUploader struct{}

func (u *NoopWaitingLogUploader) UploadLog(blobId string, filepath string, cancelCh chan struct{}) (string, error) {
	<-cancelCh
	return "fake_blob_url", nil
}
