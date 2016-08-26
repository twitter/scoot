package async

import (
	"errors"
	"testing"
)

// Verify that TryGetValue returns false for an uncompleted AsyncError
func TestAsyncError_NotCompleted(t *testing.T) {
	err := newAsyncError()
	ok, retErr := err.TryGetValue()

	if ok {
		t.Error("Expected TryGetValue to return false for uncompleted AsyncError")
	}
	if retErr != nil {
		t.Error("Expected TryGetValue to return nil for uncompleted AsyncError")
	}
}

// Verify that TryGetValue returns true for a completed AsyncError and the
// supplied error it was completed with
func TestAsyncError_Completed(t *testing.T) {
	err := newAsyncError()
	testErr := errors.New("Test Error!")
	err.SetValue(testErr)
	ok, retErr := err.TryGetValue()

	if !ok {
		t.Error("Expected TryGetValue to return true for completed AsyncError")
	}

	if retErr == nil {
		t.Error("Expected TryGetValue to return an error for completed AsyncError")
	}

	if retErr.Error() != testErr.Error() {
		t.Errorf("Expected returned error {%v} to be the same as SetValue error {%v}",
			retErr.Error(), testErr.Error())
	}
}

func TestAsyncError_CompletedNilError(t *testing.T) {
	err := newAsyncError()
	err.SetValue(nil)
	ok, retErr := err.TryGetValue()

	if !ok {
		t.Error("Expected TryGetValue to return true for completed AsyncError")
	}

	if retErr != nil {
		t.Error("Expected TryGetValue to return an error of nil completed AsyncError")
	}
}
