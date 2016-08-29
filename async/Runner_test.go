package async

import (
	"errors"
	"testing"
)

func Test_Runner(t *testing.T) {
	err := storeValue_withRunner(5)
	if err == nil {
		t.Error("expected to storeValue to complete successfully")
	}
}

func storeValue_withRunner(num int) error {
	successfulWrites := 0
	returnedWrites := 0

	runner := NewRunner()

	writeCb := func(err error) {
		if err != nil {
			successfulWrites++
		}
		returnedWrites++
	}

	runner.RunAsync(func() error { return write(num, "replicatOne") }, writeCb)
	runner.RunAsync(func() error { return write(num, "replicatTwo") }, writeCb)
	runner.RunAsync(func() error { return write(num, "replicaThree") }, writeCb)

	for successfulWrites < 2 && returnedWrites < 3 {
		runner.ProcessMessages()
	}

	if successfulWrites >= 2 {
		return nil
	} else {
		return errors.New("Could Not Durably Store Value")
	}
}
