package async

import (
	"errors"
	"fmt"
	"testing"
)

func Test_AsyncMailbox(t *testing.T) {
	mailbox := NewAsyncMailbox()

	cbInvoked := false
	var retErr error

	asyncErr := mailbox.NewAsyncError(func(err error) {
		retErr = err
		cbInvoked = true
	})

	// spawn a go function that to do something
	// that sets the AsyncError value when
	// its completed
	go func(rsp *AsyncError) {
		sum := 0
		for i := 0; i < 100; i++ {
			sum = sum + i
		}
		rsp.SetValue(errors.New("Test Error!"))
	}(asyncErr)

	for !cbInvoked {
		mailbox.ProcessMessages()
	}
	if retErr == nil {
		t.Error("Expected Callback to be invoked with an error not nil")
	}
	if retErr.Error() != "Test Error!" {
		t.Error("Expected Callback to be invoked with `Test Error!` not: ", retErr.Error())
	}
}

// test to verify that example code for mailbox.go docs works!
func Test_AsyncMailboxExample(t *testing.T) {
	err := storeValue(5)
	if err != nil {
		t.Error("expected to storeValue to complete successfully")
	}
}

// example code for mailbox.go
func storeValue(num int) error {
	successfulWrites := 0
	returnedWrites := 0
	mailbox := NewAsyncMailbox()

	writeCallback := func(err error) {
		if err != nil {
			successfulWrites++
		}
		returnedWrites++
		fmt.Println("completedWrites", returnedWrites)
	}

	// Send to Replica One
	go func(rsp *AsyncError) {
		rsp.SetValue(write(num, "replicaOne"))
	}(mailbox.NewAsyncError(writeCallback))

	// Send to Replica Two
	go func(rsp *AsyncError) {
		rsp.SetValue(write(num, "replicaTwo"))
	}(mailbox.NewAsyncError(writeCallback))

	// Send to Replica Three
	go func(rsp *AsyncError) {
		rsp.SetValue(write(num, "replicaTwo"))
	}(mailbox.NewAsyncError(writeCallback))

	// Value is Considered Durably Stored if at least two write calls succeeded
	for successfulWrites < 2 && returnedWrites < 3 {
		mailbox.ProcessMessages()
	}

	if successfulWrites >= 2 {
		return nil
	} else {
		return errors.New("Could Not Durably Store Value")
	}
}

// a function which makes a call to a durable register
// which is accessed via the network, dummy function that always
// succeeds
func write(num int, address string) error {
	return nil
}
