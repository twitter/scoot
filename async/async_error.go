package async

// AsyncError is an async value that will eventually return an error
// It is similar to a Promise/Future which returns an error
// The value is supplied by calling SetValue.
// Once the value is supplied AsyncError is considered completed
// The value can be retrieved once AsyncError is completed via TryGetValue
type AsyncError struct {
	errCh     chan error
	val       error
	completed bool
}

func newAsyncError() *AsyncError {
	return &AsyncError{
		errCh: make(chan error, 1),
	}
}

// Sets the value for the AsyncError.  Marks AsyncError as Completed or Fulfilled.
// This method should only ever be called once per AsyncError instance.
// Calling this method more than once will panic
func (e *AsyncError) SetValue(err error) {
	e.errCh <- err
	close(e.errCh)
}

// Returns the Status of this AsyncError:
// Completed(true) or Pending(false)
// and the value of the AsyncError if it is Completed.
//
// The returned bool is true if Completed, false if Pending
// If Completed the returned error is the Value of this AsyncError
// If AsyncError is not completed the returned error is nil.
func (e *AsyncError) TryGetValue() (bool, error) {
	if e.completed {
		return true, e.val
	} else {
		select {
		case err := <-e.errCh:
			e.val = err
			e.completed = true
			return true, err
		default:
			return false, nil
		}
	}
}
