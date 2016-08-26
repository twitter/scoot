package async

// AsyncError is an async value that will eventually return an error
// It is similair to a Promise/Future which returns an error
// The value is supplied by calling SetValue.
// Once the value is supplied AsyncError is considered completed
// The value can be retrieved once AsyncError is completed via TryGetValue
type AsyncError struct {
	errCh chan error
}

func newAsyncError() *AsyncError {
	return &AsyncError{
		errCh: make(chan error, 1),
	}
}

// Sets the value for the AsyncError.  Marks AsyncError as Completed or Fulfilled.
// This method should only ever be called once per AsyncError instance.
// Calling it more than once it will block indefinitely
func (p *AsyncError) SetValue(err error) {
	p.errCh <- err
}

// Returns the Status of this AsyncError, Completed or Pending
// and the value of the AsyncError if it is Completed.
//
// The returned bool is true if Completed, false if Pending
// If Completed the returned error is the Value of this AsyncError
// If AsyncError is not completed the returned error is nil.
//
// Calling this method after true has been returned will panic
func (p *AsyncError) TryGetValue() (bool, error) {
	select {
	case err := <-p.errCh:
		close(p.errCh)
		return true, err
	default:
		return false, nil
	}
}
