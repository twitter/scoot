package async

// An AsyncRunner is a helper class to spawn Go Routines to run
// AsyncFunctions and to associate callbacks with them.  This builds
// ontop of AsyncMailbox to make simplify the code that needs to be written.
//
// The below example is a storeValue function, which tries to store
// A value durably.  A value is considered durably stored if it successfully
// writes to two of three replicas. We want to write to all replicas in parallel
// and return as soon as two writes succeed. We return an error if < 2 writes succeed
//
// func storeValue(num int) error {
//   successfulWrites := 0
//   returnedWrites := 0
//
//   runner := NewAsyncRunner()
//
//   writeCb := func(err error) {
//     if err != nil {
//       successfulWrites++
//     }
//     returnedWrites++
//   }
//
//   runner.RunAsync(func() error { return write(num, "replicatOne") }, writeCb)
//   runner.RunAsync(func() error { return write(num, "replicatTwo") }, writeCb)
//   runner.RunAsync(func() error { return write(num, "replicaThree") }, writeCb)
//
//   for successfulWrites < 2 && returnedWrites < 3 {
//     runner.ProcessMessages()
//   }
//
//   if successfulWrites >= 2 {
//     return nil
//   } else {
//     return errors.New("Could Not Durably Store Value")
//   }
// }
//  // a function which makes a call to a durable register
//  // which is accessed via the network
//  func write (num int, address string) error { ... }
//
type AsyncRunner struct {
	bx *AsyncMailbox
}

func NewAsyncRunner() AsyncRunner {
	return AsyncRunner{
		bx: NewAsyncMailbox(),
	}
}

// Function that takes no parameters and returns an error
type AsyncFunction func() error

// RunAsync creates a go routine to run the specified function f.
// The callback, cb, is invoked once f is completed by calling ProcessMessages.
func (r *AsyncRunner) RunAsync(f AsyncFunction, cb AsyncErrorResponseHandler) {
	asyncErr := r.bx.NewAsyncError(cb)
	go func(rsp *AsyncError) {
		err := f()
		rsp.SetValue(err)
	}(asyncErr)
}

// Invokes all callbacks of completed asyncfunctions.
// Callbacks are ran synchronously and by the calling go routine
func (r *AsyncRunner) ProcessMessages() {
	r.bx.ProcessMessages()
}
