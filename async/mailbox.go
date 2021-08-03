package async

// An AsyncMailbox stores AsyncErrors and their associated callbacks
// and invokes them once the AsyncError is completed
//
// Often times we may spawn go routines in an event loop to do some concurrent work,
// go routines provide no way to return a response, however we may want
// to be notified if the work the go routine was doing completed successfully
// or unsuccessfully, and then take some action based on that result.
// AsyncMailbox provides a construct to do this.
//
// The below example is a storeValue function, which tries to store
// A value durably.  A value is considered durably stored if it successfully
// writes to two of three replicas. We want to write to all replicas in parallel
// and return as soon as two writes succeed. We return an error if < 2 writes succeed
//
//  func storeValue(num int) error {
//    successfulWrites := 0
//    returnedWrites := 0
//    mailbox := NewAsyncMailbox()
//
//    writeCallback := func (err error) {
//      if err != nil {
//        successfulWrites++
//      }
//      returnedWrites++
//    }
//
//    // Send to Replica One
//    go func(rsp *AsyncError){
//      rsp.SetValue(write(num, "replicaOne"))
//    }(mailbox.NewAsyncError(writeCallback))
//
//    // Send to Replica Two
//    go func(rsp *AsyncError){
//      rsp.SetValue(write(num, "replicaTwo"))
//    }(mailbox.NewAsyncError(writeCallback))
//
//    // Send to Replica Three
//    go func(rsp *AsyncError){
//      rsp.SetValue(write(num, "replicaThree"))
//    }(mailbox.NewAsyncError(writeCallback))
//
//    // Value is Considered Durably Stored if at least two write calls succeeded
//    for sucessfullWrites < 2 && returnedWrites < 3 {
//       mailbox.ProcessMessages()
//    }
//
//    if successfulWrites >= 2 {
//      return nil
//    } else {
//      return errors.New("Could Not Durably Store Value")
//    }
//
//  // a function which makes a call to a durable register
//  // which is accessed via the network
//  func write (num int, address string) error { ... }
//
// A Mailbox is not a concurrent structure and should only
// ever be accessed from a single go routine.  This ensures that the callbacks
// are always executed within the same context and only one at a time.
// A Mailbox for keeping track of in progress AsyncMessages.
// This structure is not thread-safe.
type Mailbox struct {
	msgs []message
}

// The function type of the callback invoked when an AsyncError is Completed
type AsyncErrorResponseHandler func(error)

// async message is a struct composed of an AsyncError
// and its associated callback
type message struct {
	Err      *AsyncError
	callback AsyncErrorResponseHandler
}

func newMessage(cb AsyncErrorResponseHandler) message {
	return message{
		Err:      newAsyncError(),
		callback: cb,
	}
}

func NewMailbox() *Mailbox {
	return &Mailbox{
		msgs: make([]message, 0),
	}
}

func (bx *Mailbox) Count() int {
	return len(bx.msgs)
}

// Creates a NewAsyncError and associates the supplied callback with it.
// Once the AsyncError has been completed, SetValue called, the callback
// will be invoked on the next execution of ProcessMessages
func (bx *Mailbox) NewAsyncError(cb AsyncErrorResponseHandler) *AsyncError {
	msg := newMessage(cb)
	bx.msgs = append(bx.msgs, msg)
	return msg.Err
}

// Processes the mailbox.  For all messages with completed AsyncErrors
// the callback function and removes the message from the mailbox
func (bx *Mailbox) ProcessMessages() {
	var unCompletedMsgs []message
	for _, msg := range bx.msgs {
		ok, err := msg.Err.TryGetValue()

		// if a AsyncErr's value has been set, invoke the callback
		if ok {
			msg.callback(err)
		} else {
			unCompletedMsgs = append(unCompletedMsgs, msg)
		}
	}

	// reset inProgress messages to unCompletedMsgs only
	bx.msgs = unCompletedMsgs
}
