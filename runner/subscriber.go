package runner

// SubscriptionID identifies a subscription as an opaque string.
type SubscriptionID string

// SubscriptionDefinition defines what is watched by a Subscription.
type SubscriptionDefinition struct {
	// IDs of Runs to watch, or empty to watch all
	Runs []RunId
	// States to watch. To watch state n, set the nth bit in StateMask.
	StateMask int
}

// An Event is an ID and the status that was recorded.
type Event struct {
	ID     EventID
	Status ProcessStatus
}

// An EventID identifies an Event; it's Subscription and a Sequence number.
type EventID struct {
	Sub SubscriptionID
	Seq int
}

// PollOpts describes options for our Poll
type PollOpts struct {
	// How long to wait for new Events
	Timeout time.Duration

	// We might add:
	// maximum number of evens to return
	// MaxEvents int
}

// A Subscription is a Stream of Events.
// A Subscription is identified by a(n opaque string) SubscriptionID
// Each Event is identified by an integer sequence number (0-indexed with no gaps)
// Each Event is its ID and the ProcessStatus
// Subscriber maintains a limited history for each Subscription.
// Subscriber doesn't (can't) maintain infinite history. It drops events when:
// *) a newer Event is passed as `since` to Stream or Poll
// *) the Event is received from the channel returned from Stream
// *) the Subscription has too many Events

// Subscriber allows subscribing to Runner Events.
// Its methods can be broken down into:
//   Manage Subscriptions
//   Listen to a Subscription
type Subscriber interface {
	// Manage Subscriptions

	// Subscribe creates a new Subscription.
	// def describes what Events to listen for
	// Subscribe returns a SubscriptionID or an error if it could not be created.
	Subscribe(def SubscriptionDefinition) (SubscriptionID, error)

	// Unsubscribe stops listening for id and frees related storage.
	// Unsubscribe returns an error if it could not Unsubscribe.
	Unsubscribe(id SubscriptionID) error

	// Subscriptions returns the current Subscriptions, or an error
	Subscriptions() ([]SubscriptionID, error)

	// Listen to a subscription

	// Stream streams Events in a Subscription.
	// The Events are in the same Subscription as `since`.
	// The Events are in sequence number order; they may have gaps (if the Subscriber had to
	// drop events due to memory)
	// Stream returns an error if Events cannot be streamed.
	Stream(since EventID) (chan Event, error)

	// Poll returns Events in a Subscription.
	// The Events are in the same Subscription as `since`.
	// The Events are in sequence number order; they may have gaps (if the Subscriber had to
	// drop events due to memory)
	// Poll takes options for how long to wait before returning.
	// Poll returns an error if Events cannot be polled.
	Poll(since EventID, opts PollOpts) ([]Event, error)
}
