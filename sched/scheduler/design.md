# Scheduler Design
The Scoot Scheduler has to deal with errors and delays from the other systems in Cloud Scoot. How can we build it so it's easy to get right, test, verify, and reason about? This doc describes a Scheduler design based on the Actor Concurrency Model.


## Overview ##
The Scoot Scheduler's smarts are put in a state machine called the Coordinator. The Coordinator is based on the [Actor model][actorwiki] of concurrency. This is a good match for our problem, because:

  - Asynchronous RPCs let our scheduler continue even as backends pause
  - Messages in and out make our code easy to test

The Coordinator runs in a loop:

  - Receive a message and update state
  - Plan new Actions
  - Send RPCs

The Coordinator has a few changes from the standard Actor model:

  - State instead of Message is the input
  - Messages to the same saga are delivered in-order. (So we don't end a saga before we end a task)
  - Each RPC must result in exactly one reply. (So we know when we're done)

## Payoff
The brains of our system are a function from a State to a Plan. We can log for each tick of our Scheduler its input State and generated Plan. Debugging is figuring out why it was the wrong plan.

`planner_test.go` is the culmination of this. It's tests of the form:

  - input State
  - expected Plan

Whenever we fix a bug, we can add tests that would have failed before the fix. We don't have to stretch to get the system into a bad state; we just hand it a bad state.

(Note: there are lots of ways to structure this kind of functional test. The current form is kind of odd, and maybe too much like a DSL. So if the form turns you off, yeah, I'm not super happy with it either; let's fix it)

The other payoff is `end_to_end_test.go`, which brings up the whole system with a `chaosController` that lets us futz with the system. We can make test cases that start jobs running and then introduce instability as they're running, to make sure the whole system responds well.

## Coordinator
The Coordinator is a State Machine. Other systems send us messages which update our state, and then we figure out how to respond and respond by dispatching rpcs.

The Coordinator runs its loop in one goroutine. This means we don't have to worry about locking its state. But it also means that code in that goroutine can't make synchronous calls. (That's the trade-off of the Actor model)

## How to Do Something in the Scheduler
This design splits doing something into phases:

  1. decide to do something
  1. send the message
  1. process the reply

In code, these translate into:

  1. `planner` examines the `schedulerState` and generates an `action` which updates `schedulerState` and returns `rpc`'s
  1. `coordinator` dispatches the `rpc`'s to their backends
  1. `reply` arrives at the `coordinator`, which updates the `schedulerState`

Let's trace through this flow.

## State
The Coordinator's state is the Scheduler's view of the world. This is represented by `schedulerState` (in `state.go`).

`schedulerState` contains only data, not the objects that are clients to backends. This enforces the Actor model, so the Planner code can't accidentally make synchronous calls.

## Planner
The Planner takes a `schedulerState` and creates a slice of `action`. This is the central logic of the Scheduler. It includes:

  - Worker Monitoring. Figure out which nodes are available or busy or dead or need to be pinged.
  - Task Babysitting. End tasks that have finished, or mark tasks for reexecution if running has failed.
  - Work Distribution. Assign tasks to workers.

## `action`
An `action` (in `action.go`) is an intent by the Planner to do something. `action.apply` runs in the `coordinator` goroutine and is responsible for:

  - Updating `schedulerState`. E.g., mark we're starting task `t` on worker `w`. We need to do this so we don't run `t2` on `w` or `t` on `w2`.
  - returning `rpc`'s to perform the action

## `rpc`
An `rpc` (in `action.go`) describes an RPC to make to a backend. (To make the code more straightforward, we don't just use `rpc` as an interface, but match `rpc`'s to the different backends when we dispatch).

The `coordinator` dispatches the work of the `rpc` (talking to another system) to a separate goroutine.

## `logSagaMsg`
The `coordinator` dispatches messages to the Saga Log with a greater guarantee than the Actor Model provides. What? Why?

We may have several messages to the Saga Log in-flight. E.g., when we end the last task we also want to end the Saga. If we start each RPC in its own goroutine, the end saga message may get to the saga log before the end task message. Whoops.

So, we provide a stronger guarantee: messages to the same Saga Log are sent one-at-a-time, in-order.

This code is in `coordinator.go`: `sagaWriteLoop` writes one at a time; `sagaBufferLoop` maintains the buffer for one Saga and makes sure that the `coordinator` loop doesn't block sending. (An unbounded buffered channel would do this without the need for the loop, but golang doesn't provide them).

## `reply`
A `reply` (in `action.go`) is the reply to an `rpc`. The goroutine running the RPC sends the `reply` over `coordinator`'s `replyCh`.

The `rpc` updates the `schedulerState` to reflect the result. (Or it may be an unrecoverable error, which sets the error on the `coordinator`. Over time, we should be better able to recover from errors, but may still want e.g. a dead-letter queue in the `coordinator`.

## `coordinator`
The `coordinator` object looks big (its definition is ~20 lines!). This concerns me, but I think much of this bulk is useful.

We've separated data (which lives in `schedulerState`) from behavior (the client backends like `workers` and `sagas`). We've also separated incoming RPCs (`queueCh`) from where we send RPCs (`queueItems`).

## `listener`
The `listener` (in `logger.go`) lets a caller subscribe to events in the `coordinator`. Right now, this is useful for debugging without having to add/remove log statements. It will also be useful for firing events in end_to_end_test (e.g., remove this node right when we're about to schedule a job on it).

##`chaosController`
The `chaosController` (in `chaos.go`) lets a caller introduce chaos. You can make any backend start returning errors or delays.

## Backends
The Scoot Scheduler coordinates Scoot work across other systems:
  - Work Queue
  - Cluster Membership
  - Workers
  - Saga Log

Any of these systems can encounter an error or a delay. The Scheduler has to be resilient to these errors and delays.

## Concurrency - why the Actor Model?
Why the [Actor][actorwiki] model when Go is built to offer CSP (which is different)?

CSP lets you make asynchronous RPCs, but the send has to be synchronous.

Using the Actor Model takes some work. We need to make the function in our actor loop not block so that our Scheduler can continue to make progress.

Thinking of the Scheduler as an Actor means pulling our state into one object that lives in one thread/goroutine, which gives us the debug/test/reasonability advantages described above.

Given that our Scheduler is one system in a world of concurrency of mutability, the Actor model (with our modifications for ordered sagalog delivery) seems like a good fit for what we're doing.



   [actorwiki]: <https://en.wikipedia.org/wiki/Actor_model>