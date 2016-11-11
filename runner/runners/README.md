# Runner Impl Design #
package runner defines Runner, which lets clients start, stop, and get the status of Scoot Commands. This package contains implementations of Runner.

There are three main components of our Runner Impl:
* Statuses
* Controller
* Invoker

## Statuses (statuses.go) ##
Statuses is a(n in-memory) database of ProcessStatus. It has two facets:
* Read Statuses (by implementing runner.Statuser). Allows reading the current state and waiting for new results.
* Write Statuses. Statuses are written by the Controller, below.

## Controller (single.go and queue.go) ##
Controller controls the Runs, starting a new one (Run()) or ending one (Abort()). It offers mutual exclusion (don't run two at a time) and a queue (in queue.go; single.go doesn't). It writes to a Statuses.

To actually start a process, it uses an Invoker.

## Invoker (invoke.go) ##
Invoker invokes a Scoot command. This involves:
* setup (check out a snapshot)
* post-processing (save output)
* babysitting (implement timeout and abort)
* updates while running

Invoker doesn't implement any interface; it's just a struct with useful methods. Conceptually, it lowers a Scoot Command (about Snapshots) to an Execer command (about directories that are paths in the local filesystem).

## Implementing Runner (combine.go) ##
ControllerAndStatuserRunner is a struct with a Controller and a Statuser that implements the Runner interface by delegating to the Controller and Statuser. This allows making Controllers pluggable.

(The alternative would be to have each Controller implement Runner by writing each Statuser method to call the Statuser)