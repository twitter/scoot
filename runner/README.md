# Runner Design #
package runner is structs and interfaces to run Commands in Scoot. package runners contains implementations. This doc describes both.

## Summary ##
* Service (interface) combines Controller and StatusReader
* Controller (interface) controls when to Run a Command.
  * Controller calls Invoker
  * Controller writes to StatusWriter
* runners.Invoker (struct) runs one command using Execer and snapshot.Filer
  * implements Abort and Timeout
* runners.Statuses (struct) offers read and write of RunStatus
  * satisfies interfaces StatusWriter and StatusReader

## Invoker (runner/runners/invoker.go) ##
Invoker runs a Scoot command. This involves:
* setup (check out a snapshot)
* post-processing (save output)
* babysitting (implement timeout and abort)
* streaming RunStatus'es over the course of a run

Conceptually, it lowers a Scoot Command from a high-level abstraction (Snapshots, etc.) to an Execer command (about directories that are paths in the local filesystem). Concretely,  it checks out the input snapshot's files, then uses an Execer to run the command. When the command has finished, it created a snapshot of the command's output.

## Controller (runner/controller.go) ##
Controller controls the Runs, starting a new one (Run()) or ending one (Abort()). It implements:
* tenancy (run one at a time? Or multiple? Fixed, or based on system utilization?)
* queueing (should we accept work we can't start right now)

When a Controller wants to start a Run of a Command, it will use an Invoker.

For now we have one implementation, QueueController

## Status interfaces (runner/statuses.go) ##
There are several interfaces having to do with reading/writing RunStatus'es.

StatusWriter for writing Statuses.

There are more reader interfaces:
* StatusReader includes all the ones below
* LegacyStatusReader offers legacy APIs
* StatusQuerier offers Query, which allows waiting for some period of time
* StatusQueryNower offers QueryNow which is the same as Query, but does not allow waiting

## StatusManager implementations (runner/runners/statuses.go) ##
StatusManager holds RunStatus'es and implements the various Status interfaces.

## Service (runner/runner.go) ##
Service includes the ability to control commands (Controller) and watch the status (StatusReader).

## Relation to Execer ##
Execer is lower-level than runner. Runner deals with Scoot Abstractions; but Execer is just about Unix abstractions: files, directories, etc.

OSExecer calls os/exec.

SimExecer simulates behavior based on the args passed to it. This lets a caller script the behavior of the Execer.
