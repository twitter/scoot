package runner is structs and interfaces to run Scoot processes.

## Summary ##
* Invoker runs one Command using Execer (and snapshot.Filer, etc.)
* Controller controls when to Run something (using an Invoker)
* Statuses queries Process history
  * StatusesSync doesn't offer waiting, so requires client polling
  * LegacyStatuses is the old Read API
  * StatusWriter writes Process status (called by Controller)

## Invoker (invoker.go) ##
Invoker runs a Scoot command. This involves:
* setup (check out a snapshot)
* post-processing (save output)
* babysitting (implement timeout and abort)
* updates while running

Conceptually, it lowers a Scoot Command from a high-level abstraction (Snapshots, etc.) to an Execer command (about directories that are paths in the local filesystem).

## Controller (controller.go) ##
Controller controls the Runs, starting a new one (Run()) or ending one (Abort()). It implements:
* tenancy (run one at a time? Or multiple? Fixed, or based on system utilization?)
* queueing (should we accept work we can't start right now)

When a Controller wants to start a Run of a Command, it will use an Invoker.

## Statuses (statuses.go) ##
Statuses allows querying for Process Status.

StatusesSync is a subset of Statuses that doesn't offer waiting. This is necessary because RPC transports like Thrift don't like waiting to respond. (We expect to offer a client-side polling library that can offer Statuses on top of only a StatusesSync).

StatusWriter allows creating new Process's and updating existing Process's Status.

LegacyStatuses offers a Legacy query API. Deprecated, and we intend to remove it.

## Relation to Execer ##
Execer is lower-level than runner. Runner deals with Scoot Abstractions; but Execer is just about Unix abstractions: files, directories, etc.

OSExecer calls os/exec.

SimExecer simulates behavior based on the args passed to it. This lets a caller script the behavior of the Execer.
