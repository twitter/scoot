# Scoot Scheduler

The Scoot Scheduler is responsible for receiving job requests from the Cloud
API, and distributing the jobs to workers and maintaining and communicating
the state of jobs run.

TODO: better document the maze of types here.
TODO: populate the new JobDef data, but don't yet set up the primary Scoot fields
TODO: Pass ExecuteRequest through Worker/Runner APIs
TODO: request definitions tests?

The scheduler code contains the following packages:
* __sched__ - scheduler go objects: jobs, tasks and states, and thrift versions of these objects
  * __schedthrift__ - generated code from the thrift definitions
  * __scheduler__ - interfaces and implementations for job scheduling
  * __worker__ - interface for scheduler to run tasks on a worker
    * __workers__ - implementations of the worker interface (polling worker, etc), that invoke a runner
