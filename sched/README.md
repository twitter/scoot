# Scoot Scheduler

The Scoot Scheduler is responsible for receiving job requests from the Cloud
API, and distributing the jobs to workers and maintaining and communicating
the state of jobs run.

The scheduler code contains the following packages:
* __sched__ - definitions for jobs, tasks, and states
  * __thrift definitions for the above__
* __schedthrift__ - generated code from the thrift definitions
* __scheduler__ - interfaces and implementations for job scheduling
* __worker__ - interface for communication between the scheduler and workers
* __workers__ - implementations of the worker interface (polling worker, etc). Note that this is
not the implementation of the Scoot Worker itself
