/*
package server provides StatefulScheduler which distributes tasks to a cluster of nodes.

* Concepts *
JobPriority:
  0 Default, these jobs will receive node quota only when P1,2 jobs have satisfied their minimum node quota.
  1 These jobs will receive node quota only when P2 jobs have satisfied their minimum node quota.
  2 These jobs get a baseline node quota first.
Note: Lower priority jobs are given a chance once MinNodesForGivenJob for higher priority jobs is satisfied.

SoftMaxSchedulableTasks:
  This limit helps determine nodes per job (see NodeScaleFactor) but doesn’t actually result in scheduler backpressure.
  The max is determined dynamically, using the greater of number of healthy nodes and number of outstanding tasks.
  If the max is equal to healthy nodes, that means each job can be fully scheduled, with partial scheduling otherwise.

NumHealthyNodes:
  The total number of nodes in the cluster which are capable of running a task, even if currently busy.

NodeScaleFactor:
  Used to calculate how many tasks a job can run without adversely affecting other jobs.
  We account for job priority by increasing the scale factor by an appropriate percentage.
  = (NumHealthyNodes / SoftMaxSchedulableTasks) * (1 + Job.Priority * SomeMultiplier)

MinNodesForGivenJob:
  = ceil(min(NumFreeNodes, Job.NumRequestedTasks * NodeScaleFactor, Job.NumRemainingTasks))

MaxJobsPerRequestor,  MaxRequestors:
  These limits are somewhat arbitrary and are only meant to prevent spamming, not to ensure fairness.
  Scheduler will apply backpressure if we hit these limits.

* Logic *
Schedule Loop:
Group new job requests with existing jobs sharing the same RequestTag
Add remaining unmatched requests to the jobs queue but limit number of jobs per Requestor

Select Node Preference:
   Free nodes with the same SnapshotID as the given task, where the last ran task is different.
   Free nodes with the same SnapshotID as the given task.
   Free nodes not related to any current job.
   Any free node.
*/
package server
