/*
package scheduler provides StatefulScheduler which distributes tasks to a cluster of nodes.

* Concepts *
JobPriority:
  0 Default, these jobs will receive node quota only when P1,2,3 jobs have satisfied their minimum node quota.
  1 These jobs will receive node quota only when P2,3 jobs have satisfied their minimum node quota.
  2 These jobs will receive node quota only when P3 jobs have satisfied their minimum node quota.
  3 Run ahead of P0,1,2, acquiring as many nodes as possible, killing youngest lower priority tasks if no nodes are free
Note: Lower priority jobs are given a chance once MinNodesForGivenJob for higher priority jobs is satisfied.
      However, priority 3 jobs are greedy and have no minimum number of nodes whereupon they defer to lower priorities.

SoftMaxSchedulableTasks:
  This limit helps determine nodes per job (see NodeScaleFactor) but doesn’t actually result in scheduler backpressure.
  The max is determined dynamically, using the greater of number of healthy nodes and max outstanding tasks thus far.
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

For Job in Jobs.Priority3,
 Select NumAssignedNodes=min(Job.NumRemainingTasks, NumFreeNodes + NumKillableNodes from P0,1,2 jobs)
For Job in Jobs.Priority2 + Jobs.Priority1 + Jobs.Priority0:
 Select NumAssignedNodes=min(Job.NumRemainingTasks, MinNodesForGivenJob, NumFreeNodes)

Select Node Preference:
   Free nodes with the same SnapshotID as the given task, where the last ran task is different.
   Free nodes with the same SnapshotID as the given task.
   Free nodes not related to any current job.
   Any free node.
   If Priority3: busy node with smallest run duration from P0 tasks first, then P1, then P2.
*/
package scheduler
