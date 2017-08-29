/*
package scheduler provides StatefulScheduler which distributes tasks to a cluster of nodes.

* Concepts *
JobPriority:
  0 Default, queue new runs until all higher priority jobs are satisfied
  1 Run ahead of priority=0, but otherwise treated the same as priority=0
  2 Run ahead of priority<=1, killing youngest lower priority tasks if no nodes are free (up to MinRunningNodesForGivenJob)
  3 Run ahead of priority<=2, acquiring as many nodes as possible, killing youngest lower priority tasks if no nodes are free
Note: Lower priority jobs are given a chance once MinRunningNodesForGivenJob for higher priority jobs is satisfied.
      However, priority 3 jobs are greedy and have no minimum number of nodes whereupon they defer to lower priorities.

SoftMaxSchedulableTasks:
  This limit helps determine nodes per job but doesn’t actually result in scheduler backpressure.

NumRunningNodes:
  The total number of nodes in the cluster which are capable of running a task, even if currently busy.

NodeScaleFactor:
  Used to calculate how many tasks a job can run without adversely affecting other jobs.
  We account for job priority by increasing the scale factor by an appropriate percentage.
  = (NumRunningNodes / SoftMaxSchedulableTasks) * (1 + Job.Priority * SomeMultiplier)

MinRunningNodesForGivenJob:
  = ceil(min(NumFreeNodes, Job.NumRequestedTasks * NodeScaleFactor, Job.NumRemainingTasks))

MaxJobsPerRequestor,  MaxRequestors:
  These limits are somewhat arbitrary and are only meant to prevent spamming, not to ensure fairness.
  Scheduler will apply backpressure if we hit these limits.

* Logic *
Schedule Loop:
Group new job requests with existing jobs sharing the same RequestTag
Add remaining unmatched requests to the jobs queue but limit number of jobs per Requestor

For Job in Jobs.Priority3,
 Select NumAssignedNodes=min(Job.NumRemainingTasks, NumFreeNodes + NumKillableNodes w/ level<3)
For Job in Jobs.Priority2:
 Select NumAssignedNodes=min(Job.NumRemainingTasks, MinRunningNodesForGivenJob, NumFreeNodes + NumKillableNodes w/ level<2)
For Job in Jobs.Priority1:
 Select NumAssignedNodes=min(Job.NumRemainingTasks, MinRunningNodesForGivenJob, NumFreeNodes)
For Job in Jobs.Priority0:
 Select NumAssignedNodes=min(Job.NumRemainingTasks, MinRunningNodesForGivenJob, NumFreeNodes)

Select Node Preference:
   Free nodes with the same SnapshotID as the given task, where the last ran task is different.
   Free nodes with the same SnapshotID as the given task.
   Free nodes not related to any current job.
   Any free node.
   If priority >= 2: busy node with smallest run duration from priority0 tasks.
   If priority >= 2: busy node with smallest run duration from priority1 tasks.
   If priority >= 3: busy node with smallest run duration from priority2 tasks.
*/
package scheduler
