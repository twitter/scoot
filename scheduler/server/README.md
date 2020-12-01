# Scoot Scheduler
## Components:
- Scheduler interface (scheduler.go) - scheduler interface
- statefulScheduler (stateful_scheduler.go) - scheduler implementation
- clusterState (cluster_state.go) - keeps track of the available nodes and nodes running tasks
- jobState (job_state.go) - tracks the state of each job (running, waiting and completed tasks)
- taskRunner (task_runner.go) - starts tasks running and collects the results
- LoadBasedAlg (load_based_sched_alg.go) - the algorithm for computing the list of tasks to start/stop with
each scheduler loop iteration.

# Scheduling Algorithm
(load_based_scheduling_alg.go)

This scheduling algorithm computes the list of tasks to start and stop:

_Given_
- classes with load %s that defined the number of scoot workers we are targeting for running the class's tasks
- jobs with
    - list of tasks waiting to start
    - a requestor that maps jobs to classes
-  we have the number of idle workers waiting to run a task

_GetTasksToBeAssigned()_ is the top level entry to the algorithm. It
- determines if the system needs rebalancing (as per the rebalancing thresholds - see rebalancing below)
- if the system needs rebalancing it computes the tasks that should be stopped and the tasks that should be started
- otherwise, it just computes the tasks that should be started
- it returns the list of tasks that should be started and list of tasks that should be stopped

## get number of tasks to start, and list of tasks to stop:
(when not rebalancing)
### **entitled workers**
(entitlementTasksToStart())

The class %'s define the target number of workers that should be allocated to that class's tasks when scoot is fully
loaded.  We call this the number of workers the class is _entitled_ to use.
When the algorithm is assigning tasks to workers it will try to start tasks to meet each class's **_entitlement_**.

The algorithm finds the classes that have waiting tasks and are under their _entitlement_ and normalizes the 
original class load percents for the classes in this set.  It then computes the entitled workers for each class
using the number of available workers, the normalized percents and number of tasks waiting in the class.  If a
class does not have enough tasks to meet its entitlement, there will still be available workers after 
processing all the classes.  When this happens the algorithm repeats the entitlement computation (re-normalize 
%s, allocate workers). Each iteration will either allocate all available workers or allocate all of at least 
one class's waiting tasks. When all available workers are allocated or all class's waiting tasks or entitlements 
have been allocated the entitlement computation is complete.    

### **loaned workers**
(workerLoanAllocation())

It may be the case that some classes are under-utilizing their entitlements, but other classes have more tasks than
their entitlement.  When this happens, the algorithm allows the 'over entitlement' classes to run tasks on more than
their entitled number of workers, the classes under-utilizing their entitlement are,in effect, _loaning_ workers from
to the classes with more tasks than their entitlement.

The loan part of the algorithm normalizes the load %s to the classes with waiting tasks and allocates the unused workers 
as per these normalized percents.  If the _loan_ amount for a class is larger than the number of waiting tasks in that class,
there will still be unallocated workers after processing each class.  When this happens the algorithm repeats the loan
computation re-normalizing the %s to the classes with waiting tasks and allocating the still unallocated workers.  The
iteration finishes when all unallocated workers have been assigned to a class or when all the classes waiting tasks
have been allocated to a worker.

**Note** the entitlement and loan computation does not assign a specific worker to a task, it simply computes the number of
workers that can be used to start tasks in each class.

### **re-balancing**
(rebalanceClassTasks())

Each scheduling iteration tries to bring the task allocation back to the original class
entitlements, but it could be the case that long running tasks slowly create
an imbalance in the worker to class numbers (long running tasks tying up loaned workers).
As such, the algorithm periodically _re-balances_ the running workers back toward the original target
%s by stopping tasks that have been started on loaned workers.  It will select the most recently started
tasks till the running task to class allocation meets the original entitlement targets.

The re-balancing is triggered by computing the max difference of the percent under/over class load for each 
class with waiting tasks. We call this the _delta entitlement spread_. When the delta entitlement spread has
been over a threshold for a set period of time, the algorithm computes the list of tasks to stop and start 
to bring the classes to their entitled number of workers.

## selecting tasks to start
The (sub) algorithm for selecting tasks to start in a class uses a round robin approach pulling tasks from jobs that
have been grouped by the number of running tasks.  It starts with jobs with the least number of running tasks, selecting
one task from each job then moving on to the jobs with the next least number of running tasks till the target number of
tasks have been collected.

## exposed scheduling parameters
Scheduler api for the algorithm:
- GetClassLoadPercents - returns the map of className:%
- SetClassLoadPercents - sets the load %s from a map of className:%
- GetRequestorMap - returns the map of className:requestor_re, where requestor_re is the regular
expression for matching requestor values in the job defs
- SetRequestorMap - sets the map of className:requestor_re, where requestor_re is the regular
expression for matching requestor values in the job defs
- GetRebalanceMinimumDuration - return minimum number of minutes the system must be over
the delta entitlement spread before triggering re-balancing
- SetRebalanceMinimumDuration - set the minimum number of minutes the system must be over
the delta entitlement spread before triggering re-balancing
- GetRebalanceThreshold - return the threshold the delta entitlement spread must be over before triggering
re-balancing
- SetRebalanceThreshold - set the threshold the delta entitlement spread must be over before triggering
re-balancing

# Old Scheduling Algorithm
See doc.go