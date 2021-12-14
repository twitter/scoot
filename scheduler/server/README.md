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
- a defined set of job classes with load % for each class, such that the load %s define the number of scoot workers targeted for running the class's tasks
- a mapping of job requestors to job classes (using reg exp to match requestor values)
- jobs with
    - list of tasks waiting to start (in descending duration order)
-  we have the number of idle workers waiting to run a task

_**Overview**_  
The algorithm selects the list of tasks that should be started on the idle workers.  The objective of the algorithm
is to maintain the target load %.  When a given job class does not have enough tasks to use all the workers in that class's target load %, the unused workers will be used to run other class tasks (we call this loaning a worker).

It may be the case that long running tasks running on 'loaned' workers make it impossible for the algorithm to bring the task allocations back in line with the original target load %s.  To address this, the algorithm has a 'rebalancing' feature that when turned on, will cancel the most recently started tasks for classes using loaned workers. 

_GetTasksToBeAssigned()_ (the top level entry to the algorithm):
- determine if the system needs rebalancing (as per the rebalancing thresholds - see rebalancing below)
- if the system needs rebalancing, 
    - compute the tasks that should be stopped,
- compute the number tasks to start for each class
- compute the list of tasks that should be started
- return the list of tasks that should be started and list of tasks that should be stopped

## Get number of tasks to start, and list of tasks to stop:
(rebalance happens first, but it is described below since we've never turned it on)
### **compute number of tasks to start for each class as per the load % 'entitlements'**
(_entitlementTasksToStart()_)

A class's _entitlement_ is the number of workers * the class's load % - it is the number of workers that should be
allocated to that tasks in that class. When the algorithm is computing tasks to start, it will try to meet each
class' _entitlement_.  
1. For each class with tasks waiting to start, it's _unused entitlement_ is the class's
_entitlement_ - number of that class's tasks currently running (minimum unused entitlement is 0). In addition, 
if a class does not have tasks waiting to start, it's _unused entitlement_ is 0.
2. Compute each class's _unused entitlement %_: the class's _unused entitlement / sum(all classes _unused entitlement_s)
3. compute the number of tasks to start for each class: min(number tasks waiting to
start, _unused entitlement % * number of idle (unallocated) workers)

When a class's number of waiting tasks < the number to start, there will still be
available workers after computing the number of tasks to start for each class.When this happens the algorithm
repeats the steps listed above computing the additional number of tasks to start for each class. Each iteration
will either allocate all available workers, all of at least one class's waiting tasks or a class's full
_entitlement_. When all available workers are
allocated or all class's waiting tasks or _entitlements_ have been allocated the iteration stops and the entitlement computation is complete.    

It may be the case that some classes are under-utilizing their _entitlements_, but other classes have more tasks
than their _entitlement_ waiting. When this happens the entitlement allocation will complete, but there will still
be unallocated workers (the total number of tasks to start is still less than the number of idle workers) and 
tasks waiting to start. When this happens the algorithm proceeds to compute the number of workers to '_loan_' to each class:

### **loaned workers**
(_workerLoanAllocation()_)

The loan part of the algorithm computes the loan distribution %s as follows:

1. normalize the original load %s for classes with waiting tasks.  
2. compute the total workers that will be loaned (sum of current loaned workers + number of idle workers that would still be unassigned after the entitlement distribution above).
3. compute what the worker loan distribution would be if all of the currently loaned workers plus newly available workers were distributed as per the normalized loan %s
4. adjust the worker loan distribution by subtracting the number of currently loaned workers for each class.  If classes are
exceeding their loan distribution (from step 3) then their adjusted loan distribution is 0
5. compute class final loan %s as each class's adjusted loan distribution / sum of the adjusted loan distributions

Each class's _loan amount_ is computed as the class final loan % (from step 5) * number of workers still available or number of
waiting tasks whichever is smaller

When the _loan_ amount for a class is larger than the number of waiting tasks in that class, there will still be available 
workers after processing each class.  When this happens the algorithm repeats the loan computation for the still unallocated 
workers.  The iteration finishes when all unallocated workers have been assigned to a class or when all the classes waiting 
tasks have been allocated to a worker.

**Note** the entitlement and loan computation does not assign a specific worker to a task, it simply computes the number of
workers that can be used to start tasks in each class.

### rebalance
(_rebalanceClassTasks()_)

Each scheduling iteration naturally brings the running task allocations back to the original class
entitlements, but it could be the case that long running tasks holding on to loaned workers slowly cause the number of
running tasks for each class to be far from the target load percents.
When this occurs, the algorithm _re-balances_ the running workers back toward the original target
%s by stopping tasks that have been started on loaned workers.  It will select the most recently started
tasks till the running task to class allocation meets the original entitlement targets.

The re-balancing is triggered by computing the max difference of the percent under/over class load for each 
class with waiting tasks. We call this the _delta entitlement spread_. When the delta entitlement spread has
been over a threshold for a set period of time, the algorithm computes the list of tasks to stop and start 
to bring the classes to their entitled number of workers.

## Example
### **example 1:** Test_Class_Task_Start_Cnts(), scenario 1 - takes 2 iterations to allocate all workers based on entitlement 
totalWorkers: 1000, 710 running tasks, 290 idle workers
| class | load % | running tasks | waiting tasks | entitlement |
| ----- | ------ | ------------- | ------------- | ----------- |
| c0 | 30% | 200 | 290 | 300 |
| c1 | 25% | 300 | 230 | 250 |
| c2 | 20% | 0 | 150 | 200 |
| c3 | 15% | 100 | 150 | 150 |
| c4 | 10% | 110 | 90 | 100 |
| c5 | 0% | 0 | 328 | 0 | 0 |

|           | iter 1               |                  |                                  | iter 2               |                  |              | start  |      
| :-------: | :------------------- | :--------------- | :------------------------------- | -------------------: | ---------------: | -----------: | -----: | 
|           | 290 idle workers     |                  | 174 allocated                    | 16 idle workers      |                  | 16 allocated |        |
| **class** | **_entitled_ tasks** | **normalized %** |                                  | **_entitled_ tasks** | **normalized %** |              |        |
| c0        | 300-200=100          | 100/350=29%      | .29*290=84                       | 300-284=16           | 16/26=62%        | .62*16=10    |  94    |
| c1        | 250-300-> 0          | 0%               | 0                                | 0                    | 0%               | 0            |  0     |
| c2        | 200-0=200            | 200/350=57%      | .57*290=165 -> 150 tasks waiting | 0                    | 0%               | 0            |  150   |
| c3        | 150-100=50           | 50/350=14%       | .14*290=40                       | 150-140=10           | 10/26=38%        | .38*16=6     |  46    |

### **example 2:** Test_Class_Task_Start_Cnts(), scenario 3 - entitlement plus loan

totalWorkers: 1000, 710 running tasks, 290 idle workers
| class | load % | running tasks | waiting tasks | entitlement | loaned |
| ----- | ------ | ------------- | ------------- | ----------- | ------ |
| c0    | 30%    | 200           | 10            | 300         | 0      |
| c1    | 25%    | 300           | 230           | 250         | 50     |
| c2    | 20%    | 0             | 0             | 200         | 0      |
| c3    | 15%    | 100           | 50            | 150         | 0      |
| c4    | 10%    | 110           | 90            | 100         | 10     |


|           | entitlement          |                   |                                 | loan               |                        | start |      
| :-------: | :------------------- | :---------------- | :------------------------------ | -----------------: | ---------------------: | -----:| 
|           | 290 idle workers     |                   | 60 allocated                    | 230 idle+60 loaned | 230 allocated          |       |
| **class** | **_entitled_ tasks** | **normalized %**  |                                 | **normalized %**   |                        |       |
| c0        | 300-200=100          | 100/150=67%       | .67*290=194 -> 10 tasks waiting | 0                  | 0                      | 10    |
| c1        | 250-300-> 0          | 0%                | 0                               | 25/35=72%          | .72*290=207, 206-50=157| 157   |
| c2        | 0 waiting tasks      | 0%                | 0                               | 0                  | 0                      | 0     |
| c3        | 150-100=50           | 50/150=33%        | .33*290=98-> 50 tasks waiting   | 0                  | 0                      | 50    |
| c4        | 100-110-> 0          | 0%                | 0                               | 10/35=28%          | .29*290=83, 84-10=73   | 73    |


## selecting tasks to start
Selecting tasks to start in a class uses a round robin approach selecting tasks from jobs with the least number of
running tasks.  This makes sure that all jobs in a given task are using an equitable number of the workers allocate
to that class (one job with many tasks won't use up all the workers allocated to that class).

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
