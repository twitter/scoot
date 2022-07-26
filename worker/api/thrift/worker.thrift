# Worker server Thrift API interface definition

# See github.com/twitter/scoot/README.md for local Thrift prerequisites
# 
# To Generate files, run from top level (github.com/twitter/scoot) repo directory:
#     $ make thrift-worker-go
#     We remove worker/api/gen-go/worker/worker-remote/ due to thrift being unable to
#     distinguish between local and included package prefixes.
#     We optimize for the golang library code and remove a main-package client the thrift tool generates.

enum Status {
  UNKNOWN = 0      # Catch-all for indeterminate RunStatusStates. Also an "end state"
  PENDING = 1      # Waiting to run
  RUNNING = 2      # Running
  COMPLETE = 3     # Succeeded or failed yielding an exit code
  FAILED = 4       # Run mechanism failed and is no longer running
  ABORTED = 5      # User requested that the run be killed, or task preempted by Scheduler
  TIMEDOUT = 6     # Run timed out and was killed
}

// Note, each worker has its own runId space which is unrelated to any external ids.
struct RunStatus {
  1: required Status status
  2: required string runId
  3: optional string outUri
  4: optional string errUri
  5: optional string error
  6: optional i32 exitCode
  7: optional string snapshotId
  8: optional string jobId
  9: optional string taskId
  10: optional string tag
}

struct WorkerStatus {
  1: required list<RunStatus> runs  # All runs
  2: required bool initialized      # True if the worker has finished with any long-running init tasks.
  3: required bool isHealthy        # True if the worker is in a healthy state to accept new tasks.
  4: required string error          # Set when a general worker error unrelated to a specific run has occurred.
}

struct RunCommand {
  1: required list<string> argv       # Binary followed by any number of arguments.
  2: optional map<string,string> env  # Mapping of env name to value.
  3: optional string snapshotId       # Scheme'd id, could be a patchId, sha1, etc.
  4: optional i32 timeoutMs           # Kill the job if it hasn't completed in time (Status.TIMEOUT).
  5: optional string jobId
  6: optional string taskId
  7: optional string tag
}

service Worker {
  WorkerStatus QueryWorker()         # Overall worker node status.
  // TODO(dbentley): add a method to Query a status. Cf. runner/status_rw.go
  RunStatus Run(1: RunCommand cmd)   # Run a command and return job Status.
  RunStatus Abort(1: string runId)   # Returns ABORTED if aborted, FAILED if already ended, and UNKNOWN otherwise.
}
