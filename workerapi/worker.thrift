enum Status {
  UNKNOWN = 0      # Reserved.
  PENDING = 1      # Run scheduled but not yet started.
  RUNNING = 2      # Run is happening.
  COMPLETE = 3     # Succeeded or failed yielding an exit code. Only state with an exit code.
  FAILED = 4       # Run mechanism failed and run is no longer active. Retry may or may not work.
  ABORTED = 5      # User requested that the run be killed.
  TIMEDOUT = 6     # Run timed out and was killed.
  BADREQUEST = 7   # Invalid or error'd request. Original worker state not affected. Retry may work after mutation.
}

// Note, each worker has its own runId space which is unrelated to any external ids.
struct RunStatus {
  1: required Status status
  2: required string runId
  3: optional string outUri
  4: optional string errUri
  5: optional string error
  6: optional i32 exitCode
}

// TODO: add useful load information when it comes time to have multiple runs.
struct WorkerStatus {
  1: required list<RunStatus> runs  # All runs excepting what's been Erase()'d
}

struct RunCommand {
  1: required list<string> argv       # Binary followed by any number of arguments.
  2: optional map<string,string> env  # Mapping of env name to value.
  3: optional string snapshotId       # Scheme'd id, could be a patchId, sha1, etc.
  4: optional i32 timeoutMs           # Kill the job if it hasn't completed in time (Status.TIMEOUT).
}

//TODO: add a method to kill the worker if we can articulate unrecoverable issues.
service Worker {
  WorkerStatus QueryWorker()         # Overall worker node status.
  RunStatus Run(1: RunCommand cmd)   # Run a command and return job Status.
  RunStatus Abort(1: string runId)   # Returns ABORTED if aborted, FAILED if already ended, and UNKNOWN otherwise.
  void Erase(1: string runId)        # Remove run from the history of runs (trims WorkerStatus.ended). Optional.
}
