enum Status {
  UNKNOWN = 0
  PENDING = 1
  RUNNING = 2
  COMPLETE = 3
  FAILED = 4
  ABORTED = 5
  TIMEDOUT = 6
}

struct RunStatus {
  1: required Status status
  2: required string runId
  3: optional string outUri
  4: optional string errUri
  5: optional string error
  6: optional i32 exitCode
}

// VersionId will be empty by default. To change this, pass a getVersion() func to server.Handler.
// - Semantics are up to the implementer, this needn't be a library version, it could be a gitSha, etc.
//
// TODO: add useful load information when it comes time to have multiple runs.
struct WorkerStatus {
  1: required list<RunStatus> runs  # All runs excepting what's been Erase()'d
  2: optional string versionId      # Version associated with this worker.
}

struct RunCommand {
  1: required list<string> argv       # Binary followed by any number of arguments.
  2: optional map<string,string> env  # Binary followed by any number of arguments.
  3: optional string snapshotId       # Scheme'd id, could be a patchId, sha1, etc.
  4: optional i32 timeoutMs           # Kill the job if it hasn't completed in time (Status.TIMEOUT).
}

//TODO: add a method to kill the worker if we can articulate unrecoverable issues.
service Worker {
  WorkerStatus QueryWorker()         # Overall worker node status.
  RunStatus Run(1: RunCommand cmd)   # Run a command and return job Status.(RUNNING|FAILED).
  RunStatus Abort(1: string runId)   # Returns ABORTED if aborted, FAILED if already ended, and UNKNOWN otherwise.
  void Erase(1: string runId)        # Remove run from the history of runs (trims WorkerStatus.ended). Optional.
}
