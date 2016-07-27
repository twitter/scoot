enum Status {
  UNKNOWN = 0
  RUNNING = 1
  COMPLETED = 2
  ABORTED = 3
  TIMEOUT = 4
  ORPHANED = 5
  INVALID = 6
}

// VersionId will be empty by default. To change this, pass a getVersion() func to server.Handler.
// - Semantics are up to the implementer, this needn't be a library version, it could be a gitSha, etc.
//
//TODO: add useful load information when it comes time to have multiple runs.
struct WorkerStatus {
  1: required list<string> running
  2: optional list<string> ended
  3: optional string versionId
}

struct RunStatus {
  1: required Status status
  2: optional string runId
  3: optional string outUri
  4: optional string errUri
  5: optional string info
  6: optional i32 exitCode
}

struct RunCommand {
  1: required list<string> argv   # Binary followed by any number of arguments.
  2: optional string snapshotId   # Scheme'd id, could be a patchId, sha1, etc.
  3: optional i32 timeoutMs       # Kill the job if it hasn't completed in time (Status.TIMEOUT).
  4: optional i32 leaseMs         # Kill the job if it hasn't been queried recently (Status.ORPHANED).
}

//TODO: add a method to kill the worker if we can articulate unrecoverable issues.
service Worker {
  WorkerStatus QueryWorker()         # Overall worker node status.
  RunStatus Run(1: RunCommand cmd)   # Run a command and return job Status.(RUNNING|INVALID).
  RunStatus Query(1: string runId)   # May return any job status except Status.INVALID.
  RunStatus Abort(1: string runId)   # Returns ABORTED if aborted, INVALID if already ended, and UNKNOWN otherwise.
  void Clear(1: string runId)        # Remove this run from the stored history of runs. This is optional bookkeeping.
}
