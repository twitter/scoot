
struct Command {
  1: list<string> argv,
  2: map<string, string> envVars,
  3: i64 timeout,
  4: string snapshotId,
}

struct TaskDefinition {
  1: Command command,
}

struct JobDefinition {
  1: string jobType,
  2: map<string, TaskDefinition> tasks,
}