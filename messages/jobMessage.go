package messages

/*
 * Message representing a Job, Scoot can Schedule
 */
type Job struct {
	Id        string
	Jobtype   string
	Tasks     []Task
	ContextId string
}

/*
 * Message representing a Task that is part of a Scoot Job.
 */
type Task struct {
	Id         string
	Commands   []string
	SnapshotId string
	ContextId  string
}
