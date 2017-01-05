package gitdb

// A Stream is a sequence of SnapshotWithHistory's.
type streamConfig struct {
	name    string
	refSpec string
}

type streamValue struct {
	sha string
}

func (v streamValue) value() {}
