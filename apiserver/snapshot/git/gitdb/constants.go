package gitdb

// SnapshotKind describes the kind of a Snapshot: is it an FSSnapshot or a GitCommitSnapshot
// kind instead of type because type is a keyword
type SnapshotKind string

type AutoUploadDest int

const (
	KindFSSnapshot        SnapshotKind = "fs"
	KindGitCommitSnapshot SnapshotKind = "gc"

	AutoUploadNone AutoUploadDest = iota
	AutoUploadTags
	AutoUploadBundlestore
)

var kinds = map[SnapshotKind]bool{
	KindFSSnapshot:        true,
	KindGitCommitSnapshot: true,
}
